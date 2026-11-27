package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/client"
	"bog/verify/listener"
)

// TestRestoreDrillShippedServer exercises the separately built server binary
// on a disposable restore, first for read-only parity and then for writable recovery.
func TestRestoreDrillShippedServer(t *testing.T) {
	binary, supplied := os.LookupEnv("BOG_RESTORE_DRILL_BINARY")
	if !supplied {
		t.Skip("set BOG_RESTORE_DRILL_BINARY to the shipped retained-restore server")
	}
	if binary == "" {
		t.Fatal("BOG_RESTORE_DRILL_BINARY is empty")
	}
	binary, err := filepath.Abs(binary)
	check(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	bundle, rows, point := makeBundle(t)
	restored := filepath.Join(t.TempDir(), "restored")
	check(t, backup.Restore(ctx, bundle, restored))
	reserved, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	check(t, err)
	addr := reserved.Addr().(*net.TCPAddr)
	check(t, reserved.Close())
	args := append(compareArgs(bundle, "http://"+addr.String())[1:], "-json")
	opts, err := parseOptions("drill", args)
	check(t, err)
	current, err := startDrillCandidate(t, ctx, binary, restored, addr)
	check(t, err)

	server, err := client.New(opts.baseURL, nil)
	check(t, err)
	defer server.CloseIdleConnections()
	server.MaxPageBytes = opts.maxPageBytes
	boundary := restartFunc(func(ctx context.Context) error {
		if err := current.checkListener(addr.Port); err != nil {
			return err
		}
		if err := current.stop(); err != nil {
			return err
		}
		next, err := startDrillCandidate(t, ctx, binary, restored, addr)
		if err != nil {
			return err
		}
		current = next
		return nil
	})
	var out bytes.Buffer
	status, err := drillBackup(ctx, opts, server, boundary, &out)
	if err != nil || status != exitOK {
		t.Fatalf("restore drill exit %d: %v\n%s", status, err, &out)
	}
	result := decodeDrillResult(t, out.Bytes())
	if result.SchemaVersion != 1 || !result.Parity || result.ExitStatus != exitOK || result.Diagnostic != nil ||
		result.RecoveryPoint == nil || *result.RecoveryPoint != *resultPoint(point) {
		t.Fatalf("unexpected drill result:\n%s", &out)
	}
	for name, phase := range map[string]drillPhaseResult{
		"before_restart": result.BeforeRestart, "after_restart": result.AfterRestart,
	} {
		if phase.Status != "matched" || phase.Counts == nil ||
			phase.Counts.Rows != 4 || phase.Counts.ReferencePages != 2 || phase.Counts.ServerPages < 2 {
			t.Fatalf("%s has unexpected parity counts in %s", name, &out)
		}
	}

	// Backfill across the checkpoint and committed tail ties. Reverse lexical
	// order within the new batch makes an incorrect line tie-break visible.
	added := []client.Row{
		{TS: opts.query.Start, Line: "keep-backfill-z"},
		{TS: opts.query.Start, Line: "keep-backfill-a"},
	}
	var batch bytes.Buffer
	encoder := json.NewEncoder(&batch)
	for _, row := range added {
		check(t, encoder.Encode(row))
	}
	check(t, current.checkListener(addr.Port))
	outcome, err := server.Ingest(ctx, batch.Bytes())
	if err != nil || outcome != client.IngestCommitted {
		t.Fatalf("post-drill ingest: want HTTP 204, got outcome %v, error %v", outcome, err)
	}
	server.CloseIdleConnections()
	if err := boundary.WaitForRestart(ctx); err != nil {
		t.Fatalf("restart after acknowledged ingest: %v", err)
	}

	// Walk the entire fixture, including the rows excluded by the CLI drill.
	query := opts.query
	query.Contains = ""
	query.End = query.End.Add(time.Nanosecond)
	want := slices.Concat(rows, added)
	slices.SortStableFunc(want, func(a, b client.Row) int { return a.TS.Compare(b.TS) })
	check(t, compareRecoveredRows(ctx, server, query, opts.maxPages, want))
	check(t, current.checkListener(addr.Port))
	check(t, current.stop())

	verified, err := backup.Verify(ctx, bundle)
	check(t, err)
	if verified != point {
		t.Fatalf("reference recovery point changed: want %s, got %s", formatPoint(point), formatPoint(verified))
	}
}

func compareRecoveredRows(ctx context.Context, server *client.Client, query client.Query, maxPages int, want []client.Row) error {
	walk, err := server.NewBoundedWalk(query, maxPages)
	if err != nil {
		return err
	}
	reference := func(context.Context) (queryPage, error) {
		// compare consumes its input pages.
		return queryPage{rows: slices.Clone(want), terminal: true}, nil
	}
	live := func(ctx context.Context) (queryPage, error) {
		p, err := walk.NextPage(ctx)
		return queryPage{rows: p.Rows, terminal: p.NextCursor == ""}, err
	}
	if _, _, err := compare(ctx, reference, live); err != nil {
		return fmt.Errorf("post-ingest recovery: %w", err)
	}
	return nil
}

func TestCompareRecoveredRows(t *testing.T) {
	t.Parallel()
	start, err := time.Parse(time.RFC3339, fixtureStart)
	check(t, err)
	query := client.Query{Start: start, End: start.Add(time.Second), Limit: 2}
	want := []client.Row{{TS: start, Line: "z"}, {TS: start, Line: "a"}, {TS: start, Line: "a"}}
	for _, tc := range []struct {
		name      string
		pages     [][]string
		maxPages  int
		wantError string
	}{
		{"empty terminal page", [][]string{{"z", "a"}, {"a"}, nil}, 3, ""},
		{"missing duplicate", [][]string{{"z", "a"}, nil}, 3, "length mismatch at row 3"},
		{"reordered ties", [][]string{{"a", "z"}, {"a"}}, 3, "line mismatch at row 1"},
		{"unexpected trailing row", [][]string{{"z", "a"}, {"a"}, {"extra"}}, 3, "length mismatch at row 4"},
		{"budget before terminal", [][]string{{"z", "a"}, {"a"}, nil}, 2, client.ErrPageBudget.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			responses := make(map[string]client.Page)
			cursor := ""
			for i, lines := range tc.pages {
				var page client.Page
				for _, line := range lines {
					page.Rows = append(page.Rows, client.Row{TS: start, Line: line})
				}
				if i+1 < len(tc.pages) {
					page.NextCursor = fixtureCursor(byte(i + 1))
				}
				responses[cursor] = page
				cursor = page.NextCursor
			}
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page, ok := responses[r.URL.Query().Get("cursor")]
				if !ok {
					http.Error(w, "unexpected cursor", http.StatusBadRequest)
					return
				}
				if page.NextCursor != "" {
					w.Header().Set("Bog-Next-Cursor", page.NextCursor)
				}
				encoder := json.NewEncoder(w)
				for _, row := range page.Rows {
					if err := encoder.Encode(row); err != nil {
						t.Errorf("write query row: %v", err)
						return
					}
				}
			}))
			defer httpServer.Close()
			server, err := client.New(httpServer.URL, nil)
			check(t, err)
			defer server.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = compareRecoveredRows(ctx, server, query, tc.maxPages, want)
			if tc.wantError == "" {
				check(t, err)
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("want error containing %q, got %v", tc.wantError, err)
			}
		})
	}
}

type drillCandidate struct {
	pid  int
	done chan struct{}
	err  error
	log  bytes.Buffer
	stop func() error
}

func startDrillCandidate(t *testing.T, ctx context.Context, binary, dir string, addr *net.TCPAddr) (*drillCandidate, error) {
	t.Helper()
	p := &drillCandidate{done: make(chan struct{})}
	cmd := exec.CommandContext(ctx, binary, "-data-dir", dir, "-addr", addr.String())
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = &p.log, &p.log
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start restored server: %w", err)
	}
	p.pid = cmd.Process.Pid
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	p.stop = sync.OnceValue(func() error {
		signalErr := cmd.Process.Signal(syscall.SIGTERM)
		if errors.Is(signalErr, os.ErrProcessDone) {
			signalErr = nil
		}
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
		case <-timer.C:
			killErr := cmd.Process.Kill()
			if errors.Is(killErr, os.ErrProcessDone) {
				killErr = nil
			}
			signalErr = errors.Join(signalErr, errors.New("restored server shutdown timed out"), killErr)
			select {
			case <-p.done:
			case <-time.After(2 * time.Second):
				return errors.Join(signalErr, errors.New("restored server did not exit after kill"))
			}
		}
		if err := errors.Join(signalErr, p.err); err != nil {
			return fmt.Errorf("stop restored server: %w\n%s", err, &p.log)
		}
		return nil
	})
	t.Cleanup(func() { check(t, p.stop()) })

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.waitReady(probeCtx, addr); err != nil {
		return nil, fmt.Errorf("wait for restored server: %w", err)
	}
	return p, nil
}

func (p *drillCandidate) checkListener(port int) error {
	owned, err := listener.Owns(p.pid, port)
	if err != nil {
		return err
	}
	if !owned {
		return fmt.Errorf("restored server PID %d does not own port %d", p.pid, port)
	}
	return nil
}

func (p *drillCandidate) waitReady(ctx context.Context, addr *net.TCPAddr) error {
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	probe := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		owned, err := listener.Owns(p.pid, addr.Port)
		if err != nil {
			return err
		}
		if owned {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr.String()+"/ready", nil)
			if err != nil {
				return err
			}
			resp, err := probe.Do(req)
			if err != nil {
				return err
			}
			_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			if err := errors.Join(readErr, resp.Body.Close()); err != nil {
				return err
			}
			switch resp.StatusCode {
			case http.StatusOK:
				return p.checkListener(addr.Port)
			case http.StatusServiceUnavailable:
				// Retry a pending readiness check within the startup deadline.
			default:
				return fmt.Errorf("readiness returned HTTP %d", resp.StatusCode)
			}
		}
		select {
		case <-p.done:
			return fmt.Errorf("restored server exited during startup (%v)\n%s", p.err, &p.log)
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
