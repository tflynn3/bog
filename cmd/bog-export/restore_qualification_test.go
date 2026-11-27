//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/client"
	querymodel "bog/verify/query-model"
)

const recoveryAppend = `{"ts":"2025-01-01T00:00:01Z","line":"match + post-restore append"}` + "\n" + rowA

var exportRestoreCases = []struct {
	name string
	run  func(*testing.T, context.Context, func() *qualificationIssuer)
}{
	{"interrupted export on live restore", qualifyInterruptedRestoreExport},
	{"separate restore and post-ack crash", qualifyPostRestoreCrash},
}

func recoveryFilter(t *testing.T, contains string) querymodel.Filter {
	t.Helper()
	from, startErr := time.Parse(time.RFC3339Nano, start)
	until, endErr := time.Parse(time.RFC3339Nano, end)
	if err := errors.Join(startErr, endErr); err != nil {
		t.Fatal(err)
	}
	// Include the export's exclusive upper-bound row in the recovery walk.
	if contains == "" {
		until = until.Add(time.Second)
	}
	return querymodel.Filter{Start: from, End: until, Contains: contains}
}

func decodeRecoveryRows(in io.Reader) ([]client.Row, error) {
	dec := json.NewDecoder(in)
	var rows []client.Row
	for {
		var row client.Row
		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode row %d: %w", len(rows)+1, err)
		}
		rows = append(rows, row)
	}
}

func appendRecoveryLedger(t *testing.T, ledger *querymodel.Log, batch string) {
	t.Helper()
	rows, err := decodeRecoveryRows(strings.NewReader(batch))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		ledger.Append([]querymodel.Row{{Time: row.TS, Line: row.Line}})
	}
}

func compareRecoveryRows(got []client.Row, want []querymodel.Entry) error {
	for i := 0; i < min(len(got), len(want)); i++ {
		if !got[i].TS.Equal(want[i].Time) || got[i].Line != want[i].Line {
			return fmt.Errorf("record %d = %s %q, want %s %q (append ordinal %d)",
				i+1, got[i].TS, got[i].Line, want[i].Time, want[i].Line, want[i].Ordinal)
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("terminal row count = %d, want %d", len(got), len(want))
	}
	return nil
}

func assertRecoveryOutput(t *testing.T, output string, ledger *querymodel.Log, watermark int, filter querymodel.Filter) {
	t.Helper()
	got, err := decodeRecoveryRows(strings.NewReader(output))
	if err != nil {
		t.Fatal(err)
	}
	want, err := ledger.Match(watermark, filter)
	if err != nil {
		t.Fatal(err)
	}
	if err := compareRecoveryRows(got, want); err != nil {
		t.Fatal(err)
	}
}

func recoveryClient(t *testing.T, base string) *client.Client {
	t.Helper()
	c, err := client.New(base, &http.Transport{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	c.MaxPageBytes = 4096
	return c
}

// Qualify the public atomic writer while cutting a continuation from the
// live issuer. The destination must still refer to the previous export
// when the retry and terminal requests arrive.
func qualifyRestoredAtomicDestination(t *testing.T, ctx context.Context, issuer *qualificationIssuer, ledger *querymodel.Log, filter querymodel.Filter) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "published.ndjson")
	prior := []byte("prior export\n")
	if err := os.WriteFile(path, prior, 0600); err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	}()

	var trace exportQueryTrace
	cut := trace.handler(t, issuer.proxy(t), 2, nil)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(data, prior) {
			t.Errorf("destination during query %s = %q, %v; want prior export", r.URL.Path, data, err)
		}
		cut.ServeHTTP(w, r)
	}))
	defer front.Close()
	q := client.Query{Start: filter.Start, End: filter.End, Contains: filter.Contains, Limit: 2}
	if err := recoveryClient(t, front.URL).ExportFile(ctx, q, 3, client.PageRetry{MaxRetries: 1}, path); err != nil {
		t.Fatalf("publish atomic export after interrupted delivery: %v", err)
	}
	tokens := trace.snapshot()
	if len(tokens) != 4 || tokens[0] != "" || tokens[1] == "" || tokens[1] != tokens[2] || tokens[3] == "" || tokens[3] == tokens[2] {
		t.Fatalf("atomic export requests = %q, want opening, cut, identical continuation and terminal request", tokens)
	}
	assertRecoveryOutput(t, string(readExportArtifact(t, path)), ledger, ledger.HighWater(), filter)
	got, err := io.ReadAll(old)
	if err != nil || !bytes.Equal(got, prior) {
		t.Fatalf("previous destination descriptor = %q, %v", got, err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("atomic export left staging artifacts: %v", entries)
	}
}

func qualifyInterruptedRestoreExport(t *testing.T, ctx context.Context, launch func() *qualificationIssuer) {
	t.Helper()
	issuer := launch()
	filter := recoveryFilter(t, "match + ")
	var ledger querymodel.Log
	appendRecoveryLedger(t, &ledger, exportSeed)
	watermark := ledger.HighWater()
	qualifyRestoredAtomicDestination(t, ctx, issuer, &ledger, filter)

	// Interrupt after observing a partial continuation from the issuer.
	// Reuse the saved cursor after acknowledging backfill on that issuer.
	out := qualifyExportInterruption(t, ctx, exportDeliveryInterruption, issuer.proxy(t), func(ctx context.Context) {
		seedExportIssuer(t, ctx, issuer.base, exportBackfill)
		appendRecoveryLedger(t, &ledger, exportBackfill)
	})
	assertRecoveryOutput(t, out, &ledger, watermark, filter)
	fresh := newExportQualification(t, issuer.base)
	fresh.run(t, ctx, exitOK, "", "-max-pages", "4")
	assertRecoveryOutput(t, string(readExportArtifact(t, fresh.output.Name())), &ledger, ledger.HighWater(), filter)
	if err := issuer.requireListener(); err != nil {
		t.Fatal(err)
	}
	if err := issuer.stop(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	t.Log("issuer reaped after atomic publication and exact checkpoint resume verified against the live restore, including a separate fresh walk")
}

type ackCrashObservation struct {
	Status int
	Killed bool
}

func ingestAtCrashBoundary(ctx context.Context, httpClient *http.Client, base, batch string,
	requireListener, kill func() error,
) (observation ackCrashObservation, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/ingest", strings.NewReader(batch))
	if err != nil {
		return observation, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if err := requireListener(); err != nil {
		return observation, fmt.Errorf("pre-ingest listener: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return observation, fmt.Errorf("post-restore ingest: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close ingest response: %w", closeErr))
		}
	}()
	observation.Status = resp.StatusCode
	if resp.StatusCode != http.StatusNoContent {
		return observation, fmt.Errorf("post-restore ingest HTTP %d, want 204", resp.StatusCode)
	}
	// Signal from the header path. Reading or closing the body, logging,
	// querying and listener checks must all wait until after the signal.
	if err := kill(); err != nil {
		return observation, fmt.Errorf("kill and reap acknowledged issuer: %w", err)
	}
	observation.Killed = true
	return observation, nil
}

func qualifyPostRestoreCrash(t *testing.T, ctx context.Context, launch func() *qualificationIssuer) {
	t.Helper()
	var ledger querymodel.Log
	appendRecoveryLedger(t, &ledger, exportSeed)
	issuer := launch()
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	observation, err := ingestAtCrashBoundary(ctx, httpClient, issuer.base, recoveryAppend, issuer.requireListener,
		func() error { return issuer.stop(syscall.SIGKILL) })
	if err != nil || observation.Status != http.StatusNoContent || !observation.Killed || !issuer.reaped {
		t.Fatalf("post-restore crash: %+v, reaped=%t, error=%v", observation, issuer.reaped, err)
	}
	appendRecoveryLedger(t, &ledger, recoveryAppend)

	restarted := launch()
	filter := recoveryFilter(t, "")
	want, err := ledger.Match(ledger.HighWater(), filter)
	if err != nil {
		t.Fatal(err)
	}
	c := recoveryClient(t, restarted.base)
	walk, err := c.NewBoundedWalkWithContext(ctx, client.Query{Start: filter.Start, End: filter.End, Limit: 2}, 8)
	if err != nil {
		t.Fatal(err)
	}
	var got []client.Row
	for {
		page, err := walk.NextPage()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("fresh walk after acknowledged crash: %v", err)
		}
		got = append(got, page.Rows...)
		if len(got) > len(want) {
			t.Fatalf("restarted issuer returned more than %d records", len(want))
		}
	}
	if err := compareRecoveryRows(got, want); err != nil {
		t.Fatal(err)
	}
	if err := restarted.requireListener(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.stop(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	t.Logf("append_status=%d observed_signal=%d replay_rows=%d crash_candidate_reaped=%t restart_candidate_reaped=%t",
		observation.Status, syscall.SIGKILL, len(got), issuer.reaped, restarted.reaped)
}

func qualificationRestoreRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(t.TempDir(), "restore-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
			return
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("verify restore fixture removal: %v", err)
			return
		}
		t.Log("restore fixture removed")
	})
	return dir
}

func TestRecoveryLedgerDetectsChangedReplay(t *testing.T) {
	t.Parallel()
	var ledger querymodel.Log
	appendRecoveryLedger(t, &ledger, rowA+rowB+rowA)
	want, err := ledger.Match(ledger.HighWater(), recoveryFilter(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := decodeRecoveryRows(strings.NewReader(rowA + rowB + rowA))
	if err != nil {
		t.Fatal(err)
	}
	reordered := slices.Clone(rows)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	for _, tc := range []struct {
		name string
		rows []client.Row
		want string
	}{
		{"exact", rows, ""},
		{"ties swapped", reordered, "record 1"},
		{"duplicate omitted", rows[:2], "terminal row count"},
		{"extra duplicate", append(slices.Clone(rows), rows[2]), "terminal row count"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareRecoveryRows(tc.rows, want)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Errorf("replay comparison = %v, want %q", err, tc.want)
			}
		})
	}
}
