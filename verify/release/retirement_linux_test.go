package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	bogclient "bog/internal/client"
	querymodel "bog/verify/query-model"
)

// Retention is a successful observation of a live pin and its artifact. A
// timeout provides no such observation, even if a prior sample saw a pin.
type retirementRetainedError struct {
	pins     uint64
	artifact string
}

func (e *retirementRetainedError) Error() string {
	return fmt.Sprintf("deliberately retained %d predecessor pin(s) with %q open", e.pins, e.artifact)
}

// Match an open file by device and inode in this PID alone. The parent keeps
// its own descriptor open across the test so a process-wide scan of the
// fixture directory or a scan of the verifier's FDs cannot prove drain.
func retirementCandidateHasFile(ctx context.Context, pid int, file os.FileInfo) (bool, error) {
	if pid <= 0 {
		return false, fmt.Errorf("invalid fixture PID %d", pid)
	}
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		info, err := os.Stat(filepath.Join(dir, entry.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue // A descriptor can close between readdir and stat.
		}
		if err != nil {
			return false, err
		}
		if os.SameFile(info, file) {
			return true, nil
		}
	}
	return false, ctx.Err()
}

type retirementRead struct {
	done chan struct{}
	err  error // read only after done closes
}

func startRetirementRead(ctx context.Context, pager *releasePager, req querymodel.WireRequest, id int) *retirementRead {
	read := &retirementRead{done: make(chan struct{})}
	go func() {
		defer close(read.done)
		read.err = checkRetirementPage(context.WithValue(ctx, retirementReaderKey{}, id), pager, req, 2, retirementTailContents, false)
	}()
	return read
}

func (r *retirementRead) wait(ctx context.Context) error {
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func checkRetirementPage(ctx context.Context, pager *releasePager, req querymodel.WireRequest, second int, line string, more bool) error {
	page, _, err := fetchOraclePage(ctx, pager, req)
	if err != nil {
		return err
	}
	want := []querymodel.Row{{Time: fixtureTime(second), Line: line}}
	if !slices.Equal(page.Rows, want) || (page.NextCursor != "") != more {
		return fmt.Errorf("query page = %+v, want rows %v and continuation present=%t", page, want, more)
	}
	return nil
}

func checkRetirementService(ctx context.Context, client *http.Client, base string) error {
	q := url.Values{"start": {fixtureStart}, "end": {fixtureEnd}, "limit": {"3"}}
	res, err := request(ctx, client, http.MethodGet, base+"/query?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	var want bytes.Buffer
	enc := json.NewEncoder(&want)
	for i, line := range []string{"successor backfill", "held first row", retirementTailContents} {
		if err := enc.Encode(fixtureWireRow(i, line)); err != nil {
			return err
		}
	}
	if res.status != http.StatusOK || res.cursorHeader || !res.framed ||
		res.contentType != "application/x-ndjson" || !bytes.Equal(res.body, want.Bytes()) {
		return fmt.Errorf("successor query = HTTP %d with body %q, want exact rows %q", res.status, res.body, want.Bytes())
	}
	return nil
}

func observeRetirementHold(ctx context.Context, client *http.Client, base string, published backfillCompaction,
	root *os.Root, pid int, old, active tailParityStorage,
) (*retirementRetainedError, error) {
	pins, err := observeTailParityPins(ctx, client, base, published)
	if err != nil {
		return nil, retirementPhase("observe held pins", err)
	}
	if pins != 1 {
		return nil, fmt.Errorf("held predecessor pins = %d, want one", pins)
	}
	if _, err := retiredTailParityFiles(root, old, active); err != nil {
		return nil, err
	}
	open, err := retirementCandidateHasFile(ctx, pid, old.files[old.tail])
	if err != nil {
		return nil, retirementPhase("observe held descriptor", err)
	}
	if !open {
		return nil, errors.New("fixture lacks its pinned predecessor tail descriptor")
	}
	if err := ctx.Err(); err != nil {
		return nil, retirementPhase("observe retained predecessor", err)
	}
	return &retirementRetainedError{pins: pins, artifact: old.tail}, nil
}

func probeRetirementFixture(ctx context.Context, raw *http.Client, base string, root *os.Root, pid int,
	old, active tailParityStorage, retain bool,
) (err error) {
	pager, err := newReleasePager(base, raw, retirementPageCap)
	if err != nil {
		return err
	}
	// The fixture's ordered barriers govern the in-flight requests.
	pager.pageTimeout = retirementReaderBudget
	filter := querymodel.Filter{Start: pager.query.Start, End: pager.query.End}
	req := querymodel.WireRequest{Filter: filter, Limit: 1, MaxPageBytes: retirementPageCap}
	first, _, err := fetchOraclePage(ctx, pager, req)
	if err != nil {
		return retirementPhase("open predecessor capture", err)
	}
	if !slices.Equal(first.Rows, []querymodel.Row{{Time: fixtureTime(1), Line: "held first row"}}) || first.NextCursor == "" {
		return fmt.Errorf("expected a continuation on the first predecessor row, got %+v", first)
	}
	req.Cursor = first.NextCursor

	readCtx, cancelReads := context.WithCancel(ctx)
	var readers []*retirementRead
	defer func() {
		cancelReads()
		reap, cancel := context.WithTimeout(context.Background(), candidateStopTimeout)
		defer cancel()
		for _, reader := range readers {
			if waitErr := reader.wait(reap); waitErr != nil {
				var retained *retirementRetainedError
				if errors.As(err, &retained) && errors.Is(waitErr, context.Canceled) {
					continue // Deliberate-pin case ends by canceling the last reader.
				}
				err = errors.Join(err, retirementPhase("join reader", waitErr))
			}
		}
	}()
	admit := func(id int) error {
		readers = append(readers, startRetirementRead(readCtx, pager, req, id))
		return retirementBarrier(ctx, raw, base, http.MethodGet, "entered", id)
	}
	finish := func(id int) error {
		if err := retirementBarrier(ctx, raw, base, http.MethodPost, "release", id); err != nil {
			return err
		}
		return retirementPhase(fmt.Sprintf("accept reader %d page", id), readers[id].wait(ctx))
	}

	if err := admit(0); err != nil {
		return err
	}
	published, err := publishReleaseGeneration(ctx, raw, base, 2, 3, nil)
	if err != nil {
		return retirementPhase("publish successor", err)
	}
	if err := admit(1); err != nil {
		return err
	}
	if _, err := observeRetirementHold(ctx, raw, base, published, root, pid, old, active); err != nil {
		return err
	}

	params := url.Values{"action": {"retire"}, "cursor": {req.Cursor}}
	res, err := request(ctx, raw, http.MethodPost, base+"/_capacity/compaction?"+params.Encode(), nil)
	if err != nil {
		return retirementPhase("retire predecessor capture", err)
	}
	if res.status != http.StatusNoContent || len(res.body) != 0 || res.cursorHeader {
		return fmt.Errorf("retire predecessor capture: expected empty HTTP 204, got HTTP %d with body %q", res.status, res.body)
	}
	_, response, err := fetchOraclePage(ctx, pager, req)
	status, ok := err.(*bogclient.HTTPError)
	if !ok || !errors.Is(status, bogclient.ErrCaptureExpired) || response == nil ||
		len(response.Header.Values(nextCursor)) != 0 || hasCursorTrailer(response) {
		if err != nil {
			return retirementPhase("retired cursor did not return a clean HTTP 410", err)
		}
		return errors.New("retired cursor still served a page")
	}

	if err := finish(0); err != nil {
		return err
	}
	if err := checkRetirementService(ctx, raw, base); err != nil {
		return retirementPhase("serve successor while predecessor is pinned", err)
	}
	held, err := observeRetirementHold(ctx, raw, base, published, root, pid, old, active)
	if err != nil {
		return err
	}
	if retain {
		return held
	}
	if err := finish(1); err != nil {
		return err
	}
	// The release barrier acknowledges completion of the HTTP handler, so
	// both observations must succeed now without polling for reclamation.
	pins, err := observeTailParityPins(ctx, raw, base, published)
	if err != nil {
		return retirementPhase("observe final pin drain", err)
	}
	if pins != 0 {
		return fmt.Errorf("final predecessor pins = %d, want zero", pins)
	}
	remaining, err := remainingTailParityArtifact(ctx, root, pid, retirementPredecessor, old.files)
	if err != nil {
		return retirementPhase("observe final reclamation", err)
	}
	if remaining != "" {
		return fmt.Errorf("final release did not reclaim predecessor: %s", remaining)
	}
	for name, before := range active.files {
		after, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if !os.SameFile(before, after) {
			return fmt.Errorf("successor artifact %q changed during predecessor reclamation", name)
		}
	}
	return retirementPhase("serve successor after reclamation", checkRetirementService(ctx, raw, base))
}

func retirementStorage(t *testing.T, root *os.Root, names []string) tailParityStorage {
	t.Helper()
	result := tailParityStorage{manifest: names[0], tail: names[1], files: make(map[string]os.FileInfo)}
	for _, name := range names {
		writeFixture(t, filepath.Join(root.Name(), name), []byte(retirementTailContents))
		info, err := root.Lstat(name)
		if err != nil {
			t.Fatal(err)
		}
		result.files[name] = info
	}
	return result
}

func TestRetainedGenerationRetirement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		retain bool
		stall  bool
	}{
		{name: "final reader drains"},
		{name: "deliberate pin retention", retain: true},
		{name: "missing reader times out", stall: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			root, err := os.OpenRoot(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeTailFixture(t, root) })
			old := retirementStorage(t, root, retirementPredecessor)
			active := retirementStorage(t, root, retirementSuccessor)
			parentTail, err := root.Open(old.tail)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeTailFixture(t, parentTail) })

			ctx, cancel := context.WithTimeout(t.Context(), retirementFixtureBudget)
			defer cancel()
			transport := &http.Transport{}
			defer transport.CloseIdleConnections()
			raw := &http.Client{
				Transport:     retirementReaderTransport{transport},
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			var pid int
			var outcome error
			err = serveDirectoryWithCandidate(ctx, retirementFixtureBinary(t), dir, raw, nil, "", (*candidateProcess).stop,
				func(ctx context.Context, base string, candidatePID int) error {
					pid = candidatePID
					open, err := retirementCandidateHasFile(ctx, pid, old.files[old.tail])
					if err != nil {
						return fmt.Errorf("inspect fixture before capture: %w", err)
					}
					if open {
						return errors.New("fixture held a predecessor descriptor before capture")
					}
					if tc.stall {
						deadline, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
						defer cancel()
						outcome = retirementBarrier(deadline, raw, base, http.MethodGet, "entered", 0)
					} else {
						outcome = probeRetirementFixture(ctx, raw, base, root, pid, old, active, tc.retain)
					}
					return nil // Check the observation separately from listener and teardown errors.
				})
			if err != nil {
				t.Fatalf("fixture lifecycle: %v (probe: %v)", err, outcome)
			}
			if _, err := candidateExited(pid, true); !errors.Is(err, syscall.ECHILD) {
				t.Errorf("fixture PID %d was not reaped: %v", pid, err)
			}
			switch {
			case tc.stall:
				timeout, ok := outcome.(*retirementFixtureTimeout)
				if !ok || timeout.phase != "reader 0 entered barrier" || !errors.Is(timeout, context.DeadlineExceeded) {
					t.Fatalf("outcome = %v, want typed admission barrier timeout", outcome)
				}
			case tc.retain:
				held, ok := outcome.(*retirementRetainedError)
				if !ok || held.pins != 1 || held.artifact != old.tail || errors.Is(held, context.DeadlineExceeded) {
					t.Fatalf("outcome = %v, want one deliberately retained tail pin", outcome)
				}
			default:
				if outcome != nil {
					t.Fatal(outcome)
				}
				info, err := parentTail.Stat()
				if err != nil || !os.SameFile(info, old.files[old.tail]) {
					t.Fatalf("parent descriptor did not survive candidate reclamation: info=%v err=%v", info, err)
				}
			}
		})
	}
}
