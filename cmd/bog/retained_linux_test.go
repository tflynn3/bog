package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

const retainedTestTS = "2025-01-01T00:00:00"

func retainedRows(t *testing.T, stamp string, lines ...string) ([]byte, []store.Row) {
	t.Helper()
	at, err := store.ParseTime(stamp)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]store.Row, len(lines))
	var b []byte
	for i, line := range lines {
		rows[i] = store.Row{TS: at, Line: line}
		encoded, err := json.Marshal(rows[i])
		if err != nil {
			t.Fatal(err)
		}
		b = append(append(b, encoded...), '\n')
	}
	return b, rows
}

func mustAppendRows(t *testing.T, w interface {
	Append([]byte, []store.Row) error
}, stamp string, lines ...string) {
	t.Helper()
	b, rows := retainedRows(t, stamp, lines...)
	if err := w.Append(b, rows); err != nil {
		t.Fatal(err)
	}
}

func startRetainedTest(t *testing.T, path string) (*retained, func()) {
	t.Helper()
	r, err := openRetained(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.drain(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := errors.Join(r.close(), r.owner.Close()); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(stop)
	return r, stop
}

func requirePageLines(t *testing.T, s *retainedPages, req httpapi.PageRequest, want ...string) httpapi.PageResult {
	t.Helper()
	result, err := s.Page(t.Context(), req, store.NewQueryWork(t.Context(), 128<<20))
	if err != nil {
		t.Fatal(err)
	}
	lines := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		lines = append(lines, row.Row.Line)
	}
	if !slices.Equal(lines, want) {
		t.Fatalf("query returned %q, want %q", lines, want)
	}
	return result
}

func retainedTestFilter(t *testing.T, contains string) httpapi.PageFilter {
	t.Helper()
	start, err := store.ParseTime(retainedTestTS + "Z")
	if err != nil {
		t.Fatal(err)
	}
	return httpapi.PageFilter{Start: start, End: start.Add(time.Second), Contains: contains}
}

func requestAdvance(t *testing.T, r *retained, ordinal uint64) {
	t.Helper()
	r.mu.Lock()
	err := r.startLocked(ordinal)
	job := r.job
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-job.done:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish")
	}
	if status, err := r.compactionStatus(false, ordinal); err != nil || status.Covered != ordinal {
		t.Fatalf("advance = %+v, %v", status, err)
	}
}

func TestRetainedCutoffReconcilesLaterCommitsAndDrainsCapture(t *testing.T) {
	dir := privateDataDir(t)
	r, stop := startRetainedTest(t, dir)
	s := &retainedPages{storage: r}
	filter := retainedTestFilter(t, "needle")
	// The old view includes ties, a duplicate, a case-sensitive miss, a
	// backfilled row and a row exactly at the exclusive end.
	mustAppendRows(t, r, retainedTestTS+".5Z", "needle original", "NEEDLE ignored", "needle original")
	mustAppendRows(t, r, "2025-01-01T00:00:01Z", "needle at end")
	mustAppendRows(t, r, retainedTestTS+"Z", "needle first")
	req := httpapi.PageRequest{Filter: filter, Limit: 1, Snapshot: true}
	first := requirePageLines(t, s, req, "needle first")
	if !first.More || first.Capture == "" {
		t.Fatalf("expected captured continuation: %+v", first)
	}
	defer func() {
		if err := s.Release(first.Capture); err != nil {
			t.Error(err)
		}
	}()

	r.mu.Lock()
	err := r.startLocked(2)
	cutoff := r.batches[0].End
	stagedView := r.viewLocked()
	r.mu.Unlock()
	if err == nil {
		t.Fatal("accepted cutoff inside a frame")
	}
	// Stage from the captured end and force one commit after that end,
	// before selecting. This is the same transaction the worker runs.
	mustAppendRows(t, r, retainedTestTS+".5Z", "needle later tie")
	if err := r.compact(&advance{cutoff: cutoff, ordinal: 3, view: stagedView}); err != nil {
		t.Fatal(err)
	}
	a, err := r.owner.ReadAuthority()
	if err != nil {
		t.Fatal(err)
	}
	if a.Ordinal != 6 || a.NextOrdinal != 3 || a.Base != cutoff || a.Sequence != 1 {
		t.Fatalf("selected authority: %+v", a)
	}
	tail, err := os.ReadFile(filepath.Join(dir, a.Tail))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(tail)
	if a.SuffixSHA256 != hex.EncodeToString(sum[:]) || int64(len(tail)) != a.End-a.Base {
		t.Fatal("selected authority does not bind the exact rebased suffix")
	}

	resume := req
	resume.Capture, resume.After, resume.Limit = first.Capture, first.Rows[0].Position, 10
	result := requirePageLines(t, s, resume, "needle original", "needle original")
	if result.More {
		t.Fatal("held view included a later commit")
	}
	requirePageLines(t, s, resume, "needle original", "needle original")
	requirePageLines(t, s, httpapi.PageRequest{Filter: filter, Limit: 10},
		"needle first", "needle original", "needle original", "needle later tie")
	if _, err := os.Stat(filepath.Join(dir, "bog.wal")); err != nil {
		t.Fatalf("held predecessor lost its backing: %v", err)
	}
	if err := s.Release(first.Capture); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Page(t.Context(), resume, nil); !errors.Is(err, httpapi.ErrCaptureExpired) {
		t.Fatalf("released capture is still readable: %v", err)
	}
	stop()
	if _, err := os.Stat(filepath.Join(dir, "bog.wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("predecessor still present after shutdown: %v", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, a.Tail), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.WriteString(f, "torn")
	if err := errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	restarted, _ := startRetainedTest(t, dir)
	requirePageLines(t, &retainedPages{storage: restarted}, httpapi.PageRequest{Filter: filter, Limit: 10},
		"needle first", "needle original", "needle original", "needle later tie")
	file, err := os.Stat(filepath.Join(dir, a.Tail))
	if err != nil || file.Size() != int64(len(tail)) {
		t.Fatalf("restart did not discard only the uncommitted suffix: %v, %v", file, err)
	}
}

func TestRetainedOrdinaryQuerySpansBoundedScans(t *testing.T) {
	r, _ := startRetainedTest(t, privateDataDir(t))
	lines := []string{strings.Repeat("a", 700<<10), strings.Repeat("b", 700<<10), strings.Repeat("c", 700<<10)}
	mustAppendRows(t, r, retainedTestTS+"Z", lines...)
	requestAdvance(t, r, 3)
	source := &retainedPages{storage: r}
	limits := httpapi.DefaultQueryResourceLimits()
	api := httpapi.NewWithPageSourceAndQueryResources(source, r, 4<<20, 1, 64<<20, limits)
	defer func() {
		if err := api.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	resp := httptest.NewRecorder()
	api.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/query?start="+retainedTestTS+"Z&end=2025-01-01T00:00:01Z&limit=3", nil))
	if resp.Code != http.StatusOK || resp.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("ordinary response: HTTP %d %v", resp.Code, resp.Header())
	}
	rows, err := store.Decode(resp.Body.Bytes())
	if err != nil || len(rows) != len(lines) {
		t.Fatalf("decode ordinary response with %d rows: %v", len(rows), err)
	}
	for i, row := range rows {
		if row.Line != lines[i] {
			t.Fatalf("row %d changed across source scans", i)
		}
	}
}

func TestRetainedStreamsLegacyWALIntoWritableAuthority(t *testing.T) {
	dir := privateDataDir(t)
	legacy, err := wal.Open(dir, new(store.Store))
	if err != nil {
		t.Fatal(err)
	}
	line := strings.Repeat("x", 3<<20)
	b, rows := retainedRows(t, retainedTestTS+"Z", line)
	for i := 0; i < 6; i++ {
		if err := legacy.Append(b, rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	r, stop := startRetainedTest(t, dir)
	a, err := r.owner.ReadAuthority()
	if err != nil || a.NextOrdinal == 0 || a.Ordinal != 6 {
		t.Fatalf("migration did not durably select the full input: %+v, %v", a, err)
	}
	mustAppendRows(t, r, retainedTestTS+"Z", "after migration")
	stop()
	restarted, _ := startRetainedTest(t, dir)
	filter := retainedTestFilter(t, "after migration")
	requirePageLines(t, &retainedPages{storage: restarted}, httpapi.PageRequest{Filter: filter, Limit: 10}, "after migration")
}
