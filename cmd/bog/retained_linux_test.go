package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	lineA  = "{\"ts\":\"2025-01-01T00:00:01Z\",\"line\":\"needle a\"}\n"
	lineB  = "{\"ts\":\"2025-01-01T00:00:01Z\",\"line\":\"needle b\"}\n"
	lineC  = "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"needle backfill\"}\n"
	lineUp = "{\"ts\":\"2025-01-01T00:00:01Z\",\"line\":\"NEEDLE\"}\n"
)

func checkOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func testAppend(t *testing.T, r *retained, payload string) {
	t.Helper()
	rows, err := store.DecodeIngest([]byte(payload))
	checkOK(t, err)
	checkOK(t, r.Append([]byte(payload), rows))
}

func testAdvance(t *testing.T, r *retained, cutoff uint64, during func()) {
	t.Helper()
	r.mu.Lock()
	err := r.startLocked(cutoff, during != nil)
	done := r.busy
	r.mu.Unlock()
	checkOK(t, err)
	if during != nil {
		during()
	}
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint did not complete")
	}
	status, err := r.controlStatus("status", cutoff)
	checkOK(t, err)
	if status.State != "complete" || status.CoveredRows != cutoff {
		t.Fatalf("unexpected checkpoint status: %+v", status)
	}
}

func testClose(t *testing.T, r *retained) {
	t.Helper()
	r.stop()
	checkOK(t, r.drain(t.Context()))
	checkOK(t, r.close())
}

func testPage(t *testing.T, r *retained, capture string, after httpapi.PagePosition, limit int, snapshot bool) httpapi.PageResult {
	t.Helper()
	source := retainedPages{r}
	req := httpapi.PageRequest{Snapshot: snapshot, Capture: capture, After: after, Limit: limit, Filter: httpapi.PageFilter{
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2025, 1, 1, 0, 0, 2, 0, time.UTC), Contains: "needle",
	}}
	page, err := source.Page(t.Context(), req, store.NewQueryWork(t.Context(), 1<<20))
	checkOK(t, err)
	return page
}

func assertLines(t *testing.T, page httpapi.PageResult, want ...string) {
	t.Helper()
	var got []string
	for _, row := range page.Rows {
		got = append(got, row.Row.Line)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("lines = %q, want %q", got, want)
	}
}

func TestRetainedReplacementCarriesLaterCommitsAndDrains(t *testing.T) {
	dir := t.TempDir()
	r, err := openRetained(dir)
	checkOK(t, err)
	testAppend(t, r, lineA+lineA)
	testAppend(t, r, lineB+lineUp)
	a := testPage(t, r, "", "", 1, true)
	assertLines(t, a, "needle a")
	if !a.More {
		t.Fatal("missing continuation")
	}
	old := r.tail.Name()
	testAdvance(t, r, 2, func() { testAppend(t, r, lineC) })
	want := []string{"needle backfill", "needle a", "needle a", "needle b"}
	assertLines(t, testPage(t, r, a.Capture, a.Rows[0].Position, 10, true), "needle a", "needle b")
	assertLines(t, testPage(t, r, "", "", 10, false), want...)
	_, err = os.Stat(filepath.Join(dir, old))
	checkOK(t, err)
	status, err := r.controlStatus("status", 2)
	checkOK(t, err)
	if status.Metrics.Retired != 1 {
		t.Fatalf("retired pins = %d, want 1", status.Metrics.Retired)
	}
	source := retainedPages{r}
	checkOK(t, source.Release(a.Capture))
	checkOK(t, source.Release(a.Capture))
	if _, err := os.Stat(filepath.Join(dir, old)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old tail still present: %v", err)
	}

	tailName := r.tail.Name()
	tailEnd := r.point.CommittedEnd - r.tail.base
	testClose(t, r)
	f, err := os.OpenFile(filepath.Join(dir, tailName), os.O_WRONLY|os.O_APPEND, 0)
	checkOK(t, err)
	_, err = f.WriteString("torn")
	checkOK(t, errors.Join(err, f.Close()))
	r, err = openRetained(dir)
	checkOK(t, err)
	assertLines(t, testPage(t, r, "", "", 10, false), want...)
	info, err := r.tail.Stat()
	checkOK(t, err)
	if info.Size() != tailEnd {
		t.Fatalf("recovery retained uncommitted bytes: %d", info.Size())
	}
	testClose(t, r)
}

type backupTestView struct{ recoveryView }

func (v backupTestView) Point() backup.RecoveryPoint { return backup.RecoveryPoint(v.point) }

func TestRetainedWritableRestoreAndSecondAdvance(t *testing.T) {
	r, err := openRetained(t.TempDir())
	checkOK(t, err)
	testAppend(t, r, lineA)
	testAppend(t, r, lineB)
	testAdvance(t, r, 1, nil)
	view, err := r.recoveryView(r.point, r.manifest, r.tail)
	checkOK(t, err)
	dest := t.TempDir()
	bundle, restore := filepath.Join(dest, "bundle"), filepath.Join(dest, "restore")
	checkOK(t, backup.ExportRecoveryView(t.Context(), backupTestView{view}, bundle, tailLimit))
	checkOK(t, backup.Restore(t.Context(), bundle, restore))
	testClose(t, r)
	r, err = openRetained(restore)
	checkOK(t, err)
	testAppend(t, r, lineC)
	testAdvance(t, r, 2, nil)
	assertLines(t, testPage(t, r, "", "", 10, false), "needle backfill", "needle a", "needle b")
	if r.point.CoveredOffset != r.tail.base || r.point.CommittedOrdinal != 3 || r.point.NextOrdinal != 2 {
		t.Fatalf("rebased view is inconsistent: %+v", r.point)
	}
	testClose(t, r)
	r, err = openRetained(restore)
	checkOK(t, err)
	assertLines(t, testPage(t, r, "", "", 10, false), "needle backfill", "needle a", "needle b")
	testClose(t, r)
}

func TestLegacyMigrationStreamsFrames(t *testing.T) {
	dir := t.TempDir()
	legacy, err := wal.Open(dir, &store.Store{})
	checkOK(t, err)
	data := []byte(lineA)
	rows, err := store.Decode(data)
	checkOK(t, err)
	for range advanceFrames + 1 {
		checkOK(t, legacy.Append(data, rows))
	}
	checkOK(t, legacy.Close())
	r, err := openRetained(dir)
	checkOK(t, err)
	if r.point.NextOrdinal != advanceFrames+1 || r.point.CommittedOrdinal != r.point.NextOrdinal || len(r.batches) != 0 {
		t.Fatalf("legacy data remained in the replay suffix: %+v", r.point)
	}
	assertLines(t, testPage(t, r, "", "", 1, false), "needle a")
	if _, err := os.Stat(filepath.Join(dir, "bog.wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy WAL remains after migration: %v", err)
	}
	testClose(t, r)
}

func TestRejectMalformedWritableAuthority(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(string) string
	}{
		{"duplicate field", func(raw string) string { return `{"version":1,` + raw[1:] }},
		{"missing ordinal", func(raw string) string { return strings.Replace(raw, `"next_ordinal":1,`, "", 1) }},
		{"bad tail name", func(raw string) string { return strings.Replace(raw, "bog.wal.tail-x", "../bog.wal.tail-x", 1) }},
		{"bad byte range", func(raw string) string { return strings.Replace(raw, `"committed_end":72`, `"committed_end":8`, 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(authority{Version: 2, Generation: strings.Repeat("1", 64),
				CoveredOffset: 72, NextOrdinal: 1, CommittedEnd: 72, CommittedOrdinal: 1,
				SuffixSHA256: hexSum(sha256.Sum256(nil)), Tail: "bog.wal.tail-x", TailBase: 72})
			checkOK(t, err)
			raw = []byte(tc.edit(string(raw)))
			record := []byte(`{"payload":` + string(raw) + `,"sha256":"` + hexSum(sha256.Sum256(raw)) + `"}`)
			if _, _, err := decodeAuthority(record); err == nil {
				t.Fatal("accepted malformed selector")
			}
		})
	}
}

func TestRecoveryViewHasIndependentReaders(t *testing.T) {
	r, err := openRetained(t.TempDir())
	checkOK(t, err)
	testAppend(t, r, lineA)
	testAdvance(t, r, 1, nil)
	testAppend(t, r, lineB)
	view, err := r.recoveryView(r.point, r.manifest, r.tail)
	checkOK(t, err)
	for range 2 {
		reader, err := view.OpenSuffix()
		checkOK(t, err)
		raw, err := io.ReadAll(reader)
		checkOK(t, errors.Join(err, reader.Close()))
		header := wal.FrameHeader([]byte(lineB))
		if !slices.Equal(raw, append(header[:], lineB...)) || sha256.Sum256(raw) != view.Point().SuffixSHA256 {
			t.Fatal("recovery view did not reproduce its exact suffix")
		}
	}
	testClose(t, r)
}
