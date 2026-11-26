package checkpoint

import (
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

func TestPublishedAndUncoveredMatchWALRecovery(t *testing.T) {
	dir := privateDir(t)
	appendBatches := func(payloads ...string) {
		t.Helper()
		writer, err := wal.Open(dir, &store.Store{})
		if err != nil {
			t.Fatal(err)
		}
		closeOnCleanup(t, writer)
		for _, payload := range payloads {
			batch, err := store.Decode([]byte(payload))
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Append([]byte(payload), batch); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	const duplicate = "{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"literal.*\"}\n"
	appendBatches(duplicate + duplicate)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, l)
	prefix, err := l.Publish()
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	const backfill = "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"literal.* backfill\"}\n" +
		"{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"literalX\"}\n" +
		"{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"literal.* outside\"}\n"
	const largeStart = "{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"literal.*"
	const largeEnd = "\"}\n"
	largest := largeStart + strings.Repeat("x", store.MaxBatch-len(largeStart)-len(largeEnd)) + largeEnd
	appendBatches(backfill, largest)
	var oracle store.Store
	writer, err := wal.Open(dir, &oracle)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, l)
	p, err := l.OpenPublished()
	if err != nil {
		t.Fatal(err)
	}
	if m := p.Manifest(); m.CoveredOffset != prefix.CoveredOffset || m.NextOrdinal != 2 {
		t.Fatalf("unexpected covered boundary: %+v", m)
	}
	var all []BlockRow
	if err := p.Scan(func(row BlockRow) error {
		all = append(all, row)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	frameSizes := []int{3, 1}
	frame := 0
	if err := l.ScanUncovered(p, func(first uint64, rows []store.Row) error {
		if frame >= len(frameSizes) || len(rows) != frameSizes[frame] || first != uint64(len(all)) {
			t.Fatalf("uncovered frame %d has first ordinal %d and %d rows", frame, first, len(rows))
		}
		for i, row := range rows {
			all = append(all, BlockRow{row, first + uint64(i)})
		}
		frame++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if frame != len(frameSizes) || len(all) != 6 {
		t.Fatalf("recovered %d uncovered batches and %d total rows", frame, len(all))
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Row.TS.Before(all[j].Row.TS) })
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	const literal = "literal.*"
	var got []store.Row
	for _, row := range all {
		if !row.Row.TS.Before(start) && row.Row.TS.Before(end) && strings.Contains(row.Row.Line, literal) {
			got = append(got, row.Row)
		}
	}
	if want := oracle.Query(start, end, literal, 10); !slices.Equal(got, want) || len(want) != 4 {
		t.Fatalf("recovered query differs from WAL replay, row counts %d and %d", len(got), len(want))
	}
}
