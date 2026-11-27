//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
)

// Use a single valid segment so the unit cases can deterministically
// exercise allocation and reference cleanup without a large fixture.
func scanMemoryUnitImage(t *testing.T) (files, []checkpoint.BlockRow, checkpoint.Manifest) {
	t.Helper()
	rows := []checkpoint.BlockRow{{Row: store.Row{TS: replayFrom, Line: strings.Repeat("Z", 2*scanMemoryPageBytes)}}}
	block, err := checkpoint.EncodeBlock(rows)
	requireRetainedOK(t, err)
	const name = "bog-segment-00000000000000000000000000000000"
	m := checkpoint.Manifest{
		Version: 1, CoveredOffset: 128, NextOrdinal: 1, SourceSHA256: retainedDigest([]byte(walMagic)),
		Segments: []checkpoint.Segment{{Name: name, Size: int64(len(block)), SHA256: retainedDigest(block)}},
	}
	envelope, _, err := retainedEnvelope(m)
	requireRetainedOK(t, err)
	return files{manifestFile: envelope, name: block}, rows, m
}

func TestScanMemoryReaderCleanup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		readers    int
		captures   uint64
		heapLimit  uint64
		want       error
		wrongFirst bool
	}{
		{name: "cancellation at overflow", readers: 8, captures: 8, heapLimit: 128 << 20},
		{name: "partial acquisition", readers: 2, captures: 1, heapLimit: 32 << 20, want: checkpoint.ErrExhausted},
		{name: "heap overage", readers: 2, captures: 2, heapLimit: 1, want: errReplayLimit},
		{name: "early scan failure", readers: 2, captures: 2, heapLimit: 32 << 20, wrongFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image, rows, _ := scanMemoryUnitImage(t)
			path := filepath.Join(t.TempDir(), "catalog")
			requireRetainedOK(t, writeRetainedImage(t.Context(), path, image))
			b := checkpoint.NewBudget(checkpoint.Limits{checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: tc.captures})
			catalog := history.NewCatalog(b)
			defer func() { requireRetainedOK(t, catalog.Close()) }()
			gen, err := catalog.Replace(t.Context(), path)
			requireRetainedOK(t, err)
			fds, err := retainedReaderFDs(path)
			requireRetainedOK(t, err)
			report := scanMemoryReport{Concurrency: tc.readers, Limits: scanMemoryBudget(tc.readers)}
			report.Limits.HeapGrowth = tc.heapLimit
			query := history.Query{Start: replayFrom, End: replayFrom.Add(time.Second)}
			first := rows[0]
			if tc.wrongFirst {
				first.Ordinal++
			}

			ctx, cancel := context.WithTimeout(t.Context(), scanMemoryJoinDeadline)
			defer cancel()
			err = measureRetainedScans(ctx, catalog, b, query, first, path, fds, &report)
			switch {
			case tc.wrongFirst:
				if err == nil || !strings.Contains(err.Error(), "does not match expected overflow row") {
					t.Fatalf("wrong row returned %v", err)
				}
			case tc.want != nil:
				if !errors.Is(err, tc.want) {
					t.Fatalf("measureRetainedScans = %v, want %v", err, tc.want)
				}
			default:
				requireRetainedOK(t, err)
				if report.CanceledScans != tc.readers || report.PausedCaptures != tc.captures {
					t.Fatalf("incomplete cancellation evidence: %+v", report)
				}
			}

			requireRetainedOK(t, checkScanMemoryIdle(path, fds, b))
			requireRetainedOK(t, rejectScanMemoryWork(ctx, catalog, query, uint64(len(first.Row.Line)-1)))
			requireRetainedOK(t, checkScanMemoryIdle(path, fds, b))
			n, err := retryScanMemory(ctx, catalog, query, rows, report.Limits.WorkPerReader)
			requireRetainedOK(t, err)
			if n != len(rows) {
				t.Fatalf("retry returned %d rows, want %d", n, len(rows))
			}
			requireRetainedOK(t, catalog.Close())
			select {
			case <-gen.Drained:
			default:
				t.Fatal("generation still pinned after retry and close")
			}
			requireRetainedOK(t, checkRetainedReaderBudget(b))
			requireRetainedOK(t, checkRetainedReaderFDs(path, nil))
		})
	}
}
