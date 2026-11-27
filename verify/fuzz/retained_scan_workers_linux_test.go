package fuzz

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
)

func measureRetainedScans(t *testing.T, catalog *history.Catalog, budget *checkpoint.Budget, query history.Query, first checkpoint.BlockRow, path string, catalogFDs []string, report *scanMemoryReport) {
	t.Helper()
	baseline := scanLiveHeap()
	heap := scanHeapSamples{baseline: baseline, peak: baseline}
	ctx, cancel := context.WithTimeout(t.Context(), interruptionDeadline)
	defer cancel()
	readers := make([]*history.Reader, 0, report.Concurrency)
	defer func() { must(t, closeScanMemoryReaders(readers)) }()
	for range report.Concurrency {
		r, err := catalog.AcquireCurrentUntil(ctx, time.Time{})
		must(t, err)
		readers = append(readers, r)
	}

	ticker := time.NewTicker(scanMemorySampleInterval)
	defer ticker.Stop()
	must(t, coordinateScanMemoryWorkers(ctx, interruptionDeadline, len(readers), ticker.C, heap.sample,
		func(ctx context.Context, index int, arrive func()) error {
			return materializeScanMemoryLine(ctx, readers[index], query, first, arrive)
		},
		func() error {
			// All visitors have materialized the overflow line and hold it
			// across both the immediate sample and a forced collection.
			heap.sample()
			report.PausedHeap = scanLiveHeap()
			heap.observe(report.PausedHeap)
			lineBytes := uint64(len(first.Row.Line)) * uint64(len(readers))
			if report.PausedHeap < baseline+lineBytes {
				return fmt.Errorf("paused heap %d below catalog baseline %d plus %d bytes of live rows", report.PausedHeap, baseline, lineBytes)
			}
			if err := closeScanMemoryReaders(readers); err != nil {
				return err
			}
			inFlight := checkpoint.Limits{checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: uint64(len(readers))}
			if got := budget.Usage().InUse; got != inFlight {
				return fmt.Errorf("reservations with active scans and closed readers: %v, want %v", got, inFlight)
			}
			report.PausedCaptures = inFlight[checkpoint.LiveCaptures]
			activeFDs, err := scanMemoryFixtureFDs("/proc/self/fd", path)
			if err != nil {
				return err
			}
			if !slices.Equal(activeFDs, catalogFDs) {
				return fmt.Errorf("fixture descriptors at overflow barrier = %v, want %v", activeFDs, catalogFDs)
			}
			report.PausedFDs = len(activeFDs)
			return nil
		},
	))

	heap.sample()
	postCancel := budget.Usage().InUse
	if want := (checkpoint.Limits{checkpoint.PinnedGenerations: 1}); postCancel != want {
		t.Fatalf("reservations after cancellation: %v, want %v", postCancel, want)
	}
	report.PostCancelCaptures = postCancel[checkpoint.LiveCaptures]
	afterCancel, err := scanMemoryFixtureFDs("/proc/self/fd", path)
	must(t, err)
	if !slices.Equal(afterCancel, catalogFDs) {
		t.Fatalf("fixture descriptors after cancellation = %v, want %v", afterCancel, catalogFDs)
	}
	report.PostCancelFDs = len(afterCancel)
	report.CanceledScans = len(readers)
	report.CatalogHeap = heap.baseline
	report.PeakHeap = heap.peak
	report.PeakIncrementalHeap = heap.peak - heap.baseline
	report.HeapSamples = heap.count
	report.PostCancelHeap = scanLiveHeap()
}

func closeScanMemoryReaders(readers []*history.Reader) error {
	var err error
	for _, reader := range readers {
		err = errors.Join(err, reader.Close())
	}
	return err
}

func materializeScanMemoryLine(ctx context.Context, reader *history.Reader, query history.Query, first checkpoint.BlockRow, arrive func()) error {
	visited := false
	return reader.ScanWithWork(ctx, query, func(row checkpoint.BlockRow) error {
		if visited {
			return fmt.Errorf("visited ordinal %d after cancellation", row.Ordinal)
		}
		if row != first {
			return fmt.Errorf("first row ordinal=%d line_bytes=%d does not match expected overflow row", row.Ordinal, len(row.Row.Line))
		}
		visited = true
		arrive()
		<-ctx.Done()
		runtime.KeepAlive(row.Row.Line)
		// The storage scan observes cancellation while advancing and releases its pin.
		return nil
	}, store.NewQueryWork(ctx, httpapi.DefaultQueryWorkBudget))
}

func retryScanMemoryRows(ctx context.Context, reader *history.Reader, query history.Query, want []checkpoint.BlockRow) (int, error) {
	index := 0
	err := reader.ScanWithWork(ctx, query, func(row checkpoint.BlockRow) error {
		if index == len(want) {
			return fmt.Errorf("extra retry row at index %d (ordinal %d)", index, row.Ordinal)
		}
		if row != want[index] {
			return fmt.Errorf("retry row %d differs: got ordinal=%d timestamp=%s line_bytes=%d, want ordinal=%d timestamp=%s line_bytes=%d",
				index, row.Ordinal, row.Row.TS, len(row.Row.Line), want[index].Ordinal, want[index].Row.TS, len(want[index].Row.Line))
		}
		index++
		return nil
	}, store.NewQueryWork(ctx, httpapi.DefaultQueryWorkBudget))
	if err == nil && index != len(want) {
		err = fmt.Errorf("retry returned %d rows, want %d", index, len(want))
	}
	return index, err
}
