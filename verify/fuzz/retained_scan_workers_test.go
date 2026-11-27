//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
)

func measureRetainedScans(ctx context.Context, catalog *history.Catalog, budget *checkpoint.Budget, query history.Query,
	first checkpoint.BlockRow, path string, catalogFDs []string, report *scanMemoryReport) (err error) {
	baseline := scanLiveHeap()
	heap := scanHeapSamples{baseline: baseline, peak: baseline}
	readers := make([]*history.Reader, 0, report.Concurrency)
	defer func() {
		err = errors.Join(err, closeScanMemoryReaders(readers), checkScanMemoryIdle(path, catalogFDs, budget))
	}()
	for range report.Concurrency {
		r, err := catalog.AcquireCurrentUntil(ctx, time.Time{})
		if err != nil {
			return err
		}
		readers = append(readers, r)
	}

	ticker := time.NewTicker(scanMemorySampleInterval)
	defer ticker.Stop()
	sample := func() error {
		heap.sample()
		return checkScanMemoryBytes("sampled scan heap", baseline, heap.peak, report.Limits.HeapGrowth)
	}
	err = coordinateScanMemoryWorkers(ctx, scanMemoryJoinDeadline, len(readers), ticker.C, sample,
		func(ctx context.Context, index int, arrive func()) error {
			return materializeScanMemoryLine(ctx, readers[index], query, first, report.Limits.WorkPerReader, arrive)
		},
		func() error {
			// All visitors have materialized the overflow line and hold it
			// across both the immediate sample and a forced collection.
			if err := sample(); err != nil {
				return err
			}
			report.PausedHeap = scanLiveHeap()
			heap.observe(report.PausedHeap)
			if err := closeScanMemoryReaders(readers); err != nil {
				return err
			}
			inFlight := checkpoint.Limits{checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: uint64(len(readers))}
			if got := budget.Usage().InUse; got != inFlight {
				return fmt.Errorf("reservations with active scans and closed readers: %v, want %v", got, inFlight)
			}
			activeFDs, err := retainedReaderFDs(path)
			if err != nil {
				return err
			}
			if !slices.Equal(activeFDs, catalogFDs) {
				return fmt.Errorf("fixture descriptors at overflow barrier = %v, want %v", activeFDs, catalogFDs)
			}
			report.PausedCaptures = inFlight[checkpoint.LiveCaptures]
			report.PausedFDs = len(activeFDs)
			return checkScanMemoryBytes("sampled scan heap", baseline, heap.peak, report.Limits.HeapGrowth)
		})
	if err != nil {
		return err
	}

	if err := sample(); err != nil {
		return err
	}
	afterCancel, err := retainedReaderFDs(path)
	if err != nil {
		return err
	}
	report.PostCancelCaptures = budget.Usage().InUse[checkpoint.LiveCaptures]
	report.PostCancelFDs = len(afterCancel)
	report.CanceledScans = len(readers)
	report.CatalogHeap = heap.baseline
	report.PeakHeap = heap.peak
	report.PeakIncrementalHeap = heap.peak - heap.baseline
	report.HeapSamples = heap.count
	report.PostCancelHeap = scanLiveHeap()
	return nil
}

func checkScanMemoryIdle(path string, catalogFDs []string, budget *checkpoint.Budget) error {
	var err error
	if got, want := budget.Usage().InUse, (checkpoint.Limits{checkpoint.PinnedGenerations: 1}); got != want {
		err = fmt.Errorf("idle catalog reservations: %v, want %v", got, want)
	}
	return errors.Join(err, checkRetainedReaderFDs(path, catalogFDs))
}

func closeScanMemoryReaders(readers []*history.Reader) error {
	var err error
	for _, reader := range readers {
		err = errors.Join(err, reader.Close())
	}
	return err
}

func materializeScanMemoryLine(ctx context.Context, reader *history.Reader, query history.Query, first checkpoint.BlockRow, workLimit uint64, arrive func()) error {
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
	}, store.NewQueryWork(ctx, workLimit))
}

func rejectScanMemoryWork(ctx context.Context, catalog *history.Catalog, query history.Query, workLimit uint64) (err error) {
	reader, err := catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	err = reader.ScanWithWork(ctx, query, func(row checkpoint.BlockRow) error {
		return fmt.Errorf("scan visited ordinal %d before work-budget rejection", row.Ordinal)
	}, store.NewQueryWork(ctx, workLimit))
	if err != store.ErrQueryWorkExhausted {
		return fmt.Errorf("expected clean scan work-budget rejection, got %v", err)
	}
	return nil
}

func retryScanMemory(ctx context.Context, catalog *history.Catalog, query history.Query, want []checkpoint.BlockRow,
	workLimit uint64) (_ int, err error) {
	reader, err := catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	return retryScanMemoryRows(ctx, reader, query, want, workLimit)
}

func retryScanMemoryRows(ctx context.Context, reader *history.Reader, query history.Query, want []checkpoint.BlockRow,
	workLimit uint64) (int, error) {
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
	}, store.NewQueryWork(ctx, workLimit))
	if err == nil && index != len(want) {
		err = fmt.Errorf("retry returned %d rows, want %d", index, len(want))
	}
	return index, err
}
