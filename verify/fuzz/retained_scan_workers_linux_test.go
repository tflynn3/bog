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
	"bog/internal/httpapi"
	"bog/internal/store"
)

func measureRetainedScans(parent context.Context, catalog *history.Catalog, budget *checkpoint.Budget, query history.Query,
	first checkpoint.BlockRow, path string, catalogFDs []string, report *scanMemoryReport) (err error) {
	baseline := scanLiveHeap()
	heap := scanHeapSamples{baseline: baseline, peak: baseline}
	ctx, cancel := context.WithTimeout(parent, interruptionDeadline)
	defer cancel()
	readers, err := acquireScanMemoryReaders(ctx, catalog.AcquireCurrentUntil, report.Concurrency)
	defer func() {
		err = errors.Join(err, closeScanMemoryReaders(readers), scanCatalogBaseline(path, catalogFDs, budget))
	}()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(scanMemorySampleInterval)
	defer ticker.Stop()
	err = coordinateScanMemoryWorkers(ctx, interruptionDeadline, len(readers), ticker.C, heap.sample,
		func(ctx context.Context, index int, arrive func()) error {
			return materializeScanMemoryLine(ctx, readers[index], query, first, arrive)
		},
		func() error {
			// All visitors hold the overflow line across both samples.
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
	)
	if err != nil {
		return err
	}

	heap.sample()
	report.PostCancelCaptures = budget.Usage().InUse[checkpoint.LiveCaptures]
	afterCancel, err := scanMemoryFixtureFDs("/proc/self/fd", path)
	if err != nil {
		return err
	}
	report.PostCancelFDs = len(afterCancel)
	report.CanceledScans = len(readers)
	report.CatalogHeap = heap.baseline
	report.PeakHeap = heap.peak
	report.PeakIncrementalHeap = heap.peak - heap.baseline
	report.HeapSamples = heap.count
	report.PostCancelHeap = scanLiveHeap()
	return nil
}

func acquireScanMemoryReaders(ctx context.Context, acquire func(context.Context, time.Time) (*history.Reader, error),
	count int) ([]*history.Reader, error) {
	readers := make([]*history.Reader, 0, count)
	for range count {
		reader, err := acquire(ctx, time.Time{})
		if err != nil {
			return readers, err
		}
		readers = append(readers, reader)
	}
	return readers, nil
}

func scanCatalogBaseline(path string, catalogFDs []string, budget *checkpoint.Budget) error {
	var err error
	if got := budget.Usage().InUse; got != (checkpoint.Limits{checkpoint.PinnedGenerations: 1}) {
		err = fmt.Errorf("reservations after reader cleanup: %v", got)
	}
	fds, fdErr := scanMemoryFixtureFDs("/proc/self/fd", path)
	if fdErr == nil && !slices.Equal(fds, catalogFDs) {
		fdErr = fmt.Errorf("fixture descriptors after reader cleanup = %v, want %v", fds, catalogFDs)
	}
	return errors.Join(err, fdErr)
}

func checkScanMemoryRejections(ctx context.Context, catalog *history.Catalog, budget *checkpoint.Budget, path string,
	catalogFDs []string, query history.Query, count int) (int, error) {
	// Exercise cleanup after acquiring all allowed captures and refusing one.
	readers, err := acquireScanMemoryReaders(ctx, catalog.AcquireCurrentUntil, count+1)
	if !errors.Is(err, checkpoint.ErrExhausted) || len(readers) != count {
		err = fmt.Errorf("capture admission acquired %d/%d before returning %v", len(readers), count, err)
	} else {
		reader := readers[0]
		visited := false
		workErr := reader.ScanWithWork(ctx, query, func(checkpoint.BlockRow) error {
			visited = true
			return nil
		}, store.NewQueryWork(ctx, 1))
		err = nil
		if visited || !errors.Is(workErr, store.ErrQueryWorkExhausted) {
			err = fmt.Errorf("bounded scan visited=%t and returned %v", visited, workErr)
		}
	}
	if err := errors.Join(err, closeScanMemoryReaders(readers), scanCatalogBaseline(path, catalogFDs, budget)); err != nil {
		return 0, err
	}

	// Cancel on the second acquisition, after the first reader has a pin.
	partial, cancel := context.WithCancel(ctx)
	defer cancel()
	acquired := false
	readers, err = acquireScanMemoryReaders(partial, func(ctx context.Context, until time.Time) (*history.Reader, error) {
		if acquired {
			cancel()
		}
		reader, err := catalog.AcquireCurrentUntil(ctx, until)
		acquired = reader != nil
		return reader, err
	}, 2)
	if err != context.Canceled || len(readers) != 1 {
		err = fmt.Errorf("partial acquisition acquired %d readers before returning %v", len(readers), err)
	} else {
		err = nil
	}
	if err := errors.Join(err, closeScanMemoryReaders(readers), scanCatalogBaseline(path, catalogFDs, budget)); err != nil {
		return 0, err
	}
	return 3, nil
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
