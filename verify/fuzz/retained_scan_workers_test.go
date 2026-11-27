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

type scanMemoryWorkload struct {
	catalog   *history.Catalog
	budget    *checkpoint.Budget
	path      string
	catalogFD []string
	rows      []checkpoint.BlockRow
	readers   int
}

func (w scanMemoryWorkload) checkUsage(captures uint64) error {
	want := checkpoint.Limits{checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: captures}
	if got := w.budget.Usage().InUse; got != want {
		return fmt.Errorf("live reservations = %v, want %v", got, want)
	}
	return nil
}

func (w scanMemoryWorkload) checkFDs() (int, error) {
	fds, err := retainedReaderFDs(w.path)
	if err != nil {
		return 0, err
	}
	if !slices.Equal(fds, w.catalogFD) {
		return len(fds), fmt.Errorf("fixture descriptors = %v, want %v", fds, w.catalogFD)
	}
	return len(fds), nil
}

func (w scanMemoryWorkload) measure(parent context.Context, phase scanMemoryPhase, limits scanMemoryBudget) (report scanMemoryPhaseReport, err error) {
	ctx, cancel := context.WithTimeout(parent, scanMemoryPhaseDeadline)
	defer cancel()
	report.Phase = phase
	report.WorkLimit = scanMemoryWorkLimit(phase, len(w.rows[0].Row.Line))
	report.Limits = limits

	// Force a collection between phases. Use TotalAlloc for a deterministic
	// accounting window even when GC happens during the scan.
	before := readScanMemoryStats(true)
	report.BaselineHeap = before.heap
	heap := scanHeapSamples{baseline: before.heap, peak: before.heap}

	readers := make([]*history.Reader, 0, w.readers)
	closeReaders := func() (err error) {
		for _, r := range readers {
			err = errors.Join(err, r.Close())
		}
		return err
	}
	defer func() { err = errors.Join(err, closeReaders()) }()
	for range w.readers {
		r, err := w.catalog.AcquireCurrentUntil(ctx, time.Time{})
		if err != nil {
			if errors.Is(err, checkpoint.ErrExhausted) {
				err = errors.Join(errScanWorkerBudget, err)
			}
			return report, fmt.Errorf("capture acquisition: %w", err)
		}
		readers = append(readers, r)
	}
	ticker := time.NewTicker(scanMemorySampleInterval)
	defer ticker.Stop()
	query := history.Query{Start: scanMemoryStart, End: scanMemoryStart.Add(3 * time.Second)}
	err = coordinateScanMemoryWorkers(ctx, scanMemoryJoinDeadline, w.readers, ticker.C, heap.sample, phase == scanCancel,
		func(ctx context.Context, index int, barrier func() error) error {
			return scanMemoryRowsWithBarrier(ctx, readers[index], query, w.rows, phase, report.WorkLimit, barrier)
		}, func() error {
			heap.sample()
			report.PausedHeap = scanLiveHeap()
			heap.observe(report.PausedHeap)
			lineBytes := uint64(len(w.rows[0].Row.Line)) * uint64(w.readers)
			if report.PausedHeap < before.heap+lineBytes {
				return fmt.Errorf("paused heap %d below baseline %d plus %d live line bytes", report.PausedHeap, before.heap, lineBytes)
			}
			// The scans must keep their own references after all reader
			// handles close. Every exit path must release those references.
			if err := closeReaders(); err != nil {
				return err
			}
			usage := w.budget.Usage().InUse
			report.PausedCaptures = usage[checkpoint.LiveCaptures]
			report.PausedPins = usage[checkpoint.PinnedGenerations]
			var err error
			report.PausedFDs, err = w.checkFDs()
			return errors.Join(err, w.checkUsage(uint64(w.readers)), w.checkCaptureLimit(ctx))
		})
	if err != nil {
		closeErr := closeReaders()
		_, fdErr := w.checkFDs()
		return report, errors.Join(err, closeErr, fdErr, w.checkUsage(0))
	}
	after := readScanMemoryStats(false)
	heap.observe(after.heap)
	report.PeakHeap = heap.peak
	report.HeapSamples = heap.count
	report.Allocated, err = scanAllocationDelta(before, after)
	if err != nil {
		return report, err
	}
	report.PostHeap = scanLiveHeap()
	usage := w.budget.Usage().InUse
	report.PostCaptures = usage[checkpoint.LiveCaptures]
	report.PostPins = usage[checkpoint.PinnedGenerations]
	report.PostFDs, err = w.checkFDs()
	report.RowsPerReader, _ = phase.outcome(len(w.rows))
	report.CompletedReaders = w.readers
	return report, errors.Join(err, w.checkUsage(0), report.checkBudget())
}

func (w scanMemoryWorkload) checkCaptureLimit(ctx context.Context) error {
	extra, err := w.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if extra != nil {
		return errors.Join(errors.New("capture admission exceeded the configured limit"), err, extra.Close())
	}
	var exhausted *checkpoint.ExhaustedError
	want := checkpoint.ExhaustedError{
		Resource: checkpoint.LiveCaptures, Limit: uint64(w.readers), InUse: uint64(w.readers), Need: 1,
	}
	if !errors.As(err, &exhausted) || *exhausted != want {
		return errors.Join(fmt.Errorf("capture admission at capacity: got %v, want %v", err, &want), err)
	}
	return nil
}

func scanMemoryRowsWithBarrier(ctx context.Context, reader *history.Reader, query history.Query, rows []checkpoint.BlockRow,
	phase scanMemoryPhase, work uint64, barrier func() error) error {
	index := 0
	wantRows, wantErr := phase.outcome(len(rows))
	err := reader.ScanWithWork(ctx, query, func(row checkpoint.BlockRow) error {
		if index >= wantRows {
			return fmt.Errorf("unexpected row at index %d (ordinal %d)", index, row.Ordinal)
		}
		if row != rows[index] {
			return fmt.Errorf("row %d differs: got ordinal=%d timestamp=%s line_bytes=%d, want ordinal=%d timestamp=%s line_bytes=%d",
				index, row.Ordinal, row.Row.TS, len(row.Row.Line), rows[index].Ordinal, rows[index].Row.TS, len(rows[index].Row.Line))
		}
		index++
		if index == 1 {
			err := barrier()
			runtime.KeepAlive(row.Row.Line)
			if phase == scanCancel && err == context.Canceled {
				// Require cancellation from the scanner as it advances.
				return nil
			}
			return err
		}
		return nil
	}, store.NewQueryWork(ctx, work))
	if err != wantErr || index != wantRows {
		if wantErr == nil && errors.Is(err, store.ErrQueryWorkExhausted) {
			err = errors.Join(errScanWorkerBudget, err)
		}
		return errors.Join(fmt.Errorf("%s returned %d rows and %v, want %d rows and %v", phase, index, err, wantRows, wantErr), err)
	}
	return nil
}
