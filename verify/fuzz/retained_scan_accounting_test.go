package fuzz

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"bog/internal/store"
)

const (
	scanMemorySegments          = 1024
	scanMemoryJoinDeadline      = 2 * time.Second
	scanMemoryPhaseDeadline     = 12 * time.Second
	scanMemoryStartupAllocLimit = 128 << 20
	scanMemoryLivePerReader     = 16 << 20
	scanMemoryHeapPerReader     = 20 << 20
	scanMemoryAllocPerReader    = 24 << 20
	scanMemoryWorkPerReader     = 128 << 20
)

var (
	errScanWorkerStartup = errors.New("retained scan harness startup failed")
	errScanWorkerRun     = errors.New("retained scan worker execution failed")
	errScanWorkerBudget  = errors.New("retained scan workload budget exceeded")
)

type scanMemoryStats struct {
	heap  uint64
	total uint64
}

func readScanMemoryStats(collect bool) scanMemoryStats {
	if collect {
		runtime.GC()
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return scanMemoryStats{heap: stats.HeapAlloc, total: stats.TotalAlloc}
}

func scanAllocationDelta(before, after scanMemoryStats) (uint64, error) {
	if after.total < before.total {
		return 0, errors.New("Go cumulative allocation counter decreased")
	}
	return after.total - before.total, nil
}

type scanMemoryBudget struct {
	LiveHeap        uint64 `json:"paused_live_heap_bytes"`
	IncrementalHeap uint64 `json:"incremental_heap_bytes"`
	Allocated       uint64 `json:"allocated_bytes"`
}

func scanMemoryLimits(readers int) scanMemoryBudget {
	return scanMemoryBudget{
		LiveHeap:        scanMemoryLivePerReader * uint64(readers),
		IncrementalHeap: scanMemoryHeapPerReader * uint64(readers),
		Allocated:       scanMemoryAllocPerReader * uint64(readers),
	}
}

type scanMemoryPhaseReport struct {
	Phase            scanMemoryPhase  `json:"phase"`
	WorkLimit        uint64           `json:"work_limit_per_reader"`
	Limits           scanMemoryBudget `json:"limits"`
	BaselineHeap     uint64           `json:"baseline_heap_bytes"`
	PeakHeap         uint64           `json:"peak_sampled_heap_bytes"`
	PausedHeap       uint64           `json:"paused_post_gc_heap_bytes"`
	PostHeap         uint64           `json:"post_gc_heap_bytes"`
	Allocated        uint64           `json:"allocated_bytes"`
	HeapSamples      int              `json:"heap_samples"`
	PausedFDs        int              `json:"paused_fixture_fds"`
	PostFDs          int              `json:"post_fixture_fds"`
	PausedCaptures   uint64           `json:"paused_captures"`
	PausedPins       uint64           `json:"paused_generation_pins"`
	PostPins         uint64           `json:"post_generation_pins"`
	PostCaptures     uint64           `json:"post_captures"`
	RowsPerReader    int              `json:"rows_per_reader"`
	CompletedReaders int              `json:"completed_readers"`
}

func (r scanMemoryPhaseReport) checkBudget() error {
	if r.PeakHeap < r.BaselineHeap || r.PausedHeap < r.BaselineHeap {
		return fmt.Errorf("%s heap sample is below its baseline", r.Phase)
	}
	increment := r.PeakHeap - r.BaselineHeap
	var err error
	live := r.PausedHeap - r.BaselineHeap
	if live > r.Limits.LiveHeap {
		err = fmt.Errorf("%w: %s paused live heap %d > %d bytes", errScanWorkerBudget, r.Phase, live, r.Limits.LiveHeap)
	}
	if increment > r.Limits.IncrementalHeap {
		err = errors.Join(err, fmt.Errorf("%w: %s incremental heap %d > %d bytes", errScanWorkerBudget, r.Phase, increment, r.Limits.IncrementalHeap))
	}
	if r.Allocated > r.Limits.Allocated {
		err = errors.Join(err, fmt.Errorf("%w: %s allocated %d > %d bytes", errScanWorkerBudget, r.Phase, r.Allocated, r.Limits.Allocated))
	}
	return err
}

type scanMemoryPhase string

const (
	scanSuccess     scanMemoryPhase = "success"
	scanCancel      scanMemoryPhase = "cancel"
	scanParserLimit scanMemoryPhase = "parser-limit"
	scanRetry       scanMemoryPhase = "retry"
)

var scanMemoryPhases = [...]scanMemoryPhase{scanSuccess, scanCancel, scanParserLimit, scanRetry}

// One charge per segment, a seek charge of one for the overflow segment
// and two for the three-row segments, and one parsed header per segment.
// Allow the first line exactly. Materializing the next line must exhaust
// the real storage work meter before the visitor can see a second row.
func scanMemoryWorkLimit(phase scanMemoryPhase, firstLineBytes int) uint64 {
	if phase == scanParserLimit {
		return uint64(4*scanMemorySegments - 1 + firstLineBytes)
	}
	return scanMemoryWorkPerReader
}

func (p scanMemoryPhase) outcome(rows int) (int, error) {
	switch p {
	case scanCancel:
		return 1, context.Canceled
	case scanParserLimit:
		return 1, store.ErrQueryWorkExhausted
	default:
		return rows, nil
	}
}
