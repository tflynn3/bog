package fuzz

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"time"
)

const scanMemorySampleInterval = 5 * time.Millisecond

type scanHeapSamples struct {
	baseline uint64
	peak     uint64
	count    int
}

func (s *scanHeapSamples) observe(bytes uint64) {
	s.peak = max(s.peak, bytes)
	s.count++
}

func (s *scanHeapSamples) sample() {
	s.observe(scanHeapAlloc())
}

func scanHeapAlloc() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

func scanLiveHeap() uint64 {
	runtime.GC()
	return scanHeapAlloc()
}

// Each scan announces arrival from its visitor and sends one result after
// all storage defers have run. The deferred join consumes every unfinished
// result under one timeout, including when a scan or the coordinator fails.
func coordinateScanMemoryWorkers(parent context.Context, joinTimeout time.Duration, count int, ticks <-chan time.Time, sample func(),
	scan func(context.Context, int, func()) error, paused func() error) (err error) {
	ctx, cancel := context.WithCancel(parent)
	type result struct {
		index int
		err   error
	}
	arrivals := make(chan struct{}, count)
	results := make(chan result, count)
	for index := range count {
		go func() {
			results <- result{index, scan(ctx, index, func() { arrivals <- struct{}{} })}
		}()
	}

	finished := 0
	defer func() {
		cancel()
		drain, stop := context.WithTimeout(context.Background(), joinTimeout)
		defer stop()
		for finished < count {
			select {
			case r := <-results:
				finished++
				if r.err != context.Canceled {
					err = errors.Join(err, fmt.Errorf("scan %d returned %v after shutdown, want context.Canceled", r.index, r.err), r.err)
				}
			case <-drain.Done():
				err = errors.Join(err, fmt.Errorf("joined %d/%d scans: %w", finished, count, drain.Err()))
				return
			}
		}
	}()

	for ready := 0; ready < count; {
		select {
		case <-arrivals:
			ready++
		case r := <-results:
			finished++
			if err := ctx.Err(); err != nil {
				return err
			}
			if r.err == nil {
				return fmt.Errorf("scan %d exited before cancellation", r.index)
			}
			return fmt.Errorf("scan %d exited before cancellation: %w", r.index, r.err)
		case <-ticks:
			sample()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(paused(), ctx.Err())
}

func TestScanMemoryHeapSamples(t *testing.T) {
	samples := scanHeapSamples{baseline: 100, peak: 100}
	for _, value := range []uint64{90, 170, 120, 155} {
		samples.observe(value)
	}
	if samples.peak-samples.baseline != 70 || samples.count != 4 {
		t.Fatalf("samples = %+v", samples)
	}
}

func TestCoordinateScanMemoryWorkers(t *testing.T) {
	pausedFailure := errors.New("invalid paused snapshot")
	for _, tc := range []struct {
		name        string
		early       bool
		preCanceled bool
		pausedErr   error
		wantError   string
	}{
		{name: "all materialized"},
		{name: "paused snapshot failed", pausedErr: pausedFailure, wantError: pausedFailure.Error()},
		{name: "early scan error", early: true, wantError: "exited before cancellation"},
		{name: "parent canceled", preCanceled: true, wantError: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if tc.preCanceled {
				cancel()
			}
			const count = 8
			ticks := make(chan time.Time, 1)
			ticks <- time.Now()
			sampled := make(chan struct{})
			var active, released atomic.Int32
			paused := false
			err := coordinateScanMemoryWorkers(ctx, time.Second, count, ticks, func() { close(sampled) },
				func(ctx context.Context, index int, arrive func()) error {
					defer released.Add(1)
					select {
					case <-sampled:
					case <-ctx.Done():
						return ctx.Err()
					}
					if tc.early && index == 0 {
						return errors.New("bad row")
					}
					active.Add(1)
					defer active.Add(-1)
					arrive()
					<-ctx.Done()
					return ctx.Err()
				},
				func() error {
					paused = true
					if got := active.Load(); got != count {
						return fmt.Errorf("paused with %d active scans, want %d", got, count)
					}
					return tc.pausedErr
				},
			)
			if got := released.Load(); got != count || active.Load() != 0 {
				t.Fatalf("returned with %d released scans and %d active", got, active.Load())
			}
			if paused != (!tc.early && !tc.preCanceled) {
				t.Fatalf("paused = %v", paused)
			}
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error = %v, want %q", err, tc.wantError)
			}
		})
	}
}

func TestCoordinateScanMemoryWorkersBoundsJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	release := make(chan struct{})
	exited := make(chan struct{}, 2)
	err := coordinateScanMemoryWorkers(ctx, 25*time.Millisecond, 2, nil, nil,
		func(ctx context.Context, _ int, arrive func()) error {
			defer func() { exited <- struct{}{} }()
			arrive()
			<-release
			return ctx.Err()
		}, func() error { return nil },
	)
	close(release)
	for range cap(exited) {
		select {
		case <-exited:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "joined 0/2") {
		t.Fatalf("join error = %v", err)
	}
}
