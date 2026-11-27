package fuzz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

func scanHeapAlloc() uint64 { return readScanMemoryStats(false).heap }

func scanLiveHeap() uint64 { return readScanMemoryStats(true).heap }

// A worker reports arrival once, then waits until every worker has reached
// the same point. Results arrive only after the scan has released its pin.
func coordinateScanMemoryWorkers(parent context.Context, joinTimeout time.Duration, count int,
	ticks <-chan time.Time, sample func(), cancelAtBarrier bool,
	scan func(context.Context, int, func() error) error, paused func() error) (err error) {
	ctx, cancel := context.WithCancel(parent)
	type result struct {
		index int
		err   error
	}
	arrivals := make(chan struct{}, count)
	results := make(chan result, count)
	resume := make(chan struct{})
	for index := range count {
		go func() {
			results <- result{index, scan(ctx, index, sync.OnceValue(func() error {
				arrivals <- struct{}{}
				select {
				case <-resume:
					return ctx.Err()
				case <-ctx.Done():
					return ctx.Err()
				}
			}))}
		}()
	}

	finished := 0
	defer func() {
		if err != nil {
			cancel()
		}
		defer cancel()
		drain, stop := context.WithTimeout(context.Background(), joinTimeout)
		defer stop()
		for finished < count {
			select {
			case r := <-results:
				finished++
				if r.err != nil && (err == nil || r.err != ctx.Err()) {
					err = errors.Join(err, fmt.Errorf("scan %d: %w", r.index, r.err))
				}
			case <-ticks:
				sample()
			case <-drain.Done():
				err = errors.Join(err, fmt.Errorf("joined %d/%d scans: %w", finished, count, drain.Err()))
				return
			}
		}
		err = errors.Join(err, parent.Err())
	}()

	for ready := 0; ready < count; {
		select {
		case <-arrivals:
			ready++
		case <-ticks:
			sample()
		case r := <-results:
			finished++
			if err := ctx.Err(); err != nil {
				return err
			}
			if r.err != nil {
				return fmt.Errorf("scan %d exited before the barrier: %w", r.index, r.err)
			}
			return fmt.Errorf("scan %d completed before the barrier", r.index)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := paused(); err != nil {
		return err
	}
	if cancelAtBarrier {
		cancel()
	}
	close(resume)

	return nil
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
		cancel      bool
		wantError   string
	}{
		{name: "all resumed"},
		{name: "all canceled", cancel: true},
		{name: "paused snapshot failed", pausedErr: pausedFailure, wantError: pausedFailure.Error()},
		{name: "early scan error", early: true, wantError: "exited before the barrier"},
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
			err := coordinateScanMemoryWorkers(ctx, time.Second, count, ticks, func() { close(sampled) }, tc.cancel,
				func(ctx context.Context, index int, arrive func() error) error {
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
					err := arrive()
					if tc.pausedErr == nil && tc.cancel && err != context.Canceled {
						return fmt.Errorf("barrier returned %v during cancellation", err)
					}
					if err == context.Canceled {
						return nil // The coordinator has separate error paths.
					}
					return err
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
	err := coordinateScanMemoryWorkers(ctx, 25*time.Millisecond, 2, nil, nil, false,
		func(ctx context.Context, _ int, arrive func() error) error {
			defer func() { exited <- struct{}{} }()
			_ = arrive()
			<-release
			return ctx.Err()
		}, func() error { return errors.New("barrier failed") },
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
