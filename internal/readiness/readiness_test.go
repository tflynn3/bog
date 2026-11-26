package readiness

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestSnapshot(t *testing.T) {
	now := time.Now()
	fresh := sample{at: now.Add(-time.Second), headroom: true}
	low := sample{at: fresh.at}
	expired := now.Add(-maxEvidenceAge)
	recentWrite := outcome{state: durable, at: now}
	tests := []struct {
		name   string
		sample sample
		commit outcome
		want   status
	}{
		{"starting", sample{}, outcome{}, status{Reason: "sampling_pending"}},
		{"available", fresh, outcome{}, status{Ready: true, Reason: "ready"}},
		{"low", low, outcome{}, status{Reason: "low_disk_headroom"}},
		{"failed", sample{at: fresh.at, failed: true}, outcome{}, status{Reason: "sample_unavailable"}},
		{"stale", sample{at: expired, headroom: true}, outcome{}, status{Reason: "sample_stale"}},
		{"rollback with fresh headroom", fresh, outcome{rolledBack, expired, "enospc"}, status{Reason: "wal_rollback", Category: "enospc"}},
		{"rollback with stale headroom", sample{at: expired}, outcome{rolledBack, expired, "eio"}, status{Reason: "wal_rollback", Category: "eio"}},
		{"uncertain with fresh headroom", fresh, outcome{uncertain, now, "other"}, status{Reason: "recovery_required", Category: "other"}},
		{"durable after low", low, recentWrite, status{Ready: true, Reason: "ready"}},
		{"low after durable", low, outcome{state: durable, at: expired}, status{Reason: "low_disk_headroom"}},
		{"durable before initial sample", sample{}, recentWrite, status{Ready: true, Reason: "ready"}},
		{"stale durable and no sample", sample{}, outcome{state: durable, at: expired}, status{Reason: "sampling_pending"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &monitor{sample: tt.sample, outcome: tt.commit}
			if got := m.snapshot(now); got != tt.want {
				t.Fatalf("snapshot = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestMonitorConcurrentObservations(t *testing.T) {
	m := &monitor{}
	now := time.Now()
	const observers = 32
	var wg sync.WaitGroup
	for i := range observers {
		wg.Go(func() {
			at := now.Add(time.Duration(i) * time.Millisecond)
			m.observe(outcome{state: durable, at: at})
			m.snapshot(now)
		})
	}
	wg.Go(func() {
		m.observe(outcome{state: rolledBack, at: now.Add(time.Second), cause: "enospc"})
	})
	wg.Wait()
	if got := m.snapshot(now.Add(time.Second)); got.Ready || got.Category != "enospc" {
		t.Fatalf("older durable observations masked rollback: %+v", got)
	}

	m.observe(outcome{state: durable, at: now.Add(3 * time.Second)})
	if got := m.snapshot(now.Add(3 * time.Second)); !got.Ready {
		t.Fatalf("post-failure append did not clear rollback: %+v", got)
	}
	m.observe(outcome{state: uncertain, at: now.Add(4 * time.Second)})
	m.observe(outcome{state: durable, at: now.Add(5 * time.Second)})
	if got := m.snapshot(now.Add(5 * time.Second)); got.Reason != "recovery_required" || got.Ready {
		t.Fatalf("uncertainty was cleared: %+v", got)
	}
}

func TestSamplerLateResultAndBoundedCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		started := make(chan struct{}, 1)
		release := make(chan struct{}, 1)
		defer close(release)
		defer cancel()
		var calls atomic.Int32
		m := start(ctx, sampleInterval, func() (bool, error) {
			calls.Add(1)
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return true, nil
		})

		synctest.Wait()
		if len(started) != 1 {
			t.Fatal("initial sample did not start")
		}
		<-started
		time.Sleep(maxEvidenceAge + time.Second)
		if got := m.snapshot(time.Now()); got.Ready || got.Reason != "sampling_pending" {
			t.Fatalf("readiness during blocked startup probe = %+v", got)
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("started %d probes while first probe was blocked", n)
		}

		release <- struct{}{}
		synctest.Wait()
		if got := m.snapshot(time.Now()); got.Ready || got.Reason != "sample_stale" {
			t.Fatalf("delayed result presented as fresh: %+v", got)
		}

		time.Sleep(sampleInterval)
		synctest.Wait()
		if len(started) != 1 {
			t.Fatal("second sample did not start")
		}
		cancel()
		time.Sleep(2 * sampleInterval)
		if n := calls.Load(); n != 2 {
			t.Fatalf("started %d probes after cancellation", n)
		}
		release <- struct{}{}
		synctest.Wait()
		if got := m.snapshot(time.Now()); got.Ready || got.Reason != "sample_stale" {
			t.Fatalf("blocked probe published after cancellation: %+v", got)
		}
	})
}

func TestSamplerFailureAndRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		m := start(ctx, sampleInterval, func() (bool, error) {
			calls++
			switch calls {
			case 1:
				return true, nil
			case 2:
				return false, errors.New("probe failed")
			default:
				return true, nil
			}
		})
		for i, want := range []string{"ready", "sample_unavailable", "ready"} {
			if i > 0 {
				time.Sleep(sampleInterval)
			}
			synctest.Wait()
			if got := m.snapshot(time.Now()); got.Reason != want {
				t.Fatalf("sample %d = %+v, want %s", i, got, want)
			}
		}
	})
}
