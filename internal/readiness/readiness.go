// Package readiness reports whether a replayed Bog server can accept writes.
package readiness

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

const (
	sampleInterval = time.Second
	maxEvidenceAge = 5 * time.Second
)

type commitState uint8

const (
	durable commitState = iota + 1
	rolledBack
	uncertain
)

type outcome struct {
	state commitState
	at    time.Time
	cause string
}

type sample struct {
	at       time.Time
	headroom bool
	failed   bool
}

type status struct {
	Ready    bool   `json:"ready"`
	Reason   string `json:"reason"`
	Category string `json:"category,omitempty"`
}

type monitor struct {
	mu      sync.Mutex
	sample  sample
	outcome outcome
}

// start runs one probe at a time, starting immediately and pausing between
// probes. A probe's age starts before the call, so delayed results can't be
// mistaken for fresh observations.
func start(ctx context.Context, interval time.Duration, probe func() (bool, error)) *monitor {
	m := &monitor{}
	go func() {
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			if ctx.Err() != nil {
				return
			}
			at := time.Now()
			headroom, err := probe()
			if ctx.Err() != nil {
				return
			}
			m.mu.Lock()
			m.sample = sample{at: at, headroom: headroom, failed: err != nil}
			m.mu.Unlock()
			timer.Reset(interval)
		}
	}()
	return m
}

// observe takes outcomes in WAL order using their completion timestamps.
// For the base WAL's Append fallback, started is the time the call began.
// Overlapping successful returns cannot prove recovery from a failure.
func (m *monitor) observe(result outcome, started time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if result.state == 0 || m.outcome.state == uncertain {
		return
	}
	if result.state != uncertain && !result.at.After(m.outcome.at) {
		return
	}
	if result.state == durable && m.outcome.state == rolledBack &&
		!started.IsZero() && !started.After(m.outcome.at) {
		return
	}
	m.outcome = result
}

func (m *monitor) snapshot(now time.Time) status {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.outcome.state {
	case uncertain:
		return status{Reason: "recovery_required", Category: m.outcome.cause}
	case rolledBack:
		return status{Reason: "wal_rollback", Category: m.outcome.cause}
	case durable:
		if m.outcome.at.After(m.sample.at) && now.Sub(m.outcome.at) < maxEvidenceAge {
			return status{Ready: true, Reason: "ready"}
		}
	}
	switch {
	case m.sample.at.IsZero():
		return status{Reason: "sampling_pending"}
	case now.Sub(m.sample.at) >= maxEvidenceAge:
		return status{Reason: "sample_stale"}
	case m.sample.failed:
		return status{Reason: "sample_unavailable"}
	case !m.sample.headroom:
		return status{Reason: "low_disk_headroom"}
	default:
		return status{Ready: true, Reason: "ready"}
	}
}

// withReady adds an exact, uncached GET /ready route to an existing API.
// Probes run in the sampler and never on the HTTP request path.
func withReady(api http.Handler, m *monitor, latest func() outcome) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			api.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if latest != nil {
			m.observe(latest(), time.Time{})
		}
		current := m.snapshot(time.Now())
		w.Header().Set("Content-Type", "application/json")
		if !current.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		if err := json.NewEncoder(w).Encode(current); err != nil {
			// The response has started. Abort the stream as net/http expects
			// when a handler can no longer send a complete response.
			panic(http.ErrAbortHandler)
		}
	})
}
