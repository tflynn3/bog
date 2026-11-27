package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type diskReadiness struct {
	ready            bool
	reason, category string
}

var (
	diskReady    = diskReadiness{ready: true, reason: "ready"}
	diskPending  = diskReadiness{reason: "sampling_pending"}
	diskLow      = diskReadiness{reason: "low_disk_headroom"}
	diskRollback = diskReadiness{reason: "wal_rollback", category: "enospc"}
)

type readinessObservation struct {
	Lifetime int            `json:"lifetime"`
	Status   *int           `json:"http_status"`
	Ready    *bool          `json:"ready"`
	Reason   string         `json:"reason,omitempty"`
	Category string         `json:"category,omitempty"`
	Elapsed  time.Duration  `json:"elapsed_ns"`
	Headers  *time.Duration `json:"response_headers_ns"`
	Body     *time.Duration `json:"response_body_complete_ns"`
	Error    string         `json:"detail,omitempty"`
}

func (r readinessObservation) state() diskReadiness {
	if r.Ready == nil {
		return diskReadiness{}
	}
	return diskReadiness{*r.Ready, r.Reason, r.Category}
}

func readDiskReadiness(ctx context.Context, client *http.Client, addr string) (readinessObservation, error) {
	const maxBody = 1024
	start := time.Now()
	result, transportErr := exchange(ctx, client, addr, http.MethodGet, "/ready", nil, maxBody, start)
	obs := readinessObservation{Elapsed: time.Since(start), Headers: result.headersAt, Body: result.bodyAt}
	if result.response == nil {
		return obs, transportErr
	}
	status := result.response.StatusCode
	obs.Status = &status
	if len(result.body) > maxBody {
		return obs, fmt.Errorf("oversized readiness response: %w", transportErr)
	}
	if result.bodyAt == nil {
		return obs, transportErr
	}
	var decoded struct {
		Ready    *bool  `json:"ready"`
		Reason   string `json:"reason"`
		Category string `json:"category"`
	}
	if err := json.Unmarshal(result.body, &decoded); err != nil {
		return obs, errors.Join(transportErr, fmt.Errorf("decode readiness: %w", err))
	}
	if decoded.Ready == nil || decoded.Reason == "" ||
		(*decoded.Ready && status != http.StatusOK) ||
		(!*decoded.Ready && status != http.StatusServiceUnavailable) {
		return obs, errors.Join(transportErr, fmt.Errorf("inconsistent readiness HTTP %d: %.200q", status, result.body))
	}
	obs.Ready, obs.Reason, obs.Category = decoded.Ready, decoded.Reason, decoded.Category
	return obs, transportErr
}

const readinessTimeout = 10 * time.Second
const readinessPoll = 100 * time.Millisecond

// Cover the five-second readiness evidence lifetime and another sample tick.
const retargetObservation = 6 * time.Second

type readinessMismatch struct{ error }

func (e readinessMismatch) Unwrap() error { return e.error }

// A request ended by the observation window may wrap its context error in
// transport and body-read errors. A joined error can also carry a separate
// failure, such as a failed body close, that still belongs in the evidence.
func readinessInterrupted(err, contextErr error) bool {
	if err == contextErr {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		for _, cause := range causes {
			if !readinessInterrupted(cause, contextErr) {
				return false
			}
		}
		return len(causes) != 0
	}
	if cause := errors.Unwrap(err); cause != nil {
		return readinessInterrupted(cause, contextErr)
	}
	return false
}

// A known transient state is allowed only before the first expected sample.
func pollDiskReadiness(ctx context.Context, want, transient diskReadiness, stableFor time.Duration,
	probe func(context.Context) (readinessObservation, error), record func(readinessObservation, error),
) (readinessObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline() // WithTimeout always supplies a deadline.
	windowErr := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// The deadline timer and ticker can wake in either order.
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
	ticker := time.NewTicker(readinessPoll)
	defer ticker.Stop()
	var since time.Time
	var last readinessObservation
	for {
		if err := windowErr(); err != nil {
			return last, fmt.Errorf("waiting for readiness %q: %w", want.reason, err)
		}
		got, err := probe(ctx)
		if expired := windowErr(); expired != nil && readinessInterrupted(err, expired) {
			if got.Ready == nil {
				return last, fmt.Errorf("waiting for readiness %q: %w", want.reason, expired)
			}
			// The response still carried a complete, validated body.
			err = nil
		}
		last = got
		stable := false
		if err == nil {
			switch state := got.state(); {
			case state == want:
				now := time.Now()
				if since.IsZero() {
					since = now
				}
				stable = now.Sub(since) >= stableFor
			case state != transient || !since.IsZero():
				err = readinessMismatch{fmt.Errorf("readiness %+v, want %+v", state, want)}
			}
		}
		record(got, err)
		if expired := windowErr(); expired != nil {
			return last, fmt.Errorf("waiting for readiness %q: %w", want.reason, expired)
		}
		if err != nil {
			if _, mismatch := err.(readinessMismatch); mismatch {
				return got, err
			}
			return got, fmt.Errorf("probe readiness %q: %w", want.reason, err)
		}
		if stable {
			return got, nil
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}
