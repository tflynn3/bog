package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bog/verify/capacityevidence"
)

func TestObserveCorrelatedCapacity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, heldExtra, retiredExtra, problem string
		cancelPending                          bool
	}{
		{
			name:         "observed zero",
			heldExtra:    `,"replay_tail_bytes":0,"oldest_predecessor_pin_age_ns":0,"predecessor_pin_generation":7`,
			retiredExtra: `,"reclaimed_bytes":0,"reclaimed_predecessor_generation":7,"reclaimed_replacement_generation":8`,
		},
		{
			name:      "missing replay",
			heldExtra: `,"oldest_predecessor_pin_age_ns":0,"predecessor_pin_generation":7`,
			problem:   "missing replay_tail_bytes",
		},
		{
			name:      "wrong pin generation",
			heldExtra: `,"replay_tail_bytes":0,"oldest_predecessor_pin_age_ns":0,"predecessor_pin_generation":8`,
			problem:   "belongs to a different generation",
		},
		{
			name:         "credit for wrong replacement",
			heldExtra:    `,"replay_tail_bytes":0,"oldest_predecessor_pin_age_ns":0,"predecessor_pin_generation":7`,
			retiredExtra: `,"reclaimed_bytes":0,"reclaimed_predecessor_generation":7,"reclaimed_replacement_generation":9`,
			problem:      "attributed to a different predecessor or replacement",
		},
		{
			name:         "duplicate credit",
			heldExtra:    `,"replay_tail_bytes":0,"oldest_predecessor_pin_age_ns":0,"predecessor_pin_generation":7`,
			retiredExtra: `,"reclaimed_bytes":0,"reclaimed_bytes":0`,
			problem:      "invalid reclamation metric",
		},
		{
			name:      "no credit before cancellation",
			heldExtra: `,"replay_tail_bytes":0,"oldest_predecessor_pin_age_ns":0,"predecessor_pin_generation":7`,
			problem:   "capacity reclamation unconfirmed", cancelPending: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Complete the first status request before starting pin drain.
			replies := make(chan string, 2)
			replies <- `"retired_generation_pins":1` + tc.heldExtra
			replies <- `"retired_generation_pins":0` + tc.retiredExtra
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				select {
				case metrics := <-replies:
					wire := `{"state":"complete","generation":8,"covered_rows":16,"committed_rows":24,"metrics":{` + metrics + `}}`
					if _, err := w.Write([]byte(wire)); err != nil && req.Context().Err() == nil {
						t.Error(err)
					}
				default:
					t.Error("unexpected status request")
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			p := &backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 8, 16, 24),
				service: s, control: "/compact", dir: t.TempDir(), origin: time.Now()}
			p.Publication = &publicationProof{Generation: 8}
			p.Correlation = &capacityevidence.CycleCorrelation{PredecessorGeneration: new(uint64(7))}
			err := p.observeHeldPin(t.Context())
			if err == nil && tc.cancelPending {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				err = p.observeReclamation(ctx, runtimeSample{}, time.Now())
				if !errors.Is(err, context.Canceled) {
					t.Errorf("observeReclamation = %v, want cancellation", err)
				}
			} else if err == nil {
				err = p.observeDrain(t.Context(), p.boundary("capture_retired"), drainBudget)
			}
			if tc.problem != "" {
				if err == nil || !strings.Contains(err.Error(), tc.problem) || p.Correlation.ReclaimedBytes != nil {
					t.Fatalf("evidence = %+v, error = %v, want %q", p.Correlation, err, tc.problem)
				}
				return
			}
			proof := p.Correlation
			if err != nil || proof.ReplayTailBytes == nil || *proof.ReplayTailBytes != 0 ||
				proof.PredecessorPinAge == nil || *proof.PredecessorPinAge != 0 ||
				proof.ReclaimedBytes == nil || *proof.ReclaimedBytes != 0 ||
				proof.FinalPinReleaseNS == nil || proof.ReclaimedObservedNS == nil ||
				*proof.FinalPinReleaseNS != *proof.ReclaimedObservedNS {
				t.Fatalf("evidence = %+v, error = %v", proof, err)
			}
		})
	}
}

func TestObserveCapacityPeakPreservesReadFailure(t *testing.T) {
	t.Parallel()
	cause := errors.New("kernel counter unavailable")
	p := &backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 8, 16, 24), origin: time.Now()}
	p.Correlation = &capacityevidence.CycleCorrelation{}
	p.readMemoryPeak = func(context.Context) (uint64, error) { return 0, cause }
	err := p.observePeak(t.Context())
	peak := p.Correlation.CgroupPeak
	if !errors.Is(err, cause) || peak == nil || peak.Error != cause.Error() || peak.Bytes != nil {
		t.Fatalf("peak = %+v, error = %v", peak, err)
	}
}

func TestCapacityCorrelationCLIRequiresProof(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "capacity.json")
	if err := writeReport(path, passingCutoverReport()); err != nil {
		t.Fatal(err)
	}
	publish := func(string, report) error {
		t.Error("validation unexpectedly attempted to publish a report")
		return nil
	}
	if err := run(t.Context(), []string{"-validate-cutover", path}, publish); err != nil {
		t.Fatalf("historical validation: %v", err)
	}
	if err := run(t.Context(), []string{"-validate-capacity-correlation", path}, publish); err == nil ||
		!strings.Contains(err.Error(), "missing capacity correlation") {
		t.Fatalf("correlation validation = %v, want missing capacity correlation", err)
	}
}
