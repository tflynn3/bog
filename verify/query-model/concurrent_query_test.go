package querymodel_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"bog/internal/store"
	model "bog/verify/query-model"
)

// queryDemandMeter accounts for distinct live allocations at fixture
// barriers. It takes a high-water mark when each resource is added.
type queryDemandMeter struct {
	mu        sync.Mutex
	demand    model.QueryDemand
	captures  map[string]captureAcquisition
	lifetimes []model.QueryCaptureLifetime
	err       error
}

type captureAcquisition struct {
	order int
	start time.Time
}

func (m *queryDemandMeter) acquire(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.captures == nil {
		m.captures = make(map[string]captureAcquisition)
	}
	if _, exists := m.captures[id]; exists {
		m.err = errors.New("duplicate source capture acquisition")
	}
	m.captures[id] = captureAcquisition{len(m.captures) + len(m.lifetimes) + 1, time.Now()}
	m.demand.LiveCaptures = len(m.captures)
	m.demand.PeakCaptures = max(m.demand.PeakCaptures, m.demand.LiveCaptures)
}

func (m *queryDemandMeter) release(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	acquired, exists := m.captures[id]
	if !exists {
		m.err = errors.New("source released an unobserved capture")
		return
	}
	delete(m.captures, id)
	m.demand.LiveCaptures = len(m.captures)
	m.demand.ReleasedCaptures++
	lifetime := time.Since(acquired.start)
	m.lifetimes = append(m.lifetimes, model.QueryCaptureLifetime{Acquisition: acquired.order, Duration: lifetime})
	m.demand.MaxCaptureLifetime = max(m.demand.MaxCaptureLifetime, lifetime)
}

func (m *queryDemandMeter) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = errors.Join(m.err, err)
}

func (m *queryDemandMeter) enter(point model.QueryHoldPoint, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch point {
	case model.PreparedDelivery:
		m.demand.PreparedResponses++
		m.demand.PreparedBytes += n
		m.demand.PeakPreparedBytes = max(m.demand.PeakPreparedBytes, m.demand.PreparedBytes)
	case model.ScanBufferReady:
		m.demand.ScanBuffers++
		m.demand.ScanBufferBytes += n
		m.demand.PeakScanBufferBytes = max(m.demand.PeakScanBufferBytes, m.demand.ScanBufferBytes)
	}
}

func (m *queryDemandMeter) leave(point model.QueryHoldPoint, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch point {
	case model.PreparedDelivery:
		m.demand.PreparedResponses--
		m.demand.PreparedBytes -= n
	case model.ScanBufferReady:
		m.demand.ScanBuffers--
		m.demand.ScanBufferBytes -= n
	}
}

func (m *queryDemandMeter) observe(ctx context.Context) (model.QueryDemand, []model.QueryCaptureLifetime, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.demand, slices.Clone(m.lifetimes), errors.Join(m.err, ctx.Err())
}

func TestConcurrentRetainedQueryDemand(t *testing.T) {
	t.Parallel()
	for _, offset := range []int{0, -7 * 3600, 5*3600 + 30*60} {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			t.Parallel()
			cfg := replacementConfig()
			cfg.Epoch = cfg.Epoch.In(time.FixedZone("fixture", offset))
			cfg.WorkBudget = 32 << 10
			var report model.ConcurrentQueryReport
			err := launchMeasuredReplacementFixture(t, t.Context(), cfg.WorkBudget, model.ConcurrentQuerySeed(cfg.Epoch),
				&queryDemandMeter{}, func(f model.ConcurrentQueryAdapter) error {
					var err error
					report, err = model.VerifyConcurrentQueryDemand(t.Context(), cfg, f)
					return err
				})
			if err != nil {
				t.Fatalf("report=%+v: %v", report, err)
			}
			if !report.AdmissionRefused || report.ByteIdenticalRetry != [2]bool{true, true} ||
				report.Held[0].Rows != 21 || report.Held[1].Rows != 5 ||
				report.Fresh[0].Rows != 24 || report.Fresh[1].Rows != 8 ||
				report.Replacement != (model.ReplacementGeneration{Generation: 2, CoveredRows: 23, CommittedRows: 24, Complete: true}) {
				t.Fatalf("incomplete workload evidence: %+v", report)
			}
			final := report.Samples[len(report.Samples)-1]
			overlap := report.Samples[1]
			const scanSlots = 2 * 12
			sparseBytes := report.Samples[len(report.Samples)-3].Demand.PreparedBytes
			denseBytes := overlap.Demand.PreparedBytes - sparseBytes
			if overlap.Point != "overlapping_preparation_and_scans" ||
				final.Demand.PeakScanBufferBytes != scanSlots*int64(unsafe.Sizeof(store.PositionedRow{})) ||
				final.Demand.PeakPreparedBytes != overlap.Demand.PreparedBytes+max(denseBytes, sparseBytes) ||
				final.Demand.PeakCaptures != 4 || final.Demand.ReleasedCaptures != 6 ||
				report.ReleaseAfterDrain <= 0 {
				t.Fatalf("missing measured overlap or drain: %+v", report)
			}
			cancel := report.ReleaseAfterCancel
			for _, delay := range []time.Duration{cancel.SparseScan, cancel.DenseScan, cancel.DenseDelivery} {
				if delay <= 0 {
					t.Fatalf("invalid cancellation-to-release observation %s: %+v", delay, report)
				}
			}
			for i, order := range []int{4, 3, 1, 2, 5, 6} {
				lifetime := report.CaptureLifetimes[i]
				if lifetime.Acquisition != order || lifetime.Duration <= 0 || lifetime.Duration > final.Demand.MaxCaptureLifetime {
					t.Fatalf("capture release %d: got %+v, want acquisition %d and positive lifetime", i, lifetime, order)
				}
			}
			t.Logf("fixture goos=%s goarch=%s", runtime.GOOS, runtime.GOARCH)
			logQueryReport(t, "concurrent_query_demand", report)
		})
	}
}

func logQueryReport(t *testing.T, name string, report any) {
	t.Helper()
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s=%s", name, body)
}

func TestConcurrentRetainedQueryRejectsFalseEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, want string
		alter      func(*model.ConcurrentQueryAdapter)
	}{
		{"lost scan buffer", "overlapping_preparation_and_scans", func(f *model.ConcurrentQueryAdapter) {
			observe := f.Observe
			f.Observe = func(ctx context.Context) (model.QueryDemand, []model.QueryCaptureLifetime, error) {
				d, lifetimes, err := observe(ctx)
				d.ScanBufferBytes = 0
				return d, lifetimes, err
			}
		}},
		{"premature reclamation", "after_dense_delivery_cancel", func(f *model.ConcurrentQueryAdapter) {
			observe, pins := f.Observe, f.RetiredPins
			f.RetiredPins = func(ctx context.Context) (uint64, error) {
				d, _, err := observe(ctx)
				if err != nil || d.PreparedResponses == 1 {
					return 0, err
				}
				return pins(ctx)
			}
		}},
		{"post-replacement byte drift", "differs from the original encoded body", func(f *model.ConcurrentQueryAdapter) {
			fetch, activate := f.Fetch, f.Activate
			replaced := false
			f.Activate = func(ctx context.Context, cutoff int) (model.ReplacementGeneration, error) {
				generation, err := activate(ctx, cutoff)
				replaced = err == nil
				return generation, err
			}
			f.Fetch = func(ctx context.Context, q model.WireRequest) (model.WirePage, error) {
				page, err := fetch(ctx, q)
				if err == nil && replaced && q.Cursor != "" && page.NextCursor == "" {
					driftReplacementBody(&page)
				}
				return page, err
			}
		}},
		{"missing delivery barrier", "delivery or scan barrier was not reached", func(f *model.ConcurrentQueryAdapter) {
			f.Hold = func(ctx context.Context, q model.WireRequest, _ model.QueryHoldPoint, _ func() (bool, error)) (model.WirePage, error) {
				return f.Fetch(ctx, q)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := replacementConfig()
			cfg.WorkBudget = 32 << 10
			var trialErr error
			err := launchMeasuredReplacementFixture(t, t.Context(), cfg.WorkBudget, model.ConcurrentQuerySeed(cfg.Epoch),
				&queryDemandMeter{}, func(f model.ConcurrentQueryAdapter) error {
					tc.alter(&f)
					_, trialErr = model.VerifyConcurrentQueryDemand(t.Context(), cfg, f)
					return nil // Check cleanup independently of the expected trial error.
				})
			if err != nil || trialErr == nil || !strings.Contains(trialErr.Error(), tc.want) {
				t.Fatalf("trial error=%v, cleanup error=%v, want %q", trialErr, err, tc.want)
			}
		})
	}
}
