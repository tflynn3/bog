package querymodel_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	model "bog/verify/query-model"
)

var captureWireLimits = model.WireLimits{MaxPageBytes: 256, MaxPages: 12, Timeout: 15 * time.Second}

func newCaptureDeliveryFixture(t *testing.T, retained bool) model.CaptureDeliveryFixture {
	t.Helper()
	f := &runtimeQueryFixture{retained: retained, budget: 32 << 10, concurrency: 4}
	f.startIssuer()
	t.Cleanup(func() {
		if err := f.closeIssuer(); err != nil {
			t.Error(err)
		}
	})
	s := newQuerySocketFixture(t, f.handler)
	fixture := model.CaptureDeliveryFixture{
		Acknowledge: func(ctx context.Context, rows []model.Row) error { return acknowledgeFixtureBatch(ctx, s.client, rows) },
		Fetch:       s.fetch, Refuse: s.refuse,
		Interrupt: s.interrupt, HoldResume: s.holdResume,
		Retire: func(ctx context.Context, _ model.Filter, token string) error {
			retirer, ok := f.handler.(interface{ RetireQueryCapture(string) error })
			if !ok {
				return fmt.Errorf("%s issuer %T does not expose RetireQueryCapture", pageSourceName(retained), f.handler)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return retirer.RetireQueryCapture(token)
		},
	}
	if retained {
		fixture.Publish = f.publish
	}
	return fixture
}

func TestQueryCaptureSurvivesFailedOpeningWrite(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(pageSourceName(retained), func(t *testing.T) {
			t.Parallel()
			f := newCaptureDeliveryFixture(t, retained)
			if err := model.VerifyCaptureDelivery(t.Context(), seededTraceConfig(0).Epoch, captureWireLimits, model.ResumeAfterHeaders, f); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func pageSourceName(retained bool) string {
	if retained {
		return "retained"
	}
	return "legacy"
}

func requireQuery104(t *testing.T) {
	t.Helper()
	if os.Getenv("BOG_QUERY_VERIFY_104") != "1" {
		t.Skip("set BOG_QUERY_VERIFY_104=1 to qualify the #104 HTTP contract")
	}
}

func TestQuery104CaptureLifecycle(t *testing.T) {
	requireQuery104(t)
	for _, tc := range []struct {
		name     string
		scenario model.CaptureDeliveryScenario
		retained bool
	}{
		{"legacy-slot-reuse", model.ReuseBeforeHeaders, false},
		{"retained-slot-reuse", model.ReuseBeforeHeaders, true},
		{"legacy-retirement", model.DrainAtCapacity, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newCaptureDeliveryFixture(t, tc.retained)
			if err := model.VerifyCaptureDelivery(t.Context(), seededTraceConfig(0).Epoch, captureWireLimits, tc.scenario, f); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQuery104FittingPages(t *testing.T) {
	requireQuery104(t)
	for _, retained := range []bool{false, true} {
		t.Run(pageSourceName(retained), func(t *testing.T) {
			for _, limit := range []int{1, 10} {
				t.Run(fmt.Sprintf("opening-limit-%d", limit), func(t *testing.T) {
					t.Parallel()
					f := newCaptureDeliveryFixture(t, retained)
					if err := model.VerifyFittingPages(t.Context(), seededTraceConfig(0).Epoch, captureWireLimits, limit, f); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestRetainedCaptureRetirementDrainsAtCapacity(t *testing.T) {
	f := newCaptureDeliveryFixture(t, true)
	if err := model.VerifyCaptureDelivery(t.Context(), seededTraceConfig(0).Epoch, captureWireLimits, model.DrainAtCapacity, f); err != nil {
		t.Fatal(err)
	}
}

func TestFittingPageWitnessOverTCP(t *testing.T) {
	for _, tc := range []struct {
		name       string
		limit      int
		wrongOrder bool
		want       string
	}{
		{name: "opening byte cut", limit: 10},
		{name: "resume byte cut", limit: 1},
		{name: "reject fitting rows out of order", limit: 10, wrongOrder: true, want: "append ordinal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			peer := &fixturePeer{captures: make(map[string]scriptedCapture), trimCap: 256}
			s := newQuerySocketFixture(t, peer)
			fixture := model.CaptureDeliveryFixture{
				Acknowledge: func(ctx context.Context, rows []model.Row) error {
					return acknowledgeFixtureBatch(ctx, s.client, rows)
				},
				Fetch: func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
					page, err := s.fetch(ctx, req)
					if tc.wrongOrder && len(page.Rows) >= 3 {
						page.Rows[1], page.Rows[2] = page.Rows[2], page.Rows[1]
					}
					return page, err
				},
				Refuse: s.refuse,
			}
			err := model.VerifyFittingPages(t.Context(), seededTraceConfig(0).Epoch, captureWireLimits, tc.limit, fixture)
			if tc.want == "" && err != nil {
				t.Fatal(err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestQueryRefusalWitnessOverTCP(t *testing.T) {
	const want = "expected refusal\n"
	for _, tc := range []struct {
		name     string
		body     string
		header   http.Header
		oversize bool
		ok       bool
	}{
		{name: "complete", body: want, ok: true},
		{name: "body drift", body: "different refusal\n"},
		{name: "empty cursor header", body: want, header: http.Header{cursorHeader: {""}}},
		{name: "cursor trailer", body: want, header: http.Header{"Trailer": {cursorHeader}}},
		{name: "truncated body", body: want, oversize: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newQuerySocketFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for name, values := range tc.header {
					w.Header()[name] = values
				}
				w.Header().Set("Content-Type", "text/plain")
				if tc.oversize {
					w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)+1))
				}
				w.WriteHeader(http.StatusUnprocessableEntity)
				if n, err := io.WriteString(w, tc.body); err != nil || n != len(tc.body) {
					t.Errorf("write refusal body: wrote %d bytes, error %v", n, err)
				}
			}))
			start := seededTraceConfig(0).Epoch
			req := model.WireRequest{Filter: model.Filter{Start: start, End: start.Add(time.Second)}, Limit: 1, MaxPageBytes: 256}
			if err := s.refuse(t.Context(), req, http.StatusUnprocessableEntity, want); (err == nil) != tc.ok {
				t.Fatalf("refusal validation success=%t, want %t, error %v", err == nil, tc.ok, err)
			}
		})
	}
}
