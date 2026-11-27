package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	model "bog/verify/query-model"
)

func selectiveAdmissionConfig() model.SelectiveAdmissionConfig {
	return model.SelectiveAdmissionConfig{
		Epoch:      time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
		SeedRows:   4096,
		WorkBudget: 16 << 20,
		// Two broad plans fit. A third scan exceeds this budget,
		// even while a second scan has allocated only its first arrays.
		ScanBytes:        210 << 10,
		QueryConcurrency: 4,
		Limits:           model.WireLimits{MaxPages: 4, MaxPageBytes: 256, Timeout: 16 * time.Minute},
		CursorLifetime:   15 * time.Minute,
		ExpiryGrace:      time.Second,
	}
}

type selectiveAppender struct{ store *store.Store }

func (a selectiveAppender) Append(_ []byte, rows []store.Row) error {
	a.store.Publish(rows)
	return nil
}

type selectiveScanGateKey struct{}

type selectiveScanContext struct {
	context.Context
	polls   int
	entered chan struct{}
}

// QueryWork polls the context before scan charges. The fixture has 4095
// in-range rows and few index runs. By this poll the legacy opening has
// charged setup and grown its match array, with rows still left to scan.
func (c *selectiveScanContext) Err() error {
	c.polls++
	if c.polls == 1024 {
		close(c.entered)
		<-c.Done()
	}
	return c.Context.Err()
}

type selectiveRecorderTransport struct{ handler http.Handler }

func (s selectiveRecorderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if scan, ok := req.Context().Value(selectiveScanGateKey{}).(*selectiveScanContext); ok {
		req = req.WithContext(scan)
	}
	out := httptest.NewRecorder()
	s.handler.ServeHTTP(out, req)
	if req.Body != nil {
		if err := req.Body.Close(); err != nil {
			return nil, err
		}
	}
	res := out.Result()
	// net/http adds a Content-Length for small buffered errors. Supply the
	// same framing to the recorder, which does not run a server connection.
	if res.StatusCode >= 400 && res.ContentLength < 0 {
		res.ContentLength = int64(out.Body.Len())
		res.Header.Set("Content-Length", strconv.Itoa(out.Body.Len()))
	}
	return res, nil
}

func newSelectiveAdmissionFixture(t *testing.T, cfg model.SelectiveAdmissionConfig) model.SelectiveAdmissionAdapter {
	t.Helper()
	s := new(store.Store)
	resources := httpapi.DefaultQueryResourceLimits()
	resources.ScanBytes = cfg.ScanBytes
	api := httpapi.NewWithQueryResources(s, selectiveAppender{s}, int(cfg.Limits.MaxPageBytes),
		cfg.QueryConcurrency, cfg.WorkBudget, resources)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := api.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	const base = "http://selective-fixture"
	transport := selectiveRecorderTransport{api}
	c, fetch, err := newWitnessClient(base, transport, cfg.Limits.MaxPageBytes)
	if err != nil {
		t.Fatal(err)
	}
	raw := &http.Client{Transport: transport}
	refuse := func(ctx context.Context, req model.WireRequest, status int, body string) error {
		return observeQueryRefusal(ctx, raw, base, req, status, body)
	}
	return model.SelectiveAdmissionAdapter{
		Acknowledge: func(ctx context.Context, rows []model.Row) error {
			return acknowledgeFixtureBatch(ctx, c, rows)
		},
		Fetch:  fetch,
		Refuse: refuse,
		CancelScan: func(ctx context.Context, req model.WireRequest, whileScanning func() error) error {
			requestCtx, stopRequest := context.WithTimeout(ctx, 3*time.Second)
			defer stopRequest()
			handlerCtx, cancel := context.WithCancel(requestCtx)
			scan := &selectiveScanContext{Context: handlerCtx, entered: make(chan struct{})}
			var wg sync.WaitGroup
			var requestErr error
			wg.Go(func() {
				wireCtx := context.WithValue(requestCtx, selectiveScanGateKey{}, scan)
				requestErr = refuse(wireCtx, req, http.StatusRequestTimeout, "request cancelled\n")
			})
			defer wg.Wait()
			defer cancel()

			var overlapErr error
			select {
			case <-scan.entered:
				overlapErr = whileScanning()
			case <-requestCtx.Done():
				overlapErr = fmt.Errorf("opening did not reach the scan barrier: %w", requestCtx.Err())
			}
			cancel()
			wg.Wait()
			return errors.Join(overlapErr, requestErr, requestCtx.Err())
		},
	}
}

func TestSelectiveQueryAdmissionFixture(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		scenario                      model.SelectiveAdmissionScenario
		opens, resumes, scanRefusals  int
		selective, cancelled, expired bool
	}{
		{model.SelectiveAlongsideCursor, 1, 1, 0, true, false, false},
		{model.SelectiveScanExhaustion, 2, 1, 2, false, false, false},
		{model.SelectiveScanCancellation, 2, 1, 1, true, true, false},
		{model.SelectiveScanExpiry, 3, 2, 3, false, false, true},
	} {
		t.Run(string(tc.scenario), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := selectiveAdmissionConfig()
				f := newSelectiveAdmissionFixture(t, cfg)
				report, err := model.VerifySelectiveAdmission(t.Context(), cfg, tc.scenario, f)
				if err != nil {
					t.Fatalf("%+v: %v", report, err)
				}
				if report.AcknowledgedRows != cfg.SeedRows || report.BroadOpens != tc.opens ||
					report.BroadResumes != tc.resumes || report.ScanRefusals != tc.scanRefusals ||
					report.Cancelled != tc.cancelled || report.Expired != tc.expired ||
					report.LiveBeforeExpiry != tc.expired || report.Reused != (tc.cancelled || tc.expired) {
					t.Fatalf("unexpected admission report: %+v", report)
				}
				if tc.selective && (report.Empty.Pages != 1 || report.Empty.BodyBytes != 0 ||
					report.Empty.Rows != 0 || report.Sparse.Rows != 3 || report.Sparse.Pages != 2 || !report.SparseTerminalReplay) {
					t.Fatalf("incomplete selective evidence: %+v", report)
				}
				if tc.expired && report.ExpiryRecoveryAfter != cfg.CursorLifetime+cfg.ExpiryGrace {
					t.Fatalf("recovered at %s, want %s", report.ExpiryRecoveryAfter, cfg.CursorLifetime+cfg.ExpiryGrace)
				}
			})
		})
	}
}

func TestSelectiveAdmissionRejectsFalseEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		scenario model.SelectiveAdmissionScenario
		want     string
		fault    func(*model.SelectiveAdmissionAdapter)
	}{
		{
			"empty response without body witness", model.SelectiveAlongsideCursor, "missing encoded page witness",
			func(f *model.SelectiveAdmissionAdapter) {
				fetch := f.Fetch
				f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
					page, err := fetch(ctx, req)
					if req.Filter.Contains == "missing-needle" {
						page.EncodedBody = nil
					}
					return page, err
				}
			},
		},
		{
			"sparse continuation drops tied duplicate", model.SelectiveAlongsideCursor, "missing continuation",
			func(f *model.SelectiveAdmissionAdapter) {
				fetch := f.Fetch
				f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
					page, err := fetch(ctx, req)
					if req.Filter.Contains == "needle" && req.Cursor != "" && len(page.Rows) == 2 {
						page.Rows = page.Rows[:1]
					}
					return page, err
				}
			},
		},
		{
			"slot refusal masquerades as scan refusal", model.SelectiveScanExhaustion, "before scan exhaustion",
			func(f *model.SelectiveAdmissionAdapter) {
				fetch := f.Fetch
				f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
					page, err := fetch(ctx, req)
					if err == model.ErrSelectiveScanCapacity {
						return page, errors.New("query capacity exhausted")
					}
					return page, err
				}
			},
		},
		{
			"scan refusal with a leaked cursor", model.SelectiveScanExhaustion, "before scan exhaustion",
			func(f *model.SelectiveAdmissionAdapter) {
				fetch := f.Fetch
				f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
					page, err := fetch(ctx, req)
					if err == model.ErrSelectiveScanCapacity {
						page.NextCursor = "secret-cursor"
					}
					return page, err
				}
			},
		},
		{
			"missing cancellation barrier", model.SelectiveScanCancellation, "did not reach the allocation barrier",
			func(f *model.SelectiveAdmissionAdapter) {
				f.CancelScan = func(context.Context, model.WireRequest, func() error) error {
					return nil
				}
			},
		},
		{
			"cancellation leaves charge in place", model.SelectiveScanCancellation, "reuse scan capacity after cancellation",
			func(f *model.SelectiveAdmissionAdapter) {
				cancel := f.CancelScan
				f.CancelScan = func(ctx context.Context, req model.WireRequest, callback func() error) error {
					if err := cancel(ctx, req, callback); err != nil {
						return err
					}
					// Consume the newly released headroom before recovery.
					_, err := f.Fetch(ctx, req)
					return err
				}
			},
		},
		{
			"expired cursor gets wrong status", model.SelectiveScanExpiry, "authenticated resume after expiry",
			func(f *model.SelectiveAdmissionAdapter) {
				refuse := f.Refuse
				f.Refuse = func(ctx context.Context, req model.WireRequest, _ int, body string) error {
					return refuse(ctx, req, http.StatusServiceUnavailable, body)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := selectiveAdmissionConfig()
				f := newSelectiveAdmissionFixture(t, cfg)
				tc.fault(&f)
				_, err := model.VerifySelectiveAdmission(t.Context(), cfg, tc.scenario, f)
				if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "secret-cursor") {
					t.Fatalf("got %v, want %q without cursor disclosure", err, tc.want)
				}
			})
		})
	}
}

func TestSelectiveScanRefusalWireWitness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		header     http.Header
		clean      bool
	}{
		{"scan", model.SelectiveScanCapacityBody, nil, true},
		{"query slot", "query capacity exhausted\n", nil, false},
		{"capture slot", "query capture capacity exhausted\n", nil, false},
		{"work", "query work budget exhausted\n", nil, false},
		{"rows", model.SelectiveScanCapacityBody + "{\"line\":\"leak\"}\n", nil, false},
		{"cursor header", model.SelectiveScanCapacityBody, http.Header{cursorHeader: {"opaque"}}, false},
		{"cursor trailer", model.SelectiveScanCapacityBody, http.Header{"Trailer": {cursorHeader}}, false},
		{"incomplete body", model.SelectiveScanCapacityBody, http.Header{"Content-Length": {"100"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				for name, values := range tc.header {
					for _, value := range values {
						w.Header().Add(name, value)
					}
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				n, err := io.WriteString(w, tc.body)
				if err != nil || n != len(tc.body) {
					t.Errorf("write refusal: wrote %d of %d bytes: %v", n, len(tc.body), err)
				}
				if tc.header.Get("Trailer") != "" {
					w.Header().Set(cursorHeader, "opaque")
				}
			}))
			t.Cleanup(s.Close)
			cfg := selectiveAdmissionConfig()
			_, fetch, err := newWitnessClient(s.URL, s.Client().Transport, cfg.Limits.MaxPageBytes)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			page, err := fetch(ctx, model.WireRequest{
				Filter: model.Filter{Start: cfg.Epoch, End: cfg.Epoch.Add(time.Second)},
				Limit:  1, MaxPageBytes: cfg.Limits.MaxPageBytes,
			})
			if err == nil || (err == model.ErrSelectiveScanCapacity) != tc.clean ||
				page.EncodedBody != nil || page.NextCursor != "" || len(page.Rows) != 0 || page.BodyBytes != 0 {
				t.Fatalf("clean=%t, got page=%+v error=%v", tc.clean, page, err)
			}
		})
	}
}
