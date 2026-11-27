package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type readinessTransport func(*http.Request) (*http.Response, error)

func (f readinessTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type readinessBody struct {
	io.Reader
	close func() error
}

func (b readinessBody) Close() error { return b.close() }

type readinessReader func([]byte) (int, error)

func (f readinessReader) Read(p []byte) (int, error) { return f(p) }

func TestDiskReadinessResponse(t *testing.T) {
	t.Parallel()
	const readyBody = `{"ready":true,"reason":"ready"}`
	closeFailure := errors.New("body close failed")
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		closeErr error
		want     diskReadiness
		problem  string
	}{
		{"ready", 200, readyBody, nil, diskReady, ""},
		{"low", 503, `{"ready":false,"reason":"low_disk_headroom"}`, nil, diskLow, ""},
		{"rollback", 503, `{"ready":false,"reason":"wal_rollback","category":"enospc"}`, nil, diskRollback, ""},
		{"wrong status", 503, readyBody, nil, diskReadiness{}, "inconsistent readiness"},
		{"missing ready", 503, `{"reason":"low_disk_headroom"}`, nil, diskReadiness{}, "inconsistent readiness"},
		{"missing reason", 200, `{"ready":true}`, nil, diskReadiness{}, "inconsistent readiness"},
		{"partial JSON", 200, readyBody[:len(readyBody)-1], nil, diskReadiness{}, "decode readiness"},
		{"trailing JSON", 200, readyBody + "{}", nil, diskReadiness{}, "decode readiness"},
		{"oversized", 200, readyBody + strings.Repeat(" ", 1024), nil, diskReadiness{}, "oversized readiness"},
		{"close failure", 200, readyBody, closeFailure, diskReady, closeFailure.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			closed := 0
			client := &http.Client{Transport: readinessTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.Path != "/ready" {
					t.Errorf("unexpected readiness request %s %s", req.Method, req.URL)
				}
				return &http.Response{StatusCode: tc.status, Body: readinessBody{strings.NewReader(tc.body), func() error {
					closed++
					return tc.closeErr
				}}}, nil
			})}
			obs, err := readDiskReadiness(t.Context(), client, "127.0.0.1:1234")
			got := obs.state()
			if got != tc.want || closed != 1 {
				t.Errorf("readiness = %+v, closes = %d, want %+v and one close", got, closed, tc.want)
			}
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Errorf("wanted error %q, got %v", tc.problem, err)
			}
			if tc.closeErr != nil && !errors.Is(err, tc.closeErr) {
				t.Errorf("close failure lost: %v", err)
			}
		})
	}
}

func TestObserveDiskReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		samples       []diskReadiness
		want, interim diskReadiness
		stableFor     time.Duration
		cancelAfter   time.Duration
		wantElapsed   time.Duration
		problem       string
	}{
		{"startup sample", []diskReadiness{diskPending, diskReady}, diskReady, diskPending, 0, 0, readinessPoll, ""},
		{"healthy after retarget", []diskReadiness{diskReady}, diskReady, diskReadiness{}, retargetObservation, 0, retargetObservation, ""},
		{"full after retarget", []diskReadiness{diskLow}, diskLow, diskReadiness{}, retargetObservation, 0, retargetObservation, ""},
		{"stale ready gives way to wrong filesystem", []diskReadiness{diskReady, diskLow}, diskReady, diskReadiness{}, retargetObservation, 0, readinessPoll, "low_disk_headroom"},
		{"stale low gives way to wrong filesystem", []diskReadiness{diskLow, diskReady}, diskLow, diskReadiness{}, retargetObservation, 0, readinessPoll, "ready"},
		{"never samples disk full", []diskReadiness{diskReady}, diskLow, diskReady, 0, 0, readinessTimeout, "deadline exceeded"},
		{"stability tick coincides with deadline", []diskReadiness{diskReady}, diskReady, diskReadiness{}, readinessTimeout, 0, readinessTimeout, "deadline exceeded"},
		{"canceled while sampling", []diskReadiness{diskReady}, diskLow, diskReady, 0, 2 * readinessPoll, 2 * readinessPoll, "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				if tc.cancelAfter != 0 {
					deadline, cancel := context.WithCancel(ctx)
					defer cancel()
					timer := time.AfterFunc(tc.cancelAfter, cancel)
					defer timer.Stop()
					ctx = deadline
				}
				calls := 0
				client := &http.Client{Transport: readinessTransport(func(*http.Request) (*http.Response, error) {
					sample := tc.samples[min(calls, len(tc.samples)-1)]
					calls++
					status := http.StatusServiceUnavailable
					if sample.ready {
						status = http.StatusOK
					}
					body := fmt.Sprintf(`{"ready":%t,"reason":%q,"category":%q}`, sample.ready, sample.reason, sample.category)
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
				})}
				start := time.Now()
				diag := newCaseDiagnostics()
				evidence := &readinessEvidence{ExpectedReason: tc.want.reason, StableFor: tc.stableFor}
				_, err := pollDiskReadiness(ctx, tc.want, tc.interim, tc.stableFor, func(ctx context.Context) (readinessObservation, error) {
					return readDiskReadiness(ctx, client, "127.0.0.1:1234")
				}, func(obs readinessObservation, err error) {
					diag.observeReadiness(evidence, obs, err)
				})
				if evidence.Attempts != calls || evidence.First.state() != tc.samples[0] ||
					evidence.Last.state() != tc.samples[min(calls-1, len(tc.samples)-1)] {
					t.Fatalf("readiness samples: %+v (calls=%d)", evidence, calls)
				}
				if tc.wantElapsed == readinessTimeout {
					// The tick at the deadline must never dispatch another probe.
					if wantCalls := int(readinessTimeout / readinessPoll); calls != wantCalls || !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("calls = %d, want %d, terminal error = %v", calls, wantCalls, err)
					}
				}
				wantDivergence := strings.HasPrefix(tc.name, "stale")
				if _, mismatch := errors.AsType[readinessMismatch](err); mismatch != wantDivergence {
					t.Fatalf("readiness mismatch = %t, want %t (err: %v)", mismatch, wantDivergence, err)
				}
				if (evidence.FirstFailure != nil) != wantDivergence || diag.UncertainWAL != nil {
					t.Fatalf("unexpected failure or WAL evidence: %+v, %+v", evidence, diag.UncertainWAL)
				}
				if elapsed := time.Since(start); elapsed != tc.wantElapsed {
					t.Errorf("elapsed = %v, want %v", elapsed, tc.wantElapsed)
				}
				if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
					t.Errorf("wanted error %q, got %v", tc.problem, err)
				}
			})
		})
	}
}

func TestReadinessEvidenceWhenPollingEnds(t *testing.T) {
	t.Parallel()
	const pendingBody = `{"ready":false,"reason":"sampling_pending"}`
	const readyBody = `{"ready":true,"reason":"ready"}`
	closeFailure := errors.New("body close failed")
	transportFailure := errors.New("connection reset")
	for _, tc := range []struct {
		name        string
		cancel      bool
		clientLimit time.Duration
		status      int
		body        string
		readBlocked bool
		closeErr    error
		requestErr  error
		wantState   diskReadiness
		wantFailure string
	}{
		{name: "canceled before headers", cancel: true, wantState: diskPending},
		{name: "deadline during body", status: 200, readBlocked: true, wantState: diskPending},
		{name: "deadline during body with failed close", status: 200, readBlocked: true, closeErr: closeFailure, wantFailure: closeFailure.Error()},
		{name: "valid response at deadline", status: 200, body: readyBody, wantState: diskReady},
		{name: "valid response with deadline during close", status: 200, body: readyBody, closeErr: context.DeadlineExceeded, wantState: diskReady},
		{name: "mismatch at deadline", status: 503, body: `{"ready":false,"reason":"low_disk_headroom"}`, wantState: diskLow, wantFailure: "low_disk_headroom"},
		{name: "malformed response at deadline", status: 200, body: "{", wantFailure: "decode readiness"},
		{name: "transport failure at deadline", requestErr: transportFailure, wantFailure: transportFailure.Error()},
		{name: "client timeout inside window", clientLimit: readinessPoll, wantFailure: context.DeadlineExceeded.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				calls, closes := 0, 0
				client := &http.Client{Timeout: tc.clientLimit, Transport: readinessTransport(func(req *http.Request) (*http.Response, error) {
					calls++
					if calls == 1 {
						return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(pendingBody))}, nil
					}
					if tc.cancel {
						timer := time.AfterFunc(readinessPoll/2, cancel)
						defer timer.Stop()
					}
					if tc.status == 0 || !tc.readBlocked {
						<-req.Context().Done()
					}
					if tc.status == 0 {
						if tc.requestErr != nil {
							return nil, tc.requestErr
						}
						return nil, req.Context().Err()
					}
					var body io.Reader = strings.NewReader(tc.body)
					if tc.readBlocked {
						body = readinessReader(func([]byte) (int, error) {
							<-req.Context().Done()
							return 0, req.Context().Err()
						})
					}
					return &http.Response{StatusCode: tc.status, Body: readinessBody{body, func() error {
						closes++
						return tc.closeErr
					}}}, nil
				})}
				diag := newCaseDiagnostics()
				evidence := &readinessEvidence{ExpectedReason: diskReady.reason}
				last, err := pollDiskReadiness(ctx, diskReady, diskPending, 0, func(ctx context.Context) (readinessObservation, error) {
					obs, err := readDiskReadiness(ctx, client, "127.0.0.1:1234")
					obs.Lifetime = 2
					return obs, err
				}, func(obs readinessObservation, err error) {
					diag.observeReadiness(evidence, obs, err)
				})
				wantErr, wantCloses := context.DeadlineExceeded, 0
				if tc.cancel {
					wantErr = context.Canceled
				}
				if tc.status != 0 {
					wantCloses = 1
				}
				wantPrefix := `waiting for readiness "ready":`
				if tc.clientLimit != 0 {
					wantPrefix = `probe readiness "ready":`
				}
				if !errors.Is(err, wantErr) || !strings.HasPrefix(err.Error(), wantPrefix) {
					t.Fatalf("terminal error = %v, want prefix %s and cause %v", err, wantPrefix, wantErr)
				}
				wantAttempts := 2
				if tc.wantState == diskPending {
					wantAttempts = 1
				}
				if calls != 2 || closes != wantCloses || evidence.Attempts != wantAttempts {
					t.Fatalf("calls = %d, closes = %d, recorded samples = %d, want 2, %d, %d", calls, closes, evidence.Attempts, wantCloses, wantAttempts)
				}
				if last.state() != tc.wantState || last.Lifetime != 2 || evidence.Last.state() != tc.wantState || evidence.Last.Lifetime != 2 {
					t.Fatalf("returned sample = %+v, recorded sample = %+v, want state %+v from lifetime 2", last, evidence.Last, tc.wantState)
				}
				if tc.wantFailure == "" {
					if evidence.FirstFailure != nil || evidence.Last.Error != "" || last.Body == nil {
						t.Fatalf("expected a completed sample without failure, got %+v", evidence.Last)
					}
				} else if evidence.FirstFailure == nil || !strings.Contains(evidence.FirstFailure.Error, tc.wantFailure) {
					t.Fatalf("sample failure = %+v, want %q", evidence.FirstFailure, tc.wantFailure)
				}
			})
		})
	}
}
