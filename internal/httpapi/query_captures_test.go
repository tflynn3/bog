package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
)

type trackedPageSource struct {
	legacyPageSource
	expiresAt time.Time
	expired   atomic.Bool
	releases  atomic.Int32
	closeErr  error
}

func (s *trackedPageSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	if req.Capture != "" && s.expired.Load() {
		return PageResult{}, fmt.Errorf("storage generation retired: %w", ErrCaptureExpired)
	}
	result, err := s.legacyPageSource.Page(ctx, req, work)
	if err == nil && req.Snapshot && req.Capture == "" {
		result.ExpiresAt = s.expiresAt
	}
	return result, err
}

func (s *trackedPageSource) Release(capture string) error {
	s.releases.Add(1)
	return errors.Join(s.legacyPageSource.Release(capture), s.closeErr)
}

func captureFixtureSource() *trackedPageSource {
	s := new(store.Store)
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Publish([]store.Row{{TS: stamp, Line: "first"}, {TS: stamp, Line: "second"}})
	return &trackedPageSource{legacyPageSource: legacyPageSource{store: s}}
}

func requestCapturePage(h http.Handler, params string) *httptest.ResponseRecorder {
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+params, nil))
	return out
}

func issueCapture(t *testing.T, a *API) string {
	t.Helper()
	out := requestCapturePage(a, "&page=1&limit=1")
	token := out.Header().Get("Bog-Next-Cursor")
	if out.Code != 200 || token == "" {
		t.Fatalf("capture: status=%d headers=%v body=%s", out.Code, out.Header(), out.Body.String())
	}
	return token
}

func TestQueryCaptureUsesEarlierDeadline(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source time.Duration
		want   time.Duration
	}{
		{"source deadline", time.Minute, time.Minute},
		{"HTTP deadline", time.Hour, queryCaptureLifetime},
		{"optional source deadline", 0, queryCaptureLifetime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source := captureFixtureSource()
				if tc.source != 0 {
					source.expiresAt = time.Now().Add(tc.source)
				}
				a := NewWithPageSource(source, nil, 4096, 1, DefaultQueryWorkBudget)
				token := issueCapture(t, a)
				path := "&cursor=" + token
				time.Sleep(tc.want - time.Nanosecond)
				for range 2 {
					out := requestCapturePage(a, path)
					if out.Code != 200 || out.Header().Get("Bog-Next-Cursor") != "" {
						t.Fatalf("live retry: status=%d headers=%v body=%s", out.Code, out.Header(), out.Body.String())
					}
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				if got := source.releases.Load(); got != 1 {
					t.Fatalf("idle expiry released %d captures", got)
				}
				assertQueryRejection(t, requestCapturePage(a, path), 410, "query capture expired")
				assertQueryRejection(t, requestCapturePage(a, path+"&contains=other"), 400, "invalid cursor")
				if err := a.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestQueryCaptureExpiryDrainsAdmittedDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := captureFixtureSource()
		source.expiresAt = time.Now().Add(time.Minute)
		a := NewWithPageSource(source, nil, 4096, 2, DefaultQueryWorkBudget)
		path := "&cursor=" + issueCapture(t, a)
		writer := &blockingQueryWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
		var once sync.Once
		unblock := func() { once.Do(func() { close(writer.release) }) }
		defer unblock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			a.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, queryBounds+path, nil))
		}()
		waitForAdmission(t, writer.entered, "continuation delivery")
		time.Sleep(time.Minute)
		synctest.Wait()
		assertQueryRejection(t, requestCapturePage(a, path), 410, "query capture expired")
		if got := source.releases.Load(); got != 0 {
			t.Fatalf("expiry released %d captures during delivery", got)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close during delivery: %v", err)
		}
		unblock()
		waitForAdmission(t, done, "continuation drain")
		if err := a.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := source.releases.Load(); got != 1 {
			t.Fatalf("drain released %d captures", got)
		}
	})
}

func TestQueryCaptureReleaseOnSourceExpiry(t *testing.T) {
	source := captureFixtureSource()
	a := NewWithPageSource(source, nil, 4096, 1, DefaultQueryWorkBudget)
	path := "&cursor=" + issueCapture(t, a)
	source.expired.Store(true)
	for range 2 {
		assertQueryRejection(t, requestCapturePage(a, path), 410, "query capture expired")
	}
	if got := source.releases.Load(); got != 1 {
		t.Fatalf("released %d captures", got)
	}
	if err := a.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestQueryReleasesUnpublishedCaptures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params string
		budget uint64
		status int
	}{
		{"terminal", "&page=1", DefaultQueryWorkBudget, 200},
		{"uncertified", "&page=1&limit=1", 75, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := captureFixtureSource()
			a := NewWithPageSource(source, nil, 4096, 1, tc.budget)
			for range maxQueryCaptures + 1 {
				out := requestCapturePage(a, tc.params)
				if tc.status == 503 {
					assertQueryRejection(t, out, 503, "query work budget exhausted")
				} else if out.Code != 200 || out.Header().Get("Bog-Next-Cursor") != "" {
					t.Fatalf("status=%d headers=%v body=%s", out.Code, out.Header(), out.Body.String())
				}
			}
			if got := source.releases.Load(); got != maxQueryCaptures+1 {
				t.Fatalf("released %d unpublished captures", got)
			}
			if err := a.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueryCaptureCapacityAndCloseErrors(t *testing.T) {
	source := captureFixtureSource()
	source.closeErr = errors.New("test release failure")
	a := NewWithPageSource(source, nil, 4096, 1, DefaultQueryWorkBudget)
	path := "&cursor=" + issueCapture(t, a)
	for range maxQueryCaptures - 1 {
		issueCapture(t, a)
	}
	assertQueryRejection(t, requestCapturePage(a, "&page=1"), 503, "query capacity exhausted")
	if err := a.Close(t.Context()); !errors.Is(err, source.closeErr) {
		t.Fatalf("close error: %v", err)
	}
	assertQueryRejection(t, requestCapturePage(a, path), 410, "query capture expired")
	if got := source.releases.Load(); got != maxQueryCaptures {
		t.Fatalf("released %d retained captures", got)
	}
}
