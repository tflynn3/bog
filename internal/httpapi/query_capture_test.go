package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/store"
)

type observedPageSource struct {
	PageSource
	expires      time.Time
	pageError    func(PageRequest) error
	releaseError error
	releases     atomic.Int32
}

func (s *observedPageSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	if s.pageError != nil {
		if err := s.pageError(req); err != nil {
			return PageResult{}, err
		}
	}
	page, err := s.PageSource.Page(ctx, req, work)
	if req.Capture == "" {
		page.ExpiresAt = s.expires
	}
	return page, err
}

func (s *observedPageSource) Release(capture string) error {
	err := s.PageSource.Release(capture)
	s.releases.Add(1)
	return errors.Join(err, s.releaseError)
}

func newObservedPageSource(t *testing.T) *observedPageSource {
	t.Helper()
	s := new(store.Store)
	stamp := boundsForDay(t).Start.Add(time.Second)
	s.Publish([]store.Row{{TS: stamp, Line: "first"}, {TS: stamp, Line: "second"}, {TS: stamp, Line: "last"}})
	return &observedPageSource{PageSource: newLegacyPageSource(s)}
}

func TestQueryCaptureDeadlineDrainsAdmittedDelivery(t *testing.T) {
	for _, sourceLifetime := range []time.Duration{0, time.Minute, time.Hour} {
		t.Run(sourceLifetime.String(), func(t *testing.T) {
			now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			var clock atomic.Int64
			clock.Store(now.UnixNano())
			source := newObservedPageSource(t)
			if sourceLifetime != 0 {
				source.expires = now.Add(sourceLifetime)
			}
			a := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 2, DefaultQueryWorkBudget)
			a.captures.now = func() time.Time { return time.Unix(0, clock.Load()) }
			first := queryForTest(a, "&page=1&limit=1")
			token := first.Header().Get("Bog-Next-Cursor")
			c, err := a.decodeCursor(token, a.filterDigest(boundsForDay(t)))
			lifetime := queryCaptureLifetime
			if sourceLifetime > 0 {
				lifetime = min(lifetime, sourceLifetime)
			}
			deadline := now.Add(lifetime)
			if first.Code != http.StatusOK || err != nil || !c.deadline.Equal(deadline) {
				t.Fatalf("first: status=%d, expires=%v, want %v, err=%v", first.Code, c.deadline, deadline, err)
			}

			path := "&cursor=" + token
			for range 2 {
				terminal := queryForTest(a, path)
				if terminal.Code != http.StatusOK || terminal.Header().Get("Bog-Next-Cursor") != "" ||
					source.releases.Load() != 0 {
					t.Fatalf("terminal retry: status=%d, releases=%d", terminal.Code, source.releases.Load())
				}
			}

			writer := &blockingQueryWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
			done := make(chan struct{})
			var stop sync.Once
			release := func() {
				stop.Do(func() { close(writer.release) })
				waitForAdmission(t, done, "held delivery to finish")
			}
			defer release()
			go func() {
				defer close(done)
				a.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, queryBounds+path, nil))
			}()
			waitForAdmission(t, writer.entered, "terminal delivery to begin")
			clock.Store(deadline.UnixNano())
			assertQueryRejection(t, queryForTest(a, path), http.StatusGone, "query capture expired")
			if source.releases.Load() != 0 {
				t.Fatal("released a capture during an admitted delivery")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := a.Shutdown(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("shutdown while held: %v", err)
			}
			release()
			if err := a.Shutdown(t.Context()); err != nil || source.releases.Load() != 1 {
				t.Fatalf("shutdown: %v, releases=%d", err, source.releases.Load())
			}
		})
	}
}

func TestQueryReleasesUnpublishedCaptures(t *testing.T) {
	for _, tc := range []struct {
		name, params, message string
		status                int
		budget                uint64
		resumeError           error
	}{
		{"terminal", "&page=1&limit=3", "", http.StatusOK, DefaultQueryWorkBudget, nil},
		{"budget during proof", "&page=1&limit=1", "query work budget exhausted", http.StatusServiceUnavailable, 140, nil},
		{"expired during proof", "&page=1&limit=1", "query capture expired", http.StatusGone, DefaultQueryWorkBudget, ErrCaptureExpired},
		{"source fault during proof", "&page=1&limit=1", "query failed", http.StatusInternalServerError, DefaultQueryWorkBudget, errors.New("storage fault")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := newObservedPageSource(t)
			source.pageError = func(req PageRequest) error {
				if req.Capture != "" {
					return tc.resumeError
				}
				return nil
			}
			a := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, tc.budget)
			out := queryForTest(a, tc.params)
			if tc.status != http.StatusOK {
				assertQueryRejection(t, out, tc.status, tc.message)
			} else if out.Code != tc.status || out.Header().Get("Bog-Next-Cursor") != "" {
				t.Fatalf("terminal first page: %d %v", out.Code, out.Header())
			}
			if source.releases.Load() != 1 {
				t.Fatalf("releases=%d, want 1", source.releases.Load())
			}
			if err := a.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueryCaptureShutdownSurfacesReleaseError(t *testing.T) {
	source := newObservedPageSource(t)
	source.releaseError = errors.New("close pinned source")
	a := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	out := queryForTest(a, "&page=1&limit=1")
	if out.Code != http.StatusOK || out.Header().Get("Bog-Next-Cursor") == "" {
		t.Fatalf("initial page: %d %v", out.Code, out.Header())
	}
	if err := a.Shutdown(t.Context()); !errors.Is(err, source.releaseError) || source.releases.Load() != 1 {
		t.Fatalf("shutdown: err=%v releases=%d", err, source.releases.Load())
	}
}

func TestSourceCanRetirePublishedCapture(t *testing.T) {
	source := newObservedPageSource(t)
	var expired atomic.Bool
	var requests atomic.Int32
	source.pageError = func(req PageRequest) error {
		if req.Capture != "" {
			requests.Add(1)
			if expired.Load() {
				return ErrCaptureExpired
			}
		}
		return nil
	}
	a := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	defer func() {
		ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
		defer cancel()
		if err := a.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	first := queryForTest(a, "&page=1&limit=1")
	token := first.Header().Get("Bog-Next-Cursor")
	if first.Code != http.StatusOK || token == "" {
		t.Fatalf("initial page: %d %v", first.Code, first.Header())
	}

	expired.Store(true)
	path := "&cursor=" + token
	previous := requests.Load()
	for range 2 {
		assertQueryRejection(t, queryForTest(a, path), http.StatusGone, "query capture expired")
	}
	if requests.Load() != previous+1 || source.releases.Load() != 1 {
		t.Fatalf("requests=%d, previous=%d, releases=%d", requests.Load(), previous, source.releases.Load())
	}
}

func TestQueryCaptureAdmissionIsBounded(t *testing.T) {
	source := newObservedPageSource(t)
	a := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	for i := range maxQueryCaptures {
		out := queryForTest(a, "&page=1&limit=1")
		if out.Code != http.StatusOK || out.Header().Get("Bog-Next-Cursor") == "" {
			t.Fatalf("capture %d: %d %v", i, out.Code, out.Header())
		}
	}
	assertQueryRejection(t, queryForTest(a, "&page=1&limit=1"), http.StatusServiceUnavailable, "query capacity exhausted")
	if out := queryForTest(a, "&limit=1"); out.Code != http.StatusOK {
		t.Fatalf("ordinary query while captures are full: %d %q", out.Code, out.Body.String())
	}
	ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil || source.releases.Load() != maxQueryCaptures {
		t.Fatalf("shutdown: %v, releases=%d", err, source.releases.Load())
	}
}
