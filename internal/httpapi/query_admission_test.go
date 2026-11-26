package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

const admissionWaitTimeout = 5 * time.Second

func waitForAdmission[T any](t *testing.T, ch <-chan T, step string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(admissionWaitTimeout):
		t.Fatalf("timed out waiting for %s", step)
		var zero T
		return zero
	}
}

func TestQueryAdmissionSaturationAndCancellation(t *testing.T) {
	s := &store.Store{}
	log, err := wal.Open(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	a := &API{store: s, wal: log, slots: make(chan struct{}, 16)}
	entered := make(chan context.Context, 2)
	release := make(chan struct{})
	query := queryAdmission(2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- r.Context():
		case <-release:
			return
		case <-r.Context().Done():
			return
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	mux := http.NewServeMux()
	mux.Handle("/query", query)
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/ingest", a.ingest)

	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	var releaseOnce sync.Once
	releaseWorkers := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		cancel()
		releaseWorkers()
		done := make(chan struct{})
		go func() {
			workers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(admissionWaitTimeout):
			t.Error("timed out waiting for admission workers to exit during cleanup")
		}
	})
	start := func(ctx context.Context, method, path string, body *strings.Reader) <-chan *httptest.ResponseRecorder {
		done := make(chan *httptest.ResponseRecorder, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			out := httptest.NewRecorder()
			mux.ServeHTTP(out, httptest.NewRequest(method, path, body).WithContext(ctx))
			done <- out
		}()
		return done
	}
	first := start(ctx, "GET", "/query", strings.NewReader(""))
	if got := waitForAdmission(t, entered, "first query to enter"); got != ctx {
		t.Fatal("handler did not receive request context")
	}
	second := start(context.Background(), "GET", "/query", strings.NewReader(""))
	waitForAdmission(t, entered, "second query to enter")

	overload := start(context.Background(), "GET", "/query?page=1", strings.NewReader(""))
	out := waitForAdmission(t, overload, "overload response")
	if out.Code != http.StatusServiceUnavailable || out.Header().Get("Bog-Next-Cursor") != "" || strings.Contains(out.Header().Get("Content-Type"), "ndjson") {
		t.Fatalf("overload response: %d, headers %v, body %q", out.Code, out.Header(), out.Body.String())
	}
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/healthz", "", http.StatusOK},
		{"POST", "/ingest", `{"ts":"2025-01-01T00:00:00Z","line":"ok"}`, http.StatusNoContent},
	} {
		request := start(context.Background(), tc.method, tc.path, strings.NewReader(tc.body))
		out = waitForAdmission(t, request, tc.path+" response")
		if out.Code != tc.status {
			t.Fatalf("%s: got %d, want %d: %s", tc.path, out.Code, tc.status, out.Body.String())
		}
	}
	cancel()
	waitForAdmission(t, first, "cancelled query to exit")
	third := start(context.Background(), "GET", "/query", strings.NewReader(""))
	waitForAdmission(t, entered, "replacement query to enter")
	releaseWorkers()
	waitForAdmission(t, second, "second query to exit")
	waitForAdmission(t, third, "replacement query to exit")
}

func TestQueryAdmissionCancelledAndInvalidLimit(t *testing.T) {
	called := false
	h := queryAdmission(1, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", "/query", nil).WithContext(ctx))
	if called || out.Code != http.StatusRequestTimeout {
		t.Fatalf("cancelled request: called=%v, status=%d", called, out.Code)
	}
	out = httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", "/query", nil))
	if !called {
		t.Fatal("cancelled request consumed an admission slot")
	}
	for _, limit := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("limit %d did not panic", limit)
				}
			}()
			queryAdmission(limit, h)
		}()
	}
}
