package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bog/internal/store"
	"bog/internal/wal"
)

func TestQueryAdmissionSaturationAndCancellation(t *testing.T) {
	s := &store.Store{}
	log, err := wal.Open(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	a := &API{store: s, wal: log, slots: make(chan struct{}, 16)}
	entered := make(chan context.Context, 2)
	release := make(chan struct{})
	query := queryAdmission(2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.Context()
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
	defer cancel()
	done := make(chan struct{}, 2)
	start := func(ctx context.Context) {
		go func() {
			defer func() { done <- struct{}{} }()
			mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/query", nil).WithContext(ctx))
		}()
	}
	start(ctx)
	if got := <-entered; got != ctx {
		t.Fatal("handler did not receive request context")
	}
	start(context.Background())
	<-entered

	out := httptest.NewRecorder()
	mux.ServeHTTP(out, httptest.NewRequest("GET", "/query?page=1", nil))
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
		out = httptest.NewRecorder()
		mux.ServeHTTP(out, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if out.Code != tc.status {
			t.Fatalf("%s: got %d, want %d: %s", tc.path, out.Code, tc.status, out.Body.String())
		}
	}
	cancel()
	<-done
	start(context.Background())
	<-entered
	close(release)
	<-done
	<-done
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
