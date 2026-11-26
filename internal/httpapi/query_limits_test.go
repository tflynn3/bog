package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

type blockedWriter struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
}

func (w *blockedWriter) Header() http.Header { return w.header }
func (w *blockedWriter) WriteHeader(int)     {}
func (w *blockedWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}
func TestQueryAdmissionAndIngestIndependence(t *testing.T) {
	s := &store.Store{}
	log, err := wal.Open(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	s.Publish([]store.Row{{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "hello"}})
	config := DefaultQueryConfig()
	config.Concurrency = 1
	h := NewWithQueryConfig(s, log, config)
	path := "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z"
	bw := &blockedWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { h.ServeHTTP(bw, httptest.NewRequest("GET", path, nil)); close(done) }()
	<-bw.entered
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", path, nil))
	if out.Code != 503 {
		t.Fatalf("query %d", out.Code)
	}
	out = httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", "/healthz", nil))
	if out.Code != 200 {
		t.Fatalf("health %d", out.Code)
	}
	out = httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("POST", "/ingest", strings.NewReader(`{"ts":"2025-01-01T00:00:00Z","line":"ingest"}`)))
	if out.Code != 204 {
		t.Fatalf("ingest %d", out.Code)
	}
	close(bw.release)
	<-done
}
func TestQueryBudgetResponse(t *testing.T) {
	s := &store.Store{}
	s.Publish([]store.Row{{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "hello"}})
	config := DefaultQueryConfig()
	config.Limits.Work = 1
	h := NewWithQueryConfig(s, nil, config)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&page=1", nil))
	if out.Code != 422 || out.Header().Get("Bog-Next-Cursor") != "" || out.Body.String() != "query work budget exhausted\n" {
		t.Fatalf("response %d %q", out.Code, out.Body.String())
	}
}
