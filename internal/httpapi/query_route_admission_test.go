package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

type blockedQueryWriter struct {
	*httptest.ResponseRecorder
	ctx     context.Context
	entered chan struct{}
}

func (w *blockedQueryWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.ctx.Done()
	return 0, w.ctx.Err()
}

func TestQueryRouteAdmission(t *testing.T) {
	s := &store.Store{}
	log, err := wal.Open(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	h := NewWithQueryLimits(s, log, DefaultQueryMaxResponseBytes, 1)
	serve := func(method, target, body string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(method, target, strings.NewReader(body)))
		return out
	}
	batch := `{"ts":"2025-01-01T00:00:01Z","line":"one"}` + "\n" + `{"ts":"2025-01-01T00:00:02Z","line":"two"}` + "\n" + `{"ts":"2025-01-01T00:00:03Z","line":"three"}`
	if out := serve("POST", "/ingest", batch); out.Code != http.StatusNoContent {
		t.Fatal(out.Code, out.Body.String())
	}
	bounds := "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z"
	first := serve("GET", bounds+"&page=1&limit=1", "")
	cursor := first.Header().Get("Bog-Next-Cursor")
	if first.Code != http.StatusOK || cursor == "" {
		t.Fatal(first.Code, first.Body.String())
	}
	continuation := bounds + "&limit=1&cursor=" + url.QueryEscape(cursor)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &blockedQueryWriter{ResponseRecorder: httptest.NewRecorder(), ctx: ctx, entered: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(writer, httptest.NewRequest("GET", continuation, nil).WithContext(ctx))
	}()
	waitQuerySignal(t, writer.entered)
	overload := serve("GET", continuation, "")
	if overload.Code != http.StatusServiceUnavailable || overload.Body.String() != "query capacity exhausted\n" || overload.Header().Get("Bog-Next-Cursor") != "" || strings.Contains(overload.Header().Get("Content-Type"), "ndjson") {
		t.Fatalf("overload: %d %v %q", overload.Code, overload.Header(), overload.Body.String())
	}
	cancelled, cancelAdmission := context.WithCancel(context.Background())
	cancelAdmission()
	rejected := httptest.NewRecorder()
	h.ServeHTTP(rejected, httptest.NewRequest("GET", continuation, nil).WithContext(cancelled))
	if rejected.Code != http.StatusRequestTimeout || rejected.Body.String() != "request cancelled\n" || rejected.Header().Get("Bog-Next-Cursor") != "" || strings.Contains(rejected.Header().Get("Content-Type"), "ndjson") {
		t.Fatalf("cancelled admission: %d %v %q", rejected.Code, rejected.Header(), rejected.Body.String())
	}
	if out := serve("POST", "/query", ""); out.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method: %d", out.Code)
	}
	if out := serve("GET", "/healthz", ""); out.Code != http.StatusOK {
		t.Fatalf("health: %d", out.Code)
	}
	if out := serve("POST", "/ingest", `{"ts":"2025-01-01T00:00:04Z","line":"four"}`); out.Code != http.StatusNoContent {
		t.Fatalf("ingest: %d", out.Code)
	}
	cancel()
	waitQuerySignal(t, done)
	retry := serve("GET", continuation, "")
	if retry.Code != http.StatusOK || !strings.Contains(retry.Body.String(), `"two"`) || retry.Header().Get("Bog-Next-Cursor") == "" {
		t.Fatalf("retry: %d %v %q", retry.Code, retry.Header(), retry.Body.String())
	}
	if out := serve("GET", "/query?invalid=1", ""); out.Code != http.StatusBadRequest {
		t.Fatalf("invalid: %d", out.Code)
	}
	if out := serve("GET", continuation, ""); out.Code != http.StatusOK {
		t.Fatalf("after error: %d", out.Code)
	}
}

func TestQueryRouteInvalidConcurrency(t *testing.T) {
	for _, n := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("concurrency %d did not panic", n)
				}
			}()
			NewWithQueryLimits(&store.Store{}, nil, DefaultQueryMaxResponseBytes, n)
		}()
	}
}

func waitQuerySignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for query handler")
	}
}
