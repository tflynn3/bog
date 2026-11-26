package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/store"
)

const admissionWaitTimeout = 5 * time.Second

type immediateAdmissionAppender struct{}

func (immediateAdmissionAppender) Append([]byte, []store.Row) error { return nil }

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

const queryBounds = "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z"

type blockingQueryWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
}

func (w *blockingQueryWriter) Write([]byte) (int, error) {
	close(w.entered)
	<-w.release
	return 0, io.ErrClosedPipe
}

func assertQueryRejection(t *testing.T, out *httptest.ResponseRecorder, status int, message string) {
	t.Helper()
	_, hasCursor := out.Header()["Bog-Next-Cursor"]
	if out.Code != status || out.Body.String() != message+"\n" ||
		hasCursor || !strings.HasPrefix(out.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("rejection: status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
	}
}

func TestLiveQueryAdmissionThroughWriteFailure(t *testing.T) {
	s := new(store.Store)
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	s.Publish([]store.Row{{TS: stamp, Line: "hit"}, {TS: stamp, Line: "hit"}})
	mux := NewWithQueryLimits(s, immediateAdmissionAppender{}, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	writer := &blockingQueryWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writer.release) }) }
	done := make(chan struct{})
	go func() {
		defer close(done)
		mux.ServeHTTP(writer, httptest.NewRequest("GET", queryBounds+"&page=1&limit=1", nil).WithContext(ctx))
	}()
	t.Cleanup(func() {
		cancel()
		release()
		waitForAdmission(t, done, "query writer cleanup")
	})
	waitForAdmission(t, writer.entered, "query to reach response write")

	request := func(ctx context.Context, method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		ctx, stop := context.WithTimeout(ctx, admissionWaitTimeout)
		defer stop()
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			out := httptest.NewRecorder()
			mux.ServeHTTP(out, httptest.NewRequest(method, path, strings.NewReader(body)).WithContext(ctx))
			done <- out
		}()
		return waitForAdmission(t, done, path+" response")
	}
	background := context.Background()
	assertQueryRejection(t, request(background, "GET", queryBounds, ""), 503, "query capacity exhausted")
	cancel()
	assertQueryRejection(t, request(ctx, "GET", queryBounds, ""), 408, "request cancelled")
	assertQueryRejection(t, request(background, "GET", queryBounds, ""), 503, "query capacity exhausted")

	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/healthz", "", http.StatusOK},
		{"POST", "/ingest", `{"ts":"2025-01-01T00:00:00Z","line":"ok"}`, http.StatusNoContent},
		{"POST", "/query", "", http.StatusMethodNotAllowed},
	} {
		out := request(background, tc.method, tc.path, tc.body)
		if out.Code != tc.status {
			t.Fatalf("%s %s: got %d, want %d: %s", tc.method, tc.path, out.Code, tc.status, out.Body.String())
		}
	}
	release()
	waitForAdmission(t, done, "query to return from failed write")
	out := request(background, "GET", queryBounds+"&page=1&limit=1", "")
	if out.Code != http.StatusOK || out.Header().Get("Bog-Next-Cursor") == "" || !strings.Contains(out.Body.String(), "hit") {
		t.Fatalf("replacement query: status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
	}
}

type queryScanContext struct {
	context.Context
	cancel   context.CancelFunc
	checks   int
	cancelAt int
}

func (c *queryScanContext) Err() error {
	c.checks++
	if c.checks == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}

func TestLiveQueryCancellationBeforeCommit(t *testing.T) {
	s := new(store.Store)
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	row := store.Row{TS: stamp, Line: "same"}
	s.Publish([]store.Row{row, row})
	h := NewWithQueryLimits(s, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	path := queryBounds + "&page=1&limit=1"
	for _, tc := range []struct {
		name    string
		checkAt int
	}{
		{"lookahead", 7},
		{"line encoding", 9},
		{"delivery after cursor prepared", 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			out := httptest.NewRecorder()
			req := httptest.NewRequest("GET", path, nil).WithContext(&queryScanContext{Context: ctx, cancel: cancel, cancelAt: tc.checkAt})
			h.ServeHTTP(out, req)
			assertQueryRejection(t, out, http.StatusRequestTimeout, "request cancelled")

			out = httptest.NewRecorder()
			h.ServeHTTP(out, httptest.NewRequest("GET", path, nil))
			if out.Code != http.StatusOK || out.Header().Get("Bog-Next-Cursor") == "" {
				t.Fatalf("retry: status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
			}
		})
	}
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
