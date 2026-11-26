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

func TestLiveQueryWorkBudgetAndByteTruncation(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	row := store.Row{TS: stamp, Line: "same"}
	s := new(store.Store)
	s.Publish([]store.Row{row, row, row, row, row})
	const line = "{\"ts\":\"2025-01-01T00:00:01Z\",\"line\":\"same\"}\n"
	makeAPI := func(budget uint64) *API { return newAPI(s, nil, 2*len(line), budget) }
	query := func(h http.Handler, params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest("GET", queryBounds+params, nil))
		return out
	}
	assertPage := func(a *API, out *httptest.ResponseRecorder, lines, ordinal int) string {
		t.Helper()
		token := out.Header().Get("Bog-Next-Cursor")
		count, pos, err := decodeMemoryCursor(a, token)
		if out.Code != 200 || out.Body.String() != strings.Repeat(line, lines) || err != nil || count != 5 || pos.Ordinal != ordinal {
			t.Fatalf("page: status=%d headers=%v body=%q capture count=%d position=%+v err=%v", out.Code, out.Header(), out.Body.String(), count, pos, err)
		}
		return token
	}

	// Four setup units, then five per candidate. The one-row anchor scan
	// needs two candidates. Extending it costs 32 more including the seek
	// and anchor validation. Recovering the truncated cursor costs 22 more.
	const firstPass, extension, rescan = 14, 32, 22
	const encoding = 2 * uint64(len("same"))
	const delivery = uint64((2*len(line) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit)
	for _, tc := range []struct {
		name   string
		budget uint64
	}{
		{"anchor lookahead", firstPass - 1},
		{"first page extension", firstPass},
		{"encoding", firstPass + extension + encoding - 1},
		{"response delivery", firstPass + extension + encoding + delivery - 1},
		{"byte truncation rescan", firstPass + extension + encoding + delivery + rescan - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertQueryRejection(t, query(makeAPI(tc.budget).handler(1), "&page=1&limit=5"), 503, "query work budget exhausted")
		})
	}
	a := makeAPI(DefaultQueryWorkBudget)
	h := a.handler(1)
	assertPage(a, query(h, "&page=1&limit=5"), 2, 1)
	token := assertPage(a, query(h, "&page=1&limit=1"), 1, 0)

	auth := 1 + uint64(len(token))
	path := "&cursor=" + token
	// Vary the work budget between synchronous requests to the same issuer.
	a.queryWorkBudget = auth - 1
	assertQueryRejection(t, query(h, path+"&limit=2"), 503, "query work budget exhausted")
	a.queryWorkBudget = DefaultQueryWorkBudget
	assertPage(a, query(h, path+"&limit=4"), 2, 2)
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
		{"response delivery", 11},
		{"continuation token validation", 13},
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
