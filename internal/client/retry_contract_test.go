package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const contractStamp = "2025-01-01T00:00:00.123456789Z"

func contractRow(line string) string {
	return fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", contractStamp, line)
}

func writeContractPage(t *testing.T, w http.ResponseWriter, body, cursor string) {
	t.Helper()
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if cursor != "" {
		w.Header().Set("Bog-Next-Cursor", cursor)
	}
	if n, err := io.WriteString(w, body); err != nil || n != len(body) {
		t.Errorf("write page: %d of %d bytes, %v", n, len(body), err)
	}
}

func TestWalkStopsAtRepeatedCursor(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := r.URL.Query().Get("cursor")
		n := calls.Add(1)
		var want, line, next string
		switch n {
		case 1:
			line, next = "first", fixtureCursor
		case 2:
			want, line, next = fixtureCursor, "rejected", fixtureCursor
		case 3:
			want, line, next = fixtureCursor, "resumed", fixtureNext
		case 4:
			want, line = fixtureNext, "last"
		default:
			t.Errorf("unexpected request %d: %s", n, r.RequestURI)
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		if request != want {
			t.Errorf("request %d used cursor %q, want %q", n, request, want)
		}
		writeContractPage(t, w, contractRow(line), next)
	}))
	defer server.Close()
	c, err := New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.http.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var received []string
	consume := func(p Page) error {
		for _, row := range p.Rows {
			received = append(received, row.Line)
		}
		return nil
	}
	cursor, err := c.Walk(ctx, Query{}, consume)
	var protocol *HTTPError
	if cursor != fixtureCursor || !errors.As(err, &protocol) || protocol.StatusCode != http.StatusOK ||
		protocol.Retryable() || protocol.CursorRejected || !strings.Contains(err.Error(), "does not advance") {
		t.Fatalf("walk returned cursor %q and error %v", cursor, err)
	}
	if calls.Load() != 2 || !slices.Equal(received, []string{"first"}) {
		t.Fatalf("walk made %d requests and delivered %q before retry", calls.Load(), received)
	}
	end, err := c.WalkFrom(ctx, Query{}, cursor, consume)
	if err != nil || end != "" || calls.Load() != 4 || !slices.Equal(received, []string{"first", "resumed", "last"}) {
		t.Fatalf("resumed walk returned cursor %q and error %v after %d requests and rows %q", end, err, calls.Load(), received)
	}
}

func TestInterruptedHTTP10Continuation(t *testing.T) {
	first := contractRow("first")
	prefix := contractRow("tie")
	complete := prefix + contractRow("tie") + contractRow(strings.Repeat("x", 64<<10))
	last := contractRow("last")
	type requests struct {
		first, interrupted, retry, terminal string
	}
	var seen requests
	// The client and the server synchronize through these channels so the
	// test controls the interruption even on a fast loopback connection.
	stalled, interrupt := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			seen.first = r.RequestURI
			writeContractPage(t, w, first, fixtureCursor)
		case 2:
			seen.interrupted = r.RequestURI
			conn, rw, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer func() {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Error(err)
				return
			}
			// Keep the unsent tail behind a gate, modelling an interrupted
			// write blocked after the first whole record. A close-delimited
			// response at this boundary would otherwise look complete.
			if _, err := fmt.Fprintf(rw, "HTTP/1.0 200 OK\r\nContent-Type: application/x-ndjson\r\nContent-Length: %d\r\nBog-Next-Cursor: %s\r\n\r\n%s", len(complete), fixtureNext, prefix); err != nil {
				t.Error(err)
				return
			}
			if err := rw.Flush(); err != nil {
				t.Error(err)
				return
			}
			close(stalled)
			select {
			case <-interrupt:
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting to interrupt the continuation")
			}
		case 3:
			seen.retry = r.RequestURI
			writeContractPage(t, w, complete, fixtureNext)
		case 4:
			seen.terminal = r.RequestURI
			writeContractPage(t, w, last, "")
		default:
			t.Error("requested another page after the terminal response")
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	c, err := New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.http.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start, err := time.Parse(time.RFC3339Nano, contractStamp)
	if err != nil {
		t.Fatal(err)
	}
	q := Query{Start: start, End: start.Add(time.Second), Contains: "café + /", Limit: 3}
	type walkResult struct {
		cursor string
		err    error
	}
	result := make(chan walkResult, 1)
	pages := make(chan Page, 4)
	go func() {
		cursor, err := c.Walk(ctx, q, func(p Page) error {
			pages <- p
			return nil
		})
		result <- walkResult{cursor, err}
	}()
	select {
	case <-stalled:
		close(interrupt)
	case <-ctx.Done():
		close(interrupt)
		t.Fatal("continuation never wrote its complete first record")
	}
	var failed walkResult
	select {
	case failed = <-result:
	case <-ctx.Done():
		t.Fatal("walk did not return after the response was interrupted")
	}
	var status *HTTPError
	if failed.cursor != fixtureCursor || !errors.Is(failed.err, io.ErrUnexpectedEOF) ||
		!errors.As(failed.err, &status) || status.StatusCode != http.StatusOK || status.Retryable() {
		t.Fatalf("interrupted walk returned cursor %q and error %v", failed.cursor, failed.err)
	}
	if len(pages) != 1 {
		t.Fatalf("consumer received %d pages before retry, want 1", len(pages))
	}
	accepted := <-pages
	if accepted.NextCursor != fixtureCursor || len(accepted.Rows) != 1 || accepted.Rows[0].Line != "first" {
		t.Fatalf("first accepted page: %+v", accepted)
	}
	var received []string
	cursor, err := c.WalkFrom(ctx, q, failed.cursor, func(p Page) error {
		for _, row := range p.Rows {
			if !row.TS.Equal(start) {
				t.Errorf("timestamp %s, want %s", row.TS, start)
			}
			received = append(received, row.Line)
		}
		return nil
	})
	if err != nil || cursor != "" || !slices.Equal(received, []string{"tie", "tie", strings.Repeat("x", 64<<10), "last"}) {
		t.Fatalf("retry returned cursor %q, %d ordered rows and error %v", cursor, len(received), err)
	}
	// Close joins the handlers before inspecting the request data.
	server.Close()
	if seen.interrupted != seen.retry || seen.first == "" || seen.terminal == "" || calls.Load() != 4 {
		t.Fatalf("request targets after %d requests: %+v", calls.Load(), seen)
	}
}

func TestEscapedBasePaths(t *testing.T) {
	for _, tc := range []struct{ base, prefix string }{
		{"", ""},
		{"/tenant///", "/tenant"},
		{"/tenant%2F/", "/tenant%2F"},
		{"/a%2fb/%2e%2E/%252F//", "/a%2fb/%2e%2E/%252F"},
		{"/caf%C3%A9/%7e", "/caf%C3%A9/%7e"},
	} {
		t.Run(tc.base, func(t *testing.T) {
			const batch = "sample batch"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				suffix := "/query"
				if r.Method == http.MethodPost {
					suffix = "/ingest"
				}
				target, _, _ := strings.Cut(r.RequestURI, "?")
				if target != tc.prefix+suffix {
					t.Errorf("request path %q, want %q", target, tc.prefix+suffix)
				}
				switch r.Method {
				case http.MethodGet:
					if r.URL.Query().Get("page") != "1" {
						t.Errorf("missing page parameter: %s", r.RequestURI)
					}
					writeContractPage(t, w, "", "")
				case http.MethodPost:
					body, err := io.ReadAll(r.Body)
					if err != nil || string(body) != batch {
						t.Errorf("ingest body %q: %v", body, err)
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected method %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()
			c, err := New(server.URL+tc.base, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.http.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := c.FetchPage(ctx, Query{}, ""); err != nil {
				t.Fatal(err)
			}
			if got, err := c.Ingest(ctx, []byte(batch)); err != nil || got != IngestCommitted {
				t.Fatalf("ingest outcome %v: %v", got, err)
			}
		})
	}
}

func TestQueryResponseValidation(t *testing.T) {
	row := contractRow("valid")
	for _, tc := range []struct {
		name, body string
		headers    http.Header
		limit      int64
		chunked    bool
		wantError  string
	}{
		{name: "empty terminal"},
		{name: "exact size cap", body: row, limit: int64(len(row))},
		{name: "identity encoding", body: row, headers: http.Header{"Content-Encoding": {"Identity"}}},
		{name: "length exceeds cap", body: row, limit: int64(len(row) - 1), wantError: "size limit"},
		{name: "chunked exceeds cap", body: row, limit: int64(len(row) - 1), chunked: true, wantError: "size limit"},
		{name: "invalid cap", body: row, limit: -1, wantError: "invalid response size limit"},
		{name: "duplicate encoding", body: row, headers: http.Header{"Content-Encoding": {"identity", "gzip"}}, wantError: "encoding"},
		{name: "compressed response", body: row, headers: http.Header{"Content-Encoding": {"gzip"}}, wantError: "encoding"},
		{name: "trailing cursor", body: row, headers: http.Header{"Trailer": {"Bog-Next-Cursor"}}, chunked: true, wantError: "cursor trailer"},
		{name: "duplicate cursor", body: row, headers: http.Header{"Bog-Next-Cursor": {fixtureCursor, fixtureNext}}, wantError: "cursor header"},
		{name: "empty cursor", body: row, headers: http.Header{"Bog-Next-Cursor": {""}}, wantError: "cursor header"},
		{name: "repeated request cursor", body: row, headers: http.Header{"Bog-Next-Cursor": {fixtureCursor}}, wantError: "does not advance"},
		{name: "repeated request cursor on empty page", headers: http.Header{"Bog-Next-Cursor": {fixtureCursor}}, wantError: "does not advance"},
		{name: "invalid UTF-8 after row", body: row + "{\"ts\":\"\xff\",\"line\":\"\"}\n", wantError: "UTF-8"},
		{name: "blank record after row", body: row + "\n", wantError: "record framing"},
		{name: "invalid timestamp after row", body: row + "{\"ts\":\"today\",\"line\":\"\"}\n", wantError: "timestamp"},
		{name: "missing field after row", body: row + "{\"ts\":\"" + contractStamp + "\"}\n", wantError: "missing query record field"},
		{name: "duplicate field after row", body: row + "{\"ts\":\"" + contractStamp + "\",\"line\":\"a\",\"line\":\"b\"}\n", wantError: "decode query record"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for name, values := range tc.headers {
					for _, value := range values {
						w.Header().Add(name, value)
					}
				}
				if !tc.chunked {
					writeContractPage(t, w, tc.body, "")
					return
				}
				if err := http.NewResponseController(w).Flush(); err != nil {
					t.Error(err)
					return
				}
				if n, err := io.WriteString(w, tc.body); err != nil || n != len(tc.body) {
					t.Errorf("write chunked body: %d of %d bytes, %v", n, len(tc.body), err)
				}
				if tc.headers.Get("Trailer") == "Bog-Next-Cursor" {
					w.Header().Set("Bog-Next-Cursor", fixtureNext)
				}
			}))
			defer s.Close()
			c, err := New(s.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.http.CloseIdleConnections()
			c.MaxPageBytes = tc.limit
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			page, err := c.FetchPage(ctx, Query{}, fixtureCursor)
			if tc.wantError != "" {
				var status *HTTPError
				if !errors.As(err, &status) || status.StatusCode != http.StatusOK || status.Retryable() ||
					!strings.Contains(err.Error(), tc.wantError) || len(page.Rows) != 0 || page.NextCursor != "" {
					t.Fatalf("page %+v, error %v, want %q", page, err, tc.wantError)
				}
				return
			}
			wantRows := 0
			if tc.body != "" {
				wantRows = 1
			}
			if err != nil || len(page.Rows) != wantRows || page.NextCursor != "" {
				t.Fatalf("terminal page %+v, error %v, want %d rows", page, err, wantRows)
			}
		})
	}
}

func TestContinuationRejectionsPreserveCursor(t *testing.T) {
	for _, tc := range []struct {
		body           string
		status         int
		retryable      bool
		cursorRejected bool
	}{
		{"query capacity exhausted", http.StatusServiceUnavailable, true, false},
		{"query work budget exhausted", http.StatusServiceUnavailable, true, false},
		{"invalid cursor", http.StatusBadRequest, false, true},
		{"query record exceeds response byte limit", http.StatusUnprocessableEntity, false, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("cursor") != fixtureCursor {
					t.Errorf("wrong continuation target %q", r.RequestURI)
				}
				http.Error(w, tc.body, tc.status)
			}))
			defer server.Close()
			c, err := New(server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.http.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cursor, err := c.WalkFrom(ctx, Query{}, fixtureCursor, func(Page) error {
				t.Error("consumer received a rejected page")
				return nil
			})
			var rejection *HTTPError
			if cursor != fixtureCursor || !errors.As(err, &rejection) || rejection.StatusCode != tc.status ||
				rejection.Body != tc.body+"\n" || rejection.Retryable() != tc.retryable || rejection.CursorRejected != tc.cursorRejected {
				t.Fatalf("cursor %q, rejection %+v, error %v", cursor, rejection, err)
			}
		})
	}
}
