package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/client"
)

func wireResponse(status int, fields, body string) string {
	return fmt.Sprintf("HTTP/1.1 %d %s\r\nConnection: close\r\n%s\r\n%s",
		status, http.StatusText(status), fields, body)
}

func lengthResponse(status int, headers, body string) string {
	return wireResponse(status, fmt.Sprintf("Content-Length: %d\r\n%s", len(body), headers), body)
}

func chunk(body string) string {
	return fmt.Sprintf("%x\r\n%s\r\n", len(body), body)
}

// Write HTTP/1.1 bytes directly so net/http cannot infer a Content-Length,
// finish the chunk stream, or repair any of the deliberately malformed replies.
func writeWireResponse(t *testing.T, w http.ResponseWriter, wire string) {
	t.Helper()
	conn, out, err := http.NewResponseController(w).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := conn.SetWriteDeadline(time.Now().Add(queryDeadline)); err != nil {
		t.Error(err)
		return
	}
	if _, err := out.WriteString(wire); err != nil {
		t.Error(err)
		return
	}
	if err := out.Flush(); err != nil {
		t.Error(err)
	}
}

func wireServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	var active sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		active.Add(1)
		defer active.Done()
		handler(w, req)
	}))
	t.Cleanup(func() {
		server.Close()
		// The server stops tracking connections once a handler hijacks them.
		active.Wait()
	})
	return server
}

func TestFixedBudgetWireVerdict(t *testing.T) {
	t.Parallel()
	const ndjson = "Content-Type: application/x-ndjson\r\n"
	const text = "Content-Type: text/plain\r\n"
	const chunked = "Transfer-Encoding: chunked\r\n"
	const invalidCursor = "invalid cursor\n"
	token1, token2 := strings.Repeat("A", 44), strings.Repeat("B", 44)
	header1 := nextCursor + ": " + token1 + "\r\n"
	header2 := nextCursor + ": " + token2 + "\r\n"
	encodeRow := func(line string) string {
		return fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", fixtureStart, line)
	}
	first, middle, last := encodeRow("c"), encodeRow("a"), encodeRow("b")
	lengthPages := []string{
		lengthResponse(200, ndjson+header1, first),
		lengthResponse(200, ndjson+header2, middle),
		lengthResponse(200, ndjson, last),
	}
	chunkedPages := []string{
		wireResponse(200, ndjson+chunked+header1, chunk(first)+"0\r\n\r\n"),
		wireResponse(200, ndjson+chunked+header2, chunk(middle[:5])+chunk(middle[5:])+"0\r\n\r\n"),
		wireResponse(200, ndjson+chunked, chunk(last)+"0\r\n\r\n"),
	}
	for _, tc := range []struct {
		name           string
		chunked        bool
		failPage       int
		wire           string
		status         int
		cursorRejected bool
		problem        string
	}{
		{name: "length-delimited"},
		{name: "chunked", chunked: true},
		{name: "first page close-delimited", failPage: 1, wire: wireResponse(200, ndjson+header1, first), status: 200, problem: "unverifiable framing"},
		{name: "continuation close-delimited", failPage: 2, wire: wireResponse(200, ndjson+header2, middle), status: 200, problem: "unverifiable framing"},
		{name: "truncated length", failPage: 2, wire: wireResponse(200, ndjson+header2+
			fmt.Sprintf("Content-Length: %d\r\n", len(middle)+1), middle), status: 200, problem: "unexpected EOF"},
		{name: "unterminated chunk stream", failPage: 3, wire: wireResponse(200, ndjson+chunked, chunk(last)), status: 200, problem: "unexpected EOF"},
		{name: "unterminated record", failPage: 3, wire: lengthResponse(200, ndjson, strings.TrimSuffix(last, "\n")), status: 200, problem: "invalid query record framing"},
		{name: "malformed cursor", failPage: 1, wire: lengthResponse(200, ndjson+nextCursor+": bad+/=\r\n", first), status: 200, problem: "invalid next cursor header"},
		{name: "duplicate cursor", failPage: 2, wire: lengthResponse(200, ndjson+header2+header1, middle), status: 200, problem: "invalid next cursor header"},
		{name: "cursor trailer", failPage: 3, wire: wireResponse(200, ndjson+chunked+"Trailer: "+nextCursor+"\r\n",
			chunk(last)+"0\r\n"+header1+"\r\n"), status: 200, problem: "unexpected next cursor trailer"},
		{name: "cursor after exhaustion", failPage: 3, wire: lengthResponse(200, ndjson+header1, last), problem: "issued a cursor after the final row"},
		{name: "framed invalid cursor", failPage: 2, wire: lengthResponse(400, text, invalidCursor), status: 400, cursorRejected: true, problem: "invalid cursor"},
		{name: "invalid cursor without framing", failPage: 2, wire: wireResponse(400, text, invalidCursor), status: 400, problem: "invalid cursor"},
		{name: "invalid cursor on first page", failPage: 1, wire: lengthResponse(400, text, invalidCursor), status: 400, problem: "invalid cursor"},
		{name: "unrelated bad request", failPage: 2, wire: lengthResponse(400, text, "invalid query\n"), status: 400, problem: "invalid query"},
		{name: "rejection with cursor trailer", failPage: 1, wire: wireResponse(503, text+chunked+"Trailer: "+nextCursor+"\r\n",
			chunk(budgetRejectionBody)+"0\r\n"+header1+"\r\n"), status: 503, problem: "unclean HTTP 503 budget rejection"},
		{name: "rejection with truncated body", failPage: 1, wire: wireResponse(503, text+
			fmt.Sprintf("Content-Length: %d\r\n", len(budgetRejectionBody)+1), budgetRejectionBody), status: 503, problem: "unclean HTTP 503 budget rejection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pages := lengthPages
			if tc.chunked {
				pages = chunkedPages
			}
			var calls atomic.Int32
			server := wireServer(t, func(w http.ResponseWriter, req *http.Request) {
				number := int(calls.Add(1)) - 1
				if number == 0 && req.Method == http.MethodPost && req.URL.Path == "/ingest" {
					if _, err := io.Copy(io.Discard, req.Body); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if number < 1 || number > len(pages) || req.Method != http.MethodGet || req.URL.Path != "/query" {
					t.Errorf("unexpected request %d: %s %s", number, req.Method, req.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				wire := pages[number-1]
				if number == tc.failPage {
					wire = tc.wire
				}
				writeWireResponse(t, w, wire)
			})
			launch := func(_ context.Context, _ int, check func(string) error) error { return check(server.URL) }
			stats, err := findBudgetWalk(t.Context(), server.Client(), 1, launch)
			cases := make(map[string]outcome)
			verdictErr := record(cases, "work_budget", stats, err)
			verdict := cases["work_budget"]
			if tc.failPage == 0 {
				if verdictErr != nil || verdict.Status != "pass" || stats != (budgetWalkStats{1, 0, 3, 3}) || calls.Load() != 4 {
					t.Fatalf("verdict=%+v, stats=%+v, requests=%d, error=%v", verdict, stats, calls.Load(), verdictErr)
				}
				return
			}
			phase := fmt.Sprintf("fixed-budget walk at budget 1: page %d:", tc.failPage)
			if verdictErr == nil || verdict.Status != "fail" || verdict.Detail == nil ||
				!strings.HasPrefix(*verdict.Detail, phase) || !strings.Contains(*verdict.Detail, tc.problem) {
				t.Fatalf("wanted failure at %q containing %q, got %v", phase, tc.problem, verdictErr)
			}
			if stats.Pages != tc.failPage-1 || stats.Rows != tc.failPage-1 || stats.RejectedBudgets != 0 ||
				calls.Load() != int32(tc.failPage+1) {
				t.Errorf("failed page affected evidence or allowed progress: stats=%+v, requests=%d", stats, calls.Load())
			}
			if tc.status != 0 {
				var httpErr *client.HTTPError
				if !errors.As(verdictErr, &httpErr) || httpErr.StatusCode != tc.status || httpErr.CursorRejected != tc.cursorRejected {
					t.Errorf("wanted HTTP %d with CursorRejected=%t, got %+v", tc.status, tc.cursorRejected, httpErr)
				}
			}
		})
	}
}

func TestStaleCursorRejectionWire(t *testing.T) {
	t.Parallel()
	const body = "invalid cursor\n"
	const text = "Content-Type: text/plain\r\n"
	const chunked = "Transfer-Encoding: chunked\r\n"
	const cursor = "old+/=&"
	for _, tc := range []struct {
		name, wire, problem string
	}{
		{"length delimited", lengthResponse(400, text, body), ""},
		{"chunked", wireResponse(400, text+chunked, chunk(body)+"0\r\n\r\n"), ""},
		{"close delimited", wireResponse(400, text, body), "framed=false"},
		{"truncated", wireResponse(400, text+fmt.Sprintf("Content-Length: %d\r\n", len(body)+1), body), "unexpected EOF"},
		{"unrelated bad request", lengthResponse(400, text, "invalid query\n"), "invalid query"},
		{"missing newline", lengthResponse(400, text, "invalid cursor"), "expected framed HTTP 400 invalid cursor"},
		{"expired capture", lengthResponse(410, text, "query capture expired\n"), "HTTP 410"},
		{"cursor header", lengthResponse(400, text+nextCursor+": another\r\n", body), "cursor header=true"},
		{"empty cursor header", lengthResponse(400, text+nextCursor+":\r\n", body), "cursor header=true"},
		{"cursor trailer", wireResponse(400, text+chunked, chunk(body)+"0\r\n"+nextCursor+": another\r\n\r\n"), "unexpected cursor trailer"},
		{"declared cursor trailer", wireResponse(400, text+chunked+"Trailer: bog-next-cursor\r\n", chunk(body)+"0\r\n\r\n"), "unexpected cursor trailer"},
		{"cursor trailer on length-delimited response", lengthResponse(400, text+"Trailer: Other, bog-next-cursor\r\n", body), "unexpected cursor trailer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, req *http.Request) {
				q := req.URL.Query()
				if req.Method != http.MethodGet || req.URL.Path != "/query" || q.Get("cursor") != cursor || q.Has("page") ||
					q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd || q.Get("limit") != "1" {
					t.Errorf("unexpected stale-cursor request %s %s", req.Method, req.URL)
				}
				writeWireResponse(t, w, tc.wire)
			})
			err := requireStaleCursorRejection(t.Context(), server.Client(), server.URL, cursor)
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("wanted error %q, got %v", tc.problem, err)
			}
		})
	}
}
