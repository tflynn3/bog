package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBudgetWalkWire(t *testing.T) {
	t.Parallel()
	fixture := sparseBudgetFixture()
	// Keep page three nonterminal so it can point back to page one's cursor.
	fifth := row{"2020-01-01T00:00:56Z", "@last"}
	fixture.seed = append(fixture.seed, fifth)
	fixture.want = append(fixture.want, fifth)
	cursors := []string{budgetTestCursor("first"), budgetTestCursor("second"), budgetTestCursor("third")}

	first := encodeBudgetWireRows(t, fixture.want[:2])
	second := encodeBudgetWireRows(t, fixture.want[2:3])
	third := encodeBudgetWireRows(t, fixture.want[3:4])
	last := encodeBudgetWireRows(t, fixture.want[4:])
	lengthFirst := budgetLengthPage(first, cursors[0])
	lengthSecond := budgetLengthPage(second, cursors[1])
	lengthThird := budgetLengthPage(third, cursors[2])
	lengthLast := budgetLengthPage(last, "")
	chunkSecond := budgetChunkedPage(second, cursors[1])
	chunkThird := budgetChunkedPage(third, cursors[2])

	oversize := strings.Repeat(" ", fixture.byteCap-len(second)+1) + second
	atCap := oversize[1:]
	secondChunk := fmt.Sprintf("%x\r\n%s\r\n", len(second), second)
	secondCursor := "Bog-Next-Cursor: " + cursors[1] + "\r\n"
	secondLength := fmt.Sprintf("Content-Length: %d\r\n", len(second))

	for _, tc := range []struct {
		name      string
		responses []string
		accepted  int
		problem   string
	}{
		{
			"length-delimited walk",
			[]string{lengthFirst, lengthSecond, lengthThird, lengthLast},
			4, "",
		},
		{
			"chunked continuations including terminal",
			[]string{lengthFirst, chunkSecond, chunkThird, budgetChunkedPage(last, "")},
			4, "",
		},
		{
			"chunked first page and exact byte cap on resume",
			[]string{budgetChunkedPage(first, cursors[0]), budgetChunkedPage(atCap, cursors[1]), lengthThird, lengthLast},
			4, "",
		},
		{
			"close-delimited first page",
			[]string{budgetWireResponse("Bog-Next-Cursor: "+cursors[0]+"\r\n", first)},
			0, "unverifiable framing",
		},
		{
			"close-delimited continuation",
			[]string{lengthFirst, budgetWireResponse(secondCursor, second)},
			1, "unverifiable framing",
		},
		{
			"close-delimited terminal page",
			[]string{lengthFirst, lengthSecond, lengthThird, budgetWireResponse("", last)},
			3, "unverifiable framing",
		},
		{
			"truncated length-delimited continuation after a complete record",
			[]string{lengthFirst, budgetWireResponse(fmt.Sprintf("Content-Length: %d\r\n", len(second)+1)+secondCursor, second)},
			1, "unexpected EOF",
		},
		{
			"truncated chunked continuation after a complete chunk",
			[]string{lengthFirst, budgetWireResponse("Transfer-Encoding: chunked\r\n"+secondCursor, secondChunk)},
			1, "unexpected EOF",
		},
		{
			"truncated chunked terminal record",
			[]string{lengthFirst, lengthSecond, lengthThird, budgetChunkedPage(strings.TrimSuffix(last, "\n"), "")},
			3, "invalid query record framing",
		},
		{
			"duplicate cursor headers",
			[]string{lengthFirst, budgetWireResponse(secondLength+secondCursor+secondCursor, second)},
			1, "invalid next cursor header",
		},
		{
			"comma-joined cursor headers",
			[]string{lengthFirst, budgetLengthPage(second, cursors[1]+", "+cursors[2])},
			1, "invalid next cursor header",
		},
		{
			"padded cursor",
			[]string{lengthFirst, budgetLengthPage(second, cursors[1]+"=")},
			1, "invalid next cursor header",
		},
		{
			"cursor declared and sent as trailer",
			[]string{lengthFirst, budgetWireResponse(
				"Transfer-Encoding: chunked\r\nTrailer: Bog-Next-Cursor\r\n",
				secondChunk+"0\r\n"+secondCursor+"\r\n",
			)},
			1, "unexpected next cursor trailer",
		},
		{
			"undeclared cursor trailer on terminal page",
			[]string{lengthFirst, lengthSecond, lengthThird, budgetWireResponse(
				"Transfer-Encoding: chunked\r\n",
				fmt.Sprintf("%x\r\n%s\r\n0\r\n%s\r\n", len(last), last, secondCursor),
			)},
			3, "unexpected next cursor trailer",
		},
		{
			"chunked continuation over byte cap",
			[]string{lengthFirst, budgetChunkedPage(oversize, cursors[1])},
			1, "response exceeds size limit",
		},
		{
			"nonconsecutive cursor cycle",
			[]string{lengthFirst, chunkSecond, budgetLengthPage(third, cursors[0])},
			2, "cursor cycle",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, requests := serveBudgetWire(t, fixture, cursors, tc.responses)
			client := srv.Client()

			const budget = 17
			var got budgetWalkStats
			err := tryBudgetWalk(t.Context(), client, func(_ context.Context, _ int, check func(string) error) error {
				return check(srv.URL)
			}, budget, fixture, &got)
			if tc.problem == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				want := fmt.Sprintf("page %d:", tc.accepted+1)
				if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), tc.problem) {
					t.Errorf("expected %s with %q, got %v", want, tc.problem, err)
				}
			}
			want := budgetWalkStats{Pages: tc.accepted}
			if tc.accepted > 0 {
				want.FirstCursorBudget = budget
				want.Rows = fixture.firstRows + tc.accepted - 1
			}
			if got != want {
				t.Errorf("evidence = %+v, want %+v", got, want)
			}
			if got, want := requests.Load(), int32(1+len(tc.responses)); got != want {
				t.Errorf("received %d requests, want %d", got, want)
			}
		})
	}
}

func encodeBudgetWireRows(t *testing.T, rows []row) string {
	t.Helper()
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	return body.String()
}

func budgetWireResponse(headers, body string) string {
	return "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n" + headers + "\r\n" + body
}

func budgetLengthPage(body, cursor string) string {
	headers := fmt.Sprintf("Content-Length: %d\r\n", len(body))
	if cursor != "" {
		headers += "Bog-Next-Cursor: " + cursor + "\r\n"
	}
	return budgetWireResponse(headers, body)
}

func budgetChunkedPage(body, cursor string) string {
	headers := "Transfer-Encoding: chunked\r\n"
	if cursor != "" {
		headers += "Bog-Next-Cursor: " + cursor + "\r\n"
	}
	// Split the row across chunks to exercise reassembly before NDJSON parsing.
	split := len(body) / 2
	return budgetWireResponse(headers, fmt.Sprintf("%x\r\n%s\r\n%x\r\n%s\r\n0\r\n\r\n",
		split, body[:split], len(body)-split, body[split:]))
}

func serveBudgetWire(t *testing.T, fixture budgetFixture, cursors, responses []string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	seed := encodeBudgetWireRows(t, fixture.seed)
	var requests atomic.Int32
	var handlers sync.WaitGroup
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		i := int(requests.Add(1)) - 1
		if i == 0 {
			body, err := io.ReadAll(req.Body)
			if err != nil || req.Method != http.MethodPost || req.URL.Path != "/ingest" || string(body) != seed {
				t.Errorf("unexpected seed request %s %s, body matches=%t, error=%v", req.Method, req.URL, string(body) == seed, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if i > len(responses) {
			t.Errorf("unexpected request %s %s", req.Method, req.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q := req.URL.Query()
		if req.Method != http.MethodGet || req.URL.Path != "/query" || req.Header.Get("Accept-Encoding") != "identity" ||
			q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd || q.Get("contains") != fixture.filter {
			t.Errorf("invalid client query %s %s (Accept-Encoding=%q)", req.Method, req.URL, req.Header.Get("Accept-Encoding"))
		}
		if i == 1 {
			if q.Get("page") != "1" || q.Has("cursor") || q.Get("limit") != strconv.Itoa(fixture.firstLimit) {
				t.Errorf("invalid first-page query %s", req.URL)
			}
		} else if q.Has("page") || q.Get("cursor") != cursors[i-2] || q.Get("limit") != strconv.Itoa(fixture.resumeLimit) {
			t.Errorf("invalid continuation query %s", req.URL)
		}
		writeBudgetWire(t, w, responses[i-1])
	}))
	t.Cleanup(func() {
		srv.Close()
		// Hijacked connections are no longer tracked by the HTTP server.
		handlers.Wait()
	})
	return srv, &requests
}

func writeBudgetWire(t *testing.T, w http.ResponseWriter, response string) {
	t.Helper()
	conn, buf, err := http.NewResponseController(w).Hijack()
	if err != nil {
		t.Errorf("hijack HTTP connection: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close HTTP connection: %v", err)
		}
	}()
	if err := conn.SetWriteDeadline(time.Now().Add(queryDeadline)); err != nil {
		t.Errorf("set HTTP write deadline: %v", err)
		return
	}
	if _, err := buf.WriteString(response); err != nil {
		t.Errorf("write HTTP response: %v", err)
		return
	}
	if err := buf.Flush(); err != nil {
		t.Errorf("flush HTTP response: %v", err)
	}
}
