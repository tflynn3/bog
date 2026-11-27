package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"bog/internal/client"
)

func TestBudgetRejectionWire(t *testing.T) {
	t.Parallel()
	const text = "Content-Type: text/plain; charset=utf-8\r\n"
	const chunked = "Transfer-Encoding: chunked\r\n"
	const terminalChunk = "0\r\n\r\n"
	body := budgetRejectionBody
	for _, tc := range []struct {
		name    string
		wire    string
		problem string
		cursor  bool
	}{
		{name: "length with connection close", wire: lengthResponse(503, text, body)},
		{name: "chunked with connection close", wire: wireResponse(503, text+chunked, chunk(body[:4])+chunk(body[4:])+terminalChunk)},
		{name: "length with identity encoding", wire: lengthResponse(503, text+"Content-Encoding: identity\r\n", body)},
		{name: "unrelated trailer", wire: wireResponse(503, text+chunked+"Trailer: X-Diagnostic\r\n",
			chunk(body)+"0\r\nX-Diagnostic: checked\r\n\r\n")},
		{name: "HTTP 1.1 close delimited", wire: wireResponse(503, text, body), problem: "framed=false"},
		{name: "HTTP 1.0 close delimited", wire: strings.Replace(wireResponse(503, text, body), "HTTP/1.1", "HTTP/1.0", 1), problem: "framed=false"},
		{name: "full text with truncated declared length", wire: wireResponse(503, text+fmt.Sprintf("Content-Length: %d\r\n", len(body)+1), body), problem: "unexpected EOF"},
		{name: "partial text with complete framing", wire: lengthResponse(503, text, strings.TrimSuffix(body, "\n")), problem: "unclean HTTP 503 budget rejection"},
		{name: "chunk body without terminator", wire: wireResponse(503, text+chunked, chunk(body)), problem: "unexpected EOF"},
		{name: "chunk stream without trailer termination", wire: wireResponse(503, text+chunked, chunk(body)+"0\r\n"), problem: "EOF"},
		{name: "content encoded", wire: lengthResponse(503, text+"Content-Encoding: gzip\r\n", body), problem: "identity encoding=false"},
		{name: "cursor header", wire: lengthResponse(503, text+nextCursor+": token\r\n", body), problem: "cursor header=true", cursor: true},
		{name: "empty cursor header", wire: lengthResponse(503, text+"bog-next-cursor:\r\n", body), problem: "cursor header=true", cursor: true},
		{name: "undeclared cursor trailer", wire: wireResponse(503, text+chunked,
			chunk(body)+"0\r\nbog-next-cursor: token\r\n\r\n"), problem: "cursor trailer=true", cursor: true},
		{name: "empty cursor trailer", wire: wireResponse(503, text+chunked+"Trailer: "+nextCursor+"\r\n",
			chunk(body)+"0\r\n"+nextCursor+":\r\n\r\n"), problem: "cursor trailer=true", cursor: true},
		{name: "cursor trailer declared only", wire: wireResponse(503, text+chunked+"Trailer: "+nextCursor+"\r\n",
			chunk(body)+terminalChunk), problem: "cursor trailer=true", cursor: true},
		{name: "length delimited with cursor trailer declaration", wire: lengthResponse(503, text+"Trailer: Other, bog-next-cursor\r\n", body), problem: "cursor trailer=true", cursor: true},
		{name: "NDJSON content type", wire: lengthResponse(503, "Content-Type: Application/X-NDJSON; charset=utf-8\r\n", body), problem: "Application/X-NDJSON"},
		{name: "NDJSON as second content type", wire: lengthResponse(503, text+"Content-Type: application/x-ndjson\r\n", body), problem: "application/x-ndjson"},
		{name: "appended record", wire: lengthResponse(503, text, body+"{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"partial\"}\n"), problem: "unclean HTTP 503 budget rejection"},
		{name: "capacity rejection", wire: lengthResponse(503, text, "query capacity exhausted\n"), problem: "query capacity exhausted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, walk := range []bool{false, true} {
				var calls atomic.Int32
				server := wireServer(t, func(w http.ResponseWriter, req *http.Request) {
					if req.Method == http.MethodPost && req.URL.Path == "/ingest" && walk {
						if _, err := io.Copy(io.Discard, req.Body); err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
						w.WriteHeader(http.StatusNoContent)
						return
					}
					calls.Add(1)
					filter := budgetFilter
					if walk {
						filter = ""
					}
					q := req.URL.Query()
					if req.Method != http.MethodGet || req.URL.Path != "/query" || q.Get("contains") != filter ||
						q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd || q.Get("limit") != "1" || q.Get("page") != "1" || q.Has("cursor") {
						t.Errorf("unexpected request %s %s", req.Method, req.URL)
					}
					writeWireResponse(t, w, tc.wire)
				})
				if walk {
					launch := func(ctx context.Context, _ int, check func(context.Context, string) error) error {
						return check(ctx, server.URL)
					}
					stats, err := findBudgetWalk(t.Context(), server.Client(), 1, launch)
					if len(stats.Probes) != 1 || stats.FirstCursorBudget != 0 || stats.ExactThresholdBudget != 0 ||
						stats.Pages != 0 || stats.Rows != 0 || stats.FirstBrokenContinuation != nil {
						t.Fatalf("invalid rejection evidence: %+v", stats)
					}
					probe := stats.Probes[0]
					if tc.problem == "" {
						if stats.RejectedBudgets != 1 || probe.Outcome != probeRejected || probe.Failure != nil ||
							err == nil || !strings.Contains(err.Error(), "no first-page cursor") {
							t.Errorf("clean rejection: probe=%+v, error=%v", probe, err)
						}
					} else {
						if stats.RejectedBudgets != 0 || probe.Outcome != probeIndeterminate || probe.Failure == nil ||
							probe.Failure != stats.FirstFailure || probe.Failure.HTTPStatus != 503 || probe.Failure.Page != 1 ||
							err == nil || !strings.Contains(err.Error(), tc.problem) {
							t.Errorf("unclean rejection: probe=%+v, error=%v", probe, err)
						}
					}
				} else {
					evidence, err := requireBudgetRejection(t.Context(), server.Client(), server.URL)
					if evidence.HTTPStatus != 503 || evidence.NextCursor != tc.cursor {
						t.Errorf("control evidence: %+v", evidence)
					}
					if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
						t.Errorf("control error = %v, want %q", err, tc.problem)
					}
				}
				if n := calls.Load(); n != 1 {
					t.Errorf("walk=%t: sent %d queries, want one", walk, n)
				}
			}
		})
	}
}

type budgetRoundTripper func(*http.Request) (*http.Response, error)

func (f budgetRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type budgetErrorCloser struct {
	io.Reader
	err    error
	closed bool
}

func (c *budgetErrorCloser) Close() error {
	c.closed = true
	return c.err
}

func TestBudgetRejectionPropagatesCloseError(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("closing response failed")
	body := &budgetErrorCloser{Reader: strings.NewReader(budgetRejectionBody), err: closeErr}
	raw := &http.Client{Transport: budgetRoundTripper(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable, ProtoMajor: 1, ProtoMinor: 1,
			ContentLength: int64(len(budgetRejectionBody)), Body: body, Request: req,
		}, nil
	})}
	evidence, err := requireBudgetRejection(t.Context(), raw, "http://127.0.0.1")
	var httpErr *client.HTTPError
	if !body.closed || evidence.HTTPStatus != 503 || !errors.Is(err, closeErr) || !errors.As(err, &httpErr) {
		t.Fatalf("close failure lost: closed=%t, evidence=%+v, error=%v", body.closed, evidence, err)
	}
}
