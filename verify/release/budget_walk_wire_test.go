package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBudgetWalkWireFraming(t *testing.T) {
	t.Parallel()
	fixture := sparseBudgetFixture()
	// A fourth page makes it possible to distinguish a cycle on page three
	// from a cursor on a terminal page.
	extra := row{"2020-01-01T00:00:59Z", "@end"}
	fixture.seed = append(fixture.seed, extra)
	fixture.want = append(fixture.want, extra)
	seed := budgetWireRows(t, fixture.seed)

	firstToken, secondToken, thirdToken := budgetTestCursor(0xfb), budgetTestCursor(0xff), budgetTestCursor(3)
	head := func(cursor string) string { return nextCursor + ": " + cursor + "\r\n" }
	firstBody := budgetWireRows(t, fixture.want[:2])
	middleBody := budgetWireRows(t, fixture.want[2:3])
	lateBody := budgetWireRows(t, fixture.want[3:4])
	lastBody := budgetWireRows(t, fixture.want[4:])
	firstAtCap := strings.Repeat(" ", fixture.byteCap-len(firstBody)) + firstBody
	first := budgetWireLength(firstBody, head(firstToken))
	middle := budgetWireLength(middleBody, head(secondToken))
	late := budgetWireLength(lateBody, head(thirdToken))
	last := budgetWireLength(lastBody, "")

	for _, tc := range []struct {
		name      string
		page      int // one-based page to replace, or zero for the baseline
		response  string
		wantError string
	}{
		{name: "content-length throughout"},
		{name: "chunked first page", page: 1, response: budgetWireChunked(firstBody, head(firstToken), "")},
		{name: "chunked first page at byte cap", page: 1, response: budgetWireChunked(firstAtCap, head(firstToken), "")},
		{name: "chunked continuation", page: 2, response: budgetWireChunked(middleBody, head(secondToken), "")},
		{name: "chunked terminal page", page: 4, response: budgetWireChunked(lastBody, "", "")},
		{
			name: "close-delimited first page", page: 1,
			response: budgetWireOK(head(firstToken), firstBody), wantError: "unverifiable framing",
		},
		{
			name: "close-delimited continuation", page: 2,
			response: budgetWireOK(head(secondToken), middleBody), wantError: "unverifiable framing",
		},
		{
			name: "close-delimited terminal page", page: 4,
			response: budgetWireOK("", lastBody), wantError: "unverifiable framing",
		},
		{
			name: "truncated content-length with complete row", page: 2,
			response:  budgetWireOK(fmt.Sprintf("Content-Length: %d\r\n%s", len(middleBody)+1, head(secondToken)), middleBody),
			wantError: "unexpected EOF",
		},
		{
			name: "chunk without terminating zero chunk", page: 2,
			response:  budgetWireOK("Transfer-Encoding: chunked\r\n"+head(secondToken), budgetWireChunk(middleBody)),
			wantError: "unexpected EOF",
		},
		{
			name: "short chunk", page: 4,
			response:  budgetWireOK("Transfer-Encoding: chunked\r\n", fmt.Sprintf("%x\r\n%s", len(lastBody)+1, lastBody)),
			wantError: "unexpected EOF",
		},
		{
			name: "encoded first page exceeds cap with correct rows", page: 1,
			response:  budgetWireLength(" "+firstAtCap, head(firstToken)),
			wantError: "response exceeds size limit",
		},
		{
			name: "chunked continuation exceeds encoded cap", page: 2,
			response:  budgetWireChunked(strings.Repeat(" ", fixture.byteCap+1-len(middleBody))+middleBody, head(secondToken), ""),
			wantError: "response exceeds size limit",
		},
		{
			name: "empty cursor header", page: 2,
			response: budgetWireLength(middleBody, head("")), wantError: "invalid next cursor header",
		},
		{
			name: "duplicate cursor headers", page: 2,
			response:  budgetWireLength(middleBody, head(secondToken)+"bog-next-cursor: "+secondToken+"\r\n"),
			wantError: "invalid next cursor header",
		},
		{
			name: "coalesced cursor headers", page: 2,
			response: budgetWireLength(middleBody, head(secondToken+", "+thirdToken)), wantError: "invalid next cursor header",
		},
		{
			name: "padded cursor", page: 2,
			response: budgetWireLength(middleBody, head(secondToken+"=")), wantError: "invalid next cursor header",
		},
		{
			name: "standard base64 cursor", page: 2,
			response:  budgetWireLength(middleBody, head(strings.NewReplacer("-", "+", "_", "/").Replace(secondToken))),
			wantError: "invalid next cursor header",
		},
		{
			name: "short cursor payload", page: 2,
			response:  budgetWireLength(middleBody, head(base64.RawURLEncoding.EncodeToString(make([]byte, 32)))),
			wantError: "invalid next cursor header",
		},
		{
			name: "declared cursor trailer with header", page: 2,
			response:  budgetWireChunked(middleBody, head(secondToken)+"Trailer: "+nextCursor+"\r\n", head(secondToken)),
			wantError: "unexpected next cursor trailer",
		},
		{
			name: "undeclared cursor trailer on terminal page", page: 4,
			response: budgetWireChunked(lastBody, "", head(secondToken)), wantError: "unexpected next cursor trailer",
		},
		{
			name: "declared cursor trailer without value", page: 4,
			response: budgetWireChunked(lastBody, "Trailer: "+nextCursor+"\r\n", ""), wantError: "unexpected next cursor trailer",
		},
		{
			name: "invalid row with the expected fields", page: 2,
			response:  budgetWireLength(strings.Replace(middleBody, "}", ",\"extra\":true}", 1), head(secondToken)),
			wantError: "decode query record",
		},
		{
			name: "return to earlier cursor", page: 3,
			response: budgetWireLength(lateBody, head(firstToken)), wantError: "cursor cycle",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const budget = 42
			replies := []string{first, middle, late, last}
			want := budgetWalkStats{FirstCursorBudget: budget, Pages: 4, Rows: 5}
			if tc.page != 0 {
				replies[tc.page-1] = tc.response
			}
			if tc.wantError != "" {
				replies = replies[:tc.page]
				want = budgetWalkStats{}
				if tc.page > 1 {
					want = budgetWalkStats{FirstCursorBudget: budget, Pages: tc.page - 1, Rows: tc.page}
				}
			}
			steps := []budgetWireStep{budgetWireIngest(seed)}
			requestCursors := []string{"", firstToken, secondToken, thirdToken}
			for i, reply := range replies {
				limit := fixture.resumeLimit
				if i == 0 {
					limit = fixture.firstLimit
				}
				steps = append(steps, budgetWireQuery(fixture.filter, limit, requestCursors[i], reply))
			}
			got, err := runBudgetWire(t, budget, fixture, steps)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) ||
				!strings.Contains(err.Error(), fmt.Sprintf("page %d", tc.page)) {
				t.Errorf("page %d wanted error %q, got %v", tc.page, tc.wantError, err)
			}
			if got != want {
				t.Errorf("evidence = %+v, want %+v", got, want)
			}
		})
	}
}

func TestBudgetWalkRawRejection(t *testing.T) {
	t.Parallel()
	fixture := sparseBudgetFixture()
	seed := budgetWireRows(t, fixture.seed)
	reject := func(headers, body string) string {
		return "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n" + headers + "\r\n" + body
	}
	length := fmt.Sprintf("Content-Length: %d\r\n", len(budgetRejectionBody))
	clean := reject(length+"Content-Type: text/plain\r\n", budgetRejectionBody)
	for _, tc := range []struct {
		name, raw, wantError string
	}{
		{name: "clean rejection", raw: clean},
		{
			name: "raw cursor header", raw: reject(length+nextCursor+":\r\n", budgetRejectionBody),
			wantError: "cursor header=true",
		},
		{
			name: "raw cursor trailer", wantError: "unexpected cursor trailer",
			raw: reject("Transfer-Encoding: chunked\r\nTrailer: "+nextCursor+"\r\n",
				budgetWireChunk(budgetRejectionBody)+"0\r\n"+nextCursor+": "+budgetTestCursor(1)+"\r\n\r\n"),
		},
		{
			name: "raw NDJSON content type", raw: reject(length+"Content-Type: Application/X-NDJSON; charset=utf-8\r\n", budgetRejectionBody),
			wantError: "unclean HTTP 503 budget rejection",
		},
		{
			name: "raw truncated body", wantError: "unexpected EOF",
			raw: reject(fmt.Sprintf("Content-Length: %d\r\n", len(budgetRejectionBody)+1), budgetRejectionBody),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := budgetWireQuery(fixture.filter, fixture.firstLimit, "", tc.raw)
			raw.requireIdentity = false
			steps := []budgetWireStep{
				budgetWireIngest(seed),
				budgetWireQuery(fixture.filter, fixture.firstLimit, "", clean),
				raw,
			}
			got, err := runBudgetWire(t, 1, fixture, steps)
			var want budgetWalkStats
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				want.RejectedBudgets = 1
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Errorf("wanted error %q, got %v", tc.wantError, err)
			}
			if got != want {
				t.Errorf("evidence = %+v, want %+v", got, want)
			}
		})
	}
}

type budgetWireStep struct {
	method, path, query, body, response string
	requireIdentity                     bool
}

func budgetWireIngest(body string) budgetWireStep {
	return budgetWireStep{
		method: http.MethodPost, path: "/ingest", body: body,
		response: "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
	}
}

func budgetWireQuery(filter string, limit int, cursor, response string) budgetWireStep {
	q := url.Values{"start": {fixtureStart}, "end": {fixtureEnd}, "contains": {filter}, "limit": {strconv.Itoa(limit)}}
	if cursor == "" {
		q.Set("page", "1")
	} else {
		q.Set("cursor", cursor)
	}
	return budgetWireStep{method: http.MethodGet, path: "/query", query: q.Encode(), response: response, requireIdentity: true}
}

func budgetWireRows(t *testing.T, rows []row) string {
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

func budgetWireOK(headers, body string) string {
	return "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/x-ndjson\r\n" + headers + "\r\n" + body
}

func budgetWireLength(body, headers string) string {
	return budgetWireOK(fmt.Sprintf("Content-Length: %d\r\n%s", len(body), headers), body)
}

func budgetWireChunk(body string) string {
	return fmt.Sprintf("%x\r\n%s\r\n", len(body), body)
}

func budgetWireChunked(body, headers, trailers string) string {
	// Split in the middle of the NDJSON record so framing is independent
	// of both the record boundary and the individual HTTP chunk boundaries.
	mid := len(body) / 2
	return budgetWireOK("Transfer-Encoding: chunked\r\n"+headers,
		budgetWireChunk(body[:mid])+budgetWireChunk(body[mid:])+"0\r\n"+trailers+"\r\n")
}

func runBudgetWire(t *testing.T, budget int, fixture budgetFixture, steps []budgetWireStep) (budgetWalkStats, error) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	done := make(chan error, 1)
	go func() {
		for i, step := range steps {
			conn, err := ln.Accept()
			if err == nil {
				err = respondBudgetWire(conn, step)
			}
			if err != nil {
				done <- fmt.Errorf("wire exchange %d of %d: %w", i+1, len(steps), err)
				return
			}
		}
		done <- nil
	}()
	defer func() {
		// Closing the listener also unblocks Accept when a walk stops early.
		if err := errors.Join(ln.Close(), <-done); err != nil {
			t.Error(err)
		}
	}()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), time.Duration(len(steps))*queryDeadline)
	defer cancel()
	var proof budgetWalkStats
	err = tryBudgetWalk(ctx, client, func(_ context.Context, _ int, check func(string) error) error {
		return check(base)
	}, budget, fixture, &proof)
	return proof, err
}

func respondBudgetWire(conn net.Conn, step budgetWireStep) (err error) {
	defer func() { err = errors.Join(err, conn.Close()) }()
	if err := conn.SetDeadline(time.Now().Add(queryDeadline)); err != nil {
		return err
	}
	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, req.Body.Close()) }()
	q, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return err
	}
	if req.Method != step.method || req.URL.Path != step.path || q.Encode() != step.query {
		return fmt.Errorf("unexpected request %s %s, want %s %s with query %q", req.Method, req.URL, step.method, step.path, step.query)
	}
	if step.requireIdentity && req.Header.Get("Accept-Encoding") != "identity" {
		return errors.New("query missing Bog client Accept-Encoding")
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, int64(len(step.body))+1))
	if err != nil {
		return err
	}
	if string(body) != step.body {
		return fmt.Errorf("unexpected request body (got %d bytes, want %d)", len(body), len(step.body))
	}
	n, err := io.WriteString(conn, step.response)
	if err == nil && n != len(step.response) {
		return io.ErrShortWrite
	}
	return err
}
