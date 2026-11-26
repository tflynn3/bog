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
	"testing"
	"time"

	bogclient "bog/internal/client"
)

func TestBudgetWalkWireFraming(t *testing.T) {
	t.Parallel()
	const (
		firstHeader  = "Bog-Next-Cursor: " + firstBudgetToken + "\r\n"
		secondHeader = "Bog-Next-Cursor: " + secondBudgetToken + "\r\n"
		ndjson       = "Content-Type: application/x-ndjson\r\n"
		plainText    = "Content-Type: text/plain; charset=utf-8\r\n"
	)
	length := func(status, headers, body string) string {
		return budgetWireResponse(status, headers+fmt.Sprintf("Content-Length: %d\r\n", len(body)), body)
	}
	chunk := func(body string) string {
		return fmt.Sprintf("%x\r\n%s\r\n", len(body), body)
	}
	chunked := func(status, headers, body string) string {
		return budgetWireResponse(status, headers+"Transfer-Encoding: chunked\r\n", body)
	}
	const ok, unavailable = "200 OK", "503 Service Unavailable"
	first := length(ok, ndjson+firstHeader, firstBudgetRow)
	last := length(ok, ndjson, lastBudgetRow)
	middle := length(ok, ndjson+secondHeader, secondBudgetRow)
	const chunkEnd = "0\r\n\r\n"
	split := len(secondBudgetRow) / 2

	for _, tc := range []struct {
		name      string
		pages     []string
		want      budgetWalkStats
		problem   string
		wantCause error
	}{
		{
			name: "content-length continuation", pages: []string{first, middle, last},
			want: budgetWalkStats{1, 0, 3, 3},
		},
		{
			name: "chunked continuation with unrelated trailer",
			pages: []string{first, chunked(ok, ndjson+secondHeader+"Trailer: X-Test\r\n",
				chunk(secondBudgetRow[:split])+chunk(secondBudgetRow[split:])+"0\r\nX-Test: complete\r\n\r\n"), last},
			want: budgetWalkStats{1, 0, 3, 3},
		},
		{
			name:  "close-delimited continuation",
			pages: []string{first, budgetWireResponse(ok, ndjson+secondHeader, secondBudgetRow)},
			want:  budgetWalkStats{1, 0, 1, 1}, problem: "query response has unverifiable framing",
		},
		{
			name: "truncated content-length with complete NDJSON",
			pages: []string{first, budgetWireResponse(ok, ndjson+secondHeader+
				fmt.Sprintf("Content-Length: %d\r\n", len(secondBudgetRow)+1), secondBudgetRow)},
			want: budgetWalkStats{1, 0, 1, 1}, problem: "read query page", wantCause: io.ErrUnexpectedEOF,
		},
		{
			name:  "truncated chunked with complete NDJSON",
			pages: []string{first, chunked(ok, ndjson+secondHeader, chunk(secondBudgetRow))},
			want:  budgetWalkStats{1, 0, 1, 1}, problem: "read query page", wantCause: io.ErrUnexpectedEOF,
		},
		{
			name:  "duplicate cursor headers",
			pages: []string{first, length(ok, ndjson+secondHeader+secondHeader, secondBudgetRow)},
			want:  budgetWalkStats{1, 0, 1, 1}, problem: "invalid next cursor header",
		},
		{
			name:  "comma-joined cursors",
			pages: []string{first, length(ok, ndjson+"Bog-Next-Cursor: "+secondBudgetToken+", "+extraBudgetToken+"\r\n", secondBudgetRow)},
			want:  budgetWalkStats{1, 0, 1, 1}, problem: "invalid next cursor header",
		},
		{
			name:  "invalid cursor alphabet",
			pages: []string{first, length(ok, ndjson+"Bog-Next-Cursor: +/"+secondBudgetToken[2:]+"\r\n", secondBudgetRow)},
			want:  budgetWalkStats{1, 0, 1, 1}, problem: "invalid next cursor header",
		},
		{
			name: "cursor trailer replacing continuation header",
			pages: []string{first, chunked(ok, ndjson+"Trailer: Bog-Next-Cursor\r\n",
				chunk(secondBudgetRow)+"0\r\n"+secondHeader+"\r\n")},
			want: budgetWalkStats{1, 0, 1, 1}, problem: "unexpected next cursor trailer",
		},
		{
			name: "undeclared cursor trailer on terminal page",
			pages: []string{first, middle, chunked(ok, ndjson,
				chunk(lastBudgetRow)+"0\r\n"+secondHeader+"\r\n")},
			want: budgetWalkStats{1, 0, 2, 2}, problem: "unexpected next cursor trailer",
		},
		{
			name:  "NDJSON body with wrong content type",
			pages: []string{first, length(ok, plainText+secondHeader, secondBudgetRow)},
			want:  budgetWalkStats{1, 0, 1, 1}, problem: "expected NDJSON Content-Type",
		},
		{
			name:  "clean chunked initial rejection",
			pages: []string{chunked(unavailable, plainText, chunk(budgetRejectionBody)+chunkEnd)},
			want:  budgetWalkStats{RejectedBudgets: 1}, problem: "no first-page cursor",
		},
		{
			name: "rejection with declared but omitted cursor trailer",
			pages: []string{chunked(unavailable, plainText+"Trailer: X-Test, bog-next-cursor\r\n",
				chunk(budgetRejectionBody)+chunkEnd)},
			problem: "cursor trailer=true",
		},
		{
			name: "rejection with undeclared empty cursor trailer",
			pages: []string{chunked(unavailable, plainText,
				chunk(budgetRejectionBody)+"0\r\nBog-Next-Cursor:\r\n\r\n")},
			problem: "cursor trailer=true",
		},
		{
			name:    "rejection with length-delimited cursor trailer declaration",
			pages:   []string{length(unavailable, plainText+"Trailer: Bog-Next-Cursor\r\n", budgetRejectionBody)},
			problem: "cursor trailer=true",
		},
		{
			name: "truncated rejection with exact error text",
			pages: []string{budgetWireResponse(unavailable, plainText+
				fmt.Sprintf("Content-Length: %d\r\n", len(budgetRejectionBody)+1), budgetRejectionBody)},
			problem: "unclean HTTP 503", wantCause: io.ErrUnexpectedEOF,
		},
		{
			name:    "rejection with NDJSON as a second content type",
			pages:   []string{length(unavailable, plainText+"Content-Type: Application/X-NDJSON; charset=utf-8\r\n", budgetRejectionBody)},
			problem: "unclean HTTP 503",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				i := calls
				calls++
				if i == 0 {
					body, err := io.ReadAll(req.Body)
					if err != nil || req.Method != http.MethodPost || req.URL.Path != "/ingest" ||
						string(body) != firstBudgetRow+secondBudgetRow+lastBudgetRow {
						t.Errorf("unexpected seed request %s %s, body %q, error %v", req.Method, req.URL, body, err)
					}
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if i > len(tc.pages) {
					t.Errorf("unexpected request %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				query := req.URL.Query()
				if req.Method != http.MethodGet || req.URL.Path != "/query" ||
					query.Get("start") != fixtureStart || query.Get("end") != fixtureEnd || query.Get("limit") != "1" ||
					query.Get("contains") != "" || req.Header.Get("Accept-Encoding") != "identity" {
					t.Errorf("unexpected query request %s %s, headers %v", req.Method, req.URL, req.Header)
				}
				if i == 1 {
					if query.Get("page") != "1" || query.Has("cursor") {
						t.Errorf("unexpected first-page query %s", req.URL)
					}
				} else if want := []string{firstBudgetToken, secondBudgetToken}[i-2]; query.Has("page") || query.Get("cursor") != want {
					t.Errorf("unexpected continuation query %s, want cursor %q", req.URL, want)
				}
				if err := writeBudgetWireResponse(w, tc.pages[i-1]); err != nil {
					t.Errorf("write wire page %d: %v", i, err)
				}
			}))
			got, err := findBudgetWalk(t.Context(), srv.Client(), 1, func(_ context.Context, _ int, check func(string) error) error {
				return check(srv.URL)
			})
			srv.Close()
			// Hijacked connections are outside httptest.Server's cleanup.
			// The handler holds mu through closing each such connection.
			mu.Lock()
			gotCalls := calls
			mu.Unlock()
			if gotCalls != len(tc.pages)+1 {
				t.Errorf("got %d requests, want %d including seed", gotCalls, len(tc.pages)+1)
			}
			if got != tc.want {
				t.Errorf("evidence = %+v, want %+v", got, tc.want)
			}
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Errorf("wanted error %q, got %v", tc.problem, err)
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Errorf("wanted cause %v, got %v", tc.wantCause, err)
			}
		})
	}
}

func budgetWireResponse(status, headers, body string) string {
	return "HTTP/1.1 " + status + "\r\nConnection: close\r\n" + headers + "\r\n" + body
}

func writeBudgetWireResponse(w http.ResponseWriter, response string) (err error) {
	conn, buf, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	if err := conn.SetWriteDeadline(time.Now().Add(queryDeadline)); err != nil {
		return err
	}
	if _, err := buf.WriteString(response); err != nil {
		return err
	}
	return buf.Flush()
}

func TestBudgetPageBodyCloseError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"page", http.StatusOK, firstBudgetRow},
		{"budget rejection", http.StatusServiceUnavailable, budgetRejectionBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			closeErr := errors.New("test response close failure")
			body := &budgetCloseErrorBody{Reader: strings.NewReader(tc.body), err: closeErr}
			requests := 0
			transport := budgetTestRoundTripper(func(_ *http.Request) (*http.Response, error) {
				requests++
				header := http.Header{"Content-Type": {"text/plain"}}
				if tc.status == http.StatusOK {
					header.Set("Content-Type", "application/x-ndjson")
					header.Set(nextCursor, firstBudgetToken)
				}
				return &http.Response{
					StatusCode: tc.status, Header: header, ContentLength: int64(len(tc.body)), Body: body,
				}, nil
			})
			pages, err := newBudgetPageClient("http://127.0.0.1", &http.Client{Transport: transport}, 96)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			query := bogclient.Query{Start: start, End: start.Add(10 * time.Minute), Limit: 1}
			page, rejected, err := pages.fetch(t.Context(), query, "")
			if len(page.Rows) != 0 || page.NextCursor != "" || rejected || !errors.Is(err, closeErr) {
				t.Errorf("page = %+v, rejected = %t, error = %v", page, rejected, err)
			}
			if requests != 1 || body.closes != 1 || body.Len() != 0 {
				t.Errorf("requests = %d, closes = %d, unread bytes = %d", requests, body.closes, body.Len())
			}
		})
	}
}

type budgetTestRoundTripper func(*http.Request) (*http.Response, error)

func (f budgetTestRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type budgetCloseErrorBody struct {
	*strings.Reader
	err    error
	closes int
}

func (b *budgetCloseErrorBody) Close() error {
	b.closes++
	return b.err
}
