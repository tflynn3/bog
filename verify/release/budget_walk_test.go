package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestFindBudgetWalk(t *testing.T) {
	t.Parallel()
	const (
		firstToken  = "first+/=&"
		secondToken = "second+/=&"
		firstRow    = "{\"ts\":\"" + fixtureStart + "\",\"line\":\"c\"}\n"
		secondRow   = "{\"ts\":\"" + fixtureStart + "\",\"line\":\"a\"}\n"
		lastRow     = "{\"ts\":\"" + fixtureStart + "\",\"line\":\"b\"}\n"
	)
	type page struct {
		status   int
		body     string
		cursor   *string
		ndjson   bool
		truncate bool
	}
	first := page{status: 200, body: firstRow, cursor: new(firstToken), ndjson: true}
	second := page{status: 200, body: secondRow, cursor: new(secondToken), ndjson: true}
	last := page{status: 200, body: lastRow, ndjson: true}
	rejection := page{status: 503, body: "query work budget exhausted\n"}

	for _, tc := range []struct {
		name       string
		rejections int
		bound      int
		pages      []page
		want       budgetWalkProof
		problem    string
	}{
		{"first budget", 0, 3, []page{first, second, last}, budgetWalkProof{1, 0, 3, 3}, ""},
		{"search stops at first cursor", 2, 4, []page{first, second, last}, budgetWalkProof{3, 2, 3, 3}, ""},
		{"inclusive upper bound", 2, 3, []page{first, second, last}, budgetWalkProof{3, 2, 3, 3}, ""},
		{"no cursor within bound", 3, 3, nil, budgetWalkProof{0, 3, 0, 0}, "no first-page cursor"},
		{"partial rejection", 1, 3, []page{{status: 503, body: rejection.body + "{\"t"}}, budgetWalkProof{0, 1, 0, 0}, "unclean HTTP 503"},
		{"rejection with empty cursor header", 0, 3, []page{{status: 503, body: rejection.body, cursor: new("")}}, budgetWalkProof{}, "unclean HTTP 503"},
		{"rejection with NDJSON content type", 0, 3, []page{{status: 503, body: rejection.body, ndjson: true}}, budgetWalkProof{}, "unclean HTTP 503"},
		{"rejection with unrelated error", 0, 3, []page{{status: 503, body: "busy\n"}}, budgetWalkProof{}, "unclean HTTP 503"},
		{"success without first cursor", 1, 3, []page{{status: 200, body: firstRow, ndjson: true}}, budgetWalkProof{0, 1, 0, 0}, "missing continuation cursor"},
		{"first page exceeds row limit", 0, 3, []page{{status: 200, body: firstRow + secondRow, ndjson: true}}, budgetWalkProof{}, "unexpected row"},
		{"out of append order", 0, 3, []page{{status: 200, body: secondRow, cursor: new(firstToken), ndjson: true}}, budgetWalkProof{}, "unexpected row"},
		{"empty continuation", 0, 3, []page{first, {status: 200, cursor: new(secondToken), ndjson: true}}, budgetWalkProof{1, 0, 1, 1}, "returned 0 rows"},
		{"repeated cursor", 0, 3, []page{first, {status: 200, body: secondRow, cursor: new(firstToken), ndjson: true}}, budgetWalkProof{1, 0, 1, 1}, "repeated a continuation cursor"},
		{"early termination", 0, 3, []page{first, {status: 200, body: secondRow, ndjson: true}}, budgetWalkProof{1, 0, 1, 1}, "missing continuation cursor"},
		{"rejected second page", 0, 3, []page{first, rejection}, budgetWalkProof{1, 0, 1, 1}, "page 2: expected NDJSON HTTP 200, got HTTP 503"},
		{"rejected terminal page", 0, 3, []page{first, second, {status: 410}}, budgetWalkProof{1, 0, 2, 2}, "page 3: expected NDJSON HTTP 200, got HTTP 410"},
		{"truncated terminal record", 0, 3, []page{first, second, {status: 200, body: lastRow[:len(lastRow)-2], ndjson: true}}, budgetWalkProof{1, 0, 2, 2}, "incomplete NDJSON framing"},
		{"missing terminal newline", 0, 3, []page{first, second, {status: 200, body: strings.TrimSuffix(lastRow, "\n"), ndjson: true}}, budgetWalkProof{1, 0, 2, 2}, "incomplete NDJSON framing"},
		{"truncated HTTP body", 0, 3, []page{first, second, {status: 200, body: lastRow, ndjson: true, truncate: true}}, budgetWalkProof{1, 0, 2, 2}, "unexpected EOF"},
		{"terminal cursor", 0, 3, []page{first, second, {status: 200, body: lastRow, cursor: new("extra"), ndjson: true}}, budgetWalkProof{1, 0, 2, 2}, "issued a cursor after the final row"},
		{"empty terminal cursor header", 0, 3, []page{first, second, {status: 200, body: lastRow, cursor: new(""), ndjson: true}}, budgetWalkProof{1, 0, 2, 2}, "issued a cursor after the final row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
			defer client.CloseIdleConnections()
			launched := 0
			launch := func(_ context.Context, budget int, check func(string) error) error {
				launched++
				if budget != launched {
					t.Errorf("launch %d used budget %d", launched, budget)
				}
				pages := tc.pages
				if budget <= tc.rejections {
					pages = []page{rejection}
				}
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					i := int(calls.Add(1)) - 1
					if i == 0 {
						body, err := io.ReadAll(req.Body)
						if err != nil || req.Method != http.MethodPost || req.URL.Path != "/ingest" || string(body) != firstRow+secondRow+lastRow {
							t.Errorf("unexpected seed request %s %s, %q, %v", req.Method, req.URL, body, err)
						}
						w.WriteHeader(http.StatusNoContent)
						return
					}
					if i > len(pages) {
						t.Errorf("unexpected request %s %s", req.Method, req.URL)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					q := req.URL.Query()
					if req.Method != http.MethodGet || req.URL.Path != "/query" || q.Get("start") != fixtureStart ||
						q.Get("end") != fixtureEnd || q.Get("limit") != "1" || q.Has("contains") {
						t.Errorf("unexpected query %s %s", req.Method, req.URL)
					}
					if i == 1 {
						if q.Get("page") != "1" || q.Has("cursor") {
							t.Errorf("invalid first-page query %s", req.URL)
						}
					} else if prev := pages[i-2].cursor; prev == nil || q.Has("page") || q.Get("cursor") != *prev {
						t.Errorf("invalid continuation query %s", req.URL)
					}
					p := pages[i-1]
					if p.ndjson {
						w.Header().Set("Content-Type", "application/x-ndjson")
					}
					if p.cursor != nil {
						w.Header().Set(nextCursor, *p.cursor)
					}
					if p.truncate {
						w.Header().Set("Content-Length", fmt.Sprint(len(p.body)+1))
					}
					w.WriteHeader(p.status)
					if _, err := io.WriteString(w, p.body); err != nil {
						t.Error(err)
					}
				}))
				defer srv.Close()
				err := check(srv.URL)
				if got := int(calls.Load()); got != 1+len(pages) {
					t.Errorf("budget %d received %d requests, want %d", budget, got, 1+len(pages))
				}
				return err
			}
			got, err := findBudgetWalk(t.Context(), client, tc.bound, launch)
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Errorf("wanted error %q, got %v", tc.problem, err)
			}
			if got != tc.want {
				t.Errorf("evidence = %+v, want %+v", got, tc.want)
			}
			wantLaunches := tc.rejections
			if len(tc.pages) > 0 {
				wantLaunches++
			}
			if launched != wantLaunches {
				t.Errorf("launched %d candidates, want %d", launched, wantLaunches)
			}
		})
	}
}

func TestFindBudgetWalkStopsOnCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/ingest" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		cancel()
	}))
	defer srv.Close()

	launches := 0
	_, err := findBudgetWalk(ctx, srv.Client(), 3, func(_ context.Context, _ int, check func(string) error) error {
		launches++
		return check(srv.URL)
	})
	if launches != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("launches = %d, error = %v; want one launch and cancellation", launches, err)
	}
}
