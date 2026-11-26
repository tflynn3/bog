package main

import (
	"context"
	"encoding/json"
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
)

// Keep the four fields consumed by existing admission report readers under
// test alongside the richer evidence.
func legacyBudgetCounts(stats budgetWalkStats) [4]int {
	return [4]int{stats.FirstCursorBudget, stats.RejectedBudgets, stats.Pages, stats.Rows}
}

func TestFindSparseBudgetWalk(t *testing.T) {
	t.Parallel()
	fixture := sparseBudgetFixture()
	record := func(r row) string {
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		return string(data) + "\n"
	}
	var seedBody string
	for _, r := range fixture.seed {
		seedBody += record(r)
	}
	firstBody := record(fixture.want[0]) + record(fixture.want[1])
	thirdRow, lastRow := record(fixture.want[2]), record(fixture.want[3])
	if len(firstBody) > fixture.byteCap || len(firstBody+thirdRow) <= fixture.byteCap {
		t.Fatalf("fixture does not force a byte trim at %d bytes", fixture.byteCap)
	}
	const firstToken, secondToken = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	type page struct {
		body, cursor string
	}
	first := page{firstBody, firstToken}
	middle := page{thirdRow, secondToken}
	last := page{lastRow, ""}
	for _, tc := range []struct {
		name        string
		threshold   int
		pages       []page
		wantBudgets []int
		problem     string
		override    map[int][]page
		cursorAt    int
		exactAt     int
		firstBroken int
		brokenPage  int
	}{
		{name: "search and complete walk", threshold: 6, wantBudgets: []int{4, 6, 5}, cursorAt: 6, exactAt: 6},
		{name: "first budget", threshold: 1, wantBudgets: []int{4, 2, 1}, cursorAt: 1, exactAt: 1},
		{name: "maximum budget", threshold: 8, wantBudgets: []int{4, 6, 7, 8}, cursorAt: 8, exactAt: 8},
		{name: "no issuing budget", threshold: 9, wantBudgets: []int{4, 6, 7, 8}, problem: "no first-page cursor"},
		{name: "ignored byte cap", threshold: 4, pages: []page{{firstBody + thirdRow, firstToken}}, wantBudgets: []int{4}, problem: "response exceeds size limit"},
		{name: "trimmed too many rows", threshold: 4, pages: []page{{record(fixture.want[0]), firstToken}}, wantBudgets: []int{4}, problem: "returned 1 rows, want 2"},
		{name: "cursor skipped trimmed row", threshold: 4, pages: []page{first, {lastRow, ""}}, wantBudgets: []int{4, 2, 3}, problem: "unexpected row", cursorAt: 4, exactAt: 4, firstBroken: 4, brokenPage: 2},
		{name: "leaked nonmatch", threshold: 4, pages: []page{first, middle, {record(fixture.seed[8]), ""}}, wantBudgets: []int{4, 2, 3}, problem: "unexpected row", cursorAt: 4, exactAt: 4, firstBroken: 4, brokenPage: 3},
		{name: "continued after last row", threshold: 4, pages: []page{first, middle, {lastRow, strings.Repeat("C", 44)}}, wantBudgets: []int{4, 2, 3}, problem: "after the final row", cursorAt: 4, exactAt: 4, firstBroken: 4, brokenPage: 3},
		{name: "budget failure on resume", threshold: 4, pages: []page{first, {budgetRejectionBody, ""}}, wantBudgets: []int{4, 2, 3}, problem: "page 2: Bog HTTP 503", cursorAt: 4, exactAt: 4, firstBroken: 4, brokenPage: 2},
		{name: "budget failure proving exhaustion", threshold: 4, pages: []page{first, middle, {budgetRejectionBody, ""}}, wantBudgets: []int{4, 2, 3}, problem: "page 3: Bog HTTP 503", cursorAt: 4, exactAt: 4, firstBroken: 4, brokenPage: 3},
		{
			name: "higher budget fails before a complete lower walk", threshold: 3, wantBudgets: []int{4, 2, 3},
			override: map[int][]page{4: {first, {budgetRejectionBody, ""}}},
			problem:  "at budget 4: page 2: Bog HTTP 503", cursorAt: 3, exactAt: 3, firstBroken: 4, brokenPage: 2,
		},
		{
			name: "first observed broken continuation survives lower failure", threshold: 2, wantBudgets: []int{4, 2, 1},
			override: map[int][]page{4: {first, middle, {budgetRejectionBody, ""}}, 2: {first, {budgetRejectionBody, ""}}},
			problem:  "at budget 4: page 3: Bog HTTP 503", cursorAt: 2, exactAt: 2, firstBroken: 4, brokenPage: 3,
		},
		{
			name: "broken higher walk and invalid predecessor leave threshold open", threshold: 3, wantBudgets: []int{4, 2, 3},
			override: map[int][]page{4: {first, {budgetRejectionBody, ""}}, 3: {{strings.TrimSuffix(firstBody, "\n"), firstToken}}},
			problem:  "at budget 4: page 2: Bog HTTP 503", cursorAt: 4, firstBroken: 4, brokenPage: 2,
		},
		{
			name: "higher complete walk and invalid predecessor leave threshold open", threshold: 4, wantBudgets: []int{4, 2, 3},
			override: map[int][]page{3: {{strings.TrimSuffix(firstBody, "\n"), firstToken}}},
			problem:  "at budget 3: page 1:", cursorAt: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var budgets []int
			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
			defer client.CloseIdleConnections()
			launch := func(_ context.Context, budget int, check func(string) error) error {
				budgets = append(budgets, budget)
				pages := tc.pages
				if pages == nil {
					pages = []page{first, middle, last}
				}
				if budget < tc.threshold {
					pages = []page{{budgetRejectionBody, ""}}
				}
				if override, exists := tc.override[budget]; exists {
					pages = override
				}
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					i := int(calls.Add(1)) - 1
					if i == 0 {
						body, err := io.ReadAll(req.Body)
						if err != nil || req.Method != http.MethodPost || req.URL.Path != "/ingest" || string(body) != seedBody {
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
						q.Get("end") != fixtureEnd || q.Get("contains") != fixture.filter {
						t.Errorf("unexpected query %s %s", req.Method, req.URL)
					}
					if i == 1 {
						if q.Get("limit") != strconv.Itoa(fixture.firstLimit) || q.Get("page") != "1" || q.Has("cursor") {
							t.Errorf("unexpected first-page query %s", req.URL)
						}
					} else if q.Get("limit") != strconv.Itoa(fixture.resumeLimit) || q.Has("page") || q.Get("cursor") != pages[i-2].cursor {
						t.Errorf("unexpected resume query %s", req.URL)
					}
					p := pages[i-1]
					if p.body == budgetRejectionBody {
						w.WriteHeader(http.StatusServiceUnavailable)
					} else {
						w.Header().Set("Content-Type", "application/x-ndjson")
						if p.cursor != "" {
							w.Header().Set(nextCursor, p.cursor)
						}
					}
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
			got, err := findSparseBudgetWalk(t.Context(), client, 8, fixture, launch)
			if !slices.Equal(budgets, tc.wantBudgets) {
				t.Errorf("budgets = %v, want %v", budgets, tc.wantBudgets)
			}
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Errorf("wanted error %q, got %v", tc.problem, err)
			}
			if got.FirstCursorBudget != tc.cursorAt || got.ExactThresholdBudget != tc.exactAt {
				t.Errorf("issued at %d, exact at %d; want %d and %d", got.FirstCursorBudget, got.ExactThresholdBudget, tc.cursorAt, tc.exactAt)
			}
			if got.ResponseByteCap != fixture.byteCap || got.FirstLimit != fixture.firstLimit || got.ResumeLimit != fixture.resumeLimit {
				t.Errorf("fixture bounds = %+v", got)
			}
			rejected := 0
			for _, budget := range tc.wantBudgets {
				if budget < tc.threshold && tc.override[budget] == nil {
					rejected++
				}
			}
			if got.RejectedBudgets != rejected {
				t.Errorf("clean rejections = %d, want %d", got.RejectedBudgets, rejected)
			}
			if tc.firstBroken != 0 {
				failure := got.FirstBrokenContinuation
				if failure == nil || failure.Budget != tc.firstBroken || failure.Page != tc.brokenPage || failure.Limit != fixture.resumeLimit ||
					got.FirstFailure != failure {
					t.Errorf("first broken continuation = %+v, first failure = %+v", failure, got.FirstFailure)
				}
			} else if got.FirstBrokenContinuation != nil {
				t.Errorf("unexpected broken continuation: %+v", got.FirstBrokenContinuation)
			}
			if len(got.Probes) != len(tc.wantBudgets) {
				t.Fatalf("observations = %+v", got.Probes)
			}
			for i, p := range got.Probes {
				if p.Budget != tc.wantBudgets[i] {
					t.Errorf("observation %d: %+v", i, p)
				}
				if p.Budget == tc.cursorAt {
					if !p.issued() || got.FirstRows != 2 || got.Pages != p.Pages || got.Rows != p.Rows {
						t.Errorf("lowest issuing observation = %+v, summary = %+v", p, got)
					}
					if p.Failure == nil && (p.Outcome != probeExhausted || got.Pages != 3 || got.Rows != 4) {
						t.Errorf("incomplete success: %+v", p)
					}
				}
			}
		})
	}
}

func TestFindBudgetWalk(t *testing.T) {
	t.Parallel()
	const (
		firstToken  = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		secondToken = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
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
		want       [4]int
		problem    string
	}{
		{"first budget", 0, 3, []page{first, second, last}, [4]int{1, 0, 3, 3}, ""},
		{"search stops at first cursor", 2, 4, []page{first, second, last}, [4]int{3, 2, 3, 3}, ""},
		{"inclusive upper bound", 2, 3, []page{first, second, last}, [4]int{3, 2, 3, 3}, ""},
		{"no cursor within bound", 3, 3, nil, [4]int{0, 3, 0, 0}, "no first-page cursor"},
		{"partial rejection", 1, 3, []page{{status: 503, body: rejection.body + "{\"t"}}, [4]int{0, 1, 0, 0}, "unclean HTTP 503"},
		{"rejection with empty cursor header", 0, 3, []page{{status: 503, body: rejection.body, cursor: new("")}}, [4]int{}, "unclean HTTP 503"},
		{"rejection with NDJSON content type", 0, 3, []page{{status: 503, body: rejection.body, ndjson: true}}, [4]int{}, "unclean HTTP 503"},
		{"rejection with unrelated error", 0, 3, []page{{status: 503, body: "busy\n"}}, [4]int{}, "unclean HTTP 503"},
		{"success without first cursor", 1, 3, []page{{status: 200, body: firstRow, ndjson: true}}, [4]int{0, 1, 0, 0}, "missing continuation cursor"},
		{"first page exceeds row limit", 0, 3, []page{{status: 200, body: firstRow + secondRow, ndjson: true}}, [4]int{}, "unexpected row"},
		{"out of append order", 0, 3, []page{{status: 200, body: secondRow, cursor: new(firstToken), ndjson: true}}, [4]int{}, "unexpected row"},
		{"empty continuation", 0, 3, []page{first, {status: 200, cursor: new(secondToken), ndjson: true}}, [4]int{1, 0, 1, 1}, "empty query page with next cursor"},
		{"repeated cursor", 0, 3, []page{first, {status: 200, body: secondRow, cursor: new(firstToken), ndjson: true}}, [4]int{1, 0, 1, 1}, "next cursor does not advance pagination"},
		{"early termination", 0, 3, []page{first, {status: 200, body: secondRow, ndjson: true}}, [4]int{1, 0, 1, 1}, "missing continuation cursor"},
		{"rejected second page", 0, 3, []page{first, rejection}, [4]int{1, 0, 1, 1}, "page 2: Bog HTTP 503"},
		{"rejected terminal page", 0, 3, []page{first, second, {status: 410}}, [4]int{1, 0, 2, 2}, "page 3: Bog HTTP 410"},
		{"truncated terminal record", 0, 3, []page{first, second, {status: 200, body: lastRow[:len(lastRow)-2], ndjson: true}}, [4]int{1, 0, 2, 2}, "invalid query record framing"},
		{"missing terminal newline", 0, 3, []page{first, second, {status: 200, body: strings.TrimSuffix(lastRow, "\n"), ndjson: true}}, [4]int{1, 0, 2, 2}, "invalid query record framing"},
		{"truncated HTTP body", 0, 3, []page{first, second, {status: 200, body: lastRow, ndjson: true, truncate: true}}, [4]int{1, 0, 2, 2}, "unexpected EOF"},
		{"terminal cursor", 0, 3, []page{first, second, {status: 200, body: lastRow, cursor: new(strings.Repeat("C", 44)), ndjson: true}}, [4]int{1, 0, 2, 2}, "issued a cursor after the final row"},
		{"empty terminal cursor header", 0, 3, []page{first, second, {status: 200, body: lastRow, cursor: new(""), ndjson: true}}, [4]int{1, 0, 2, 2}, "invalid next cursor header"},
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
						q.Get("end") != fixtureEnd || q.Get("limit") != "1" || q.Get("contains") != "" {
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
			if legacyBudgetCounts(got) != tc.want {
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

func TestBudgetWalkRejectsNonAdjacentCursorRepeat(t *testing.T) {
	t.Parallel()
	tokens := []string{strings.Repeat("A", 44), strings.Repeat("B", 44), strings.Repeat("A", 44)}
	rows := []row{{fixtureStart, "a"}, {fixtureStart, "b"}, {fixtureStart, "c"}, {fixtureStart, "d"}}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		i := int(requests.Add(1)) - 1
		if i >= len(tokens) {
			t.Errorf("unexpected request after repeated cursor: %s %s", req.Method, req.URL)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if i > 0 && req.URL.Query().Get("cursor") != tokens[i-1] {
			t.Errorf("unexpected resume cursor in %s", req.URL)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set(nextCursor, tokens[i])
		if err := json.NewEncoder(w).Encode(rows[i]); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	pager, err := newReleasePager(server.URL, server.Client(), 0)
	if err != nil {
		t.Fatal(err)
	}
	fixture := budgetFixture{want: rows, firstLimit: 1, resumeLimit: 1, firstRows: 1}
	probe := checkBudgetWalk(t.Context(), pager, 1, fixture)
	if probe.Failure == nil || probe.Failure.Detail != "page 3: repeated a continuation cursor" {
		t.Fatalf("wanted cursor repeat on page 3, got %v", probe.Failure)
	}
	if probe.Outcome != probeIssued || probe.Pages != 2 || probe.Rows != 2 || requests.Load() != 3 {
		t.Errorf("evidence advanced past the repeated cursor: probe=%+v, requests=%d", probe, requests.Load())
	}
}

func TestSparseBudgetWalkStopsOnRunFailure(t *testing.T) {
	t.Parallel()
	fixture := sparseBudgetFixture()
	for _, tc := range []struct {
		name            string
		cancel          bool
		startup         bool
		brokenResume    bool
		wantOutcome     budgetProbeOutcome
		wantPages       int
		wantFailurePage int
	}{
		{name: "startup", startup: true, wantOutcome: probeIndeterminate},
		{name: "cleanup after clean rejection", wantOutcome: probeRejected},
		{name: "cleanup after broken resume", brokenResume: true, wantOutcome: probeIssued, wantPages: 1, wantFailurePage: 2},
		{name: "cancellation after broken resume", brokenResume: true, cancel: true, wantOutcome: probeIssued, wantPages: 1, wantFailurePage: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/ingest" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if tc.brokenResume && req.URL.Query().Get("page") == "1" {
					w.Header().Set("Content-Type", "application/x-ndjson")
					w.Header().Set(nextCursor, strings.Repeat("A", 44))
					enc := json.NewEncoder(w)
					for _, r := range fixture.want[:fixture.firstRows] {
						if err := enc.Encode(r); err != nil {
							t.Error(err)
							return
						}
					}
					return
				}
				http.Error(w, strings.TrimSuffix(budgetRejectionBody, "\n"), http.StatusServiceUnavailable)
			}))
			defer server.Close()
			runErr := errors.New("candidate process failed")
			launches := 0
			launch := func(_ context.Context, _ int, check func(string) error) error {
				launches++
				if !tc.startup {
					if err := check(server.URL); err != nil {
						t.Errorf("unexpected fixture setup error: %v", err)
					}
				}
				if tc.cancel {
					cancel()
					return nil
				}
				return runErr
			}
			got, err := findSparseBudgetWalk(ctx, server.Client(), 8, fixture, launch)
			expectedErr := runErr
			if tc.cancel {
				expectedErr = context.Canceled
			}
			if !errors.Is(err, expectedErr) || launches != 1 {
				t.Fatalf("launches = %d, error = %v, want one launch and %v", launches, err, expectedErr)
			}
			if len(got.Probes) != 1 || got.Probes[0].Outcome != tc.wantOutcome || got.Pages != tc.wantPages ||
				got.ExactThresholdBudget != 0 || got.FirstFailure == nil || got.FirstFailure.Budget != 4 ||
				got.FirstFailure.Page != tc.wantFailurePage {
				t.Fatalf("unexpected evidence: %+v", got)
			}
			if tc.brokenResume && (got.FirstBrokenContinuation != got.FirstFailure || got.FirstCursorBudget != 4 ||
				!strings.HasPrefix(err.Error(), "fixed-budget walk at budget 4: page 2:")) {
				t.Errorf("lost the earlier continuation failure: evidence=%+v, error=%v", got, err)
			}
		})
	}
}
