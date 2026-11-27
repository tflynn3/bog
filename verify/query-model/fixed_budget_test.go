package querymodel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/client"
	model "bog/verify/query-model"
)

func TestFixedBudgetCasesOverHTTP(t *testing.T) {
	t.Parallel()
	type expectation struct {
		budget, firstLimit, firstRows int
		literal                       string
		matches                       []string
		seedCount                     int
		backfill, finalBackfill       string
		bodyBytes                     int64
	}
	want := map[string]expectation{
		"tied":       {11, 1, 1, "", []string{"c", "a", "b"}, 3, "z", "new final tie", 123},
		"duplicates": {11, 1, 1, "", []string{"c", "c", "b"}, 3, "z", "new final tie", 123},
		"sparse":     {314, 4, 2, "@", []string{"@last", "@tied", "@next", "@late"}, 46, "@new", "@new late tie", 180},
	}
	cases := model.FixedBudgetCases()
	for _, c := range cases {
		expected, ok := want[c.Fixture]
		if !ok {
			t.Fatalf("unexpected or repeated fixture %q", c.Fixture)
		}
		delete(want, c.Fixture)
		t.Run(c.Fixture, func(t *testing.T) {
			t.Parallel()
			if c.ReferenceWorkBudget != expected.budget || c.FirstLimit != expected.firstLimit ||
				c.FirstRows != expected.firstRows || c.ResumeLimit != 1 || c.ResponseByteCap != 96 {
				t.Fatalf("case parameters: %+v", c)
			}
			p := &fixturePeer{captures: make(map[string]scriptedCapture), trimCap: int(c.ResponseByteCap)}
			server := httptest.NewServer(p)
			t.Cleanup(server.Close)
			clientConn, fetch := useWireClient(t, server.URL, c.ResponseByteCap)
			acknowledge := func(ctx context.Context, rows []model.Row) error {
				var body bytes.Buffer
				enc := json.NewEncoder(&body)
				for _, row := range rows {
					if err := enc.Encode(httpFixtureRow{row.Time, row.Line}); err != nil {
						return err
					}
				}
				outcome, err := clientConn.Ingest(ctx, body.Bytes())
				if err != nil {
					return err
				}
				if outcome != client.IngestCommitted {
					return fmt.Errorf("unexpected ingest outcome %v", outcome)
				}
				return nil
			}
			var matches []string
			hookCalls := 0
			var openingCursor string
			result, err := model.RunFixedBudgetCase(t.Context(), c, acknowledge, func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
				start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
				if !req.Filter.Start.Equal(start) || !req.Filter.End.Equal(start.Add(10*time.Minute)) || req.Filter.Contains != expected.literal {
					t.Errorf("unexpected query filter %+v", req.Filter)
				}
				page, err := fetch(ctx, req)
				if req.Cursor == "" {
					openingCursor = page.NextCursor
				}
				for _, row := range page.Rows {
					matches = append(matches, row.Line)
				}
				return page, err
			}, func(ctx context.Context, cursor string) error {
				hookCalls++
				if _, ok := ctx.Deadline(); !ok || cursor == "" || cursor != openingCursor {
					return errors.New("post-capture hook lacks a deadline or the issuing token")
				}
				p.mu.Lock()
				defer p.mu.Unlock()
				if len(p.requests) != 3 || len(p.rows) != expected.seedCount+1 ||
					p.rows[expected.seedCount].Line != expected.backfill {
					return errors.New("first backfill was not acknowledged before publication")
				}
				return nil
			})
			trimmed := 0
			if c.FirstRows < c.FirstLimit {
				trimmed = 1
			}
			wantStats := model.WireStats{Pages: 3, Rows: len(expected.matches), BodyBytes: expected.bodyBytes, ByteTrimmedPages: trimmed}
			if err != nil || hookCalls != 1 || result.Outcome != model.FixedBudgetExhausted || result.WireStats != wantStats ||
				result.Page != 3 || result.Limit != 1 || result.HTTPStatus != http.StatusOK || !slices.Equal(matches, expected.matches) {
				t.Fatalf("result=%+v matches=%q error=%v", result, matches, err)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			resume := fmt.Sprintf("resume:%s:1", expected.literal)
			sequence := []string{"ingest", fmt.Sprintf("first:%s:%d", expected.literal, c.FirstLimit), "ingest", resume, "ingest", resume}
			if !slices.Equal(p.requests, sequence) {
				t.Errorf("requests %v, want %v", p.requests, sequence)
			}
			finalTie := time.Duration(0)
			if c.Fixture == "sparse" {
				finalTie = 40 * time.Second
			}
			if len(p.rows) != expected.seedCount+2 || p.rows[expected.seedCount].Line != expected.backfill ||
				!p.rows[expected.seedCount].Time.Equal(p.rows[0].Time) ||
				p.rows[expected.seedCount+1].Line != expected.finalBackfill ||
				!p.rows[expected.seedCount+1].Time.Equal(p.rows[0].Time.Add(finalTie)) {
				t.Errorf("unexpected acknowledged fixture: %v", p.rows)
			}
		})
	}
	if len(want) != 0 {
		t.Fatalf("missing fixtures: %v", want)
	}
}

func TestFixedBudgetCaseFailures(t *testing.T) {
	t.Parallel()
	budgetError := errors.New("query work budget exhausted")
	clean503 := &model.FixedBudgetHTTPError{StatusCode: http.StatusServiceUnavailable, CleanBudgetRejection: true, Cause: budgetError}
	status503 := &model.FixedBudgetHTTPError{StatusCode: http.StatusServiceUnavailable, Cause: errors.New("query capacity exhausted")}
	expired := &model.FixedBudgetHTTPError{StatusCode: http.StatusGone, Cause: errors.New("query capture expired")}
	badFraming := errors.New("interrupted framing")
	stamp := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	row := func(line string) model.Row { return model.Row{Time: stamp, Line: line} }
	const firstToken = "opaque-first"
	const nextToken = "opaque-next"
	for _, tc := range []struct {
		name, diagnostic string
		page             int
		response         model.WirePage
		fetchErr         error
		status           int
		clean            bool
	}{
		{name: "clean opening refusal", page: 1, fetchErr: clean503, status: 503, clean: true},
		{name: "capacity is not work refusal", page: 1, fetchErr: status503, status: 503, diagnostic: "query capacity exhausted"},
		{name: "cursor on refusal", page: 1, response: model.WirePage{NextCursor: firstToken}, fetchErr: clean503, status: 503, diagnostic: "fetch page"},
		{name: "body on refusal", page: 1, response: model.WirePage{BodyBytes: 1}, fetchErr: clean503, status: 503, diagnostic: "fetch page"},
		{name: "compound refusal", page: 1, fetchErr: errors.Join(clean503, badFraming), status: 503, diagnostic: "interrupted framing"},
		{name: "continuation refusal", page: 2, fetchErr: clean503, status: 503, diagnostic: "query work budget exhausted"},
		{name: "expired continuation", page: 2, fetchErr: expired, status: 410, diagnostic: "query capture expired"},
		{name: "transport failure", page: 2, fetchErr: badFraming, diagnostic: "interrupted framing"},
		{name: "malformed success", page: 2, fetchErr: &model.FixedBudgetHTTPError{StatusCode: 200, Cause: badFraming}, status: 200, diagnostic: "interrupted framing"},
		{name: "empty continuation stalls", page: 2, response: model.WirePage{NextCursor: nextToken}, status: 200, diagnostic: "empty page issued a continuation"},
		{name: "repeated token stalls", page: 2, response: model.WirePage{Rows: []model.Row{row("a")}, BodyBytes: 41, NextCursor: firstToken}, status: 200, diagnostic: "repeated continuation"},
		{name: "missing rows", page: 2, response: model.WirePage{Rows: []model.Row{row("a")}, BodyBytes: 41}, status: 200, diagnostic: "missing continuation"},
		{name: "duplicated row", page: 2, response: model.WirePage{Rows: []model.Row{row("c")}, BodyBytes: 41}, status: 200, diagnostic: "append ordinal 1"},
		{name: "out of order row", page: 2, response: model.WirePage{Rows: []model.Row{row("b")}, BodyBytes: 41}, status: 200, diagnostic: "append ordinal 1"},
		{name: "backfill leaked", page: 3, response: model.WirePage{Rows: []model.Row{row("z")}, BodyBytes: 41}, status: 200, diagnostic: "append ordinal 2"},
		{name: "cursor after exhaustion", page: 3, response: model.WirePage{Rows: []model.Row{row("b")}, BodyBytes: 41, NextCursor: "extra"}, status: 200, diagnostic: "after terminal exhaustion"},
		{name: "oversized body", page: 2, response: model.WirePage{Rows: []model.Row{row("a")}, BodyBytes: 97}, status: 200, diagnostic: "encoded body has 97 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := model.FixedBudgetCases()[0]
			acks, requests := 0, 0
			result, err := model.RunFixedBudgetCase(t.Context(), c, func(context.Context, []model.Row) error {
				acks++
				if acks != requests+1 {
					t.Fatalf("append %d after %d queries", acks, requests)
				}
				return nil
			}, func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
				requests++
				cursor := []string{"", firstToken, nextToken}
				if requests > tc.page || req.Cursor != cursor[requests-1] || req.Limit != 1 || req.MaxPageBytes != 96 {
					t.Fatalf("unexpected request %d: %+v", requests, req)
				}
				if requests == tc.page {
					return tc.response, tc.fetchErr
				}
				return model.WirePage{Rows: []model.Row{row([]string{"c", "a"}[requests-1])}, BodyBytes: 41, NextCursor: cursor[requests]}, nil
			}, func(context.Context, string) error {
				if tc.page == 1 {
					t.Error("post-capture hook ran before a verified opening page")
				}
				return nil
			})
			wantOutcome := model.FixedBudgetIndeterminate
			if tc.page > 1 {
				wantOutcome = model.FixedBudgetIssuedCursor
			}
			if tc.clean {
				wantOutcome = model.FixedBudgetCleanRejection
			}
			if result.Outcome != wantOutcome || result.HTTPStatus != tc.status || result.Page != tc.page || result.Limit != 1 ||
				result.Pages != tc.page-1 || result.Rows != tc.page-1 || acks != tc.page || requests != tc.page {
				t.Fatalf("result=%+v append calls=%d fetch calls=%d error=%v", result, acks, requests, err)
			}
			if tc.clean {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || tc.fetchErr != nil && !errors.Is(err, tc.fetchErr) {
				t.Fatalf("unexpected error %v; expected %q and cause %v", err, tc.diagnostic, tc.fetchErr)
			}
			for _, fragment := range []string{tc.diagnostic, `fixture="tied"`, "reference_work_budget=11", "response_byte_cap=96", "first_limit=1", "resume_limit=1", fmt.Sprintf("page %d limit=1", tc.page)} {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("error lacks %q: %v", fragment, err)
				}
			}
			if tc.status != 0 && !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", tc.status)) {
				t.Errorf("error lacks HTTP status: %v", err)
			}
			if strings.Contains(err.Error(), firstToken) || strings.Contains(err.Error(), nextToken) {
				t.Errorf("error disclosed an opaque token: %v", err)
			}
		})
	}
}

func TestFixedBudgetCaseStopsOnIngestFailure(t *testing.T) {
	t.Parallel()
	for _, failureAt := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(failureAt), func(t *testing.T) {
			t.Parallel()
			c := model.FixedBudgetCases()[0]
			want := errors.New("ambiguous acknowledgement")
			acks, queries := 0, 0
			var seed []model.Row
			result, err := model.RunFixedBudgetCase(t.Context(), c, func(_ context.Context, batch []model.Row) error {
				acks++
				if acks == failureAt {
					return want
				}
				if acks == 1 {
					seed = batch
				}
				return nil
			}, func(context.Context, model.WireRequest) (model.WirePage, error) {
				queries++
				return model.WirePage{Rows: seed[queries-1 : queries], BodyBytes: 41, NextCursor: fmt.Sprint("cursor-", queries)}, nil
			})
			stage := []string{"seed", "backfill before resume 1", "backfill before resume 2"}[failureAt-1]
			if !errors.Is(err, want) || !strings.Contains(err.Error(), "acknowledge "+stage) || acks != failureAt || queries != failureAt-1 ||
				result.Pages != queries || result.Page != queries {
				t.Fatalf("result=%+v acks=%d queries=%d error=%v", result, acks, queries, err)
			}
		})
	}
}

func TestFixedBudgetCaseCancellationDuringFetch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	acks := 0
	result, err := model.RunFixedBudgetCase(ctx, model.FixedBudgetCases()[0], func(context.Context, []model.Row) error {
		acks++
		return nil
	}, func(ctx context.Context, _ model.WireRequest) (model.WirePage, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("fetch has no deadline")
		}
		cancel()
		return model.WirePage{}, &model.FixedBudgetHTTPError{StatusCode: 503, CleanBudgetRejection: true}
	})
	if !errors.Is(err, context.Canceled) || result.Outcome != model.FixedBudgetIndeterminate || result.Page != 1 || result.HTTPStatus != 503 || acks != 1 {
		t.Fatalf("result=%+v acks=%d error=%v", result, acks, err)
	}
}

func TestFixedBudgetCaseStopsOnPostCaptureFailure(t *testing.T) {
	t.Parallel()
	for _, cancelHook := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelHook), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			const token = "held-opaque-token"
			want := fmt.Errorf("publication failed for %s", token)
			if cancelHook {
				want = context.Canceled
			}
			var seed model.Row
			acks, fetches, hooks := 0, 0, 0
			result, err := model.RunFixedBudgetCase(ctx, model.FixedBudgetCases()[0], func(_ context.Context, batch []model.Row) error {
				acks++
				if acks == 1 {
					seed = batch[0]
				}
				return nil
			}, func(context.Context, model.WireRequest) (model.WirePage, error) {
				fetches++
				return model.WirePage{Rows: []model.Row{seed}, NextCursor: token, BodyBytes: 41}, nil
			}, func(ctx context.Context, _ string) error {
				hooks++
				if cancelHook {
					cancel()
					<-ctx.Done()
					return nil
				}
				return want
			})
			if !errors.Is(err, want) || strings.Contains(err.Error(), token) ||
				result.Outcome != model.FixedBudgetIssuedCursor || result.Page != 1 || result.Pages != 1 ||
				acks != 2 || fetches != 1 || hooks != 1 {
				t.Fatalf("result=%+v acks=%d fetches=%d hooks=%d error=%v", result, acks, fetches, hooks, err)
			}
		})
	}
}

func TestFixedBudgetSparseRequiresTrimmedOpeningPage(t *testing.T) {
	t.Parallel()
	c := model.FixedBudgetCases()[2]
	var first model.Row
	acks, requests := 0, 0
	result, err := model.RunFixedBudgetCase(t.Context(), c, func(_ context.Context, batch []model.Row) error {
		acks++
		first = batch[0]
		return nil
	}, func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
		requests++
		if req.Limit != 4 || req.Cursor != "" {
			t.Fatalf("unexpected query %+v", req)
		}
		return model.WirePage{Rows: []model.Row{first}, BodyBytes: 45, NextCursor: "cursor"}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "first page has 1 rows, want 2") ||
		result.Outcome != model.FixedBudgetIndeterminate || result.Page != 1 || result.Limit != 4 ||
		result.HTTPStatus != http.StatusOK || result.Pages != 0 || acks != 1 || requests != 1 {
		t.Fatalf("result=%+v acks=%d requests=%d error=%v", result, acks, requests, err)
	}
}

func TestFixedBudgetCaseValidatesBeforeIngest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*model.FixedBudgetCase)
	}{
		{"unknown fixture", func(c *model.FixedBudgetCase) { c.Fixture = "unknown" }},
		{"zero reference budget", func(c *model.FixedBudgetCase) { c.ReferenceWorkBudget = 0 }},
		{"zero cap", func(c *model.FixedBudgetCase) { c.ResponseByteCap = 0 }},
		{"opening rows exceed limit", func(c *model.FixedBudgetCase) { c.FirstRows = c.FirstLimit + 1 }},
		{"resume increases limit", func(c *model.FixedBudgetCase) { c.ResumeLimit = c.FirstLimit + 1 }},
		{"zero resume limit", func(c *model.FixedBudgetCase) { c.ResumeLimit = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := model.FixedBudgetCases()[0]
			tc.change(&c)
			result, err := model.RunFixedBudgetCase(t.Context(), c, func(context.Context, []model.Row) error {
				t.Fatal("invalid case attempted ingest")
				return nil
			}, func(context.Context, model.WireRequest) (model.WirePage, error) {
				t.Fatal("invalid case attempted query")
				return model.WirePage{}, nil
			})
			if err == nil || !strings.Contains(err.Error(), "before opening page") || result.Outcome != model.FixedBudgetIndeterminate || result.Page != 0 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}
