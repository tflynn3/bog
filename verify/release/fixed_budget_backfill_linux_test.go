package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	querymodel "bog/verify/query-model"
)

const backfillByteCap = 96

var errBackfillOpeningBudget = errors.New("clean opening work-budget rejection")

type backfillCase struct {
	name          string
	startBudget   int
	fixture       budgetFixture
	beforeResumes [2]row
}

func fixedBudgetBackfillCases() []backfillCase {
	tied := budgetFixture{
		seed:       []row{{fixtureStart, "c"}, {fixtureStart, "a"}, {fixtureStart, "b"}},
		firstLimit: 1, resumeLimit: 1, firstRows: 1, byteCap: backfillByteCap,
	}
	duplicates := tied
	duplicates.seed = []row{{fixtureStart, "c"}, {fixtureStart, "c"}, {fixtureStart, "b"}}
	tiedBackfill := [2]row{{fixtureStart, "new tie"}, {fixtureStart, "new final tie"}}
	sparse := sparseBudgetFixture()
	return []backfillCase{
		{"tied", 11, tied, tiedBackfill},
		{"duplicates", 11, duplicates, tiedBackfill},
		{"sparse_byte_trim", 314, sparse, [2]row{{fixtureStart, "@new tie"}, {sparse.want[3].TS, "@new late tie"}}},
	}
}

func TestReleaseFixedBudgetBackfill(t *testing.T) {
	binary := os.Getenv("BOG_RELEASE_BINARY")
	if binary == "" {
		t.Skip("pinned release candidate required")
	}
	id, err := readIdentity(binary, os.Getenv("BOG_RELEASE_REVISION"), os.Getenv("BOG_RELEASE_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate revision=%s binary_sha256=%s fixture=%s", id.Revision, id.BinarySHA256, id.Fixture)

	ctx, cancel := context.WithTimeout(t.Context(), budgetWalkDeadline)
	defer cancel()
	for _, tc := range fixedBudgetBackfillCases() {
		t.Run(tc.name, func(t *testing.T) {
			raw := &http.Client{
				Timeout:       5 * time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			defer raw.CloseIdleConnections()
			launch := func(ctx context.Context, budget int, check func(context.Context, string) error) error {
				flags := []string{
					"-query-concurrency=1",
					"-query-work-budget=" + strconv.Itoa(budget),
					"-query-max-response-bytes=" + strconv.Itoa(tc.fixture.byteCap),
				}
				return serve(ctx, binary, raw, flags, check)
			}
			if err := searchBackfillBudget(ctx, t, raw, tc, budgetWalkMax, launch); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func searchBackfillBudget(ctx context.Context, t *testing.T, raw *http.Client, tc backfillCase, maxBudget int, launch budgetLaunch) error {
	t.Helper()
	for budget := tc.startBudget; ; budget = min(2*budget, maxBudget) {
		if err := ctx.Err(); err != nil {
			return err
		}
		var checked, rejected bool
		err := launch(ctx, budget, func(ctx context.Context, base string) error {
			checked = true
			stats, err := checkFixedBudgetBackfill(ctx, t, raw, base, tc)
			rejected = errors.Is(err, errBackfillOpeningBudget)
			t.Logf("work_budget=%d response_byte_cap=%d clean_opening_refusal=%t held=%+v",
				budget, tc.fixture.byteCap, rejected, stats)
			if rejected {
				// Let the launcher finish its listener and cleanup checks.
				return nil
			}
			return err
		})
		if err != nil {
			return fmt.Errorf("work budget %d: %w", budget, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !checked {
			return fmt.Errorf("work budget %d: candidate probe did not run", budget)
		}
		if !rejected {
			return nil
		}
		if budget == maxBudget {
			return fmt.Errorf("no cursor issued at sampled work budgets from %d through %d", tc.startBudget, maxBudget)
		}
	}
}

func checkFixedBudgetBackfill(ctx context.Context, t *testing.T, raw *http.Client, base string, tc backfillCase) (querymodel.WireStats, error) {
	t.Helper()
	fixture := tc.fixture
	pager, err := newReleasePager(base, raw, fixture.byteCap)
	if err != nil {
		return querymodel.WireStats{}, err
	}
	var ledger querymodel.Log
	acknowledge := func(ctx context.Context, batch []row) (int, error) {
		rows := make([]querymodel.Row, len(batch))
		for i, item := range batch {
			stamp, err := time.Parse(time.RFC3339Nano, item.TS)
			if err != nil {
				return 0, fmt.Errorf("parse fixture row %d: %w", i+1, err)
			}
			rows[i] = querymodel.Row{Time: stamp, Line: item.Line}
		}
		status, err := ingest(ctx, raw, base, batch)
		if err == nil {
			ledger.Append(rows)
		}
		return status, err
	}
	if _, err := acknowledge(ctx, fixture.seed); err != nil {
		return querymodel.WireStats{}, fmt.Errorf("seed backfill fixture: %w", err)
	}

	appendBackfill := func(ctx context.Context, resume int) error {
		status, err := acknowledge(ctx, tc.beforeResumes[resume-1:resume])
		t.Logf("backfill before resume=%d ingest_http_status=%d", resume, status)
		if err != nil {
			return fmt.Errorf("backfill before resume %d: %w", resume, err)
		}
		return nil
	}
	walk := querymodel.WireWalk{
		Filter:        querymodel.Filter{Start: pager.query.Start, End: pager.query.End, Contains: fixture.filter},
		RowLimits:     []int{fixture.firstLimit, fixture.resumeLimit, fixture.resumeLimit},
		FirstPageRows: fixture.firstRows,
		Limits: querymodel.WireLimits{
			MaxPages: 3, MaxPageBytes: int64(fixture.byteCap), Timeout: 8 * time.Second,
		},
	}
	requests := 0
	fetch := func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
		requests++
		// The oracle has accepted the first resume when it requests the
		// terminal page. Interleave another acknowledged append at that cut.
		if requests == 3 {
			if err := appendBackfill(ctx, 2); err != nil {
				return querymodel.WirePage{}, err
			}
		}
		page, res, err := pager.fetch(ctx, req.Filter.Contains, req.Limit, req.Cursor)
		if requests == 1 && req.Cursor == "" && res != nil && res.StatusCode == http.StatusServiceUnavailable {
			if rejectionErr := checkBudgetRejection(res, err); rejectionErr != nil {
				return querymodel.WirePage{}, rejectionErr
			}
			return querymodel.WirePage{}, errBackfillOpeningBudget
		}
		if err != nil {
			return querymodel.WirePage{}, err
		}
		if res.ContentLength < 0 || res.Header.Get("Content-Length") == "" {
			return querymodel.WirePage{}, errors.New("missing exact Content-Length on query response")
		}
		rows := make([]querymodel.Row, len(page.Rows))
		for i, item := range page.Rows {
			rows[i] = querymodel.Row{Time: item.TS, Line: item.Line}
		}
		return querymodel.WirePage{Rows: rows, NextCursor: page.NextCursor, BodyBytes: res.ContentLength}, nil
	}
	stats, err := querymodel.VerifyWireWalk(ctx, &ledger, walk, fetch, func(ctx context.Context, _ string) error {
		return appendBackfill(ctx, 1)
	})
	if err == nil && stats.Pages != walk.Limits.MaxPages {
		err = fmt.Errorf("expected to exhaust on page %d, exhausted on page %d", walk.Limits.MaxPages, stats.Pages)
	}
	return stats, err
}

func TestFixedBudgetBackfillRetryBoundary(t *testing.T) {
	t.Parallel()
	const ndjson = "Content-Type: application/x-ndjson\r\n"
	const plain = "Content-Type: text/plain\r\n"
	token1, token2 := strings.Repeat("A", 44), strings.Repeat("B", 44)
	body := func(line string) string {
		return fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", fixtureStart, line)
	}
	open := lengthResponse(200, ndjson+nextCursor+": "+token1+"\r\n", body("c"))
	resume := lengthResponse(200, ndjson+nextCursor+": "+token2+"\r\n", body("a"))
	terminal := lengthResponse(200, ndjson, body("b"))
	refusal := lengthResponse(503, plain, budgetRejectionBody)
	const ack = "HTTP/1.1 204 No Content\r\n\r\n"
	for _, tc := range []struct {
		name         string
		firstAttempt []string
		afterRefusal bool
		cleanupErr   error
		want         string
	}{
		{name: "clean opening refusal", firstAttempt: []string{ack, refusal}, afterRefusal: true},
		{name: "search ceiling", firstAttempt: []string{ack, refusal}, want: "no cursor issued"},
		{name: "cleanup after clean refusal", firstAttempt: []string{ack, refusal},
			cleanupErr: errors.New("fixture removal failed"), want: "fixture removal failed"},
		{name: "unclean opening refusal",
			firstAttempt: []string{ack, wireResponse(503, plain, budgetRejectionBody)}, want: "unclean HTTP 503"},
		{name: "first resume refused", firstAttempt: []string{ack, open, ack, refusal}, want: "page 2"},
		{name: "terminal resume refused", firstAttempt: []string{ack, open, ack, resume, ack, refusal}, want: "page 3"},
		{name: "backfill unacknowledged", firstAttempt: []string{ack, open, refusal}, want: "backfill before resume 1"},
		{name: "incomplete first page",
			firstAttempt: []string{ack, lengthResponse(200, ndjson, strings.TrimSuffix(body("c"), "\n"))},
			want:         "invalid query record framing"},
		{name: "backfill leaked into capture",
			firstAttempt: []string{ack, open, ack, resume, ack, lengthResponse(200, ndjson, body("new tie"))},
			want:         "append ordinal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := fixedBudgetBackfillCases()[0]
			fullWalk := []string{ack, open, ack, resume, ack, terminal}
			var budgets []int
			raw := new(http.Client)
			launch := func(ctx context.Context, budget int, check func(context.Context, string) error) error {
				budgets = append(budgets, budget)
				replies := tc.firstAttempt
				if len(budgets) > 1 && tc.afterRefusal {
					replies = fullWalk
				}
				raw.Transport = scriptedBackfillTransport(t, fixture, token1, token2, replies)
				return errors.Join(check(ctx, "http://127.0.0.1"), tc.cleanupErr)
			}
			maxBudget := 44
			wantBudgets := []int{11}
			switch {
			case tc.afterRefusal:
				wantBudgets = append(wantBudgets, 22)
			case tc.name == "search ceiling":
				maxBudget = 25
				wantBudgets = append(wantBudgets, 22, 25)
			}
			err := searchBackfillBudget(t.Context(), t, raw, fixture, maxBudget, launch)
			if (tc.want == "" && err != nil) || (tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want))) {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
			if !slices.Equal(budgets, wantBudgets) {
				t.Errorf("budgets = %v, want %v", budgets, wantBudgets)
			}
		})
	}
}

func scriptedBackfillTransport(t *testing.T, tc backfillCase, token1, token2 string, replies []string) http.RoundTripper {
	t.Helper()
	batches := [][]row{tc.fixture.seed, tc.beforeResumes[:1], tc.beforeResumes[1:]}
	requests := [...]struct {
		path   string
		cursor string
	}{
		{path: "/ingest"}, {path: "/query"},
		{path: "/ingest"}, {path: "/query", cursor: token1},
		{path: "/ingest"}, {path: "/query", cursor: token2},
	}
	step := 0
	t.Cleanup(func() {
		if step != len(replies) {
			t.Errorf("made %d requests, want %d", step, len(replies))
		}
	})
	return budgetRoundTripper(func(req *http.Request) (_ *http.Response, err error) {
		if req.Body != nil {
			defer func() { err = errors.Join(err, req.Body.Close()) }()
		}
		i := step
		step++
		if i >= len(replies) || req.URL.Path != requests[i].path {
			return nil, fmt.Errorf("unexpected request %d: %s %s", step, req.Method, req.URL.Path)
		}
		if req.URL.Path == "/ingest" {
			if req.Method != http.MethodPost || req.Body == nil {
				return nil, errors.New("expected POST with an ingest body")
			}
			var batch []row
			decoder := json.NewDecoder(req.Body)
			for {
				var item row
				err := decoder.Decode(&item)
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				batch = append(batch, item)
			}
			if !slices.Equal(batch, batches[i/2]) {
				return nil, fmt.Errorf("ingest batch %d = %+v, want %+v", i/2, batch, batches[i/2])
			}
		} else {
			q := req.URL.Query()
			cursor := requests[i].cursor
			wantPage := ""
			if cursor == "" {
				wantPage = "1"
			}
			if req.Method != http.MethodGet || q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd ||
				q.Get("limit") != "1" || q.Get("cursor") != cursor || q.Get("page") != wantPage {
				return nil, fmt.Errorf("unexpected query at request %d: %s %s", step, req.Method, req.URL)
			}
		}
		return http.ReadResponse(bufio.NewReader(strings.NewReader(replies[i])), req)
	})
}
