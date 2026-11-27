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

const backfillPublicationTimeout = 5 * time.Second

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
	t.Setenv("BOG_CAPACITY_CONTROL", "1")

	for _, tc := range querymodel.FixedBudgetCases() {
		t.Run(tc.Fixture, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), budgetWalkDeadline)
			defer cancel()
			raw := &http.Client{
				Transport:     http.DefaultTransport.(*http.Transport).Clone(),
				Timeout:       5 * time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			defer raw.CloseIdleConnections()
			launch := func(ctx context.Context, budget int, check func(context.Context, string) error) error {
				flags := []string{
					"-query-concurrency=1",
					"-query-work-budget=" + strconv.Itoa(budget),
					"-query-max-response-bytes=" + strconv.FormatInt(tc.ResponseByteCap, 10),
				}
				return serve(ctx, binary, raw, flags, check)
			}
			if err := searchBackfillBudget(ctx, t, raw, tc, budgetWalkMax, launch); err != nil {
				t.Fatal(err)
			}

			// A fresh capture of the larger ledger may need more work than the
			// seed walk. Use a separate fixture at the search ceiling.
			if err := launch(ctx, budgetWalkMax, func(ctx context.Context, base string) error {
				result, err := checkFixedBudgetBackfill(ctx, t, raw, base, tc, true)
				if err == nil && result.Outcome != querymodel.FixedBudgetExhausted {
					return fmt.Errorf("expected a complete held and fresh walk, got %s", result.Outcome)
				}
				return err
			}); err != nil {
				t.Fatalf("generation replacement at work budget %d: %v", budgetWalkMax, err)
			}
		})
	}
}

func searchBackfillBudget(ctx context.Context, t *testing.T, raw *http.Client, tc querymodel.FixedBudgetCase, maxBudget int, launch budgetLaunch) error {
	t.Helper()
	if maxBudget < 1 || tc.ReferenceWorkBudget < 1 {
		return errors.New("work budgets must be positive")
	}
	low, high := 1, maxBudget
	issuing, rejected := 0, 0
	// Probe the historical budget first, then bisect. Only a clean opening
	// refusal can move the lower bound. Every issuing probe must exhaust.
	budget := min(tc.ReferenceWorkBudget, high)
search:
	for low <= high {
		if err := ctx.Err(); err != nil {
			return err
		}
		var result querymodel.FixedBudgetResult
		err := launch(ctx, budget, func(ctx context.Context, base string) error {
			var err error
			result, err = checkFixedBudgetBackfill(ctx, t, raw, base, tc, false)
			t.Logf("work_budget=%d response_byte_cap=%d held=%+v", budget, tc.ResponseByteCap, result)
			return err
		})
		if err != nil {
			return fmt.Errorf("work budget %d: %w", budget, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		switch result.Outcome {
		case querymodel.FixedBudgetCleanRejection:
			rejected = budget
			if budget == high {
				break search
			}
			low = budget + 1
		case querymodel.FixedBudgetExhausted:
			issuing, high = budget, budget-1
		default:
			return fmt.Errorf("work budget %d: candidate probe has no conclusive result (%q)", budget, result.Outcome)
		}
		budget = low + (high-low)/2
	}
	if issuing == 0 {
		return fmt.Errorf("no cursor issued at work budgets through %d", maxBudget)
	}
	if rejected != issuing-1 {
		return fmt.Errorf("issuing work budget %d lacks a clean rejection at budget %d", issuing, issuing-1)
	}
	if issuing == 1 {
		t.Log("issuing_work_budget=1 (budget floor)")
	} else {
		t.Logf("issuing_work_budget=%d adjacent_lower_clean_rejection=%d", issuing, rejected)
	}
	return nil
}

func checkFixedBudgetBackfill(ctx context.Context, t *testing.T, raw *http.Client, base string, tc querymodel.FixedBudgetCase, replace bool) (querymodel.FixedBudgetResult, error) {
	t.Helper()
	pager, err := newReleasePager(base, raw, int(tc.ResponseByteCap))
	if err != nil {
		return querymodel.FixedBudgetResult{}, err
	}
	var ledger querymodel.Log
	var predecessor backfillCompaction
	acknowledge := func(ctx context.Context, batch []querymodel.Row) error {
		rows := make([]row, len(batch))
		for i, item := range batch {
			rows[i] = row{TS: item.Time.UTC().Format(time.RFC3339Nano), Line: item.Line}
		}
		status, err := ingest(ctx, raw, base, rows)
		t.Logf("acknowledge rows=%d ingest_http_status=%d", len(batch), status)
		if err != nil {
			return err
		}
		seeding := ledger.HighWater() == 0
		ledger.Append(batch)
		if replace && seeding {
			// Give the held cursor an observed, published predecessor to pin.
			predecessor, err = publishBackfillGeneration(ctx, raw, base, ledger.HighWater(), nil)
			if err != nil {
				return fmt.Errorf("publish seed generation: %w", err)
			}
			t.Logf("seed_generation=%+v", predecessor)
		}
		return nil
	}
	var filter querymodel.Filter
	fetch := func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
		filter = req.Filter
		return fetchBackfillPage(ctx, pager, req)
	}
	var hook querymodel.AfterCapture
	if replace {
		hook = func(ctx context.Context, _ string) error {
			replacement, err := publishBackfillGeneration(ctx, raw, base, ledger.HighWater(), &predecessor)
			if err != nil {
				return fmt.Errorf("publish replacement with cursor held: %w", err)
			}
			t.Logf("replacement_generation=%+v", replacement)
			return nil
		}
	}
	result, err := querymodel.RunFixedBudgetCase(ctx, tc, acknowledge, fetch, hook)
	if err != nil || result.Outcome != querymodel.FixedBudgetExhausted || !replace {
		return result, err
	}
	walk := querymodel.WireWalk{
		Filter: filter, RowLimits: []int{tc.ResumeLimit},
		Limits: querymodel.WireLimits{MaxPages: ledger.HighWater(), MaxPageBytes: tc.ResponseByteCap, Timeout: 8 * time.Second},
	}
	stats, err := querymodel.VerifyWireWalk(ctx, &ledger, walk, fetch, nil)
	if err != nil {
		return result, fmt.Errorf("fresh walk after replacement and backfill: %w", err)
	}
	t.Logf("held=%+v fresh=%+v", result.WireStats, stats)
	return result, nil
}

func fetchBackfillPage(ctx context.Context, pager *releasePager, req querymodel.WireRequest) (querymodel.WirePage, error) {
	if !req.Filter.Start.Equal(pager.query.Start) || !req.Filter.End.Equal(pager.query.End) ||
		req.MaxPageBytes != pager.client.MaxPageBytes {
		return querymodel.WirePage{}, errors.New("oracle bounds differ from the configured release pager")
	}
	page, res, err := pager.fetch(ctx, req.Filter.Contains, req.Limit, req.Cursor)
	if res == nil {
		return querymodel.WirePage{}, err
	}
	if res.StatusCode == http.StatusServiceUnavailable {
		if rejectionErr := checkBudgetRejection(res, err); rejectionErr != nil {
			err = rejectionErr
		} else {
			return querymodel.WirePage{}, &querymodel.FixedBudgetHTTPError{
				StatusCode: res.StatusCode, CleanBudgetRejection: true, Cause: err,
			}
		}
	}
	if err == nil && (res.ContentLength < 0 || res.Header.Get("Content-Length") == "") {
		err = errors.New("missing exact Content-Length on query response")
	}
	if err != nil {
		return querymodel.WirePage{}, &querymodel.FixedBudgetHTTPError{StatusCode: res.StatusCode, Cause: err}
	}
	rows := make([]querymodel.Row, len(page.Rows))
	for i, item := range page.Rows {
		rows[i] = querymodel.Row{Time: item.TS, Line: item.Line}
	}
	return querymodel.WirePage{Rows: rows, NextCursor: page.NextCursor, BodyBytes: res.ContentLength}, nil
}

type backfillCompaction struct {
	state              string
	generation         uint64
	covered, committed int
}

func readBackfillCompaction(ctx context.Context, raw *http.Client, base, method, action string, cutoff int) (backfillCompaction, error) {
	target := base + "/_capacity/compaction?action=" + action + "&cutoff=" + strconv.Itoa(cutoff)
	res, err := request(ctx, raw, method, target, nil)
	if err != nil {
		return backfillCompaction{}, fmt.Errorf("compaction %s cutoff %d: %w", action, cutoff, err)
	}
	if res.status != http.StatusOK {
		return backfillCompaction{}, fmt.Errorf("compaction %s cutoff %d returned HTTP %d: %.200s", action, cutoff, res.status, res.body)
	}
	var wire struct {
		State         *string `json:"state"`
		Generation    *uint64 `json:"generation"`
		CoveredRows   *int    `json:"covered_rows"`
		CommittedRows *int    `json:"committed_rows"`
	}
	if err := json.Unmarshal(res.body, &wire); err != nil {
		return backfillCompaction{}, fmt.Errorf("decode compaction %s: %w", action, err)
	}
	if wire.State == nil || wire.Generation == nil || wire.CoveredRows == nil || wire.CommittedRows == nil ||
		(*wire.State != "running" && *wire.State != "complete") || *wire.CoveredRows < 0 || *wire.CommittedRows < *wire.CoveredRows {
		return backfillCompaction{}, fmt.Errorf("invalid compaction %s state or row counters", action)
	}
	return backfillCompaction{*wire.State, *wire.Generation, *wire.CoveredRows, *wire.CommittedRows}, nil
}

func publishBackfillGeneration(ctx context.Context, raw *http.Client, base string, cutoff int, previous *backfillCompaction) (backfillCompaction, error) {
	ctx, cancel := context.WithTimeout(ctx, backfillPublicationTimeout)
	defer cancel()
	if previous == nil {
		initial, err := readBackfillCompaction(ctx, raw, base, http.MethodGet, "status", 0)
		if err != nil {
			return backfillCompaction{}, err
		}
		previous = &initial
	}
	if previous.covered >= cutoff {
		return backfillCompaction{}, fmt.Errorf("cutoff %d must extend predecessor coverage: %+v", cutoff, *previous)
	}
	validate := func(c backfillCompaction) error {
		if c.committed == cutoff &&
			(c.state == "complete" && c.generation > previous.generation && c.covered == cutoff ||
				c.state == "running" && c.generation == previous.generation && c.covered == previous.covered) {
			return nil
		}
		return fmt.Errorf("unexpected compaction state at cutoff %d: %+v (predecessor %+v)", cutoff, c, *previous)
	}
	started, err := readBackfillCompaction(ctx, raw, base, http.MethodPost, "start", cutoff)
	if err != nil {
		return backfillCompaction{}, err
	}
	if err := validate(started); err != nil {
		return backfillCompaction{}, err
	}
	ticker := time.NewTicker(candidatePollInterval)
	defer ticker.Stop()
	for {
		c, err := readBackfillCompaction(ctx, raw, base, http.MethodGet, "status", cutoff)
		if err != nil {
			return backfillCompaction{}, err
		}
		if err := validate(c); err != nil {
			return backfillCompaction{}, err
		}
		if c.state == "complete" {
			if started.state == "complete" && c != started {
				return backfillCompaction{}, fmt.Errorf("publication changed between start and status: %+v then %+v", started, c)
			}
			return c, ctx.Err()
		}
		if started.state == "complete" {
			return backfillCompaction{}, fmt.Errorf("compaction returned to running after publication: %+v", c)
		}
		select {
		case <-ctx.Done():
			return backfillCompaction{}, fmt.Errorf("wait for publication at cutoff %d: %w", cutoff, ctx.Err())
		case <-ticker.C:
		}
	}
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
		threshold    int
		brokenBudget int
		maxBudget    int
		budgets      []int
		cleanupErr   error
		want         string
	}{
		{name: "above reference", threshold: 15, budgets: []int{11, 28, 19, 15, 13, 14}},
		{name: "below reference", threshold: 4, budgets: []int{11, 5, 2, 3, 4}},
		{name: "budget floor", threshold: 1, budgets: []int{11, 5, 2, 1}},
		{name: "issuing at ceiling", threshold: 25, maxBudget: 25, budgets: []int{11, 18, 22, 24, 25}},
		{name: "search ceiling", firstAttempt: []string{ack, refusal}, maxBudget: 25, budgets: []int{11, 18, 22, 24, 25}, want: "no cursor issued"},
		{name: "broken continuation during bisection", threshold: 15, brokenBudget: 15,
			budgets: []int{11, 28, 19, 15}, want: "work budget 15:"},
		{name: "cleanup after clean refusal", firstAttempt: []string{ack, refusal},
			cleanupErr: errors.New("fixture removal failed"), want: "fixture removal failed"},
		{name: "unclean opening refusal",
			firstAttempt: []string{ack, wireResponse(503, plain, budgetRejectionBody)}, want: "unclean HTTP 503"},
		{name: "first resume refused", firstAttempt: []string{ack, open, ack, refusal}, want: "page 2"},
		{name: "terminal resume refused", firstAttempt: []string{ack, open, ack, resume, ack, refusal}, want: "page 3"},
		{name: "backfill unacknowledged", firstAttempt: []string{ack, open, refusal}, want: "backfill before resume 1"},
		{name: "second backfill unacknowledged", firstAttempt: []string{ack, open, ack, resume, refusal}, want: "backfill before resume 2"},
		{name: "incomplete first page",
			firstAttempt: []string{ack, lengthResponse(200, ndjson, strings.TrimSuffix(body("c"), "\n"))},
			want:         "invalid query record framing"},
		{name: "missing length on framed success",
			firstAttempt: []string{ack, wireResponse(200, ndjson+"Transfer-Encoding: chunked\r\n",
				fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body("c")), body("c")))},
			want: "missing exact Content-Length"},
		{name: "backfill leaked into capture",
			firstAttempt: []string{ack, open, ack, resume, ack, lengthResponse(200, ndjson, body("new final tie"))},
			want:         "append ordinal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := querymodel.FixedBudgetCases()[0]
			fullWalk := []string{ack, open, ack, resume, ack, terminal}
			var budgets []int
			raw := new(http.Client)
			launch := func(ctx context.Context, budget int, check func(context.Context, string) error) error {
				budgets = append(budgets, budget)
				replies := tc.firstAttempt
				if tc.threshold > 0 {
					switch {
					case budget < tc.threshold:
						replies = []string{ack, refusal}
					case budget == tc.brokenBudget:
						replies = []string{ack, open, ack, resume, ack, refusal}
					default:
						replies = fullWalk
					}
				}
				raw.Transport = scriptedBackfillTransport(t, heldBackfillRequests(token1, token2), replies)
				return errors.Join(check(ctx, "http://127.0.0.1"), tc.cleanupErr)
			}
			maxBudget := tc.maxBudget
			if maxBudget == 0 {
				maxBudget = 44
			}
			wantBudgets := tc.budgets
			if wantBudgets == nil {
				wantBudgets = []int{11}
			}
			err := searchBackfillBudget(t.Context(), t, raw, fixture, maxBudget, launch)
			if (tc.want == "" && err != nil) || (tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want))) {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
			if !slices.Equal(budgets, wantBudgets) {
				t.Errorf("budgets = %v, want %v", budgets, wantBudgets)
			}
			if tc.brokenBudget > 0 && (err == nil || !strings.Contains(err.Error(), "page 3 limit=1 HTTP 503")) {
				t.Errorf("missing continuation failure: %v", err)
			}
		})
	}
}

func TestFixedBudgetBackfillPublication(t *testing.T) {
	t.Parallel()
	const ack = "HTTP/1.1 204 No Content\r\n\r\n"
	ndjson := func(line, token string) string {
		headers := "Content-Type: application/x-ndjson\r\n"
		if token != "" {
			headers += nextCursor + ": " + token + "\r\n"
		}
		return lengthResponse(200, headers, fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", fixtureStart, line))
	}
	control := func(state string, generation, covered, committed int) string {
		body := fmt.Sprintf(`{"state":%q,"generation":%d,"covered_rows":%d,"committed_rows":%d}`, state, generation, covered, committed)
		return lengthResponse(200, "Content-Type: application/json\r\n", body)
	}
	for _, tc := range []struct {
		name, publication, lastFreshRow, want string
	}{
		{name: "observed replacement", publication: control("complete", 8, 4, 4), lastFreshRow: "new final tie"},
		{name: "stale generation", publication: control("complete", 7, 4, 4), want: "unexpected compaction state"},
		{name: "wrong cutoff", publication: control("complete", 8, 3, 4), want: "unexpected compaction state"},
		{name: "wrong committed rows", publication: control("complete", 8, 4, 5), want: "unexpected compaction state"},
		{name: "missing generation", publication: lengthResponse(200, "", `{"state":"complete","covered_rows":4,"committed_rows":4}`), want: "invalid compaction"},
		{name: "control unavailable", publication: lengthResponse(404, "", "missing"), want: "returned HTTP 404"},
		{name: "fresh capture lost backfill", publication: control("complete", 8, 4, 4), lastFreshRow: "z", want: "fresh walk after replacement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			token1, token2 := strings.Repeat("A", 44), strings.Repeat("B", 44)
			held := heldBackfillRequests(token1, token2)
			requests := []backfillRequest{
				held[0],
				{path: "/_capacity/compaction?action=status&cutoff=0"},
				{path: "/_capacity/compaction?action=start&cutoff=3"},
				{path: "/_capacity/compaction?action=status&cutoff=3"},
				held[1], held[2],
				{path: "/_capacity/compaction?action=start&cutoff=4"},
				{path: "/_capacity/compaction?action=status&cutoff=4"},
				{path: "/_capacity/compaction?action=status&cutoff=4"},
			}
			replies := []string{
				ack, control("complete", 0, 0, 3), control("running", 0, 0, 3), control("complete", 7, 3, 3),
				ndjson("c", token1), ack, control("running", 7, 3, 4), control("running", 7, 3, 4), tc.publication,
			}
			if tc.lastFreshRow != "" {
				requests = append(requests, held[3:]...)
				replies = append(replies, ndjson("a", token2), ack, ndjson("b", ""))
				cursor := ""
				for i, line := range []string{"c", "a", "b", "z", tc.lastFreshRow} {
					requests = append(requests, backfillRequest{path: "/query", cursor: cursor})
					cursor = ""
					if i < 4 {
						cursor = strings.Repeat(string(rune('C'+i)), 44)
					}
					replies = append(replies, ndjson(line, cursor))
				}
			}
			raw := &http.Client{Transport: scriptedBackfillTransport(t, requests, replies)}
			result, err := checkFixedBudgetBackfill(t.Context(), t, raw, "http://127.0.0.1", querymodel.FixedBudgetCases()[0], true)
			if tc.want == "" {
				if err != nil || result.Outcome != querymodel.FixedBudgetExhausted {
					t.Fatalf("result=%+v error=%v", result, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			} else if tc.lastFreshRow == "" && (result.Outcome != querymodel.FixedBudgetIssuedCursor || result.Page != 1) {
				t.Fatalf("unexpected progress on publication failure: %+v", result)
			}
		})
	}
}

func TestFixedBudgetBackfillPublicationCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const running = `{"state":"running","generation":7,"covered_rows":3,"committed_rows":4}`
	calls := 0
	raw := &http.Client{Transport: budgetRoundTripper(func(req *http.Request) (*http.Response, error) {
		calls++
		if deadline, ok := req.Context().Deadline(); !ok || time.Until(deadline) > backfillPublicationTimeout {
			t.Error("compaction request did not inherit the publication deadline")
		}
		if calls == 2 {
			cancel()
			return nil, req.Context().Err()
		}
		return http.ReadResponse(bufio.NewReader(strings.NewReader(lengthResponse(200, "", running))), req)
	})}
	previous := backfillCompaction{state: "complete", generation: 7, covered: 3, committed: 3}
	if _, err := publishBackfillGeneration(ctx, raw, "http://127.0.0.1", 4, &previous); !errors.Is(err, context.Canceled) || calls != 2 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}

type backfillRequest struct {
	path, cursor string
	batch        []row
}

func heldBackfillRequests(token1, token2 string) []backfillRequest {
	return []backfillRequest{
		{path: "/ingest", batch: []row{{fixtureStart, "c"}, {fixtureStart, "a"}, {fixtureStart, "b"}}},
		{path: "/query"},
		{path: "/ingest", batch: []row{{fixtureStart, "z"}}},
		{path: "/query", cursor: token1},
		{path: "/ingest", batch: []row{{fixtureStart, "new final tie"}}},
		{path: "/query", cursor: token2},
	}
}

func scriptedBackfillTransport(t *testing.T, requests []backfillRequest, replies []string) http.RoundTripper {
	t.Helper()
	if len(replies) > len(requests) {
		t.Fatal("script has more replies than expected requests")
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
		if i >= len(replies) {
			return nil, fmt.Errorf("unexpected request %d on %s", step, req.URL.Path)
		}
		want := requests[i]
		switch want.path {
		case "/ingest":
			if req.URL.Path != want.path {
				return nil, fmt.Errorf("request %d on %s, want %s", step, req.URL.Path, want.path)
			}
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
			if !slices.Equal(batch, want.batch) {
				return nil, fmt.Errorf("ingest at request %d = %+v, want %+v", step, batch, want.batch)
			}
		case "/query":
			q := req.URL.Query()
			wantPage := ""
			if want.cursor == "" {
				wantPage = "1"
			}
			if req.URL.Path != want.path || req.Method != http.MethodGet || q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd ||
				q.Get("limit") != "1" || q.Get("cursor") != want.cursor || q.Get("page") != wantPage || q.Get("contains") != "" {
				return nil, fmt.Errorf("unexpected query at request %d", step)
			}
		default:
			method := http.MethodGet
			if strings.Contains(want.path, "action=start") {
				method = http.MethodPost
			}
			if req.URL.RequestURI() != want.path || req.Method != method {
				return nil, fmt.Errorf("expected %s %s at request %d", method, want.path, step)
			}
		}
		return http.ReadResponse(bufio.NewReader(strings.NewReader(replies[i])), req)
	})
}
