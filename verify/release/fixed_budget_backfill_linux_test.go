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
	t.Setenv("BOG_CAPACITY_CONTROL", "1")

	for _, tc := range querymodel.FixedBudgetCases() {
		t.Run(tc.Fixture, func(t *testing.T) {
			t.Logf("candidate revision=%s binary_sha256=%s fixture=%s query_fixture=%s reference_work_budget=%d response_byte_cap=%d first_limit=%d resume_limit=%d",
				id.Revision, id.BinarySHA256, id.Fixture, tc.Fixture, tc.ReferenceWorkBudget, tc.ResponseByteCap, tc.FirstLimit, tc.ResumeLimit)
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
			if err := qualifyBackfillBudgets(ctx, t, raw, tc, budgetWalkMax, launch); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type backfillTrial string

const (
	ordinaryBackfill backfillTrial = "ordinary"
	retainedBackfill backfillTrial = "retained"
	heldBackfill     backfillTrial = "replacement"
)

func qualifyBackfillBudgets(ctx context.Context, t *testing.T, raw *http.Client, tc querymodel.FixedBudgetCase, maxBudget int, launch budgetLaunch) error {
	t.Helper()
	if _, err := searchBackfillBudget(ctx, t, raw, tc, maxBudget, ordinaryBackfill, launch); err != nil {
		return fmt.Errorf("ordinary budget search: %w", err)
	}
	retained, err := searchBackfillBudget(ctx, t, raw, tc, maxBudget, retainedBackfill, launch)
	if err != nil {
		return fmt.Errorf("retained budget search: %w", err)
	}

	var augmented backfillCapture
	err = launch(ctx, retained.issuing, func(ctx context.Context, base string) error {
		result, err := checkFixedBudgetBackfill(ctx, t, raw, base, tc, heldBackfill, &augmented)
		t.Logf("trial=%s work_budget=%d adjacent_lower_clean_rejection=%d held=%+v", heldBackfill, retained.issuing, retained.rejected, result)
		if err != nil {
			return err
		}
		if result.Outcome != querymodel.FixedBudgetExhausted {
			return fmt.Errorf("expected exact held exhaustion, got %s", result.Outcome)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("generation replacement at retained issuing work budget %d: %w", retained.issuing, err)
	}

	// Replay the acknowledged batches and publications on a fresh candidate.
	// Certification of the augmented capture can require a larger budget.
	if err := launch(ctx, maxBudget, func(ctx context.Context, base string) error {
		stats, err := checkFreshBackfill(ctx, t, raw, base, tc, augmented)
		t.Logf("trial=fresh work_budget=%d fresh=%+v error=%v", maxBudget, stats, err)
		return err
	}); err != nil {
		return fmt.Errorf("fresh walk after replacement and backfill at work budget %d: %w", maxBudget, err)
	}
	return nil
}

func searchBackfillBudget(ctx context.Context, t *testing.T, raw *http.Client, tc querymodel.FixedBudgetCase, maxBudget int, trial backfillTrial, launch budgetLaunch) (budgetBoundary, error) {
	t.Helper()
	if maxBudget < 1 || tc.ReferenceWorkBudget < 1 {
		return budgetBoundary{}, errors.New("work budgets must be positive")
	}
	boundary, err := bisectBudget(maxBudget, tc.ReferenceWorkBudget, func(budget int) (budgetProbeOutcome, error) {
		if err := ctx.Err(); err != nil {
			return probeIndeterminate, err
		}
		var result querymodel.FixedBudgetResult
		err := launch(ctx, budget, func(ctx context.Context, base string) error {
			t.Logf("trial=%s work_budget=%d response_byte_cap=%d", trial, budget, tc.ResponseByteCap)
			var err error
			result, err = checkFixedBudgetBackfill(ctx, t, raw, base, tc, trial, nil)
			t.Logf("trial=%s work_budget=%d held=%+v", trial, budget, result)
			return err
		})
		if err != nil {
			return probeIndeterminate, fmt.Errorf("work budget %d: %w", budget, err)
		}
		if err := ctx.Err(); err != nil {
			return probeIndeterminate, err
		}
		switch result.Outcome {
		case querymodel.FixedBudgetCleanRejection:
			return probeRejected, nil
		case querymodel.FixedBudgetExhausted:
			return probeExhausted, nil
		default:
			return probeIndeterminate, fmt.Errorf("work budget %d: candidate probe has no conclusive result (%q)", budget, result.Outcome)
		}
	})
	if err != nil {
		return budgetBoundary{}, err
	}
	if boundary.issuing == 0 {
		return budgetBoundary{}, fmt.Errorf("no cursor issued at work budgets through %d", maxBudget)
	}
	if !boundary.exact() {
		return budgetBoundary{}, fmt.Errorf("issuing work budget %d lacks a clean rejection at budget %d", boundary.issuing, boundary.issuing-1)
	}
	t.Logf("trial=%s issuing_work_budget=%d adjacent_lower_clean_rejection=%d", trial, boundary.issuing, boundary.rejected)
	return boundary, nil
}

// backfillCapture records the successful held trial's inputs for the fresh walk.
type backfillCapture struct {
	batches [][]querymodel.Row
	filter  querymodel.Filter
}

func acknowledgeBackfill(ctx context.Context, t *testing.T, raw *http.Client, base string, batch []querymodel.Row) error {
	rows := make([]row, len(batch))
	for i, item := range batch {
		rows[i] = row{TS: item.Time.UTC().Format(time.RFC3339Nano), Line: item.Line}
	}
	status, err := ingest(ctx, raw, base, rows)
	t.Logf("acknowledge rows=%d ingest_http_status=%d", len(batch), status)
	return err
}

func checkFixedBudgetBackfill(ctx context.Context, t *testing.T, raw *http.Client, base string, tc querymodel.FixedBudgetCase, trial backfillTrial, augmented *backfillCapture) (querymodel.FixedBudgetResult, error) {
	t.Helper()
	pager, err := newObservedOraclePager(base, raw, int(tc.ResponseByteCap))
	if err != nil {
		return querymodel.FixedBudgetResult{}, err
	}
	var committed int
	var predecessor backfillCompaction
	acknowledge := func(ctx context.Context, batch []querymodel.Row) error {
		if err := acknowledgeBackfill(ctx, t, raw, base, batch); err != nil {
			return err
		}
		seeding := committed == 0
		committed += len(batch)
		if augmented != nil {
			augmented.batches = append(augmented.batches, slices.Clone(batch))
		}
		if trial != ordinaryBackfill && seeding {
			seed, err := publishBackfillGeneration(ctx, raw, base, committed, nil)
			if err != nil {
				return fmt.Errorf("publish seed generation: %w", err)
			}
			predecessor = seed
			t.Logf("trial=%s seed_generation=%+v", trial, seed)
		}
		return nil
	}
	fetch := func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
		if augmented != nil {
			augmented.filter = req.Filter
		}
		var encodedBody []byte
		page, err := fetchBackfillPage(context.WithValue(ctx, oracleBodyKey{}, &encodedBody), pager, req)
		if err != nil {
			return querymodel.WirePage{}, err
		}
		if encodedBody == nil || int64(len(encodedBody)) != page.BodyBytes {
			return querymodel.WirePage{}, &querymodel.FixedBudgetHTTPError{
				StatusCode: http.StatusOK, Cause: errors.New("missing complete query body evidence"),
			}
		}
		page.EncodedBody = encodedBody
		return page, nil
	}
	var hook querymodel.AfterCapture
	if trial == heldBackfill {
		hook = func(ctx context.Context, _ string) error {
			replacement, err := publishBackfillGeneration(ctx, raw, base, committed, &predecessor)
			if err != nil {
				return fmt.Errorf("publish replacement with cursor held: %w", err)
			}
			t.Logf("trial=%s replacement_generation=%+v", trial, replacement)
			return nil
		}
	}
	return querymodel.RunStrictFixedBudgetCase(ctx, tc, acknowledge, fetch, hook)
}

func checkFreshBackfill(ctx context.Context, t *testing.T, raw *http.Client, base string, tc querymodel.FixedBudgetCase, augmented backfillCapture) (querymodel.WireStats, error) {
	t.Helper()
	pager, err := newReleasePager(base, raw, int(tc.ResponseByteCap))
	if err != nil {
		return querymodel.WireStats{}, err
	}
	var ledger querymodel.Log
	var predecessor *backfillCompaction
	for i, batch := range augmented.batches {
		if err := acknowledgeBackfill(ctx, t, raw, base, batch); err != nil {
			return querymodel.WireStats{}, fmt.Errorf("replay batch %d: %w", i+1, err)
		}
		cutoff := ledger.Append(batch)
		// Recreate the seed and first backfill publications. The second
		// backfill remains in the live tail, as it did in the held trial.
		if i < 2 {
			published, err := publishBackfillGeneration(ctx, raw, base, cutoff, predecessor)
			if err != nil {
				return querymodel.WireStats{}, fmt.Errorf("replay publication at cutoff %d: %w", cutoff, err)
			}
			t.Logf("trial=fresh published_generation=%+v", published)
			predecessor = &published
		}
	}
	walk := querymodel.WireWalk{
		Filter: augmented.filter, RowLimits: []int{tc.ResumeLimit},
		Limits: querymodel.WireLimits{MaxPages: ledger.HighWater(), MaxPageBytes: tc.ResponseByteCap, Timeout: 8 * time.Second},
	}
	return querymodel.VerifyWireWalk(ctx, &ledger, walk, func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
		return fetchBackfillPage(ctx, pager, req)
	}, nil)
}

func fetchBackfillPage(ctx context.Context, pager *releasePager, req querymodel.WireRequest) (querymodel.WirePage, error) {
	page, res, err := fetchOraclePage(ctx, pager, req)
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
	if err != nil {
		return querymodel.WirePage{}, &querymodel.FixedBudgetHTTPError{StatusCode: res.StatusCode, Cause: err}
	}
	return page, nil
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
			fullWalk := []string{ack, open, ack, resume, ack, terminal, terminal}
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
			boundary, err := searchBackfillBudget(t.Context(), t, raw, fixture, maxBudget, ordinaryBackfill, launch)
			if tc.want == "" && boundary != (budgetBoundary{issuing: tc.threshold, rejected: tc.threshold - 1}) {
				t.Errorf("boundary = %+v, want issuing %d and adjacent rejection %d", boundary, tc.threshold, tc.threshold-1)
			}
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

func TestFixedBudgetBackfillRequiresExactTerminalReplay(t *testing.T) {
	t.Parallel()
	token1, token2 := strings.Repeat("A", 44), strings.Repeat("B", 44)
	const ndjson = "Content-Type: application/x-ndjson\r\n"
	original := fmt.Sprintf("{\"ts\":%q,\"line\":\"b\"}\n", fixtureStart)
	reordered := fmt.Sprintf("{\"line\":\"b\",\"ts\":%q}\n", fixtureStart)
	if len(original) != len(reordered) || original == reordered {
		t.Fatal("terminal fixture must differ at the same encoded length")
	}
	for _, tc := range []struct {
		name, replay string
		wantOutcome  querymodel.FixedBudgetOutcome
		wantErr      string
	}{
		{name: "identical", replay: original, wantOutcome: querymodel.FixedBudgetExhausted},
		{name: "same row and size with reordered fields", replay: reordered, wantOutcome: querymodel.FixedBudgetIssuedCursor, wantErr: "replayed terminal body differs from the original encoded body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			replies := []string{
				backfillTestAck, backfillTestPage("c", token1),
				backfillTestAck, backfillTestPage("a", token2),
				backfillTestAck, lengthResponse(http.StatusOK, ndjson, original),
				lengthResponse(http.StatusOK, ndjson, tc.replay),
			}
			raw := &http.Client{Transport: scriptedBackfillTransport(t, heldBackfillRequests(token1, token2), replies)}
			result, err := checkFixedBudgetBackfill(t.Context(), t, raw, "http://127.0.0.1", querymodel.FixedBudgetCases()[0], ordinaryBackfill, nil)
			if (tc.wantErr == "" && err != nil) || (tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr))) ||
				result.Outcome != tc.wantOutcome || result.Phase != "terminal_replay" ||
				result.Pages != 3 || result.Rows != 3 || result.Page != 3 || result.Limit != 1 || result.HTTPStatus != http.StatusOK {
				t.Fatalf("terminal replay = %+v, %v, want %s with %v", result, err, tc.wantOutcome, tc.wantErr)
			}
		})
	}
}

func TestSparseBackfillBudgetBelowReference(t *testing.T) {
	t.Parallel()
	const threshold = 200
	tc := querymodel.FixedBudgetCases()[2]
	sparse := sparseBudgetFixture()
	token1, token2 := strings.Repeat("A", 44), strings.Repeat("B", 44)
	terminal := backfillRequest{path: "/query", filter: sparse.filter, cursor: token2}
	requests := []backfillRequest{
		{path: "/ingest", batch: sparse.seed},
		{path: "/query", filter: sparse.filter, limit: tc.FirstLimit},
		{path: "/ingest", batch: []row{{fixtureStart, "@new"}}},
		{path: "/query", filter: sparse.filter, cursor: token1},
		{path: "/ingest", batch: []row{{sparse.want[3].TS, "@new late tie"}}},
		terminal, terminal,
	}
	page := func(cursor string, rows ...row) string {
		var body strings.Builder
		enc := json.NewEncoder(&body)
		for _, r := range rows {
			if err := enc.Encode(r); err != nil {
				t.Fatal(err)
			}
		}
		headers := "Content-Type: application/x-ndjson\r\n"
		if cursor != "" {
			headers += nextCursor + ": " + cursor + "\r\n"
		}
		return lengthResponse(http.StatusOK, headers, body.String())
	}
	first := page(token1, sparse.want[:2]...)
	resume := page(token2, sparse.want[2])
	last := page("", sparse.want[3])
	refusal := lengthResponse(http.StatusServiceUnavailable, "Content-Type: text/plain\r\n", budgetRejectionBody)
	fullWalk := []string{backfillTestAck, first, backfillTestAck, resume, backfillTestAck, last, last}

	for _, test := range []struct {
		name       string
		broken     bool
		wantProbes []int
	}{
		{"measures adjacent rejection", false, []int{314, 157, 235, 196, 215, 205, 200, 198, 199}},
		{"fails at issuing boundary after higher success", true, []int{314, 157, 235, 196, 215, 205, 200}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var probes []int
			raw := new(http.Client)
			launch := func(ctx context.Context, budget int, check func(context.Context, string) error) error {
				probes = append(probes, budget)
				replies := fullWalk
				switch {
				case budget < threshold:
					replies = []string{backfillTestAck, refusal}
				case budget == threshold && test.broken:
					replies = []string{backfillTestAck, first, backfillTestAck, refusal}
				}
				raw.Transport = scriptedBackfillTransport(t, requests, replies)
				return check(ctx, "http://127.0.0.1")
			}
			boundary, err := searchBackfillBudget(t.Context(), t, raw, tc, budgetWalkMax, ordinaryBackfill, launch)
			if !slices.Equal(probes, test.wantProbes) {
				t.Errorf("budgets = %v, want %v", probes, test.wantProbes)
			}
			if test.broken {
				if boundary != (budgetBoundary{}) || err == nil ||
					!strings.Contains(err.Error(), "work budget 200:") || !strings.Contains(err.Error(), "page 2 limit=1 HTTP 503") {
					t.Fatalf("boundary = %+v, error = %v, want continuation failure at budget 200", boundary, err)
				}
			} else if err != nil || boundary != (budgetBoundary{issuing: threshold, rejected: threshold - 1}) {
				t.Fatalf("boundary = %+v, error = %v, want issuing budget %d", boundary, err, threshold)
			}
		})
	}
}

const backfillTestAck = "HTTP/1.1 204 No Content\r\n\r\n"

func backfillTestPage(line, token string) string {
	headers := "Content-Type: application/x-ndjson\r\n"
	if token != "" {
		headers += nextCursor + ": " + token + "\r\n"
	}
	return lengthResponse(200, headers, fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", fixtureStart, line))
}

func backfillTestCompaction(state string, generation, covered, committed int) string {
	body := fmt.Sprintf(`{"state":%q,"generation":%d,"covered_rows":%d,"committed_rows":%d}`, state, generation, covered, committed)
	return lengthResponse(200, "Content-Type: application/json\r\n", body)
}

func TestFixedBudgetBackfillPublication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, publication, lastFreshRow, want string
	}{
		{name: "observed replacement", publication: backfillTestCompaction("complete", 8, 4, 4), lastFreshRow: "new final tie"},
		{name: "stale generation", publication: backfillTestCompaction("complete", 7, 4, 4), want: "unexpected compaction state"},
		{name: "wrong cutoff", publication: backfillTestCompaction("complete", 8, 3, 4), want: "unexpected compaction state"},
		{name: "wrong committed rows", publication: backfillTestCompaction("complete", 8, 4, 5), want: "unexpected compaction state"},
		{name: "missing generation", publication: lengthResponse(200, "", `{"state":"complete","covered_rows":4,"committed_rows":4}`), want: "invalid compaction"},
		{name: "control unavailable", publication: lengthResponse(404, "", "missing"), want: "returned HTTP 404"},
		{name: "fresh capture lost backfill", publication: backfillTestCompaction("complete", 8, 4, 4), lastFreshRow: "z", want: "fresh walk after replacement"},
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
				backfillTestAck, backfillTestCompaction("complete", 0, 0, 3), backfillTestCompaction("running", 0, 0, 3), backfillTestCompaction("complete", 7, 3, 3),
				backfillTestPage("c", token1), backfillTestAck, backfillTestCompaction("running", 7, 3, 4), backfillTestCompaction("running", 7, 3, 4), tc.publication,
			}
			if tc.lastFreshRow != "" {
				requests = append(requests, held[3:]...)
				terminal := backfillTestPage("b", "")
				replies = append(replies, backfillTestPage("a", token2), backfillTestAck, terminal, terminal)
			}
			raw := &http.Client{Transport: scriptedBackfillTransport(t, requests, replies)}
			fixture := querymodel.FixedBudgetCases()[0]
			var augmented backfillCapture
			result, err := checkFixedBudgetBackfill(t.Context(), t, raw, "http://127.0.0.1", fixture, heldBackfill, &augmented)
			if tc.lastFreshRow != "" {
				if err != nil || result.Outcome != querymodel.FixedBudgetExhausted {
					t.Fatalf("held result=%+v error=%v", result, err)
				}
				requests, replies := freshBackfillScript(tc.lastFreshRow)
				raw.Transport = scriptedBackfillTransport(t, requests, replies)
				stats, freshErr := checkFreshBackfill(t.Context(), t, raw, "http://127.0.0.1", fixture, augmented)
				if freshErr != nil {
					err = fmt.Errorf("fresh walk after replacement: %w", freshErr)
				} else if stats.Pages != 5 || stats.Rows != 5 {
					t.Fatalf("fresh walk stats=%+v, want five pages and rows", stats)
				}
			}
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

// freshBackfillScript replays the seed and both writes with the same
// checkpoint/tail split before opening the augmented cursor.
func freshBackfillScript(lastRow string) ([]backfillRequest, []string) {
	held := heldBackfillRequests("", "")
	requests := []backfillRequest{
		held[0],
		{path: "/_capacity/compaction?action=status&cutoff=0"},
		{path: "/_capacity/compaction?action=start&cutoff=3"},
		{path: "/_capacity/compaction?action=status&cutoff=3"},
		held[2],
		{path: "/_capacity/compaction?action=start&cutoff=4"},
		{path: "/_capacity/compaction?action=status&cutoff=4"},
		held[4],
	}
	seed := backfillTestCompaction("complete", 7, 3, 3)
	replacement := backfillTestCompaction("complete", 8, 4, 4)
	replies := []string{backfillTestAck, backfillTestCompaction("complete", 0, 0, 3), seed, seed,
		backfillTestAck, replacement, replacement, backfillTestAck}
	lines := []string{"c", "a", "b", "z", lastRow}
	cursor := ""
	for i, line := range lines {
		requests = append(requests, backfillRequest{path: "/query", cursor: cursor})
		cursor = ""
		if i < len(lines)-1 {
			cursor = strings.Repeat(string(rune('C'+i)), 44)
		}
		replies = append(replies, backfillTestPage(line, cursor))
	}
	return requests, replies
}

func TestFixedBudgetBackfillReplacementAtRetainedBoundary(t *testing.T) {
	t.Parallel()
	const ordinaryBudget, retainedBudget, maxBudget = 11, 15, 44
	refusal := lengthResponse(503, "Content-Type: text/plain\r\n", budgetRejectionBody)
	token1, token2 := strings.Repeat("A", 44), strings.Repeat("B", 44)
	open, resume, terminal := backfillTestPage("c", token1), backfillTestPage("a", token2), backfillTestPage("b", "")
	held := heldBackfillRequests(token1, token2)
	seedRequests := []backfillRequest{
		held[0],
		{path: "/_capacity/compaction?action=status&cutoff=0"},
		{path: "/_capacity/compaction?action=start&cutoff=3"},
		{path: "/_capacity/compaction?action=status&cutoff=3"},
	}
	seed := backfillTestCompaction("complete", 7, 3, 3)
	seedReplies := []string{backfillTestAck, backfillTestCompaction("complete", 0, 0, 3), seed, seed}
	replacementRequests := []backfillRequest{
		held[1], held[2],
		{path: "/_capacity/compaction?action=start&cutoff=4"},
		{path: "/_capacity/compaction?action=status&cutoff=4"},
	}
	replacement := backfillTestCompaction("complete", 8, 4, 4)
	replacementReplies := []string{open, backfillTestAck, replacement, replacement}

	for _, tc := range []struct {
		name         string
		resumeBudget int
	}{
		{name: "exhausts at measured budget", resumeBudget: retainedBudget},
		{name: "stalls at measured budget and exhausts above it", resumeBudget: retainedBudget + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := querymodel.FixedBudgetCases()[0]
			raw := new(http.Client)
			ordinaryProbes := []int{11, 5, 8, 9, 10}
			retainedProbes := []int{11, 28, 19, 15, 13, 14}
			wantBudgets := slices.Concat(ordinaryProbes, retainedProbes, []int{retainedBudget})
			if tc.resumeBudget == retainedBudget {
				wantBudgets = append(wantBudgets, maxBudget)
			}
			var budgets []int
			withScript := func(ctx context.Context, requests []backfillRequest, replies []string, check func(context.Context, string) error) error {
				raw.Transport = scriptedBackfillTransport(t, requests, replies)
				return check(ctx, "http://127.0.0.1")
			}
			replacementScript := func(ctx context.Context, budget int, check func(context.Context, string) error) error {
				pages := []string{resume, backfillTestAck, terminal, terminal}
				if budget < tc.resumeBudget {
					pages = []string{refusal}
				}
				return withScript(ctx, slices.Concat(seedRequests, replacementRequests, held[3:]),
					slices.Concat(seedReplies, replacementReplies, pages), check)
			}
			launch := func(ctx context.Context, budget int, check func(context.Context, string) error) error {
				probe := len(budgets)
				budgets = append(budgets, budget)
				switch {
				case probe < len(ordinaryProbes):
					replies := []string{backfillTestAck, open, backfillTestAck, resume, backfillTestAck, terminal, terminal}
					if budget < ordinaryBudget {
						replies = []string{backfillTestAck, refusal}
					}
					return withScript(ctx, held, replies, check)
				case probe < len(ordinaryProbes)+len(retainedProbes):
					replies := []string{open, backfillTestAck, resume, backfillTestAck, terminal, terminal}
					if budget < retainedBudget {
						replies = []string{refusal}
					}
					return withScript(ctx, slices.Concat(seedRequests, held[1:]), slices.Concat(seedReplies, replies), check)
				case probe == len(ordinaryProbes)+len(retainedProbes):
					return replacementScript(ctx, budget, check)
				default:
					requests, replies := freshBackfillScript("new final tie")
					return withScript(ctx, requests, replies, check)
				}
			}

			err := qualifyBackfillBudgets(t.Context(), t, raw, fixture, maxBudget, launch)
			if !slices.Equal(budgets, wantBudgets) {
				t.Errorf("launch budgets = %v, want %v", budgets, wantBudgets)
			}
			if tc.resumeBudget == retainedBudget {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "generation replacement at retained issuing work budget 15") ||
				!strings.Contains(err.Error(), "page 2 limit=1 HTTP 503") {
				t.Fatalf("expected replacement continuation failure at work budget %d, got %v", retainedBudget, err)
			}
			// Demonstrate that this failure would be hidden by the old
			// replacement check at the higher budget.
			if err := replacementScript(t.Context(), maxBudget, func(ctx context.Context, base string) error {
				result, err := checkFixedBudgetBackfill(ctx, t, raw, base, fixture, heldBackfill, nil)
				if err == nil && result.Outcome != querymodel.FixedBudgetExhausted {
					return fmt.Errorf("expected exhaustion, got %s", result.Outcome)
				}
				return err
			}); err != nil {
				t.Fatalf("higher-budget control: %v", err)
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
	path, cursor, filter string
	limit                int
	batch                []row
}

func heldBackfillRequests(token1, token2 string) []backfillRequest {
	return []backfillRequest{
		{path: "/ingest", batch: []row{{fixtureStart, "c"}, {fixtureStart, "a"}, {fixtureStart, "b"}}},
		{path: "/query"},
		{path: "/ingest", batch: []row{{fixtureStart, "z"}}},
		{path: "/query", cursor: token1},
		{path: "/ingest", batch: []row{{fixtureStart, "new final tie"}}},
		{path: "/query", cursor: token2},
		{path: "/query", cursor: token2}, // Replay the terminal request.
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
			limit := want.limit
			if limit == 0 {
				limit = 1
			}
			if req.URL.Path != want.path || req.Method != http.MethodGet || q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd ||
				q.Get("limit") != strconv.Itoa(limit) || q.Get("cursor") != want.cursor || q.Get("page") != wantPage || q.Get("contains") != want.filter {
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
