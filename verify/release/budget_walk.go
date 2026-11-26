package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const (
	budgetWalkMax      = 2048
	budgetWalkDeadline = time.Minute
)

type budgetWalkStats struct {
	FirstCursorBudget       int                 `json:"first_cursor_budget"`
	RejectedBudgets         int                 `json:"rejected_budgets"`
	Pages                   int                 `json:"pages"`
	Rows                    int                 `json:"rows"`
	ExactThresholdBudget    int                 `json:"exact_threshold_budget,omitempty"`
	FirstFailure            *budgetProbeFailure `json:"first_failure,omitempty"`
	FirstBrokenContinuation *budgetProbeFailure `json:"first_broken_continuation,omitempty"`
	Probes                  []budgetProbe       `json:"probes,omitempty"`
}

type budgetWalkProof struct {
	budgetWalkStats
	SparseWalk sparseBudgetWalkProof `json:"sparse_walk"`
}

type sparseBudgetWalkProof struct {
	budgetWalkStats
	ResponseByteCap int `json:"response_byte_cap"`
	FirstLimit      int `json:"first_limit"`
	FirstRows       int `json:"first_rows"`
	ResumeLimit     int `json:"resume_limit"`
}

type budgetFixture struct {
	seed, want              []row
	filter                  string
	firstLimit, resumeLimit int
	firstRows, byteCap      int
}

type budgetLaunch func(context.Context, int, func(string) error) error

type budgetProbeOutcome string

const (
	probeIndeterminate budgetProbeOutcome = "indeterminate"
	probeRejected      budgetProbeOutcome = "clean_rejection"
	probeIssued        budgetProbeOutcome = "issued_cursor"
	probeExhausted     budgetProbeOutcome = "exhausted"
)

type budgetProbe struct {
	Budget  int                 `json:"budget"`
	Outcome budgetProbeOutcome  `json:"outcome"`
	Pages   int                 `json:"pages"`
	Rows    int                 `json:"rows"`
	Failure *budgetProbeFailure `json:"failure,omitempty"`
}

func (p budgetProbe) issued() bool {
	return p.Outcome == probeIssued || p.Outcome == probeExhausted
}

type budgetProbeFailure struct {
	Budget     int    `json:"budget"`
	Page       int    `json:"page,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
	Detail     string `json:"detail"`
	cause      error
}

func (f *budgetProbeFailure) Error() string {
	return fmt.Sprintf("fixed-budget walk at budget %d: %s", f.Budget, f.Detail)
}

func (f *budgetProbeFailure) Unwrap() error { return f.cause }

type budgetSearch struct {
	stats            budgetWalkStats
	highestRejection int
}

func (s *budgetSearch) record(probe budgetProbe) {
	s.stats.Probes = append(s.stats.Probes, probe)
	if probe.Outcome == probeRejected {
		s.stats.RejectedBudgets++
		s.highestRejection = max(s.highestRejection, probe.Budget)
	}
	if probe.issued() && (s.stats.FirstCursorBudget == 0 || probe.Budget < s.stats.FirstCursorBudget) {
		s.stats.FirstCursorBudget = probe.Budget
		s.stats.Pages, s.stats.Rows = probe.Pages, probe.Rows
	}
	if probe.Failure != nil {
		if s.stats.FirstFailure == nil {
			s.stats.FirstFailure = probe.Failure
		}
		if probe.Failure.Page > 1 && s.stats.FirstBrokenContinuation == nil {
			s.stats.FirstBrokenContinuation = probe.Failure
		}
	}
}

func (s *budgetSearch) finish(maxBudget int, stopped error) (budgetWalkStats, error) {
	budget := s.stats.FirstCursorBudget
	// Budget 1 is the floor. For a monotone search at a higher
	// budget, require a clean observation at the adjacent lower budget.
	if budget > 0 && s.highestRejection == budget-1 {
		s.stats.ExactThresholdBudget = budget
	}
	if first := s.stats.FirstFailure; first != nil {
		if stopped != nil && stopped != first {
			return s.stats, errors.Join(first, stopped)
		}
		return s.stats, first
	}
	if stopped != nil {
		return s.stats, stopped
	}
	if budget == 0 {
		return s.stats, fmt.Errorf("no first-page cursor at budgets 1 through %d", maxBudget)
	}
	if s.stats.ExactThresholdBudget == 0 {
		return s.stats, fmt.Errorf("issuing budget %d has no clean rejection at budget %d", budget, budget-1)
	}
	return s.stats, nil
}

func sparseBudgetFixture() budgetFixture {
	// The encoded matching records are short enough for two to fit in
	// the cap, but the third pushes the body over it.
	first := row{fixtureStart, "@last"}
	tie := row{fixtureStart, "@tied"}
	third := row{"2020-01-01T00:00:03Z", "@next"}
	late := row{"2020-01-01T00:00:40Z", "@late"}
	seed := []row{first, tie, third}
	for second := 4; second <= 30; second++ {
		seed = append(seed, row{fmt.Sprintf("2020-01-01T00:00:%02dZ", second), "miss"})
	}
	seed = append(seed, late)
	for second := 41; second <= 55; second++ {
		seed = append(seed, row{fmt.Sprintf("2020-01-01T00:00:%02dZ", second), "miss"})
	}
	return budgetFixture{
		seed: seed, want: []row{first, tie, third, late}, filter: "@",
		firstLimit: 4, resumeLimit: 1, firstRows: 2, byteCap: 96,
	}
}

func probeBudgetWalk(ctx context.Context, binary string, client *http.Client) (budgetWalkProof, error) {
	ctx, cancel := context.WithTimeout(ctx, budgetWalkDeadline)
	defer cancel()
	launch := func(byteCap int) budgetLaunch {
		return func(ctx context.Context, budget int, check func(string) error) error {
			flags := []string{"-query-concurrency=1", "-query-work-budget=" + strconv.Itoa(budget)}
			if byteCap > 0 {
				flags = append(flags, "-query-max-response-bytes="+strconv.Itoa(byteCap))
			}
			return serve(ctx, binary, client, flags, check)
		}
	}
	tied, tiedErr := findBudgetWalk(ctx, client, budgetWalkMax, launch(0))
	fixture := sparseBudgetFixture()
	sparse, sparseErr := findSparseBudgetWalk(ctx, client, budgetWalkMax, fixture, launch(fixture.byteCap))
	proof := budgetWalkProof{budgetWalkStats: tied, SparseWalk: sparse}
	if sparseErr != nil {
		sparseErr = fmt.Errorf("byte-trimmed sparse walk: %w", sparseErr)
	}
	return proof, errors.Join(tiedErr, sparseErr)
}

func findBudgetWalk(ctx context.Context, client *http.Client, maxBudget int, launch budgetLaunch) (budgetWalkStats, error) {
	// Equal timestamps and equal line lengths keep the fixture small while
	// making append order observable across both continuations.
	want := []row{{fixtureStart, "c"}, {fixtureStart, "a"}, {fixtureStart, "b"}}
	fixture := budgetFixture{seed: want, want: want, firstLimit: 1, resumeLimit: 1, firstRows: 1}
	var search budgetSearch
	for budget := 1; budget <= maxBudget; budget++ {
		probe, err := tryBudgetWalk(ctx, client, launch, budget, fixture)
		search.record(probe)
		if err != nil || probe.Outcome != probeRejected {
			return search.finish(maxBudget, err)
		}
	}
	return search.finish(maxBudget, nil)
}

func findSparseBudgetWalk(ctx context.Context, client *http.Client, maxBudget int, fixture budgetFixture, launch budgetLaunch) (sparseBudgetWalkProof, error) {
	proof := sparseBudgetWalkProof{ResponseByteCap: fixture.byteCap, FirstLimit: fixture.firstLimit, ResumeLimit: fixture.resumeLimit}
	var search budgetSearch
	var stopped error
	// Bisect on the first-page result. Every issuing probe still walks to
	// exhaustion, and a broken resume leaves the first-page observation intact.
	for low, high := 1, maxBudget; low <= high; {
		budget := low + (high-low)/2
		probe, err := tryBudgetWalk(ctx, client, launch, budget, fixture)
		search.record(probe)
		if err != nil || probe.Outcome == probeIndeterminate {
			stopped = err
			break
		}
		if probe.issued() {
			high = budget - 1
		} else {
			low = budget + 1
		}
	}
	stats, err := search.finish(maxBudget, stopped)
	proof.budgetWalkStats = stats
	if stats.FirstCursorBudget > 0 {
		proof.FirstRows = fixture.firstRows
	}
	return proof, err
}

func tryBudgetWalk(ctx context.Context, raw *http.Client, launch budgetLaunch, budget int, fixture budgetFixture) (budgetProbe, error) {
	probe := budgetProbe{Budget: budget, Outcome: probeIndeterminate}
	runErr := ctx.Err()
	if runErr == nil {
		runErr = launch(ctx, budget, func(base string) error {
			var err error
			probe, err = runBudgetWalk(ctx, raw, base, budget, fixture)
			return err
		})
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(runErr, ctxErr) {
			runErr = errors.Join(runErr, ctxErr)
		}
	}
	if runErr != nil {
		failure := &budgetProbeFailure{Budget: budget, Detail: runErr.Error(), cause: runErr}
		if probe.Failure == nil {
			probe.Failure = failure
		}
		return probe, failure
	}
	return probe, nil
}

func runBudgetWalk(ctx context.Context, raw *http.Client, base string, budget int, fixture budgetFixture) (budgetProbe, error) {
	probe := budgetProbe{Budget: budget, Outcome: probeIndeterminate}
	if _, err := ingest(ctx, raw, base, fixture.seed); err != nil {
		return probe, fmt.Errorf("seed budget fixture: %w", err)
	}
	pager, err := newReleasePager(base, raw, fixture.byteCap)
	if err != nil {
		return probe, fmt.Errorf("create budget pager: %w", err)
	}
	return checkBudgetWalk(ctx, pager, budget, fixture), nil
}

func checkBudgetWalk(ctx context.Context, pager *releasePager, budget int, fixture budgetFixture) budgetProbe {
	probe := budgetProbe{Budget: budget, Outcome: probeIndeterminate}
	seen := make(map[string]bool)
	cursor := ""
	for number, offset := 1, 0; offset < len(fixture.want); number++ {
		limit := fixture.resumeLimit
		count := min(limit, len(fixture.want)-offset)
		if number == 1 {
			limit, count = fixture.firstLimit, fixture.firstRows
		}
		page, res, err := pager.fetch(ctx, fixture.filter, limit, cursor)
		if res != nil && res.StatusCode == http.StatusServiceUnavailable {
			if rejectionErr := checkBudgetRejection(res, err); rejectionErr != nil {
				err = rejectionErr
			} else if number == 1 {
				probe.Outcome = probeRejected
				return probe
			}
		} else if err == nil && len(page.Rows) == 0 && (cursor != "" || page.NextCursor != "") {
			err = errors.New("empty query page made no pagination progress")
		} else if err == nil {
			err = checkBudgetPage(page, fixture.want[offset:offset+count], offset+count == len(fixture.want), seen)
		}
		if err != nil {
			failure := &budgetProbeFailure{
				Budget: budget, Page: number, Limit: limit, Detail: fmt.Sprintf("page %d: %v", number, err), cause: err,
			}
			if res != nil {
				failure.HTTPStatus = res.StatusCode
			}
			probe.Failure = failure
			return probe
		}
		offset += len(page.Rows)
		cursor = page.NextCursor
		seen[cursor] = true
		if number == 1 {
			probe.Outcome = probeIssued
		}
		probe.Pages++
		probe.Rows += len(page.Rows)
	}
	probe.Outcome = probeExhausted
	return probe
}
