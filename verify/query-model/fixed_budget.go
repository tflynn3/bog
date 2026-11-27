package querymodel

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// FixedBudgetCase describes a reference probe for a fresh, exclusively owned
// endpoint. Release verification configures the candidate and binds the
// callbacks to that same endpoint for the entire case. The reference work
// budget records the historical issuing budget; the oracle cannot observe
// candidate settings or discover an issuing budget itself.
type FixedBudgetCase struct {
	Fixture             string
	ReferenceWorkBudget int
	ResponseByteCap     int64
	FirstLimit          int
	FirstRows           int
	ResumeLimit         int
}

// FixedBudgetCases returns the historical tied and byte-trimmed sparse
// fixtures and a tied variant with an exact duplicate across a page boundary.
// The sparse fixture lowers its row limit after the opening page. Both tied
// fixtures open at one row, the minimum valid limit and the historical probe.
func FixedBudgetCases() []FixedBudgetCase {
	return []FixedBudgetCase{
		{Fixture: "tied", ReferenceWorkBudget: 11, ResponseByteCap: 96, FirstLimit: 1, FirstRows: 1, ResumeLimit: 1},
		{Fixture: "duplicates", ReferenceWorkBudget: 11, ResponseByteCap: 96, FirstLimit: 1, FirstRows: 1, ResumeLimit: 1},
		{Fixture: "sparse", ReferenceWorkBudget: 314, ResponseByteCap: 96, FirstLimit: 4, FirstRows: 2, ResumeLimit: 1},
	}
}

// FixedBudgetOutcome describes how far the probe got. Exhaustion requires
// both a complete walk and a verified terminal replay at the issuing budget.
// A clean opening rejection is evidence that the endpoint refused this trial
// before publishing a cursor.
type FixedBudgetOutcome string

const (
	FixedBudgetIndeterminate  FixedBudgetOutcome = "indeterminate"
	FixedBudgetCleanRejection FixedBudgetOutcome = "clean_rejection"
	FixedBudgetIssuedCursor   FixedBudgetOutcome = "issued_cursor"
	FixedBudgetExhausted      FixedBudgetOutcome = "exhausted"
)

// FixedBudgetResult counts verified walk pages, excluding the replay. Phase
// describes the latest lifecycle step, including terminal_replay. Page, Limit
// and HTTPStatus describe the latest query attempt, including replay under
// the original page number. Page is zero before the first request.
// HTTPStatus is zero when no status metadata is available. Successful
// fetches represent HTTP 200.
type FixedBudgetResult struct {
	WireStats
	Outcome    FixedBudgetOutcome
	Phase      string
	Page       int
	Limit      int
	HTTPStatus int
}

// FixedBudgetHTTPError lets a FetchWirePage adapter report the status of a
// failed fetch, including malformed responses. Set CleanBudgetRejection only
// after verifying a complete, explicitly framed HTTP 503 with the exact
// "query work budget exhausted\n" body, no NDJSON or cursor header/trailer,
// and no other read or close failure. For a clean rejection return this
// error directly, with an empty WirePage. Other failures may wrap it.
type FixedBudgetHTTPError struct {
	StatusCode           int
	CleanBudgetRejection bool
	Cause                error
}

func (e *FixedBudgetHTTPError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("HTTP %d: %v", e.StatusCode, e.Cause)
}

func (e *FixedBudgetHTTPError) Unwrap() error { return e.Cause }

func (c FixedBudgetCase) fixture() (seed []Row, backfill [2]Row, filter Filter, err error) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(second int, line string) Row {
		return Row{Time: start.Add(time.Duration(second) * time.Second), Line: line}
	}
	filter = Filter{Start: start, End: start.Add(10 * time.Minute)}
	switch c.Fixture {
	case "tied":
		seed = []Row{at(0, "c"), at(0, "a"), at(0, "b")}
	case "duplicates":
		seed = []Row{at(0, "c"), at(0, "c"), at(0, "b")}
	case "sparse":
		filter.Contains = "@"
		// Exactly two 2020 UTC NDJSON records fit in 96 bytes. The
		// interior and trailing misses exercise positioning and exhaustion.
		seed = []Row{at(0, "@last"), at(0, "@tied"), at(3, "@next")}
		for second := 4; second <= 30; second++ {
			seed = append(seed, at(second, "miss"))
		}
		seed = append(seed, at(40, "@late"))
		for second := 41; second <= 55; second++ {
			seed = append(seed, at(second, "miss"))
		}
		return seed, [2]Row{at(0, "@new"), at(40, "@new late tie")}, filter, nil
	default:
		return nil, backfill, Filter{}, errors.New("unknown fixed-budget fixture")
	}
	return seed, [2]Row{at(0, "z"), at(0, "new final tie")}, filter, nil
}

// RunFixedBudgetCase seeds an empty endpoint, verifies the issuing page,
// acknowledges a matching backfill before each resume, and checks every
// continuation against the original acknowledged prefix. It replays the
// terminal page with the same request and deadline using the same fetch
// adapter. The optional hook runs once after the first backfill and before
// the first resume. It can observe publication while the cursor is held and
// must honor ctx. A clean opening rejection returns a distinct outcome and
// nil error. Failures after a verified issuing page retain its progress
// evidence, including when a continuation or replay returns a clean 503.
// When either terminal response supplies EncodedBody, both must supply it
// and the bodies must match byte for byte.
func RunFixedBudgetCase(ctx context.Context, c FixedBudgetCase, acknowledge AcknowledgeBatch, fetch FetchWirePage, afterCapture ...AfterCapture) (FixedBudgetResult, error) {
	return runFixedBudgetCase(ctx, c, acknowledge, fetch, false, afterCapture)
}

// RunStrictFixedBudgetCase additionally requires exact encoded-body witnesses
// on the terminal response and its replay.
func RunStrictFixedBudgetCase(ctx context.Context, c FixedBudgetCase, acknowledge AcknowledgeBatch, fetch FetchWirePage, afterCapture ...AfterCapture) (FixedBudgetResult, error) {
	return runFixedBudgetCase(ctx, c, acknowledge, fetch, true, afterCapture)
}

func runFixedBudgetCase(ctx context.Context, c FixedBudgetCase, acknowledge AcknowledgeBatch, fetch FetchWirePage, strict bool, afterCapture []AfterCapture) (FixedBudgetResult, error) {
	result := FixedBudgetResult{Outcome: FixedBudgetIndeterminate, Phase: "setup"}
	fail := func(err error) (FixedBudgetResult, error) {
		stage := "before opening page"
		if result.Page > 0 {
			stage = fmt.Sprintf("page %d limit=%d", result.Page, result.Limit)
			if result.HTTPStatus != 0 {
				stage += fmt.Sprintf(" HTTP %d", result.HTTPStatus)
			}
		}
		return result, fmt.Errorf("fixed-budget fixture=%q reference_work_budget=%d response_byte_cap=%d first_limit=%d first_rows=%d resume_limit=%d: phase=%s %s: %w",
			c.Fixture, c.ReferenceWorkBudget, c.ResponseByteCap, c.FirstLimit, c.FirstRows, c.ResumeLimit, result.Phase, stage, err)
	}
	if acknowledge == nil || fetch == nil {
		return fail(errors.New("append acknowledgement and fetch adapters are required"))
	}
	if len(afterCapture) > 1 {
		return fail(errors.New("at most one post-capture hook is allowed"))
	}
	seed, backfill, filter, err := c.fixture()
	if err != nil {
		return fail(err)
	}
	if c.ReferenceWorkBudget <= 0 || c.FirstRows <= 0 || c.ResumeLimit > c.FirstLimit {
		return fail(errors.New("invalid fixed-budget case parameters"))
	}

	// Each reference fixture has exactly three pages, including the terminal
	// page. Reuse the wire verifier for capture, row, byte and cursor checks.
	walk := WireWalk{
		Filter: filter, FirstPageRows: c.FirstRows,
		RowLimits: []int{c.FirstLimit, c.ResumeLimit, c.ResumeLimit},
		Limits:    WireLimits{MaxPages: 3, MaxPageBytes: c.ResponseByteCap, Timeout: time.Minute},
	}
	if err := walk.validate(); err != nil {
		return fail(err)
	}
	ctx, cancel := context.WithTimeout(ctx, walk.Limits.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	var log Log
	result.Phase = "seed"
	if err := acknowledge(ctx, seed); err != nil {
		return fail(fmt.Errorf("acknowledge seed: %w", err))
	}
	log.Append(seed)
	highWater := log.HighWater()

	result.Phase = "opening"
	appendBackfill := func(ctx context.Context, resume int) error {
		batch := backfill[resume-1 : resume]
		if err := acknowledge(ctx, batch); err != nil {
			return fmt.Errorf("acknowledge backfill before resume %d: %w", resume, err)
		}
		log.Append(batch)
		return nil
	}
	var openingRejection *FixedBudgetHTTPError
	fetchObserved := func(ctx context.Context, req WireRequest) (WirePage, error) {
		result.Limit = req.Limit
		result.HTTPStatus = 0
		page, err := fetch(ctx, req)
		if err == nil {
			result.HTTPStatus = http.StatusOK
		} else {
			var observed *FixedBudgetHTTPError
			if errors.As(err, &observed) && observed != nil {
				result.HTTPStatus = observed.StatusCode
				if result.Phase == "opening" && isCleanOpeningRejection(page, err) {
					openingRejection = observed
				}
			}
		}
		return page, err
	}
	var terminal terminalReplay
	result.WireStats, err = VerifyWireWalk(ctx, &log, walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
		switch result.Page {
		case 1:
			result.Phase = "continuation"
		case 2:
			result.Phase = "second_backfill"
			if err := appendBackfill(ctx, 2); err != nil {
				return WirePage{}, err
			}
			if err := ctx.Err(); err != nil {
				return WirePage{}, err
			}
			result.Phase = "terminal_resume"
		}
		result.Page++
		page, err := fetchObserved(ctx, req)
		if err == nil && result.Page == walk.Limits.MaxPages && page.NextCursor == "" {
			terminal.record(req, page)
		}
		return page, err
	}, func(ctx context.Context, cursor string) error {
		result.Phase = "first_backfill"
		if err := appendBackfill(ctx, 1); err != nil {
			return err
		}
		if len(afterCapture) == 1 && afterCapture[0] != nil {
			result.Phase = "post_capture"
			if err := ctx.Err(); err != nil {
				return err
			}
			return afterCapture[0](ctx, cursor)
		}
		return nil
	})
	if result.Pages > 0 {
		result.Outcome = FixedBudgetIssuedCursor
	}
	if openingRejection != nil && errors.Is(err, openingRejection) && ctx.Err() == nil {
		result.Outcome = FixedBudgetCleanRejection
		return result, nil
	}
	if err != nil {
		return fail(err)
	}
	if result.Pages != walk.Limits.MaxPages {
		return fail(fmt.Errorf("expected to exhaust on page %d, exhausted on page %d", walk.Limits.MaxPages, result.Pages))
	}

	// Reuse the request that reached exhaustion, including its input cursor.
	// Match the original ledger prefix again so the replay cannot accept a
	// backfilled row or drift when the adapter reuses response storage.
	result.Phase = "terminal_replay"
	matches, err := log.Match(highWater, filter)
	if err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := terminal.verify(ctx, fetchObserved, matches, result.Rows, strict); err != nil {
		return fail(err)
	}
	result.Outcome = FixedBudgetExhausted
	return result, nil
}
