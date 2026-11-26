package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	bogclient "bog/internal/client"
)

const (
	budgetWalkMax      = 2048
	budgetWalkDeadline = time.Minute
)

type budgetWalkStats struct {
	FirstCursorBudget int `json:"first_cursor_budget"`
	RejectedBudgets   int `json:"rejected_budgets"`
	Pages             int `json:"pages"`
	Rows              int `json:"rows"`
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
	var proof budgetWalkStats
	for budget := 1; budget <= maxBudget; budget++ {
		if err := tryBudgetWalk(ctx, client, launch, budget, fixture, &proof); err != nil {
			return proof, err
		}
		if proof.FirstCursorBudget != 0 {
			return proof, nil
		}
	}
	return proof, fmt.Errorf("no first-page cursor at budgets 1 through %d", maxBudget)
}

func findSparseBudgetWalk(ctx context.Context, client *http.Client, maxBudget int, fixture budgetFixture, launch budgetLaunch) (sparseBudgetWalkProof, error) {
	proof := sparseBudgetWalkProof{ResponseByteCap: fixture.byteCap, FirstLimit: fixture.firstLimit, ResumeLimit: fixture.resumeLimit}
	// Work metering is monotone on this fixed snapshot. Bisect to verify the
	// issuing budget and its immediate predecessor without restarting the
	// candidate at every integer budget for the more expensive sparse scan.
	for low, high := 1, maxBudget; low <= high; {
		budget := low + (high-low)/2
		var trial budgetWalkStats
		err := tryBudgetWalk(ctx, client, launch, budget, fixture, &trial)
		trial.RejectedBudgets += proof.RejectedBudgets
		if trial.FirstCursorBudget != 0 {
			proof.budgetWalkStats = trial
			proof.FirstRows = fixture.firstRows
			high = budget - 1
		} else {
			proof.RejectedBudgets = trial.RejectedBudgets
			low = budget + 1
		}
		if err != nil {
			return proof, err
		}
	}
	if proof.FirstCursorBudget == 0 {
		return proof, fmt.Errorf("no first-page cursor at budgets 1 through %d", maxBudget)
	}
	return proof, nil
}

func tryBudgetWalk(ctx context.Context, client *http.Client, launch budgetLaunch, budget int, fixture budgetFixture, proof *budgetWalkStats) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fixed-budget search at budget %d: %w", budget, err)
	}
	err := launch(ctx, budget, func(base string) error {
		pages, err := newBudgetPageClient(base, client, fixture.byteCap)
		if err != nil {
			return err
		}
		if _, err := ingest(ctx, client, base, fixture.seed); err != nil {
			return fmt.Errorf("seed budget fixture: %w", err)
		}
		return checkBudgetWalk(ctx, pages, budget, fixture, proof)
	})
	if err := errors.Join(err, ctx.Err()); err != nil {
		return fmt.Errorf("fixed-budget walk at budget %d: %w", budget, err)
	}
	return nil
}

func checkBudgetWalk(ctx context.Context, pages budgetPageClient, budget int, fixture budgetFixture, proof *budgetWalkStats) error {
	start, err := time.Parse(time.RFC3339Nano, fixtureStart)
	if err != nil {
		return fmt.Errorf("parse budget fixture start: %w", err)
	}
	end, err := time.Parse(time.RFC3339Nano, fixtureEnd)
	if err != nil {
		return fmt.Errorf("parse budget fixture end: %w", err)
	}
	query := bogclient.Query{Start: start, End: end, Contains: fixture.filter, Limit: fixture.firstLimit}
	seen := make(map[string]struct{})
	cursor := ""
	for page, offset := 1, 0; offset < len(fixture.want); page++ {
		count := fixture.firstRows
		if page > 1 {
			count = min(fixture.resumeLimit, len(fixture.want)-offset)
			query.Limit = fixture.resumeLimit
		}
		res, rejected, err := pages.fetch(ctx, query, cursor)
		if err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		if rejected {
			proof.RejectedBudgets++
			return nil
		}
		for i, got := range res.Rows {
			if i >= count || (row{got.TS.UTC().Format(time.RFC3339Nano), got.Line}) != fixture.want[offset+i] {
				return fmt.Errorf("page %d: unexpected row at snapshot offset %d", page, offset+i)
			}
		}
		if len(res.Rows) != count {
			return fmt.Errorf("page %d returned %d rows, want %d", page, len(res.Rows), count)
		}
		offset += len(res.Rows)
		cursor = res.NextCursor
		if offset == len(fixture.want) {
			if cursor != "" {
				return fmt.Errorf("page %d issued a cursor after the final row", page)
			}
		} else {
			if cursor == "" {
				return fmt.Errorf("page %d missing continuation cursor", page)
			}
			if _, repeated := seen[cursor]; repeated {
				return fmt.Errorf("page %d repeated a continuation cursor", page)
			}
			seen[cursor] = struct{}{}
		}
		if page == 1 {
			proof.FirstCursorBudget = budget
		}
		proof.Pages++
		proof.Rows += len(res.Rows)
	}
	return nil
}
