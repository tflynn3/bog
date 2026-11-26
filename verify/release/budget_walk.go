package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	budgetWalkMax      = 2048
	budgetWalkDeadline = time.Minute
)

type budgetWalkProof struct {
	FirstCursorBudget int `json:"first_cursor_budget"`
	RejectedBudgets   int `json:"rejected_budgets"`
	Pages             int `json:"pages"`
	Rows              int `json:"rows"`
}

func probeBudgetWalk(ctx context.Context, binary string, client *http.Client) (budgetWalkProof, error) {
	ctx, cancel := context.WithTimeout(ctx, budgetWalkDeadline)
	defer cancel()
	return findBudgetWalk(ctx, client, budgetWalkMax, func(ctx context.Context, budget int, check func(string) error) error {
		flags := []string{"-query-concurrency=1", "-query-work-budget=" + strconv.Itoa(budget)}
		return serve(ctx, binary, client, flags, check)
	})
}

func findBudgetWalk(ctx context.Context, client *http.Client, maxBudget int, launch func(context.Context, int, func(string) error) error) (budgetWalkProof, error) {
	// Equal timestamps and equal line lengths keep the fixture small while
	// making append order observable across both continuations.
	want := []row{{fixtureStart, "c"}, {fixtureStart, "a"}, {fixtureStart, "b"}}
	var proof budgetWalkProof
	for budget := 1; budget <= maxBudget; budget++ {
		if err := ctx.Err(); err != nil {
			return proof, fmt.Errorf("fixed-budget search at budget %d: %w", budget, err)
		}
		err := launch(ctx, budget, func(base string) error {
			if _, err := ingest(ctx, client, base, want); err != nil {
				return fmt.Errorf("seed tied rows: %w", err)
			}
			first, err := requestBudgetPage(ctx, client, base, "")
			if err != nil {
				return fmt.Errorf("first page: %w", err)
			}
			if first.status == http.StatusServiceUnavailable {
				// Check the complete error body so even a partial record
				// of the expected page fails the rejection check.
				if first.cursorHeader || strings.HasPrefix(first.contentType, "application/x-ndjson") ||
					string(first.body) != "query work budget exhausted\n" {
					return fmt.Errorf("unclean HTTP 503 budget rejection (cursor header=%t, content type=%q): %.200q",
						first.cursorHeader, first.contentType, first.body)
				}
				proof.RejectedBudgets++
				return nil
			}
			return checkBudgetWalk(ctx, client, base, budget, first, want, &proof)
		})
		if err := errors.Join(err, ctx.Err()); err != nil {
			return proof, fmt.Errorf("fixed-budget walk at budget %d: %w", budget, err)
		}
		if proof.FirstCursorBudget != 0 {
			return proof, nil
		}
	}
	return proof, fmt.Errorf("no first-page cursor at budgets 1 through %d", maxBudget)
}

func requestBudgetPage(ctx context.Context, client *http.Client, base, cursor string) (response, error) {
	ctx, cancel := context.WithTimeout(ctx, queryDeadline)
	defer cancel()
	return request(ctx, client, http.MethodGet, base+queryPath(cursor, 1, ""), nil)
}

func checkBudgetWalk(ctx context.Context, client *http.Client, base string, budget int, res response, want []row, proof *budgetWalkProof) error {
	seen := make(map[string]bool)
	for i, expected := range want {
		if i > 0 {
			var err error
			res, err = requestBudgetPage(ctx, client, base, res.cursor)
			if err != nil {
				return fmt.Errorf("continue page %d: %w", i+1, err)
			}
		}
		if len(res.body) > 0 && res.body[len(res.body)-1] != '\n' {
			return fmt.Errorf("page %d has incomplete NDJSON framing", i+1)
		}
		n, err := checkRows(res, []row{expected})
		if err != nil {
			return fmt.Errorf("page %d: %w", i+1, err)
		}
		if n != 1 {
			return fmt.Errorf("page %d returned %d rows, want 1", i+1, n)
		}
		if i+1 == len(want) {
			if res.cursorHeader {
				return fmt.Errorf("page %d issued a cursor after the final row", i+1)
			}
		} else {
			if res.cursor == "" {
				return fmt.Errorf("page %d missing continuation cursor", i+1)
			}
			if seen[res.cursor] {
				return fmt.Errorf("page %d repeated a continuation cursor", i+1)
			}
			seen[res.cursor] = true
		}
		if i == 0 {
			proof.FirstCursorBudget = budget
		}
		proof.Pages++
		proof.Rows += n
	}
	return nil
}
