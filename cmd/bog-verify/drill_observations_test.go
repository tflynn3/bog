package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"bog/internal/backup"
	"bog/internal/client"
)

// These fixture observations are collected outside the drill and never
// populated from its result or from expected page counts.
type drillObservations struct {
	Before phaseObservation `json:"before_restart"`
	After  phaseObservation `json:"after_restart"`
}

type phaseObservation struct {
	Status string         `json:"status"`
	Point  *recoveryPoint `json:"recovery_point"`
	Counts *drillCounts   `json:"counts"`
}

const (
	observationNotRun     = "not_run"
	observationIncomplete = "incomplete"
	observationComplete   = "complete"
)

func observeDrillPhase(ctx context.Context, opts options, server *client.Client, expected []client.Row) (obs phaseObservation, err error) {
	obs.Status = observationIncomplete
	query := backup.Query{Start: opts.query.Start, End: opts.query.End, Contains: opts.query.Contains}
	reference, err := backup.OpenReferenceWalk(ctx, opts.bundle, query, opts.query.Limit)
	if err != nil {
		return obs, fmt.Errorf("open reference for observation: %w", err)
	}
	obs.Point = resultPoint(reference.Point())
	refRows, refPages, walkErr := observeDrillWalk(ctx, opts.maxPages, expected, func(ctx context.Context) ([]client.Row, bool, error) {
		rows, terminal, err := reference.NextPage(ctx)
		if err != nil {
			return nil, false, err
		}
		page := make([]client.Row, len(rows))
		for i, row := range rows {
			page[i] = row.Row
		}
		return page, terminal, nil
	})
	if err := errors.Join(walkErr, reference.Close()); err != nil {
		return obs, fmt.Errorf("observe reference walk: %w", err)
	}
	live, err := server.NewBoundedWalk(opts.query, opts.maxPages)
	if err != nil {
		return obs, fmt.Errorf("open server walk for observation: %w", err)
	}
	liveRows, livePages, err := observeDrillWalk(ctx, opts.maxPages, expected, func(ctx context.Context) ([]client.Row, bool, error) {
		page, err := live.NextPage(ctx)
		return page.Rows, page.NextCursor == "", err
	})
	if err != nil {
		return obs, fmt.Errorf("observe server walk: %w", err)
	}
	if refRows != liveRows {
		return obs, fmt.Errorf("observed %d reference rows and %d server rows", refRows, liveRows)
	}
	obs.Status = observationComplete
	obs.Counts = &drillCounts{Rows: liveRows, ReferencePages: refPages, ServerPages: livePages}
	return obs, nil
}

// Both streams are checked against the fixture's ordered rows independently
// of the drill's lockstep comparator. An empty terminal page counts once.
func observeDrillWalk(ctx context.Context, maxPages int, expected []client.Row, next func(context.Context) ([]client.Row, bool, error)) (uint64, int, error) {
	seen := 0
	for pages := 0; pages < maxPages; pages++ {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		rows, terminal, err := next(ctx)
		if err != nil {
			return 0, 0, err
		}
		for _, row := range rows {
			if seen >= len(expected) || !row.TS.Equal(expected[seen].TS) || row.Line != expected[seen].Line {
				return 0, 0, fmt.Errorf("fixture row %d differs from observed row", seen+1)
			}
			seen++
		}
		if terminal {
			if seen != len(expected) {
				return 0, 0, fmt.Errorf("terminal page after %d rows, expected %d", seen, len(expected))
			}
			if err := ctx.Err(); err != nil {
				return 0, 0, err
			}
			return uint64(seen), pages + 1, nil
		}
	}
	return 0, 0, client.ErrPageBudget
}

func TestDrillObservationNeedsTerminalPage(t *testing.T) {
	t.Parallel()
	bundle, rows, _ := makeBundle(t)
	expected := []client.Row{rows[0], rows[2], rows[3], rows[4]}
	a, b := fixtureCursor('a'), fixtureCursor('b')
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("cursor") {
		case "":
			writeDrillPage(t, w, drillPage{rows: expected[:2], next: a})
		case a:
			writeDrillPage(t, w, drillPage{rows: expected[2:], next: b})
		case b:
			writeDrillPage(t, w, drillPage{})
		default:
			t.Errorf("unexpected query %s", r.URL)
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	c, err := client.New(server.URL, nil)
	check(t, err)
	defer c.CloseIdleConnections()
	opts, err := parseOptions("drill", append(compareArgs(bundle, server.URL)[1:], "-max-pages", "2"))
	check(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), opts.timeout)
	defer cancel()
	before, err := observeDrillPhase(ctx, opts, c, expected)
	if !errors.Is(err, client.ErrPageBudget) {
		t.Fatalf("observation with pending terminal page: %v", err)
	}
	data, err := json.Marshal(drillObservations{Before: before, After: phaseObservation{Status: observationNotRun}})
	check(t, err)
	checkDrillFixture(t, "incomplete-observations.json", data, true)

	opts.maxPages++
	completed, err := observeDrillPhase(ctx, opts, c, expected)
	check(t, err)
	wantCounts := drillCounts{Rows: 4, ReferencePages: 2, ServerPages: 3}
	if completed.Status != observationComplete || completed.Counts == nil || *completed.Counts != wantCounts {
		t.Fatalf("observation with empty terminal page: %+v", completed)
	}
}
