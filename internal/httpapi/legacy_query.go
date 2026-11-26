package httpapi

import (
	"context"
	"net/http"

	"bog/internal/store"
)

const legacyCursorReservation = cursorTokenSize

// Small unfiltered in-memory snapshots can batch primitive work. Reserve the
// whole request allowance up front and cap the number of batched units so
// candidate bytes and encoding still run under a finite, cancellable meter.
const (
	compactSnapshotRows = 8
	compactScalarUnits  = 2
	compactBatchUnits   = 9
	compactWorkPerUnit  = 5
)

func compactSnapshotBudget(budget uint64) uint64 {
	batched := min(budget-min(budget, compactScalarUnits), compactBatchUnits)
	extra := batched * (compactWorkPerUnit - 1)
	return budget + min(extra, ^uint64(0)-budget)
}

type legacyQueryContext struct {
	context.Context
	work *store.QueryWork
}

func (c legacyQueryContext) Err() error {
	return c.work.Charge()
}

// Both constructors use the same capture registry, planner, and cursor format.
// The legacy source observes request cancellation through the supplied meter.
func (a *API) legacySnapshotQuery(w http.ResponseWriter, params queryParams, work *store.QueryWork) {
	ctx := legacyQueryContext{Context: context.Background(), work: work}
	if params.page {
		a.startQuery(w, ctx, params.filter, params.limit, work)
		return
	}
	a.resumeQuery(w, ctx, params.filter, params.limit, params.cursor, work)
}
