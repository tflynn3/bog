package httpapi

import (
	"context"
	"net/http"

	"bog/internal/store"
)

const legacyCursorReservation = cursorTokenSize

// Both constructors use the same capture registry, planner, and cursor format.
// The legacy source observes request cancellation through the supplied meter.
func (a *API) legacySnapshotQuery(w http.ResponseWriter, params queryParams, work *store.QueryWork) {
	ctx := context.Background()
	if params.page {
		a.startQuery(w, ctx, params.filter, params.limit, work)
		return
	}
	a.resumeQuery(w, ctx, params.filter, params.limit, params.cursor, work)
}
