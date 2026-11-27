package httpapi

import (
	"context"
	"errors"
	"net/http"

	"bog/internal/store"
)

func writeQueryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrQueryWorkExhausted):
		http.Error(w, "query work budget exhausted", http.StatusServiceUnavailable)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "request cancelled", http.StatusRequestTimeout)
	case errors.Is(err, ErrCaptureExpired):
		http.Error(w, "query capture expired", http.StatusGone)
	case errors.Is(err, ErrQueryCapacityExhausted):
		http.Error(w, "query capacity exhausted", http.StatusServiceUnavailable)
	case errors.Is(err, store.ErrInvalidQueryCursor):
		http.Error(w, "invalid cursor", http.StatusBadRequest)
	default:
		var oversized *oversizedQueryRecord
		if errors.As(err, &oversized) {
			http.Error(w, oversized.Error(), http.StatusUnprocessableEntity)
			return
		}
		http.Error(w, "query failed", http.StatusInternalServerError)
	}
}

func (a *API) query(w http.ResponseWriter, r *http.Request) {
	work := store.NewQueryWork(r.Context(), a.queryWorkBudget)
	params, err := parseQuery(r, cursorTokenSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if params.cursor != "" {
		a.resumeQuery(w, ctx, params.filter, params.limit, params.cursor, work)
		return
	}
	if params.page {
		if a.legacyQueryStore != nil {
			a.startLegacyQuery(w, ctx, params.filter, params.limit, work)
		} else {
			a.startQuery(w, ctx, params.filter, params.limit, work)
		}
		return
	}
	result, err := a.captures.source.Page(ctx, PageRequest{Filter: params.filter, Limit: params.limit}, work)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if len(result.Rows) > params.limit {
		writeQueryError(w, errPageSourceContract)
		return
	}
	response := queryResponse{maxBytes: a.maxResponseBytes}
	if _, err := response.appendRows(result.Rows, work); err != nil {
		writeQueryError(w, err)
		return
	}
	if err := response.reserveDelivery(work); err != nil {
		writeQueryError(w, err)
		return
	}
	response.write(w, "")
}
