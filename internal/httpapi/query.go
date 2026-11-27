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
	tokenSize := cursorTokenSize
	if a.legacy != nil {
		tokenSize = legacyCursorReservation
	}
	params, err := parseQuery(r, tokenSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if params.cursor != "" {
		if a.legacy != nil {
			a.resumeLegacyQuery(w, params, work)
		} else {
			a.resumeQuery(w, ctx, params.filter, params.limit, params.cursor, work)
		}
		return
	}
	if params.page {
		a.openQuery(w, ctx, params, work)
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

func (a *API) openQuery(w http.ResponseWriter, ctx context.Context, params queryParams, work *store.QueryWork) {
	capture, err := a.captures.reserve()
	if err != nil {
		writeQueryError(w, err)
		return
	}
	cursorMayHaveReached := false
	defer func() { a.captures.done(capture, !cursorMayHaveReached) }()

	var response queryResponse
	var token string
	if a.legacy != nil {
		response, token, err = a.prepareLegacyOpening(capture, params, work)
	} else {
		response, token, err = a.prepareRetainedOpening(ctx, capture, params, work)
	}
	if err == nil && token != "" {
		// Register the plan before the client can see the token. The opening
		// request holds its reference through delivery and failure cleanup.
		err = a.captures.publish(capture)
	}
	if err == nil {
		err = work.Charge()
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if token == "" {
		response.write(w, "")
		return
	}
	response.writeOpening(w, token, &cursorMayHaveReached)
}
