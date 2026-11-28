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
	case errors.Is(err, ErrQueryResponseCapacityExhausted):
		http.Error(w, ErrQueryResponseCapacityExhausted.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, ErrQueryScanCapacityExhausted):
		http.Error(w, ErrQueryScanCapacityExhausted.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, ErrQueryCaptureCapacityExhausted):
		http.Error(w, ErrQueryCaptureCapacityExhausted.Error(), http.StatusServiceUnavailable)
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
	if a.captures.startQuery() {
		defer a.captures.finishQuery()
	} else if params.cursor == "" {
		writeQueryError(w, ErrQueryCapacityExhausted)
		return
	}
	// A resume after Close still authenticates its cursor. The registry
	// rejects its capture before it can reach the source.
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
	response := a.newQueryResponse()
	defer response.release()
	req := PageRequest{Filter: params.filter, Limit: params.limit}
	if err := a.prepareOrdinary(ctx, req, &response, work); err != nil {
		response.release()
		writeQueryError(w, err)
		return
	}
	if err := response.reserveDelivery(work); err != nil {
		response.release()
		writeQueryError(w, err)
		return
	}
	response.write(w, "")
}

// chunkedOrdinaryPageSource uses a short-lived storage capture to assemble
// one ordinary response from bounded scans. It issues no client cursor and
// uses no persistent cursor slot.
type chunkedOrdinaryPageSource interface {
	ChunkOrdinaryQueries()
}

func (a *API) prepareOrdinary(ctx context.Context, req PageRequest, response *queryResponse, work *store.QueryWork) (err error) {
	source := a.captures.source
	_, req.Snapshot = source.(chunkedOrdinaryPageSource)
	if req.Snapshot {
		defer func() {
			if req.Capture != "" {
				err = errors.Join(err, source.Release(req.Capture))
			}
		}()
	}
	for {
		var more bool
		err := a.withPage(ctx, req, work, func(result PageResult) error {
			if req.Snapshot {
				if req.Capture == "" {
					req.Capture = result.Capture
					if !validOpaque(req.Capture) {
						return errPageSourceContract
					}
				}
				if err := checkSnapshotScan(result, req, work); err != nil {
					return err
				}
			} else if len(result.Rows) > req.Limit {
				return errPageSourceContract
			}
			n, err := response.appendRows(result.Rows, work)
			if err == nil && req.Snapshot && result.More && n == len(result.Rows) && n < req.Limit {
				req.After = result.Rows[n-1].Position
				req.Limit -= n
				more = true
			}
			return err
		})
		if err != nil || !more {
			return err
		}
	}
}

func (a *API) openQuery(w http.ResponseWriter, ctx context.Context, params queryParams, work *store.QueryWork) {
	capture, err := a.captures.reserve(a.initResources().captures)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	cursorMayHaveReached := false
	defer func() { a.captures.done(capture, !cursorMayHaveReached) }()

	response := a.newQueryResponse()
	defer response.release()
	var token string
	if a.legacy != nil {
		token, err = a.prepareLegacyOpening(capture, params, &response, work)
	} else {
		token, err = a.prepareRetainedOpening(ctx, capture, params, &response, work)
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
		response.release()
		writeQueryError(w, err)
		return
	}
	if token == "" {
		response.write(w, "")
		return
	}
	response.writeOpening(w, token, &cursorMayHaveReached)
}
