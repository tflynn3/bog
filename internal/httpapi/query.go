package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

// The HTTP deadline also bounds the stateless in-memory capture. A source
// holding storage references can expire its capture sooner.
const maxCaptureLifetime = 15 * time.Minute

type oversizedQueryRecord struct{ cap, required int }

func (e oversizedQueryRecord) Error() string {
	return fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", e.cap, e.required)
}

func writeQueryError(w http.ResponseWriter, err error) {
	var oversized oversizedQueryRecord
	switch {
	case errors.Is(err, store.ErrQueryWorkExhausted):
		http.Error(w, store.ErrQueryWorkExhausted.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, ErrQueryCapacityExhausted):
		http.Error(w, ErrQueryCapacityExhausted.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "request cancelled", http.StatusRequestTimeout)
	case errors.Is(err, store.ErrInvalidQueryCursor):
		http.Error(w, "invalid cursor", http.StatusBadRequest)
	case errors.Is(err, ErrCaptureExpired):
		http.Error(w, ErrCaptureExpired.Error(), http.StatusGone)
	case errors.As(err, &oversized):
		http.Error(w, oversized.Error(), http.StatusUnprocessableEntity)
	default:
		http.Error(w, "query failed", http.StatusInternalServerError)
	}
}

func (a *API) query(w http.ResponseWriter, r *http.Request) {
	params, err := parseQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	work := store.NewQueryWork(ctx, a.queryWorkBudget)
	req := PageRequest{Filter: params.filter, Limit: params.limit, Snapshot: params.page || params.cursor != ""}
	cursor := cursorData{Version: 1, Start: req.Filter.Start, End: req.Filter.End}
	if params.cursor != "" {
		cursor, err = a.authenticateCursor(params.cursor, req.Filter, work)
		if err != nil {
			writeQueryError(w, err)
			return
		}
		req.Capture, req.After = cursor.Capture, cursor.After
	} else if params.page {
		cursor.ExpiresAt = time.Now().Add(maxCaptureLifetime).UnixNano()
	}

	var response *queryResponse
	var token string
	var continuationWork *store.QueryWork
	if req.Snapshot {
		req.Prepare = func(page PageResult) (PagePosition, *store.QueryWork, error) {
			if params.page {
				if page.Capture == "" || !utf8.ValidString(page.Capture) {
					return "", nil, errors.New("invalid query source capture ID")
				}
				cursor.Capture = page.Capture
			}
			var after PagePosition
			var err error
			response, after, err = a.prepareQueryPage(req, page, work, false)
			if err != nil || after == "" {
				return "", nil, err
			}
			if params.page {
				if err := work.Charge(uint64(len(req.Filter.Contains))); err != nil {
					return "", nil, err
				}
				cursor.Contains = a.containsDigest(req.Filter.Contains)
			}
			cursor.After = after
			token, err = a.encodeCursor(cursor)
			if err != nil {
				return "", nil, err
			}
			// A continuation gets a fresh request budget when the client resumes.
			// Use that same allowance to check whether its response can be prepared.
			continuationWork = store.NewQueryWork(ctx, a.queryWorkBudget)
			err = chargeCursorValidation(continuationWork, len(token), len(req.Filter.Contains))
			return after, continuationWork, err
		}
	}

	page, err := a.pages.Page(ctx, req, work)
	retain := false
	if params.page && page.Capture != "" {
		defer func() {
			if !retain {
				if err := a.pages.Release(page.Capture); err != nil {
					log.Printf("release query capture: %v", err)
				}
			}
		}()
	}
	if err == nil {
		switch {
		case !req.Snapshot:
			response, _, err = a.prepareQueryPage(req, page, work, false)
		case response == nil:
			err = errors.New("query source did not prepare snapshot page")
		case token != "":
			req.After = cursor.After
			err = a.checkContinuation(req, page.Continuation, continuationWork)
			if err == nil && cursor.expired() {
				err = ErrCaptureExpired
			}
		}
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	// Once a cursor may reach the caller, its capture must remain retryable,
	// even if the response write reports an error.
	retain = token != ""
	deliverQueryPage(w, response, token)
}

func (a *API) prepareQueryPage(req PageRequest, page PageResult, work *store.QueryWork, measureOnly bool) (*queryResponse, PagePosition, error) {
	if len(page.Rows) > req.Limit || (page.More && len(page.Rows) == 0) {
		return nil, "", errors.New("invalid query source page")
	}
	response := &queryResponse{maxBytes: a.maxResponseBytes, measureOnly: measureOnly}
	emitted := 0
	for _, row := range page.Rows {
		fits, required, err := response.appendRow(row.Row, work)
		if err != nil {
			return nil, "", err
		}
		if !fits {
			if emitted == 0 {
				return nil, "", oversizedQueryRecord{a.maxResponseBytes, required}
			}
			break
		}
		emitted++
	}
	delivery := (uint64(response.size) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit
	if err := work.Charge(delivery); err != nil {
		return nil, "", err
	}
	if !req.Snapshot || (!page.More && emitted == len(page.Rows)) {
		return response, "", nil
	}
	position := page.Rows[emitted-1].Position
	if page.legacyPosition != nil {
		var err error
		position, err = page.legacyPosition(emitted, work)
		if err != nil {
			return nil, "", err
		}
	}
	if position == "" || position == req.After || !utf8.ValidString(string(position)) {
		return nil, "", errors.New("invalid query source position")
	}
	return response, position, nil
}

func (a *API) checkContinuation(req PageRequest, page *PageResult, work *store.QueryWork) error {
	if page == nil || len(page.Rows) == 0 {
		return errors.New("query source continuation returned no rows")
	}
	_, _, err := a.prepareQueryPage(req, *page, work, true)
	// An oversized next row still has its own 422 diagnostic.
	var oversized oversizedQueryRecord
	if errors.As(err, &oversized) {
		return nil
	}
	return err
}

func deliverQueryPage(w http.ResponseWriter, response *queryResponse, token string) {
	if token != "" {
		w.Header().Set("Bog-Next-Cursor", token)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Length", strconv.Itoa(response.size))
	w.WriteHeader(http.StatusOK)
	for _, record := range response.rows {
		if n, err := w.Write(record); err != nil || n != len(record) {
			// The declared length lets the client detect an interrupted body.
			return
		}
	}
}
