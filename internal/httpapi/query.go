package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"bog/internal/store"
)

var decimal = regexp.MustCompile(`^[0-9]+$`)

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
	q, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil {
		http.Error(w, "invalid query", 400)
		return
	}
	for k, v := range q {
		if (k != "start" && k != "end" && k != "contains" && k != "limit" && k != "page" && k != "cursor") || len(v) != 1 {
			http.Error(w, "invalid parameter", 400)
			return
		}
	}
	start, e1 := store.ParseTime(q.Get("start"))
	end, e2 := store.ParseTime(q.Get("end"))
	limit := 500
	var e3 error
	if v, ok := q["limit"]; ok {
		if !decimal.MatchString(v[0]) {
			e3 = errors.New("invalid limit")
		} else {
			limit, e3 = strconv.Atoi(v[0])
		}
	}
	if e1 != nil || e2 != nil || e3 != nil || end.Before(start) || limit < 1 || limit > maxQueryLimit {
		http.Error(w, "invalid query", 400)
		return
	}
	_, page := q["page"]
	cursor, continuation := q["cursor"]
	if (page && (q.Get("page") != "1" || continuation)) || (continuation && cursor[0] == "") {
		http.Error(w, "invalid pagination", 400)
		return
	}
	// Reserve space for a fixed-size cursor and ordinary HTTP headers within
	// the server's 1 MiB header limit. Preserve the request target as received:
	// re-encoding already valid query characters can inflate its size.
	if page {
		if len(r.RequestURI)+2048+4096 > 1<<20 {
			http.Error(w, "query too large for continuation", 400)
			return
		}
	}

	filter := PageFilter{Start: start.UTC(), End: end.UTC(), Contains: q.Get("contains")}
	if continuation {
		a.resumeQuery(w, r.Context(), filter, limit, cursor[0], work)
		return
	}
	if page {
		a.startQuery(w, r.Context(), filter, limit, work)
		return
	}
	result, err := a.captures.source.Page(r.Context(), PageRequest{Filter: filter, Limit: limit}, work)
	if err != nil {
		writeQueryError(w, err)
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
