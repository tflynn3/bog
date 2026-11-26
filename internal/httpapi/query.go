package httpapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

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
	if err := a.captures.beginRequest(); err != nil {
		writeQueryError(w, err)
		return
	}
	defer a.captures.endRequest()
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
		if len(r.RequestURI)+maxCursorLength+4096 > 1<<20 {
			http.Error(w, "query too large for continuation", 400)
			return
		}
	}
	req := PageRequest{
		Filter: PageFilter{Start: start.UTC(), End: end.UTC(), Contains: q.Get("contains")},
		Limit:  limit, Snapshot: page || continuation,
	}
	response := queryResponse{maxBytes: a.maxResponseBytes}
	var token string
	var err error
	switch {
	case continuation:
		var c *queryCapture
		var position cursorData
		var filter [sha256.Size]byte
		c, position, filter, err = a.openQueryCursor(req, cursor[0], work)
		if err == nil {
			defer a.captures.finish(c)
			token, err = a.resumeQuery(r.Context(), c, req, position, filter, &response, work)
			if err == nil && token != "" && !a.captures.now().Before(c.deadline) {
				a.captures.retire(c)
				err = ErrCaptureExpired
			}
		}
	case page:
		var c *queryCapture
		c, err = a.captures.reserve()
		if err == nil {
			defer a.captures.finish(c)
			token, err = a.firstQueryPage(r.Context(), c, req, &response, work)
		}
	default:
		var result PageResult
		result, err = a.source.Page(r.Context(), req, work)
		if err == nil && len(result.Rows) > req.Limit {
			err = errInvalidPage
		}
		if err == nil {
			_, err = response.appendRows(result.Rows, work)
		}
		if err == nil {
			err = work.Charge(queryDeliveryUnits(response.size))
		}
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if token != "" {
		w.Header().Set("Bog-Next-Cursor", token)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Length", strconv.Itoa(response.size))
	w.WriteHeader(http.StatusOK)
	for _, record := range response.rows {
		if n, err := w.Write(record); err != nil || n != len(record) {
			// The declared length lets the client detect the interrupted body.
			return
		}
	}
}

type oversizedQueryRecord struct{ cap, required int }

func (e *oversizedQueryRecord) Error() string {
	return fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", e.cap, e.required)
}

func (b *queryResponse) appendRows(rows []PageRow, work *store.QueryWork) (int, error) {
	for i, row := range rows {
		fits, required, err := b.appendRow(row.Row, work)
		if err != nil {
			return i, err
		}
		if !fits {
			if b.size == 0 {
				return 0, &oversizedQueryRecord{b.maxBytes, required}
			}
			return i, nil
		}
	}
	return len(rows), nil
}

func (a *API) firstQueryPage(ctx context.Context, c *queryCapture, req PageRequest, response *queryResponse, work *store.QueryWork) (string, error) {
	page, err := a.source.Page(ctx, req, work)
	if err != nil {
		return "", err
	}
	c.sourceID = page.Capture
	if err := validateSnapshotResult(req, page); err != nil {
		return "", err
	}
	c.sourceID = strings.Clone(c.sourceID)
	if !page.ExpiresAt.IsZero() && page.ExpiresAt.Before(c.deadline) {
		c.deadline = page.ExpiresAt
	}
	if !a.captures.now().Before(c.deadline) {
		return "", ErrCaptureExpired
	}
	n, err := response.appendRows(page.Rows, work)
	if err != nil {
		return "", err
	}
	if err := work.Charge(queryDeliveryUnits(response.size)); err != nil {
		return "", err
	}
	if n == len(page.Rows) && !page.More {
		return "", nil
	}
	after := PagePosition(strings.Clone(string(page.Rows[n-1].Position)))
	if err := work.Charge(uint64(len(req.Filter.Contains)), cursorUnits(len(after))); err != nil {
		return "", err
	}
	if err := a.certifyQuery(ctx, c, req.Filter, after); err != nil {
		return "", err
	}
	token := a.encodeCursor(cursorData{capture: c.id, deadline: c.deadline, after: after}, a.filterDigest(req.Filter))
	if err := work.Charge(); err != nil {
		return "", err
	}
	if err := a.captures.publish(c); err != nil {
		return "", err
	}
	return token, nil
}

func (a *API) openQueryCursor(req PageRequest, token string, work *store.QueryWork) (*queryCapture, cursorData, [sha256.Size]byte, error) {
	var filter [sha256.Size]byte
	if len(token) > maxCursorLength {
		return nil, cursorData{}, filter, store.ErrInvalidQueryCursor
	}
	if err := work.Charge(cursorTokenUnits(len(token)), uint64(len(req.Filter.Contains))); err != nil {
		return nil, cursorData{}, filter, err
	}
	filter = a.filterDigest(req.Filter)
	cursor, err := a.decodeCursor(token, filter)
	if err != nil {
		return nil, cursorData{}, filter, err
	}
	c, err := a.captures.acquire(cursor.capture)
	if err != nil {
		return nil, cursorData{}, filter, err
	}
	if !cursor.deadline.Equal(c.deadline) || int(cursor.window) >= len(c.windows) {
		a.captures.finish(c)
		return nil, cursorData{}, filter, store.ErrInvalidQueryCursor
	}
	return c, cursor, filter, nil
}

func (a *API) resumeQuery(ctx context.Context, c *queryCapture, req PageRequest, cursor cursorData, filter [sha256.Size]byte, response *queryResponse, work *store.QueryWork) (string, error) {
	req.Capture = c.sourceID
	remaining := req.Limit
	for remaining > 0 && int(cursor.window) < len(c.windows) {
		window := c.windows[cursor.window]
		// The previous window may already have supplied a valid prefix.
		// Keep enough budget to sign its last position if we stop here.
		if window.cost+window.nextTokenCost > work.Remaining() {
			if response.size == 0 {
				return "", store.ErrQueryWorkExhausted
			}
			break
		}
		req.After, req.Limit = window.after, window.limit
		result, err := a.source.Page(ctx, req, work.WithLimit(window.scanCost))
		if err != nil {
			if errors.Is(err, ErrCaptureExpired) {
				a.captures.retire(c)
			}
			return "", err
		}
		offset := int(cursor.offset)
		if (result.Capture != "" && result.Capture != c.sourceID) ||
			len(result.Rows) != window.count || result.More != window.more ||
			result.Rows[window.count-1].Position != window.last || offset >= window.count {
			return "", errInvalidPage
		}
		after := window.after
		if offset > 0 {
			after = result.Rows[offset-1].Position
		}
		if after != cursor.after {
			return "", errInvalidPage
		}

		rows := result.Rows[offset:min(window.count, offset+remaining)]
		delivered := queryDeliveryUnits(response.size)
		n, err := response.appendRows(rows, work)
		if err != nil {
			return "", err
		}
		if err := work.Charge(queryDeliveryUnits(response.size) - delivered); err != nil {
			return "", err
		}
		if n > 0 {
			cursor.after = rows[n-1].Position
			cursor.offset += uint16(n)
			remaining -= n
		}
		if int(cursor.offset) == window.count {
			cursor.window++
			cursor.offset = 0
		}
		if n < len(rows) {
			break
		}
	}
	if int(cursor.window) == len(c.windows) {
		return "", work.Charge()
	}
	if !validPagePosition(cursor.after) {
		return "", errInvalidPage
	}
	if err := work.Charge(cursorUnits(len(cursor.after))); err != nil {
		return "", err
	}
	return a.encodeCursor(cursor, filter), nil
}
