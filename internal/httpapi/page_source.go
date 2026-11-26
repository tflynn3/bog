package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"bog/internal/store"
)

// ErrCaptureExpired indicates that a previously issued capture is unavailable.
var ErrCaptureExpired = errors.New("query capture expired")

// ErrQueryCapacityExhausted indicates that no query resource slot is available.
var ErrQueryCapacityExhausted = errors.New("query capacity exhausted")

// PageFilter is a half-open UTC range and an exact case-sensitive literal.
type PageFilter struct {
	Start, End time.Time
	Contains   string
}

// PagePosition is an opaque, UTF-8 continuation key within a capture.
type PagePosition string

// PageRequest starts a snapshot capture when Capture and After are empty.
// Otherwise it scans the supplied capture strictly after After. Ordinary
// queries use Snapshot=false and need neither a persistent capture nor lookahead.
type PageRequest struct {
	Filter   PageFilter
	Limit    int
	Snapshot bool
	Capture  string
	After    PagePosition
}

// PageRow carries a row and its exclusive resume position for a snapshot.
type PageRow struct {
	Row      store.Row
	Position PagePosition
}

// PageResult reports rows in timestamp and append order. More is definitive
// for snapshot requests. A first snapshot page returns its new opaque UTF-8
// capture ID. A continuation may omit Capture or return the same ID.
// The ID and a position must fit within a signed cursor.
type PageResult struct {
	Capture string
	Rows    []PageRow
	More    bool

	// Only the legacy store needs to recover an ordinal with a shorter scan.
	legacyPosition func(int, *store.QueryWork) (PagePosition, error)
}

// PageSource supplies metered, ordered query results. Repeated scans of a
// live capture see identical rows and positions regardless of later writes.
// Page and Release may be called concurrently. A source owns the
// deadline and resource limits of any retained captures. It must clean up a
// failed acquisition and hold any operation references until Page returns.
// HTTP calls Release for a first page that publishes no cursor. Captures
// published in a cursor remain retryable through terminal pages until expiry.
type PageSource interface {
	Page(context.Context, PageRequest, *store.QueryWork) (PageResult, error)
	Release(capture string) error
}

type memoryPageSource struct{ store *store.Store }

func (m memoryPageSource) Release(string) error { return nil }

func (m memoryPageSource) Page(_ context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	filter := req.Filter
	if !req.Snapshot {
		rows, err := m.store.QueryWithWork(filter.Start, filter.End, filter.Contains, req.Limit, work)
		return PageResult{Rows: pageRows(rows)}, err
	}
	scan := func(limit int, count *int, after *store.Position, work *store.QueryWork) ([]store.Row, int, *store.Position, error) {
		return m.store.QueryPageWithWork(filter.Start, filter.End, filter.Contains, limit, count, after, work)
	}

	var rows []store.Row
	var captured int
	var anchor, next *store.Position
	var err error
	first := req.Capture == ""
	if first {
		rows, captured, next, err = scan(1, nil, nil, work)
		if err == nil && next != nil && req.Limit > 1 {
			anchor = next
			var rest []store.Row
			rest, _, next, err = scan(req.Limit-1, &captured, anchor, work)
			rows = append(rows, rest...)
		}
	} else {
		captured, err = strconv.Atoi(req.Capture)
		var position store.Position
		if err != nil || json.Unmarshal([]byte(req.After), &position) != nil {
			return PageResult{}, store.ErrInvalidQueryCursor
		}
		anchor = &position
		rows, _, next, err = scan(req.Limit, &captured, anchor, work)
	}
	if err != nil {
		return PageResult{}, err
	}
	result := PageResult{Capture: strconv.Itoa(captured), Rows: pageRows(rows), More: next != nil}
	result.legacyPosition = func(emitted int, work *store.QueryWork) (PagePosition, error) {
		position := next
		if emitted < len(rows) {
			shortLimit := emitted
			if first {
				shortLimit--
			}
			position = anchor
			if shortLimit != 0 {
				var err error
				_, _, position, err = scan(shortLimit, &captured, anchor, work)
				if err != nil {
					return "", err
				}
			}
		}
		if position == nil {
			return "", errors.New("query page has no continuation position")
		}
		data, err := json.Marshal(position)
		return PagePosition(data), err
	}
	return result, nil
}

func pageRows(rows []store.Row) []PageRow {
	result := make([]PageRow, len(rows))
	for i, row := range rows {
		result[i].Row = row
	}
	return result
}
