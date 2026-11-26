package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
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
// Capture IDs and positions may each use up to 2048 bytes.
type PageResult struct {
	Capture string
	Rows    []PageRow
	More    bool

	// ExpiresAt on the first page optionally supplies the source's deadline.
	ExpiresAt time.Time
}

// PageSource supplies metered, ordered query results. Repeating a page on a live capture
// with the same limit and position returns the same rows, positions and More
// within the original work cost, even after appends or generation replacement.
// Page and Release may be called concurrently. A source owns the
// deadline and resource limits of any retained captures. It must clean up a
// failed acquisition and hold any operation references until Page returns.
// HTTP calls Release for an unpublished capture, or when a published capture
// expires and its admitted HTTP requests have drained. Published captures
// remain retryable through terminal pages until expiry.
type PageSource interface {
	Page(context.Context, PageRequest, *store.QueryWork) (PageResult, error)
	Release(capture string) error
}

type memoryPageSource struct {
	store     *store.Store
	mu        sync.Mutex
	next      uint64
	snapshots map[string]store.QuerySnapshot
}

func newMemoryPageSource(s *store.Store) *memoryPageSource {
	return &memoryPageSource{store: s, snapshots: make(map[string]store.QuerySnapshot)}
}

func (m *memoryPageSource) Release(id string) error {
	m.mu.Lock()
	delete(m.snapshots, id)
	m.mu.Unlock()
	return nil
}

func (m *memoryPageSource) Page(_ context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	filter := req.Filter
	if !req.Snapshot {
		rows, err := m.store.QueryWithWork(filter.Start, filter.End, filter.Contains, req.Limit, work)
		result := PageResult{Rows: make([]PageRow, len(rows))}
		for i, row := range rows {
			result.Rows[i].Row = row
		}
		return result, err
	}

	id := req.Capture
	var snapshot store.QuerySnapshot
	var after *store.Position
	if id == "" {
		snapshot = m.store.CaptureQuerySnapshot()
	} else {
		var position store.Position
		if err := json.Unmarshal([]byte(req.After), &position); err != nil {
			return PageResult{}, store.ErrInvalidQueryCursor
		}
		after = &position
		m.mu.Lock()
		var ok bool
		snapshot, ok = m.snapshots[id]
		m.mu.Unlock()
		if !ok {
			return PageResult{}, ErrCaptureExpired
		}
	}
	rows, more, err := snapshot.Page(filter.Start, filter.End, filter.Contains, req.Limit, after, work)
	if err != nil {
		return PageResult{}, err
	}
	result := PageResult{Rows: make([]PageRow, len(rows)), More: more}
	for i, row := range rows {
		position, err := memoryPosition(row.Position)
		if err != nil {
			return PageResult{}, err
		}
		result.Rows[i] = PageRow{Row: row.Row, Position: position}
	}
	if id == "" {
		m.mu.Lock()
		m.next++
		id = strconv.FormatUint(m.next, 10)
		m.snapshots[id] = snapshot
		m.mu.Unlock()
	}
	result.Capture = id
	return result, nil
}

func memoryPosition(pos store.TailPosition) (PagePosition, error) {
	data, err := json.Marshal(store.Position{Time: pos.Time, Ordinal: int(pos.Ordinal)})
	return PagePosition(data), err
}
