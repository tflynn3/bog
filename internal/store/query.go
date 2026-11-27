package store

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Position identifies a row by timestamp and zero-based append ordinal.
type Position struct {
	Time    time.Time `json:"time"`
	Ordinal int       `json:"ordinal"`
}

// precedes reports whether the row is strictly after the optional lower bound.
func (p *Position) precedes(t time.Time, ordinal int) bool {
	return p == nil || t.After(p.Time) || (t.Equal(p.Time) && ordinal > p.Ordinal)
}

// TailPosition orders rows from both the checkpoint and tail by timestamp and
// zero-based global append ordinal.
type TailPosition struct {
	Time    time.Time
	Ordinal uint64
}

// PositionedRow carries the position to use when resuming after Row.
type PositionedRow struct {
	Row      Row
	Position TailPosition
}

// QueryPositionedPage returns up to limit matching rows from the first count
// published rows, ordered by UTC timestamp and global append ordinal. It
// matches a case-sensitive literal in the half-open interval [start, end).
// Keep the captured Store, count, and firstOrdinal together for the lifetime
// of the capture. The Store may continue to receive appends.
//
// After is an exclusive lower bound and may refer to a row outside the tail.
// More is true only when the scan finds a further match in the captured prefix.
// On error, the method returns no rows and more is false.
func (s *Store) QueryPositionedPage(start, end time.Time, contains string, limit, count int, firstOrdinal uint64, after *TailPosition, work *QueryWork) (rows []PositionedRow, more bool, err error) {
	if err := work.Charge(); err != nil {
		return nil, false, err
	}
	return s.queryView().positionedPage(start, end, contains, limit, count, firstOrdinal, after, work)
}

func (v queryView) positionedPage(start, end time.Time, contains string, limit, count int, firstOrdinal uint64, after *TailPosition, work *QueryWork) ([]PositionedRow, bool, error) {
	if count < 0 || count > len(v.rows) {
		return nil, false, fmt.Errorf("store: published prefix count %d outside [0, %d]", count, len(v.rows))
	}
	if count > 0 && uint64(count-1) > math.MaxUint64-firstOrdinal {
		return nil, false, errors.New("store: published prefix ordinals overflow uint64")
	}
	if limit <= 0 {
		return nil, false, errors.New("store: positioned page limit must be positive")
	}
	var lower *Position
	if after != nil {
		// Clamp a global ordinal outside the prefix to a local sentinel.
		// The ordinal only participates in comparisons at the same instant.
		ordinal := -1
		if after.Ordinal >= firstOrdinal {
			ordinal = int(min(after.Ordinal-firstOrdinal, uint64(count)))
		}
		lower = &Position{after.Time, ordinal}
	}
	matches := queryMatches{limit: limit, lookahead: true, positioned: []PositionedRow{}, firstOrdinal: firstOrdinal}
	if err := v.scan(start, end, contains, count, lower, &matches, work); err != nil {
		return nil, false, err
	}
	return matches.positioned, matches.more, nil
}

type queryView struct {
	rows     []Row
	blocks   []*blockFilter
	restored indexRun
	levels   []indexRun
}

func (s *Store) queryView() queryView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return queryView{s.rows, s.blocks, s.restored, s.runs}
}

// queryMatches collects either legacy rows or positioned rows from an ordered
// scan. A match beyond the limit establishes continuation without being stored.
type queryMatches struct {
	limit        int
	lookahead    bool
	firstOrdinal uint64
	rows         []Row
	positioned   []PositionedRow // non-nil when collecting positioned results
	ordinals     []int           // non-nil when collecting an immutable match list
	last         Position
	more         bool
}

func (p *queryMatches) full() bool {
	return len(p.rows)+len(p.positioned)+len(p.ordinals) == p.limit
}

func (p *queryMatches) reset() {
	p.rows = p.rows[:0]
	p.positioned = p.positioned[:0]
	p.ordinals = p.ordinals[:0]
}

func (p *queryMatches) add(row Row, ordinal, capacity int) (done bool) {
	if p.full() {
		p.more = true
		return true
	}
	if p.ordinals != nil {
		p.ordinals = append(p.ordinals, ordinal)
	} else if p.positioned != nil {
		if cap(p.positioned) == 0 {
			p.positioned = make([]PositionedRow, 0, min(p.limit, capacity))
		}
		p.positioned = append(p.positioned, PositionedRow{row, TailPosition{row.TS.UTC(), p.firstOrdinal + uint64(ordinal)}})
	} else {
		if cap(p.rows) == 0 {
			p.rows = make([]Row, 0, min(p.limit, capacity))
		}
		p.rows = append(p.rows, row)
		p.last = Position{row.TS, ordinal}
	}
	return !p.lookahead && p.full()
}

// QueryPage captures the published row count when both snapshot and after are nil.
// A continuation requires both values, and after must identify a matching row
// in the supplied snapshot.
func (s *Store) QueryPage(start, end time.Time, contains string, limit int, snapshot *int, after *Position) ([]Row, int, *Position, bool) {
	rows, count, next, err := s.QueryPageWithWork(start, end, contains, limit, snapshot, after, nil)
	return rows, count, next, err == nil
}

// QueryPageWithWork is QueryPage with cancellation and work accounting.
func (s *Store) QueryPageWithWork(start, end time.Time, contains string, limit int, snapshot *int, after *Position, work *QueryWork) ([]Row, int, *Position, error) {
	return s.query(start, end, contains, limit, snapshot, after, true, work)
}

func (s *Store) query(start, end time.Time, contains string, limit int, snapshot *int, after *Position, lookahead bool, work *QueryWork) ([]Row, int, *Position, error) {
	if err := work.Charge(); err != nil {
		return nil, 0, nil, err
	}
	if (snapshot == nil) != (after == nil) {
		return nil, 0, nil, ErrInvalidQueryCursor
	}
	view := s.queryView()
	count := len(view.rows)
	if snapshot != nil {
		count = *snapshot
		if count < 1 || count > len(view.rows) || after.Ordinal < 0 || after.Ordinal >= count {
			return nil, 0, nil, ErrInvalidQueryCursor
		}
		row := view.rows[after.Ordinal]
		if err := work.row(row.Line, true); err != nil {
			return nil, 0, nil, err
		}
		if !row.TS.Equal(after.Time) || row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, contains) {
			return nil, 0, nil, ErrInvalidQueryCursor
		}
	}
	matches := queryMatches{limit: limit, lookahead: lookahead, rows: []Row{}}
	if err := view.scan(start, end, contains, count, after, &matches, work); err != nil {
		return nil, 0, nil, err
	}
	var next *Position
	if matches.more {
		last := matches.last
		next = &last
	}
	return matches.rows, count, next, nil
}

func (s *Store) Query(start, end time.Time, contains string, limit int) []Row {
	rows, _ := s.QueryWithWork(start, end, contains, limit, nil)
	return rows
}

// QueryWithWork is Query with cancellation and work accounting.
func (s *Store) QueryWithWork(start, end time.Time, contains string, limit int, work *QueryWork) ([]Row, error) {
	rows, _, _, err := s.query(start, end, contains, limit, nil, nil, false, work)
	return rows, err
}
