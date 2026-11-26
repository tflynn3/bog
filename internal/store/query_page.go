package store

import (
	"strings"
	"time"
)

// SnapshotCursor identifies a row in a captured append prefix. The ordinal
// also identifies its timestamp because rows in the published prefix are immutable.
type SnapshotCursor struct {
	Count   int
	Ordinal int
}

// SnapshotPage returns parallel rows and append ordinals from one traversal.
// More reports whether the snapshot has another matching row after this page.
type SnapshotPage struct {
	Rows     []Row
	Ordinals []int
	Count    int
	More     bool
}

// QuerySnapshotPage captures a snapshot if cursor is nil. Otherwise it verifies
// the anchor against the captured append prefix and resumes strictly after it.
// The caller must bind the cursor to the original query and issuing store.
func (s *Store) QuerySnapshotPage(start, end time.Time, contains string, limit int, cursor *SnapshotCursor, work *QueryWork) (SnapshotPage, error) {
	positions := pagePositions{}
	var anchor *queryAnchor
	if cursor != nil {
		anchor = &queryAnchor{count: cursor.Count, ordinal: cursor.Ordinal}
	}
	rows, captured, next, err := s.query(start, end, contains, limit, anchor, true, &positions, work)
	if err != nil {
		return SnapshotPage{}, err
	}
	return SnapshotPage{Rows: rows, Ordinals: positions.ordinals, Count: captured, More: next != nil}, nil
}

type queryAnchor struct {
	count     int
	ordinal   int
	timestamp *time.Time // Legacy callers supply a timestamp to validate.
}

func (a queryAnchor) validate(rows []Row, start, end time.Time, contains string, work *QueryWork) (Position, error) {
	if a.count < 1 || a.count > len(rows) || a.ordinal < 0 || a.ordinal >= a.count {
		return Position{}, ErrInvalidQueryCursor
	}
	row := rows[a.ordinal]
	// An ordinal-only anchor with an empty filter needs no line inspection.
	if err := work.row(row.Line, a.timestamp != nil || contains != ""); err != nil {
		return Position{}, err
	}
	if (a.timestamp != nil && !row.TS.Equal(*a.timestamp)) ||
		row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, contains) {
		return Position{}, ErrInvalidQueryCursor
	}
	return Position{Time: row.TS, Ordinal: a.ordinal}, nil
}

// pagePositions is optional so legacy row-only callers avoid the ordinal
// allocation. A scan that is abandoned before Bloom selection resets it.
type pagePositions struct {
	ordinals []int
}

func (p *pagePositions) append(ordinal, capacity int) {
	if p == nil {
		return
	}
	if p.ordinals == nil {
		p.ordinals = make([]int, 0, capacity)
	}
	p.ordinals = append(p.ordinals, ordinal)
}

func (p *pagePositions) reset() {
	if p != nil {
		p.ordinals = p.ordinals[:0]
	}
}
