package store

import "time"

// PageAnchor resumes a snapshot after a zero-based append ordinal.
type PageAnchor struct {
	Count   int
	Ordinal int
}

// SnapshotPage contains each matching row and its corresponding append ordinal.
// HasMore indicates that at least one more match exists within Count.
type SnapshotPage struct {
	Rows     []Row
	Ordinals []int
	Count    int
	HasMore  bool
}

// QuerySnapshotPage captures the append count and row positions in one traversal.
// A nil anchor opens a snapshot. A non-nil anchor must identify a matching row
// in the supplied snapshot. Ordinals allow callers to resume after any row in
// the returned prefix, for example when a response byte cap cuts a page short.
func (s *Store) QuerySnapshotPage(start, end time.Time, contains string, limit int, anchor *PageAnchor, work *QueryWork) (SnapshotPage, error) {
	var count *int
	var after *Position
	if anchor != nil {
		count = &anchor.Count
		after = &Position{Ordinal: anchor.Ordinal}
	}
	matched, captured, err := s.query(start, end, contains, limit, count, after, true, true, work)
	if err != nil {
		return SnapshotPage{}, err
	}
	return SnapshotPage{Rows: matched.rows, Ordinals: matched.ordinals, Count: captured, HasMore: matched.more}, nil
}

type queryMatches struct {
	rows     []Row
	ordinals []int
	last     int
	more     bool
}

// add receives matches in query order and reports when the scan can stop.
func (m *queryMatches) add(row Row, ordinal, limit, count int, lookahead, collectOrdinals bool) bool {
	if len(m.rows) == limit {
		m.more = true
		return true
	}
	if cap(m.rows) == 0 {
		m.rows = make([]Row, 0, min(limit, count))
	}
	m.rows = append(m.rows, row)
	m.last = ordinal
	if collectOrdinals {
		if m.ordinals == nil {
			m.ordinals = make([]int, 0, cap(m.rows))
		}
		m.ordinals = append(m.ordinals, ordinal)
	}
	return !lookahead && len(m.rows) == limit
}
