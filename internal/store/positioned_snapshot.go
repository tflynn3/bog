package store

import (
	"errors"
	"math"
	"time"
)

// PositionedSnapshot retains one immutable publication, including the index
// and Bloom summaries used to scan it. Appends cannot change its scan cost.
type PositionedSnapshot struct {
	view         queryView
	firstOrdinal uint64
}

// CapturePositionedSnapshot captures the current store publication. The
// first ordinal identifies the first row in this store in the global log.
func (s *Store) CapturePositionedSnapshot(firstOrdinal uint64) (PositionedSnapshot, error) {
	view := s.queryView()
	if len(view.rows) > 0 && uint64(len(view.rows)-1) > math.MaxUint64-firstOrdinal {
		return PositionedSnapshot{}, errors.New("store: published ordinals overflow uint64")
	}
	return PositionedSnapshot{view: view, firstOrdinal: firstOrdinal}, nil
}

// Page returns the ordered matches strictly after after, with a lookahead
// match establishing More. The snapshot can be shared by concurrent readers.
func (s PositionedSnapshot) Page(start, end time.Time, contains string, limit int, after *TailPosition, work *QueryWork) ([]PositionedRow, bool, error) {
	if err := work.Charge(); err != nil {
		return nil, false, err
	}
	if limit <= 0 {
		return nil, false, errors.New("store: positioned page limit must be positive")
	}
	count := len(s.view.rows)
	var lower *Position
	if after != nil {
		ordinal := -1
		if after.Ordinal >= s.firstOrdinal {
			ordinal = int(min(after.Ordinal-s.firstOrdinal, uint64(count)))
		}
		lower = &Position{after.Time, ordinal}
	}
	matches := queryMatches{limit: limit, lookahead: true, positioned: []PositionedRow{}, firstOrdinal: s.firstOrdinal}
	if err := s.view.scan(start, end, contains, count, lower, &matches, work); err != nil {
		return nil, false, err
	}
	return matches.positioned, matches.more, nil
}
