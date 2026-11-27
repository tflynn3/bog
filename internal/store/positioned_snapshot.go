package store

import "time"

// PositionedSnapshot freezes the published rows and their scan indexes together.
// Appends only write beyond the captured slice lengths and replace run levels,
// so retaining these headers preserves both membership and scan work.
type PositionedSnapshot struct {
	view queryView
}

// CapturePositionedSnapshot takes an immutable view of the published prefix.
func (s *Store) CapturePositionedSnapshot() *PositionedSnapshot {
	return &PositionedSnapshot{view: s.queryView()}
}

// Page scans the whole captured prefix, with global ordinals starting at
// firstOrdinal. After is an exclusive bound and may be outside the prefix.
func (s *PositionedSnapshot) Page(start, end time.Time, contains string, limit int, firstOrdinal uint64, after *TailPosition, work *QueryWork) ([]PositionedRow, bool, error) {
	if err := work.Charge(); err != nil {
		return nil, false, err
	}
	return s.view.positionedPage(start, end, contains, limit, len(s.view.rows), firstOrdinal, after, work)
}

// PositionedMatches is a fully scanned in-memory query capture. It retains
// only the published row slice and matching ordinals, in timestamp and append
// order. Its offsets remain valid across appends and index replacement.
type PositionedMatches struct {
	rows     []Row
	ordinals []int
}

// CapturePositionedMatches scans a single published view through exhaustion.
// It returns no capture on cancellation, work exhaustion, or if there are
// more than maxMatches matches. An empty literal needs no line-byte probes.
func (s *Store) CapturePositionedMatches(start, end time.Time, contains string, maxMatches int, work *QueryWork) (*PositionedMatches, error) {
	if err := work.Charge(); err != nil {
		return nil, err
	}
	if maxMatches <= 0 {
		return nil, ErrQueryWorkExhausted
	}
	view := s.CapturePositionedSnapshot().view
	matches := queryMatches{limit: maxMatches, lookahead: true, ordinals: []int{}}
	if err := view.scan(start, end, contains, len(view.rows), nil, &matches, work); err != nil {
		return nil, err
	}
	if matches.more {
		return nil, ErrQueryWorkExhausted
	}
	return &PositionedMatches{rows: view.rows, ordinals: matches.ordinals}, nil
}

func (s *PositionedMatches) Len() int { return len(s.ordinals) }

// At reads the match at offset, which must be in [0, Len()). The operation
// takes constant work with no seek, filtering or lookahead. Callers replaying
// a page charge one unit per accessed row.
func (s *PositionedMatches) At(offset int) PositionedRow {
	ordinal := s.ordinals[offset]
	row := s.rows[ordinal]
	return PositionedRow{row, TailPosition{row.TS.UTC(), uint64(ordinal)}}
}
