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
