package store

import "time"

// PositionedSnapshot freezes the published rows and their scan indexes together.
// Appends only write beyond the captured slice lengths and replace run levels,
// so retaining these headers preserves both membership and scan work.
type PositionedSnapshot struct {
	frozen Store
}

func (s *Store) CapturePositionedSnapshot() *PositionedSnapshot {
	view := s.queryView()
	return &PositionedSnapshot{frozen: Store{
		rows: view.rows, blocks: view.blocks, restored: view.restored, runs: view.levels,
	}}
}

// Page scans the whole captured prefix, with global ordinals starting at
// firstOrdinal. After is an exclusive bound and may be outside the prefix.
func (s *PositionedSnapshot) Page(start, end time.Time, contains string, limit int, firstOrdinal uint64, after *TailPosition, work *QueryWork) ([]PositionedRow, bool, error) {
	return s.frozen.QueryPositionedPage(start, end, contains, limit, len(s.frozen.rows), firstOrdinal, after, work)
}
