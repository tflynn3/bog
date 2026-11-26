package store

import (
	"context"
	"errors"
	"math/bits"
	"time"
)

var (
	ErrQueryWorkExhausted = errors.New("query work budget exhausted")
	ErrInvalidQueryCursor = errors.New("invalid cursor")
)

// QueryWork meters a single request and may be reused across sequential scans.
// A nil meter permits unlimited work for callers of the legacy Store methods.
//
// Each scan charges one unit per index run, up to the worst-case number of
// comparisons for each binary seek, one unit per snapshot visibility probe,
// and one unit per block considered for Bloom pruning (at most 96 bit tests).
// It charges one unit per candidate visited plus
// one per decoded line byte for candidates eligible for matching. Cursor anchor
// checks and lookahead candidates use the same row and byte rate. Revisited work
// is charged again, including after an optimization falls back to another scan.
type QueryWork struct {
	ctx       context.Context
	remaining uint64
}

func NewQueryWork(ctx context.Context, units uint64) *QueryWork {
	return &QueryWork{ctx: ctx, remaining: units}
}

// Charge checks cancellation first, then reserves units before doing work.
// Multiple charges can be reserved together without overflowing their sum.
func (w *QueryWork) Charge(units ...uint64) error {
	if w == nil {
		return nil
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	remaining := w.remaining
	for _, n := range units {
		if n > remaining {
			return ErrQueryWorkExhausted
		}
		remaining -= n
	}
	w.remaining = remaining
	return nil
}

func (w *QueryWork) seek(n int) error {
	return w.Charge(uint64(bits.Len(uint(n))))
}

func (w *QueryWork) row(line string, eligible bool) error {
	if !eligible {
		return w.Charge(1)
	}
	return w.Charge(1, uint64(len(line)))
}

// QuerySnapshot freezes the published prefix and its query indexes together.
// Appends can publish new indexes without changing this view or its seek work.
type QuerySnapshot struct{ view queryView }

// CaptureQuerySnapshot captures the currently published query view.
func (s *Store) CaptureQuerySnapshot() QuerySnapshot {
	return QuerySnapshot{s.queryView()}
}

// Page scans this snapshot at a stable work cost with an exclusive position
// and definitive lookahead. Positions use zero-based append ordinals.
func (s QuerySnapshot) Page(start, end time.Time, contains string, limit int, after *Position, work *QueryWork) ([]PositionedRow, bool, error) {
	if err := work.Charge(); err != nil {
		return nil, false, err
	}
	count := len(s.view.rows)
	matches := queryMatches{limit: limit, lookahead: true, positioned: []PositionedRow{}}
	if err := s.view.scan(start, end, contains, count, after, &matches, work); err != nil {
		return nil, false, err
	}
	return matches.positioned, matches.more, nil
}
