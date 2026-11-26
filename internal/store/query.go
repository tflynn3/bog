package store

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type candidate struct{ ordinal, run, offset int }
type candidates struct {
	rows  []Row
	items []candidate
}

func (h candidates) Len() int { return len(h.items) }
func (h candidates) Less(i, j int) bool {
	return rowLess(h.rows, h.items[i].ordinal, h.items[j].ordinal)
}
func (h candidates) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *candidates) Push(x any)   { h.items = append(h.items, x.(candidate)) }
func (h *candidates) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

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
	last         Position
	more         bool
}

func (p *queryMatches) full() bool {
	return len(p.rows)+len(p.positioned) == p.limit
}

func (p *queryMatches) reset() {
	p.rows = p.rows[:0]
	p.positioned = p.positioned[:0]
}

func (p *queryMatches) add(row Row, ordinal, capacity int) (done bool) {
	if p.full() {
		p.more = true
		return true
	}
	if p.positioned != nil {
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

func (v queryView) scan(start, end time.Time, contains string, count int, after *Position, matches *queryMatches, work *QueryWork) error {
	if !start.Before(end) || matches.limit <= 0 || count == 0 {
		return nil
	}
	rows, blocks, restored, levels := v.rows, v.blocks, v.restored, v.levels
	// Publication replaces the run levels, so the captured slice is immutable.
	runs := make([]indexRun, 0, len(levels)+1)
	if restored.length > 0 {
		runs = append(runs, restored)
	}
	for _, run := range levels {
		if run.length > 0 {
			runs = append(runs, run)
		}
	}
	// Seek once to the start and cursor for each run. Upper bounds are deferred
	// until a bounded scan has had a chance to answer the query.
	offsets := make([]int, 0, len(runs))
	lower := make([]int, 0, len(runs))
	visibleRuns := runs[:0]
	for _, run := range runs {
		if err := work.Charge(1); err != nil {
			return err
		}
		if run.base >= count {
			continue
		}
		if run.ordinals == nil {
			run.length = min(run.length, count-run.base)
		}
		if err := work.seek(run.length); err != nil {
			return err
		}
		lo := sort.Search(run.length, func(j int) bool { return !rows[run.ordinal(j)].TS.Before(start) })
		lower = append(lower, lo)
		if after != nil {
			if err := work.seek(run.length - lo); err != nil {
				return err
			}
			lo += sort.Search(run.length-lo, func(j int) bool {
				ordinal := run.ordinal(lo + j)
				return after.precedes(rows[ordinal].TS, ordinal)
			})
		}
		offset, err := run.nextVisible(lo, count, work)
		if err != nil {
			return err
		}
		offsets = append(offsets, offset)
		visibleRuns = append(visibleRuns, run)
	}
	runs = visibleRuns
	active := -1
	for i, offset := range offsets {
		if offset < runs[i].length && rows[runs[i].ordinal(offset)].TS.Before(end) {
			if active >= 0 {
				active = -2
				break
			}
			active = i
		}
	}
	if active == -1 {
		return nil
	}
	directOffset := 0
	if active >= 0 {
		directOffset = offsets[active]
	}
	direct := func(scanThreshold int) (complete bool, err error) {
		run := runs[active]
		examined := 0
		for directOffset < run.length {
			ordinal := run.ordinal(directOffset)
			row := rows[ordinal]
			if !row.TS.Before(end) {
				break
			}
			if scanThreshold > 0 && examined >= scanThreshold {
				return false, nil
			}
			examined++
			if err := work.row(row.Line, true); err != nil {
				return false, err
			}
			if strings.Contains(row.Line, contains) && matches.add(row, ordinal, count) {
				return true, nil
			}
			next, err := run.nextVisible(directOffset+1, count, work)
			if err != nil {
				return false, err
			}
			directOffset = next
		}
		return true, nil
	}
	ordered := func(scanThreshold int) (complete bool, err error) {
		h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
		for i, offset := range offsets {
			if offset < runs[i].length && rows[runs[i].ordinal(offset)].TS.Before(end) {
				h.items = append(h.items, candidate{runs[i].ordinal(offset), i, offset})
			}
		}
		heap.Init(h)
		examined := 0
		for h.Len() > 0 {
			if scanThreshold > 0 && examined >= scanThreshold {
				return false, nil
			}
			current := h.items[0]
			examined++
			row := rows[current.ordinal]
			if err := work.row(row.Line, true); err != nil {
				return false, err
			}
			if strings.Contains(row.Line, contains) && matches.add(row, current.ordinal, count) {
				return true, nil
			}
			run := runs[current.run]
			next, err := run.nextVisible(current.offset+1, count, work)
			if err != nil {
				return false, err
			}
			if next < run.length && rows[run.ordinal(next)].TS.Before(end) {
				h.items[0] = candidate{run.ordinal(next), current.run, next}
				heap.Fix(h, 0)
			} else {
				heap.Pop(h)
			}
		}
		return true, nil
	}
	var runProbes []uint64
	var runEnds []int
	if len(contains) >= 4 {
		// Try a bounded scan before paying for the Bloom selection paths.
		limit := matches.limit
		scanThreshold := max(512, limit)
		if limit <= int(^uint(0)>>1)/2 {
			scanThreshold = max(512, 2*limit)
		} else {
			scanThreshold = int(^uint(0) >> 1)
		}
		// The multi-run policy uses the full-window estimate before attempting
		// the merge. A single run can answer dense pages without that estimate.
		bound := 0
		intervalStart, intervalEnd := 0, count
		implicit := active >= 0 && runs[active].ordinals == nil
		if active >= 0 {
			complete, err := direct(scanThreshold)
			if err != nil {
				return err
			}
			if complete {
				return nil
			}
		}
		if implicit {
			// An implicit run is already in append order. Resume the bounded
			// scan and inspect only blocks that may contain the literal.
			run := runs[active]
			if err := work.seek(run.length); err != nil {
				return err
			}
			hi := run.base + sort.Search(run.length, func(i int) bool { return !rows[run.base+i].TS.Before(end) })
			hi = min(hi, count)
			directOffset += run.base
			probes := literalProbes(contains)
			for directOffset < hi {
				block := directOffset / blockSize
				blockEnd := min((block+1)*blockSize, hi)
				if err := work.Charge(1); err != nil {
					return err
				}
				if block < len(blocks) && !blocks[block].mayContainProbes(probes) {
					directOffset = blockEnd
					continue
				}
				for directOffset < blockEnd {
					ordinal := directOffset
					directOffset++
					row := rows[ordinal]
					if err := work.row(row.Line, true); err != nil {
						return err
					}
					if strings.Contains(row.Line, contains) && matches.add(row, ordinal, count) {
						return nil
					}
				}
			}
			return nil
		}
		for i, run := range runs {
			if err := work.seek(run.length - lower[i]); err != nil {
				return err
			}
			hi := lower[i] + sort.Search(run.length-lower[i], func(j int) bool { return !rows[run.ordinal(lower[i]+j)].TS.Before(end) })
			bound += min(hi-lower[i], count-run.base)
		}
		if bound >= 32768 {
			if active == -2 {
				complete, err := ordered(scanThreshold)
				if err != nil {
					return err
				}
				if complete {
					return nil
				}
				// The fallback restarts the merge from the original offsets.
				matches.reset()
			}
			allImplicit := active == -2
			for i, run := range runs {
				if offsets[i] < run.length && rows[run.ordinal(offsets[i])].TS.Before(end) && run.ordinals != nil {
					allImplicit = false
					break
				}
			}
			if allImplicit {
				runProbes = literalProbes(contains)
				runEnds = make([]int, len(runs))
				for i, run := range runs {
					if run.ordinals == nil {
						if err := work.seek(run.length); err != nil {
							return err
						}
						hi := sort.Search(run.length, func(j int) bool { return !rows[run.base+j].TS.Before(end) })
						runEnds[i] = max(0, min(hi, count-run.base))
					}
				}
			} else {
				probes := literalProbes(contains)
				possible := make([]int, 0)
				possibleRows := 0
				for first := intervalStart / blockSize * blockSize; first < intervalEnd; first += blockSize {
					block := first / blockSize
					if err := work.Charge(1); err != nil {
						return err
					}
					if block < len(blocks) && !blocks[block].mayContainProbes(probes) {
						continue
					}
					possibleRows += min(first+blockSize, intervalEnd) - max(first, intervalStart)
					if possibleRows > bound/4 {
						break
					}
					possible = append(possible, first)
				}
				if possibleRows <= bound/4 {
					// Block scanning starts over in append order.
					matches.reset()
					return queryBlocks(rows, possible, count, start, end, contains, after, matches, work)
				}
			}
		}
	}
	if active >= 0 {
		_, err := direct(0)
		return err
	}
	// The unbounded merge skips non-matches within each run before touching the
	// heap, avoiding heap maintenance for every rejected row.
	h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
	advance := func(runIndex, offset int) error {
		run := runs[runIndex]
		if runProbes != nil && run.ordinals == nil {
			for offset < runEnds[runIndex] {
				ordinal := run.base + offset
				block := ordinal / blockSize
				blockEnd := min((block+1)*blockSize-run.base, runEnds[runIndex])
				if err := work.Charge(1); err != nil {
					return err
				}
				if block < len(blocks) && !blocks[block].mayContainProbes(runProbes) {
					offset = blockEnd
					continue
				}
				for offset < blockEnd {
					ordinal = run.base + offset
					line := rows[ordinal].Line
					if err := work.row(line, true); err != nil {
						return err
					}
					if strings.Contains(line, contains) {
						heap.Push(h, candidate{ordinal, runIndex, offset})
						return nil
					}
					offset++
				}
			}
			return nil
		}
		for offset < run.length {
			ordinal := run.ordinal(offset)
			row := rows[ordinal]
			if !row.TS.Before(end) {
				return nil
			}
			if err := work.row(row.Line, true); err != nil {
				return err
			}
			if strings.Contains(row.Line, contains) {
				heap.Push(h, candidate{ordinal, runIndex, offset})
				return nil
			}
			next, err := run.nextVisible(offset+1, count, work)
			if err != nil {
				return err
			}
			offset = next
		}
		return nil
	}
	for i, offset := range offsets {
		if err := advance(i, offset); err != nil {
			return err
		}
	}
	for h.Len() > 0 && !matches.full() {
		current := heap.Pop(h).(candidate)
		if matches.add(rows[current.ordinal], current.ordinal, count) {
			return nil
		}
		offset, err := runs[current.run].nextVisible(current.offset+1, count, work)
		if err != nil {
			return err
		}
		if err := advance(current.run, offset); err != nil {
			return err
		}
	}
	matches.more = h.Len() > 0
	return nil
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
