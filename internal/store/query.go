package store

import (
	"container/heap"
	"sort"
	"strings"
	"time"
)

type queryRun struct {
	indexRun
	lower int // timestamp-and-cursor lower bound before visibility filtering
	first int // first visible rank
	end   int // exclusive upper rank, computed when planning Bloom pruning
}

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

// QueryPage captures the published row count when both snapshot and after are nil.
// A continuation requires both values, and after must identify a matching row
// in the supplied snapshot.
func (s *Store) QueryPage(start, end time.Time, contains string, limit int, snapshot *int, after *Position) ([]Row, int, *Position, bool) {
	rows, count, next, err := s.QueryPageWithWork(start, end, contains, limit, snapshot, after, nil)
	return rows, count, next, err == nil
}

// QueryPageWithWork is QueryPage with cancellation and work accounting.
func (s *Store) QueryPageWithWork(start, end time.Time, contains string, limit int, snapshot *int, after *Position, work *QueryWork) ([]Row, int, *Position, error) {
	var ordinal *int
	var stamp *time.Time
	if after != nil {
		ordinal, stamp = &after.Ordinal, &after.Time
	}
	result, count, err := s.query(start, end, contains, limit, snapshot, ordinal, stamp, true, false, work)
	if err != nil {
		return nil, 0, nil, err
	}
	var next *Position
	if result.more {
		next = &Position{Time: result.rows[len(result.rows)-1].TS, Ordinal: result.last}
	}
	return result.rows, count, next, nil
}

// SnapshotPage contains the matching rows, their append ordinals in the same
// order, and the published row count captured for the walk. More reports
// whether there is another match after the last returned row.
type SnapshotPage struct {
	Rows      []Row
	Ordinals  []int
	HighWater int
	More      bool
}

// QuerySnapshotPageWithWork captures the snapshot and row positions in one
// traversal. For a continuation, supply both the original high-water mark and
// the last emitted ordinal. The timestamp is read from the immutable prefix.
func (s *Store) QuerySnapshotPageWithWork(start, end time.Time, contains string, limit int, snapshot, afterOrdinal *int, work *QueryWork) (SnapshotPage, error) {
	result, count, err := s.query(start, end, contains, limit, snapshot, afterOrdinal, nil, true, true, work)
	if err != nil {
		return SnapshotPage{}, err
	}
	return SnapshotPage{Rows: result.rows, Ordinals: result.ordinals, HighWater: count, More: result.more}, nil
}

type queryResult struct {
	rows     []Row
	ordinals []int
	last     int
	more     bool
}

func newQueryResult(positions bool, capacity int) queryResult {
	result := queryResult{rows: make([]Row, 0, capacity)}
	if positions {
		result.ordinals = make([]int, 0, capacity)
	}
	return result
}

func (r *queryResult) append(row Row, ordinal int) {
	r.rows = append(r.rows, row)
	r.last = ordinal
	if r.ordinals != nil {
		r.ordinals = append(r.ordinals, ordinal)
	}
}

func (s *Store) query(start, end time.Time, contains string, limit int, snapshot, afterOrdinal *int, afterTime *time.Time, lookahead, positions bool, work *QueryWork) (queryResult, int, error) {
	if err := work.Charge(); err != nil {
		return queryResult{}, 0, err
	}
	if (snapshot == nil) != (afterOrdinal == nil) {
		return queryResult{}, 0, ErrInvalidQueryCursor
	}
	s.mu.RLock()
	rows, blocks, restored, levels := s.rows, s.blocks, s.restored, s.runs
	s.mu.RUnlock()
	count := len(rows)
	var after *Position
	if snapshot != nil {
		count = *snapshot
		if count < 1 || count > len(rows) || *afterOrdinal < 0 || *afterOrdinal >= count {
			return queryResult{}, 0, ErrInvalidQueryCursor
		}
		row := rows[*afterOrdinal]
		if err := work.row(row.Line, true); err != nil {
			return queryResult{}, 0, err
		}
		if (afterTime != nil && !row.TS.Equal(*afterTime)) || row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, contains) {
			return queryResult{}, 0, ErrInvalidQueryCursor
		}
		after = &Position{Time: row.TS, Ordinal: *afterOrdinal}
	}
	if !start.Before(end) || limit <= 0 {
		return newQueryResult(positions, 0), count, nil
	}
	// Publication replaces the run levels, so the captured slice is immutable.
	index := make([]indexRun, 0, len(levels)+1)
	if restored.length > 0 {
		index = append(index, restored)
	}
	// Larger occupied levels are older. Once a run starts outside the
	// snapshot, no younger level can contribute a visible row.
	for i := len(levels) - 1; i >= 0; i-- {
		if run := levels[i]; run.length > 0 {
			index = append(index, run)
		}
	}
	// Upper bounds are deferred until a bounded scan has had a chance to
	// answer the query.
	runs := make([]queryRun, 0, len(index))
	for _, run := range index {
		if err := work.Charge(1); err != nil {
			return queryResult{}, 0, err
		}
		if run.base >= count {
			break
		}
		if run.ordinals == nil {
			run.length = min(run.length, count-run.base)
		}
		lo, err := run.seekStart(rows, start, after, afterTime != nil, work)
		if err != nil {
			return queryResult{}, 0, err
		}
		offset, err := run.nextVisible(lo, count, work)
		if err != nil {
			return queryResult{}, 0, err
		}
		runs = append(runs, queryRun{indexRun: run, lower: lo, first: offset})
	}
	active := -1
	for i, run := range runs {
		if run.first < run.length && rows[run.ordinal(run.first)].TS.Before(end) {
			if active >= 0 {
				active = -2
				break
			}
			active = i
		}
	}
	if active == -1 {
		return newQueryResult(positions, 0), count, nil
	}
	result := newQueryResult(positions, 0)
	// accept is called only for matching rows. The first extra match is
	// sufficient for lookahead and never enters the page.
	accept := func(ordinal int) bool {
		if len(result.rows) == limit {
			result.more = true
			return true
		}
		if len(result.rows) == 0 {
			result = newQueryResult(positions, min(limit, count))
		}
		result.append(rows[ordinal], ordinal)
		return !lookahead && len(result.rows) == limit
	}
	directOffset := 0
	if active >= 0 {
		directOffset = runs[active].first
	}
	direct := func(scanThreshold int) (bool, error) {
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
			if strings.Contains(row.Line, contains) && accept(ordinal) {
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
	var orderedHeap *candidates
	ordered := func(scanThreshold int) (bool, error) {
		if orderedHeap == nil {
			orderedHeap = &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
			for i, run := range runs {
				if run.first < run.length && rows[run.ordinal(run.first)].TS.Before(end) {
					orderedHeap.items = append(orderedHeap.items, candidate{run.ordinal(run.first), i, run.first})
				}
			}
			heap.Init(orderedHeap)
		}
		examined := 0
		for orderedHeap.Len() > 0 {
			if scanThreshold > 0 && examined >= scanThreshold {
				return false, nil
			}
			current := orderedHeap.items[0]
			examined++
			row := rows[current.ordinal]
			if err := work.row(row.Line, true); err != nil {
				return false, err
			}
			if strings.Contains(row.Line, contains) && accept(current.ordinal) {
				return true, nil
			}
			run := runs[current.run]
			next, err := run.nextVisible(current.offset+1, count, work)
			if err != nil {
				return false, err
			}
			if next < run.length && rows[run.ordinal(next)].TS.Before(end) {
				orderedHeap.items[0] = candidate{run.ordinal(next), current.run, next}
				heap.Fix(orderedHeap, 0)
			} else {
				heap.Pop(orderedHeap)
			}
		}
		return true, nil
	}
	var runProbes []uint64
	if len(contains) >= 4 {
		// Try a bounded scan before paying for the Bloom selection paths.
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
				return queryResult{}, 0, err
			}
			if complete {
				return result, count, nil
			}
		}
		if implicit {
			// An implicit run is already in append order. Resume the bounded
			// scan and inspect only blocks that may contain the literal.
			run := runs[active]
			if err := work.seek(run.length); err != nil {
				return queryResult{}, 0, err
			}
			hi := run.base + sort.Search(run.length, func(i int) bool { return !rows[run.base+i].TS.Before(end) })
			hi = min(hi, count)
			directOffset += run.base
			probes := literalProbes(contains)
			for directOffset < hi {
				block := directOffset / blockSize
				blockEnd := min((block+1)*blockSize, hi)
				if err := work.Charge(1); err != nil {
					return queryResult{}, 0, err
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
						return queryResult{}, 0, err
					}
					if !strings.Contains(row.Line, contains) {
						continue
					}
					if accept(ordinal) {
						return result, count, nil
					}
				}
			}
			return result, count, nil
		}
		if active == -2 && lookahead {
			complete, err := ordered(scanThreshold)
			if err != nil {
				return queryResult{}, 0, err
			}
			if complete {
				return result, count, nil
			}
		}
		for i := range runs {
			run := &runs[i]
			if err := work.seek(run.length - run.lower); err != nil {
				return queryResult{}, 0, err
			}
			run.end = run.lower + sort.Search(run.length-run.lower, func(j int) bool {
				return !rows[run.ordinal(run.lower+j)].TS.Before(end)
			})
			bound += min(run.end-run.lower, count-run.base)
		}
		if bound >= 32768 {
			if active == -2 && orderedHeap == nil {
				complete, err := ordered(scanThreshold)
				if err != nil {
					return queryResult{}, 0, err
				}
				if complete {
					return result, count, nil
				}
			}
			allImplicit := active == -2
			for _, run := range runs {
				if run.first < run.end && run.ordinals != nil {
					allImplicit = false
					break
				}
			}
			if allImplicit {
				runProbes = literalProbes(contains)
			} else {
				probes := literalProbes(contains)
				possible := make([]int, 0)
				possibleRows := 0
				for first := intervalStart / blockSize * blockSize; first < intervalEnd; first += blockSize {
					block := first / blockSize
					if err := work.Charge(1); err != nil {
						return queryResult{}, 0, err
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
					result, err := queryBlocks(rows, possible, count, start, end, contains, limit, after, lookahead, positions, work)
					if err != nil {
						return queryResult{}, 0, err
					}
					return result, count, nil
				}
			}
		}
	}
	if active >= 0 {
		if _, err := direct(0); err != nil {
			return queryResult{}, 0, err
		}
		return result, count, nil
	}
	if lookahead && runProbes == nil {
		if _, err := ordered(0); err != nil {
			return queryResult{}, 0, err
		}
		return result, count, nil
	}
	// Continue a bounded merge from its current ranks when per-run Bloom
	// filters can skip the remaining non-matches.
	if runProbes == nil {
		result = newQueryResult(positions, 0)
	}
	// The unbounded merge skips non-matches within each run before touching the
	// heap, avoiding heap maintenance for every rejected row.
	h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
	advance := func(runIndex, offset int) error {
		run := runs[runIndex]
		if runProbes != nil && run.ordinals == nil {
			for offset < run.end {
				ordinal := run.base + offset
				block := ordinal / blockSize
				blockEnd := min((block+1)*blockSize-run.base, run.end)
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
	if runProbes != nil {
		for _, current := range orderedHeap.items {
			if err := advance(current.run, current.offset); err != nil {
				return queryResult{}, 0, err
			}
		}
	} else {
		for i, run := range runs {
			if err := advance(i, run.first); err != nil {
				return queryResult{}, 0, err
			}
		}
	}
	for h.Len() > 0 {
		last := heap.Pop(h).(candidate)
		if accept(last.ordinal) {
			break
		}
		offset, err := runs[last.run].nextVisible(last.offset+1, count, work)
		if err != nil {
			return queryResult{}, 0, err
		}
		if err := advance(last.run, offset); err != nil {
			return queryResult{}, 0, err
		}
	}
	return result, count, nil
}

func (s *Store) Query(start, end time.Time, contains string, limit int) []Row {
	rows, _ := s.QueryWithWork(start, end, contains, limit, nil)
	return rows
}

// QueryWithWork is Query with cancellation and work accounting.
func (s *Store) QueryWithWork(start, end time.Time, contains string, limit int, work *QueryWork) ([]Row, error) {
	result, _, err := s.query(start, end, contains, limit, nil, nil, nil, false, false, work)
	return result.rows, err
}
