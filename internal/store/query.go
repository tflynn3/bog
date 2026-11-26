package store

import (
	"container/heap"
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
	a, b := h.items[i].ordinal, h.items[j].ordinal
	if h.rows[a].TS.Equal(h.rows[b].TS) {
		return a < b
	}
	return h.rows[a].TS.Before(h.rows[b].TS)
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
	return s.query(start, end, contains, limit, snapshot, after, true, work)
}

func (s *Store) query(start, end time.Time, contains string, limit int, snapshot *int, after *Position, lookahead bool, work *QueryWork) ([]Row, int, *Position, error) {
	if err := work.Charge(); err != nil {
		return nil, 0, nil, err
	}
	if (snapshot == nil) != (after == nil) {
		return nil, 0, nil, ErrInvalidQueryCursor
	}
	s.mu.RLock()
	rows, blocks, restored, levels := s.rows, s.blocks, s.restored, s.runs
	s.mu.RUnlock()
	count := len(rows)
	if snapshot != nil {
		count = *snapshot
		if count < 1 || count > len(rows) || after.Ordinal < 0 || after.Ordinal >= count {
			return nil, 0, nil, ErrInvalidQueryCursor
		}
		row := rows[after.Ordinal]
		if err := work.row(row.Line, true); err != nil {
			return nil, 0, nil, err
		}
		if !row.TS.Equal(after.Time) || row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, contains) {
			return nil, 0, nil, ErrInvalidQueryCursor
		}
	}
	if !start.Before(end) || limit <= 0 {
		return []Row{}, count, nil, nil
	}
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
			return nil, 0, nil, err
		}
		if run.base >= count {
			continue
		}
		if run.ordinals == nil {
			run.length = min(run.length, count-run.base)
		}
		if err := work.seek(run.length); err != nil {
			return nil, 0, nil, err
		}
		lo := sort.Search(run.length, func(j int) bool { return !rows[run.ordinal(j)].TS.Before(start) })
		lower = append(lower, lo)
		if after != nil {
			if err := work.seek(run.length - lo); err != nil {
				return nil, 0, nil, err
			}
			lo += sort.Search(run.length-lo, func(j int) bool {
				ordinal := run.ordinal(lo + j)
				t := rows[ordinal].TS
				return t.After(after.Time) || (t.Equal(after.Time) && ordinal > after.Ordinal)
			})
		}
		offset, err := run.nextVisible(lo, count, work)
		if err != nil {
			return nil, 0, nil, err
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
		return []Row{}, count, nil, nil
	}
	var directResult []Row
	var directLast int
	directOffset := 0
	if active >= 0 {
		directOffset = offsets[active]
	}
	direct := func(scanThreshold int) ([]Row, *Position, bool, error) {
		run := runs[active]
		examined := 0
		for directOffset < run.length {
			ordinal := run.ordinal(directOffset)
			row := rows[ordinal]
			if !row.TS.Before(end) {
				break
			}
			if scanThreshold > 0 && examined >= scanThreshold {
				return nil, nil, false, nil
			}
			examined++
			if err := work.row(row.Line, true); err != nil {
				return nil, nil, false, err
			}
			if strings.Contains(row.Line, contains) {
				if len(directResult) == limit {
					return directResult, &Position{rows[directLast].TS, directLast}, true, nil
				}
				if directResult == nil {
					directResult = make([]Row, 0, min(limit, count))
				}
				directResult = append(directResult, row)
				directLast = ordinal
				if !lookahead && len(directResult) == limit {
					return directResult, nil, true, nil
				}
			}
			next, err := run.nextVisible(directOffset+1, count, work)
			if err != nil {
				return nil, nil, false, err
			}
			directOffset = next
		}
		if directResult == nil {
			directResult = []Row{}
		}
		return directResult, nil, true, nil
	}
	ordered := func(scanThreshold int) ([]Row, *Position, bool, error) {
		h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
		for i, offset := range offsets {
			if offset < runs[i].length && rows[runs[i].ordinal(offset)].TS.Before(end) {
				h.items = append(h.items, candidate{runs[i].ordinal(offset), i, offset})
			}
		}
		heap.Init(h)
		var result []Row
		var last candidate
		examined := 0
		for h.Len() > 0 {
			if scanThreshold > 0 && examined >= scanThreshold {
				return nil, nil, false, nil
			}
			current := h.items[0]
			examined++
			row := rows[current.ordinal]
			if err := work.row(row.Line, true); err != nil {
				return nil, nil, false, err
			}
			if strings.Contains(row.Line, contains) {
				if len(result) == limit {
					return result, &Position{rows[last.ordinal].TS, last.ordinal}, true, nil
				}
				if result == nil {
					result = make([]Row, 0, min(limit, count))
				}
				result = append(result, row)
				last = current
				if !lookahead && len(result) == limit {
					return result, nil, true, nil
				}
			}
			run := runs[current.run]
			next, err := run.nextVisible(current.offset+1, count, work)
			if err != nil {
				return nil, nil, false, err
			}
			if next < run.length && rows[run.ordinal(next)].TS.Before(end) {
				h.items[0] = candidate{run.ordinal(next), current.run, next}
				heap.Fix(h, 0)
			} else {
				heap.Pop(h)
			}
		}
		if result == nil {
			result = []Row{}
		}
		return result, nil, true, nil
	}
	var runProbes []uint64
	var runEnds []int
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
			result, next, complete, err := direct(scanThreshold)
			if err != nil {
				return nil, 0, nil, err
			}
			if complete {
				return result, count, next, nil
			}
		}
		if implicit {
			// An implicit run is already in append order. Resume the bounded
			// scan and inspect only blocks that may contain the literal.
			run := runs[active]
			if err := work.seek(run.length); err != nil {
				return nil, 0, nil, err
			}
			hi := run.base + sort.Search(run.length, func(i int) bool { return !rows[run.base+i].TS.Before(end) })
			hi = min(hi, count)
			directOffset += run.base
			probes := literalProbes(contains)
			for directOffset < hi {
				block := directOffset / blockSize
				blockEnd := min((block+1)*blockSize, hi)
				if err := work.Charge(1); err != nil {
					return nil, 0, nil, err
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
						return nil, 0, nil, err
					}
					if !strings.Contains(row.Line, contains) {
						continue
					}
					if len(directResult) == limit {
						return directResult, count, &Position{rows[directLast].TS, directLast}, nil
					}
					if directResult == nil {
						directResult = make([]Row, 0, min(limit, count))
					}
					directResult = append(directResult, row)
					directLast = ordinal
					if !lookahead && len(directResult) == limit {
						return directResult, count, nil, nil
					}
				}
			}
			if directResult == nil {
				directResult = []Row{}
			}
			return directResult, count, nil, nil
		}
		for i, run := range runs {
			if err := work.seek(run.length - lower[i]); err != nil {
				return nil, 0, nil, err
			}
			hi := lower[i] + sort.Search(run.length-lower[i], func(j int) bool { return !rows[run.ordinal(lower[i]+j)].TS.Before(end) })
			bound += min(hi-lower[i], count-run.base)
		}
		if bound >= 32768 {
			if active == -2 {
				result, next, complete, err := ordered(scanThreshold)
				if err != nil {
					return nil, 0, nil, err
				}
				if complete {
					return result, count, next, nil
				}
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
							return nil, 0, nil, err
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
						return nil, 0, nil, err
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
					result, next, err := queryBlocks(rows, possible, count, start, end, contains, limit, after, lookahead, work)
					if err != nil {
						return nil, 0, nil, err
					}
					return result, count, next, nil
				}
			}
		}
	}
	if active >= 0 {
		result, next, _, err := direct(0)
		if err != nil {
			return nil, 0, nil, err
		}
		return result, count, next, nil
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
			return nil, 0, nil, err
		}
	}
	result := []Row{}
	var last candidate
	for h.Len() > 0 && len(result) < limit {
		last = heap.Pop(h).(candidate)
		if len(result) == 0 {
			result = make([]Row, 0, min(limit, count))
		}
		result = append(result, rows[last.ordinal])
		if lookahead || len(result) < limit {
			offset, err := runs[last.run].nextVisible(last.offset+1, count, work)
			if err != nil {
				return nil, 0, nil, err
			}
			if err := advance(last.run, offset); err != nil {
				return nil, 0, nil, err
			}
		}
	}
	var next *Position
	if h.Len() > 0 {
		next = &Position{rows[last.ordinal].TS, last.ordinal}
	}
	return result, count, next, nil
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
