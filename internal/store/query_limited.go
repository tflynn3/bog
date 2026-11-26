package store

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"strings"
	"time"
)

// ErrQueryBudget indicates that a query exceeded its request-local work limit.
var ErrQueryBudget = errors.New("query work budget exhausted")

// QueryLimits bounds work and result storage. Work charges one unit per seek,
// candidate visit, and Bloom block probe, plus each byte presented to matching.
type QueryLimits struct {
	Work        int64
	Rows        int64
	MatchBytes  int64
	ResultBytes int64
}

type queryMeter struct {
	ctx                       context.Context
	limits                    QueryLimits
	work, rows, bytes, result int64
}

func (m *queryMeter) charge(work, rows, bytes int64) error {
	if err := m.ctx.Err(); err != nil {
		return err
	}
	if (m.limits.Work > 0 && work > m.limits.Work-m.work) || (m.limits.Rows > 0 && rows > m.limits.Rows-m.rows) || (m.limits.MatchBytes > 0 && bytes > m.limits.MatchBytes-m.bytes) {
		return ErrQueryBudget
	}
	m.work += work
	m.rows += rows
	m.bytes += bytes
	return nil
}

// QueryContext executes a query against a captured published prefix.
func (s *Store) QueryContext(ctx context.Context, start, end time.Time, literal string, limit int, limits QueryLimits) ([]Row, error) {
	rows, _, _, _, err := s.queryLimited(ctx, start, end, literal, limit, nil, nil, false, limits)
	return rows, err
}

// QueryPageContext also validates a continuation and performs bounded lookahead.
func (s *Store) QueryPageContext(ctx context.Context, start, end time.Time, literal string, limit int, snapshot *int, after *Position, limits QueryLimits) ([]Row, int, *Position, bool, error) {
	return s.queryLimited(ctx, start, end, literal, limit, snapshot, after, true, limits)
}
func (s *Store) queryLimited(ctx context.Context, start, end time.Time, literal string, limit int, snapshot *int, after *Position, lookahead bool, limits QueryLimits) ([]Row, int, *Position, bool, error) {
	m := queryMeter{ctx: ctx, limits: limits}
	if err := m.charge(0, 0, 0); err != nil {
		return nil, 0, nil, false, err
	}
	s.mu.RLock()
	rows, blocks := s.rows, s.blocks
	runs := make([]indexRun, 0, len(s.runs)+1)
	if s.restored.length > 0 {
		runs = append(runs, s.restored)
	}
	for _, r := range s.runs {
		if r.length > 0 {
			runs = append(runs, r)
		}
	}
	count := len(rows)
	s.mu.RUnlock()
	if snapshot != nil {
		count = *snapshot
		if count < 1 || count > len(rows) || after == nil || after.Ordinal < 0 || after.Ordinal >= count {
			return nil, 0, nil, false, nil
		}
		row := rows[after.Ordinal]
		if err := m.charge(1+int64(len(row.Line)), 1, int64(len(row.Line))); err != nil {
			return nil, 0, nil, false, err
		}
		if !row.TS.Equal(after.Time) || row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, literal) {
			return nil, 0, nil, false, nil
		}
	}
	if !start.Before(end) || limit <= 0 {
		return []Row{}, count, nil, true, nil
	}
	h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
	var scanErr error
	var probes []uint64
	if len(literal) >= 4 {
		probes = literalProbes(literal)
	}
	advance := func(ri, offset int) {
		run := runs[ri]
		for offset < run.length {
			ord := run.ordinal(offset)
			row := rows[ord]
			if !row.TS.Before(end) {
				return
			}
			if probes != nil && run.ordinals == nil && ord%blockSize == 0 && ord+blockSize <= count && offset+blockSize <= run.length && ord/blockSize < len(blocks) {
				if scanErr = m.charge(1, 0, 0); scanErr != nil {
					return
				}
				if !blocks[ord/blockSize].mayContainProbes(probes) {
					offset += blockSize
					continue
				}
			}
			if scanErr = m.charge(1, 1, 0); scanErr != nil {
				return
			}
			if ord < count {
				if scanErr = m.charge(int64(len(row.Line)), 0, int64(len(row.Line))); scanErr != nil {
					return
				}
				if strings.Contains(row.Line, literal) {
					heap.Push(h, candidate{ord, ri, offset})
					return
				}
			}
			offset++
		}
	}
	for i, run := range runs {
		lo := sort.Search(run.length, func(j int) bool {
			if scanErr != nil {
				return true
			}
			scanErr = m.charge(1, 0, 0)
			return !rows[run.ordinal(j)].TS.Before(start)
		})
		if scanErr != nil {
			return nil, 0, nil, false, scanErr
		}
		if after != nil {
			lo += sort.Search(run.length-lo, func(j int) bool {
				if scanErr != nil {
					return true
				}
				scanErr = m.charge(1, 0, 0)
				o := run.ordinal(lo + j)
				t := rows[o].TS
				return t.After(after.Time) || (t.Equal(after.Time) && o > after.Ordinal)
			})
		}
		if scanErr != nil {
			return nil, 0, nil, false, scanErr
		}
		advance(i, lo)
		if scanErr != nil {
			return nil, 0, nil, false, scanErr
		}
	}
	result := make([]Row, 0, min(limit, count))
	var last candidate
	for h.Len() > 0 && len(result) < limit {
		last = heap.Pop(h).(candidate)
		row := rows[last.ordinal]
		// JSON escaping can use up to six bytes per input byte; fixed overhead covers timestamp and field names.
		size := int64(len(row.Line))*6 + 128
		if limits.ResultBytes > 0 && size > limits.ResultBytes-m.result {
			return nil, 0, nil, false, ErrQueryBudget
		}
		m.result += size
		result = append(result, row)
		if lookahead || len(result) < limit {
			advance(last.run, last.offset+1)
			if scanErr != nil {
				return nil, 0, nil, false, scanErr
			}
		}
	}
	var next *Position
	if h.Len() > 0 && len(result) > 0 {
		next = &Position{rows[last.ordinal].TS, last.ordinal}
	}
	return result, count, next, true, nil
}
