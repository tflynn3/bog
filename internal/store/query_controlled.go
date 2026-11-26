package store

import (
	"container/heap"
	"context"
	"errors"
	"strings"
	"time"
)

// ErrQueryBudget means the request could not be completed within its work limits.
var ErrQueryBudget = errors.New("query budget exhausted")

// QueryLimits bounds candidate visits (including index probes), literal input bytes,
// and the encoded response size. Zero or negative limits permit no work.
type QueryLimits struct{ Rows, MatchBytes, ResultBytes int64 }

type queryBudget struct {
	ctx                 context.Context
	limits              QueryLimits
	rows, bytes, result int64
}

func (b *queryBudget) charge(rows, bytes, result int64) error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if rows > b.limits.Rows-b.rows || bytes > b.limits.MatchBytes-b.bytes || result > b.limits.ResultBytes-b.result {
		return ErrQueryBudget
	}
	b.rows += rows
	b.bytes += bytes
	b.result += result
	return nil
}

// QueryContext selects a complete response before returning. The snapshot and
// its immutable index runs are captured under the read lock; all work follows
// after releasing it. Invalid continuation positions return valid=false.
func (s *Store) QueryContext(ctx context.Context, start, end time.Time, contains string, limit int, snapshot *int, after *Position, lookahead bool, limits QueryLimits) ([]Row, int, *Position, bool, error) {
	b := queryBudget{ctx: ctx, limits: limits}
	if err := b.charge(0, 0, 0); err != nil {
		return nil, 0, nil, true, err
	}
	s.mu.RLock()
	rows := s.rows
	runs := make([]indexRun, 0, len(s.runs)+1)
	if s.restored.length > 0 {
		runs = append(runs, s.restored)
	}
	for _, run := range s.runs {
		if run.length > 0 {
			runs = append(runs, run)
		}
	}
	count := len(rows)
	if snapshot != nil {
		count = *snapshot
	}
	s.mu.RUnlock()
	if snapshot != nil {
		if count < 1 || count > len(rows) || after == nil || after.Ordinal < 0 || after.Ordinal >= count {
			return nil, 0, nil, false, nil
		}
		row := rows[after.Ordinal]
		if err := b.charge(1, int64(len(row.Line)), 0); err != nil {
			return nil, 0, nil, true, err
		}
		if !row.TS.Equal(after.Time) || row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, contains) {
			return nil, 0, nil, false, nil
		}
	}
	if !start.Before(end) || limit <= 0 {
		return []Row{}, count, nil, true, nil
	}
	h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
	for i, run := range runs {
		lo, hi := 0, run.length
		for lo < hi {
			if err := b.charge(1, 0, 0); err != nil {
				return nil, 0, nil, true, err
			}
			mid := int(uint(lo+hi) >> 1)
			ordinal := run.ordinal(mid)
			t := rows[ordinal].TS
			before := t.Before(start)
			if after != nil && !before {
				before = t.Before(after.Time) || (t.Equal(after.Time) && ordinal <= after.Ordinal)
			}
			if before {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < run.length {
			h.items = append(h.items, candidate{run.ordinal(lo), i, lo})
		}
	}
	heap.Init(h)
	result := make([]Row, 0, min(limit, count))
	var last candidate
	for h.Len() > 0 {
		if err := b.charge(1, 0, 0); err != nil {
			return nil, 0, nil, true, err
		}
		current := heap.Pop(h).(candidate)
		row := rows[current.ordinal]
		if !row.TS.Before(end) {
			break
		}
		if current.ordinal < count {
			if err := b.charge(0, int64(len(row.Line)), 0); err != nil {
				return nil, 0, nil, true, err
			}
			if strings.Contains(row.Line, contains) {
				if len(result) == limit {
					return result, count, &Position{rows[last.ordinal].TS, last.ordinal}, true, nil
				}
				// JSON escaping expands each input byte by at most six, plus timestamp and syntax.
				if err := b.charge(0, 0, int64(len(row.Line))*6+128); err != nil {
					return nil, 0, nil, true, err
				}
				result = append(result, row)
				last = current
				if !lookahead && len(result) == limit {
					return result, count, nil, true, nil
				}
			}
		}
		next := current.offset + 1
		run := runs[current.run]
		if next < run.length {
			heap.Push(h, candidate{run.ordinal(next), current.run, next})
		}
	}
	return result, count, nil, true, nil
}
