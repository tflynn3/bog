package main

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

type operationPin struct {
	owner  *retainedStorage
	a      wal.Authority
	reader *history.Reader
	wal    *wal.Capture
	tail   []*tailBatch
	refs   int // Protected by owner.mu.
}

func (s *retainedStorage) acquireLocked(ctx context.Context, query bool) (*operationPin, error) {
	if s.closed {
		return nil, httpapi.ErrCaptureExpired
	}
	w, a, err := s.owner.CaptureSelected(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		if errors.Is(err, checkpoint.ErrExhausted) {
			err = errors.Join(httpapi.ErrQueryCapacityExhausted, err)
		}
		return nil, errors.Join(err, w.Release())
	}
	g := r.Generation()
	if [32]byte(g.ID) != a.Generation || g.Boundary.CoveredOffset != a.CoveredOffset || g.Boundary.NextOrdinal != a.NextOrdinal {
		return nil, errors.Join(errors.New("generation and captured suffix differ"), w.Release(), r.Close())
	}
	p := &operationPin{owner: s, a: a, reader: r, wal: w, refs: 1}
	if query {
		p.tail = append([]*tailBatch(nil), s.tail...)
		for _, batch := range p.tail {
			batch.refs++
		}
	}
	s.ops.Add(1)
	return p, nil
}

func (p *operationPin) release() error {
	s := p.owner
	s.mu.Lock()
	p.refs--
	last := p.refs == 0
	if last {
		for _, batch := range p.tail {
			batch.release()
		}
		p.tail = nil
	}
	s.mu.Unlock()
	if !last {
		return nil
	}
	defer s.ops.Done()
	err := errors.Join(p.wal.Release(), p.reader.Close())
	s.notify()
	return err
}

func (s *retainedStorage) Release(id string) error {
	s.mu.Lock()
	p := s.captures[id]
	delete(s.captures, id)
	s.mu.Unlock()
	if p == nil {
		return nil
	}
	return p.release()
}

const (
	pageLineTarget       = 4 << 20
	scanReservationBytes = 14 << 20
)

func (*retainedStorage) ChunkedOrdinaryQueries() {}

// A scan can keep one 4 KiB cursor buffer per checkpoint block, one block-sized
// line scratch buffer, at most 4 MiB of copied lines, and room for capture
// references, cursor metadata, tail merge, row and position arrays.

func (*retainedStorage) PageScanBytes(httpapi.PageRequest) uint64 {
	return scanReservationBytes
}

func (s *retainedStorage) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (result httpapi.PageResult, err error) {
	if req.Limit < 1 || req.Limit > 1000 {
		return result, errors.New("retained query page row limit out of range")
	}
	if err := work.Charge(1, uint64(len(req.After)), uint64(len(req.Filter.Contains))); err != nil {
		return result, err
	}
	var p *operationPin
	if req.Capture == "" {
		if err := s.lockView(ctx); err != nil {
			return result, err
		}
		if err := work.Charge(uint64(len(s.tail))); err != nil {
			s.mu.Unlock()
			return result, err
		}
		p, err = s.acquireLocked(ctx, true)
		s.mu.Unlock()
		if err != nil {
			return result, err
		}
	} else {
		s.mu.Lock()
		p = s.captures[req.Capture]
		if p != nil {
			p.refs++
		}
		s.mu.Unlock()
		if p == nil {
			return result, httpapi.ErrCaptureExpired
		}
	}
	defer func() {
		if p != nil {
			err = errors.Join(err, p.release())
		}
	}()
	result, err = p.page(ctx, req, work)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	if req.Snapshot && req.Capture == "" {
		s.mu.Lock()
		s.nextCapture++
		result.Capture = strconv.FormatUint(s.nextCapture, 10)
		s.captures[result.Capture] = p
		s.mu.Unlock()
		p = nil // HTTP owns the acquisition.
	}
	return result, nil
}

func compareRows(a, b checkpoint.BlockRow) int {
	if order := a.Row.TS.Compare(b.Row.TS); order != 0 {
		return order
	}
	return cmp.Compare(a.Ordinal, b.Ordinal)
}

func rowPosition(t time.Time, ordinal uint64) httpapi.PagePosition {
	b := t.AppendFormat(make([]byte, 0, 52), time.RFC3339Nano)
	b = append(b, '/')
	return httpapi.PagePosition(strconv.AppendUint(b, ordinal, 10))
}

func parsePosition(raw httpapi.PagePosition) (*history.Position, error) {
	if raw == "" {
		return nil, nil
	}
	ts, ordinal, ok := strings.Cut(string(raw), "/")
	t, timeErr := store.ParseTime(ts)
	n, ordinalErr := strconv.ParseUint(ordinal, 10, 64)
	if !ok || timeErr != nil || ordinalErr != nil {
		return nil, store.ErrInvalidQueryCursor
	}
	return &history.Position{Time: t, Ordinal: n}, nil
}

var errPageDone = errors.New("retained query page complete")

func (p *operationPin) page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	after, err := parsePosition(req.After)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	q := history.Query{Start: req.Filter.Start, End: req.Filter.End, Contains: req.Filter.Contains, After: after}
	tail, err := seekTail(ctx, p.tail, q, work)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	var result httpapi.PageResult
	result.Rows = make([]httpapi.PageRow, 0, req.Limit)
	var lineBytes int
	full := func(size int) bool {
		return len(result.Rows) >= req.Limit || len(result.Rows) > 0 && size > pageLineTarget-lineBytes
	}
	add := func(ts time.Time, ordinal uint64, line string) {
		out := httpapi.PageRow{Row: store.Row{TS: ts, Line: line}}
		if req.Snapshot {
			out.Position = rowPosition(ts, ordinal)
		}
		result.Rows = append(result.Rows, out)
		lineBytes += len(line)
	}
	stop := func() error {
		result.More = req.Snapshot
		return errPageDone
	}
	emitTail := func(before *history.Position) error {
		for len(tail) != 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			c := &tail[0]
			row := c.rows[c.i]
			if before != nil && !rowBefore(row, *before) {
				return nil
			}
			if err := work.Charge(1, uint64(len(row.Row.Line))); err != nil {
				return err
			}
			if strings.Contains(row.Row.Line, q.Contains) {
				if full(len(row.Row.Line)) {
					return stop()
				}
				add(row.Row.TS, row.Ordinal, strings.Clone(row.Row.Line))
			}
			c.i++
			if c.i < len(c.rows) && c.rows[c.i].Row.TS.Before(q.End) {
				heap.Fix(&tail, 0)
			} else {
				heap.Pop(&tail)
			}
		}
		return nil
	}
	err = p.reader.ScanBytesWithWork(ctx, q, func(key history.Position, line []byte) error {
		if err := emitTail(&key); err != nil {
			return err
		}
		if full(len(line)) {
			return stop()
		}
		add(key.Time, key.Ordinal, string(line))
		return nil
	}, work)
	if err == nil {
		err = emitTail(nil)
	}
	if err == errPageDone {
		err = nil
	}
	// Ordinary HTTP requests also fill the requested limit. A byte cut is
	// handled by the HTTP layer using More and the exclusive source anchor.
	return result, err
}

func rowBefore(row checkpoint.BlockRow, key history.Position) bool {
	order := row.Row.TS.Compare(key.Time)
	return order < 0 || order == 0 && row.Ordinal < key.Ordinal
}

type tailCursor struct {
	rows []checkpoint.BlockRow
	i    int
}

type tailHeap []tailCursor

func seekTail(ctx context.Context, batches []*tailBatch, q history.Query, work *store.QueryWork) (tailHeap, error) {
	h := make(tailHeap, 0, len(batches))
	for _, batch := range batches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := work.Charge(1); err != nil {
			return nil, err
		}
		rows := batch.rows
		if rows[len(rows)-1].Row.TS.Before(q.Start) || !rows[0].Row.TS.Before(q.End) {
			continue
		}
		if err := work.Charge(uint64(bits.Len(uint(len(rows))))); err != nil {
			return nil, err
		}
		i := sort.Search(len(rows), func(i int) bool {
			r := rows[i]
			if r.Row.TS.Before(q.Start) {
				return false
			}
			if q.After == nil {
				return true
			}
			order := r.Row.TS.Compare(q.After.Time)
			return order > 0 || order == 0 && r.Ordinal > q.After.Ordinal
		})
		if i < len(rows) && rows[i].Row.TS.Before(q.End) {
			h = append(h, tailCursor{rows, i})
		}
	}
	heap.Init(&h)
	return h, nil
}

func (h tailHeap) Len() int { return len(h) }
func (h tailHeap) Less(i, j int) bool {
	return compareRows(h[i].rows[h[i].i], h[j].rows[h[j].i]) < 0
}
func (h tailHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *tailHeap) Push(x any)   { *h = append(*h, x.(tailCursor)) }
func (h *tailHeap) Pop() any {
	last := len(*h) - 1
	c := (*h)[last]
	(*h)[last] = tailCursor{}
	*h = (*h)[:last]
	return c
}
