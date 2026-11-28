package main

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
)

const snapshotScanBytes = 4 << 20

type retainedPages struct {
	storage       *retained
	responseBytes int
	mu            sync.Mutex
	next          uint64
	live          map[string]*pageCapture
}

type pageCapture struct {
	view *recoveryView
	refs int // issuer reference plus admitted Page calls
}

func (s *retainedPages) rawLimit(req httpapi.PageRequest) int {
	if req.Snapshot {
		return snapshotScanBytes
	}
	return min(s.responseBytes, httpapi.DefaultQueryMaxResponseBytes)
}

type scanShape struct {
	blocks, batches, line int
}

func (e *epoch) scanShape() scanShape {
	return scanShape{e.gen.SegmentCount, len(e.batches), max(e.maxLine, e.gen.MaxLineBytes)}
}

func (s *retainedPages) PageScanBytes(req httpapi.PageRequest) uint64 {
	if req.Capture != "" {
		s.mu.Lock()
		defer s.mu.Unlock()
		if c := s.live[req.Capture]; c != nil {
			return s.scanBytes(req, c.view.shape)
		}
		return 0
	}
	s.storage.mu.Lock()
	shape := s.storage.active.scanShape()
	s.storage.mu.Unlock()
	return s.scanBytes(req, shape)
}

func roundScanSize(n, floor int) uint64 {
	return 1 << bits.Len(uint(max(n, floor)-1))
}

func (s *retainedPages) scanBytes(req httpapi.PageRequest, shape scanShape) uint64 {
	// Round the verified metadata up with room for concurrent small appends.
	// The acquired view must fit the reservation before any scan starts.
	line := roundScanSize(shape.line, 1024)
	raw := min(uint64(s.rawLimit(req))+line, uint64(req.Limit)*line)
	blocks, batches := roundScanSize(shape.blocks, 32), roundScanSize(shape.batches, 32)
	return (128 << 10) + blocks*4608 + batches*128 + line*3 + raw*5/4 +
		uint64(req.Limit)*192 + uint64(len(req.Filter.Contains))
}

func (s *retainedPages) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (result httpapi.PageResult, err error) {
	if err := work.Charge(1); err != nil {
		return result, err
	}
	if req.Capture != "" {
		s.mu.Lock()
		c := s.live[req.Capture]
		if c != nil {
			c.refs++
		}
		s.mu.Unlock()
		if c == nil {
			return result, httpapi.ErrCaptureExpired
		}
		defer func() { err = errors.Join(err, s.unref(c)) }()
		return s.scan(ctx, c.view, req, work)
	}
	s.storage.mu.Lock()
	v, err := s.storage.captureLocked(ctx)
	s.storage.mu.Unlock()
	if errors.Is(err, checkpoint.ErrExhausted) {
		return result, httpapi.ErrQueryCapacityExhausted
	}
	if err != nil {
		return result, err
	}
	pinned := false
	defer func() {
		if !pinned {
			err = errors.Join(err, v.release())
		}
	}()
	result, err = s.scan(ctx, v, req, work)
	if err == nil && req.Snapshot {
		s.mu.Lock()
		s.next++
		result.Capture = strconv.FormatUint(s.next, 10)
		if s.live == nil {
			s.live = make(map[string]*pageCapture)
		}
		s.live[result.Capture] = &pageCapture{view: v, refs: 1}
		s.mu.Unlock()
		pinned = true
	}
	return result, err
}

func (s *retainedPages) Release(id string) error {
	s.mu.Lock()
	c := s.live[id]
	delete(s.live, id)
	s.mu.Unlock()
	if c != nil {
		return s.unref(c)
	}
	return nil
}

func (s *retainedPages) unref(c *pageCapture) error {
	s.mu.Lock()
	c.refs--
	drained := c.refs == 0
	s.mu.Unlock()
	if drained {
		return c.view.release()
	}
	return nil
}

func compareRows(a, b checkpoint.BlockRow) int {
	return cmp.Or(a.Row.TS.Compare(b.Row.TS), cmp.Compare(a.Ordinal, b.Ordinal))
}

type tailCursor []checkpoint.BlockRow
type tailMerge []tailCursor

func (h tailMerge) Len() int           { return len(h) }
func (h tailMerge) Less(i, j int) bool { return compareRows(h[i][0], h[j][0]) < 0 }
func (h tailMerge) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *tailMerge) Push(v any)        { *h = append(*h, v.(tailCursor)) }
func (h *tailMerge) Pop() any {
	n := len(*h) - 1
	v := (*h)[n]
	(*h)[n] = nil
	*h = (*h)[:n]
	return v
}

func tailScan(batches []*tailBatch, q history.Query, work *store.QueryWork) (tailMerge, error) {
	var merge tailMerge
	for _, b := range batches {
		if err := work.Charge(1); err != nil {
			return nil, err
		}
		rows := b.rows
		if rows[len(rows)-1].Row.TS.Before(q.Start) || !rows[0].Row.TS.Before(q.End) {
			continue
		}
		if err := work.Charge(uint64(bits.Len(uint(len(rows))))); err != nil {
			return nil, err
		}
		at, _ := slices.BinarySearchFunc(rows, q, func(row checkpoint.BlockRow, q history.Query) int {
			if row.Row.TS.Before(q.Start) ||
				q.After != nil && compareRows(row, checkpoint.BlockRow{Row: store.Row{TS: q.After.Time}, Ordinal: q.After.Ordinal}) <= 0 {
				return -1
			}
			return 1
		})
		if at < len(rows) && rows[at].Row.TS.Before(q.End) {
			merge = append(merge, tailCursor(rows[at:]))
		}
	}
	heap.Init(&merge)
	return merge, nil
}

var errPageComplete = errors.New("retained page complete")

func (s *retainedPages) scan(ctx context.Context, v *recoveryView, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	var result httpapi.PageResult
	if req.ScanBytes != 0 && s.scanBytes(req, v.shape) > req.ScanBytes {
		return result, httpapi.ErrQueryScanCapacityExhausted
	}
	q := history.Query{Start: req.Filter.Start, End: req.Filter.End, Contains: req.Filter.Contains}
	if req.After != "" {
		ts, ord, _ := strings.Cut(string(req.After), "/")
		t, timeErr := time.Parse(time.RFC3339Nano, ts)
		n, ordErr := strconv.ParseUint(ord, 10, 64)
		if err := errors.Join(timeErr, ordErr); err != nil {
			return result, errors.Join(store.ErrInvalidQueryCursor, err)
		}
		q.After = &history.Position{Time: t, Ordinal: n}
	}
	tail, err := tailScan(v.batches, q, work)
	if err != nil {
		return result, err
	}
	raw := 0
	emit := func(row checkpoint.BlockRow) error {
		if len(result.Rows) == req.Limit || raw >= s.rawLimit(req) {
			result.More = req.Snapshot
			return errPageComplete
		}
		if err := work.Charge(1); err != nil {
			return err
		}
		item := httpapi.PageRow{Row: row.Row}
		if req.Snapshot {
			item.Position = httpapi.PagePosition(row.Row.TS.UTC().Format(time.RFC3339Nano) + "/" + strconv.FormatUint(row.Ordinal, 10))
		}
		result.Rows = append(result.Rows, item)
		raw += len(row.Row.Line)
		if !req.Snapshot && (len(result.Rows) == req.Limit || raw >= s.rawLimit(req)) {
			return errPageComplete
		}
		return nil
	}
	emitTail := func(before *checkpoint.BlockRow) error {
		for len(tail) > 0 {
			cursor := tail[0]
			row := cursor[0]
			if before != nil && compareRows(row, *before) >= 0 {
				break
			}
			if err := work.Charge(1); err != nil {
				return err
			}
			cursor = cursor[1:]
			if len(cursor) == 0 || !cursor[0].Row.TS.Before(q.End) {
				heap.Pop(&tail)
			} else {
				tail[0] = cursor
				heap.Fix(&tail, 0)
			}
			if q.Contains != "" {
				if err := work.Charge(uint64(len(row.Row.Line))); err != nil {
					return err
				}
				if !strings.Contains(row.Row.Line, q.Contains) {
					continue
				}
			}
			if err := emit(row); err != nil {
				return err
			}
		}
		return nil
	}
	err = v.reader.ScanWithWork(ctx, q, func(row checkpoint.BlockRow) error {
		if err := emitTail(&row); err != nil {
			return err
		}
		return emit(row)
	}, work)
	if err == nil {
		err = emitTail(nil)
	}
	if err == errPageComplete {
		err = nil
	}
	return result, err
}
