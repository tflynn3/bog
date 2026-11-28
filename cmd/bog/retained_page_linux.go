package main

import (
	"cmp"
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

type retainedPages struct {
	storage *retained
}

var (
	_ httpapi.PageSource          = (*retainedPages)(nil)
	_ httpapi.PageSourceScanBound = (*retainedPages)(nil)
	_ httpapi.QueryVisitorSource  = (*retainedPages)(nil)
)

type pageCapture struct {
	epoch   *epoch
	reader  *history.Reader
	batches []*tailBatch
	refs    int
	at      time.Time
}

func (s *retained) acquirePage(ctx context.Context) (*pageCapture, error) {
	if s.stopping.Load() {
		return nil, httpapi.ErrQueryCapacityExhausted
	}
	r, err := s.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if errors.Is(err, checkpoint.ErrExhausted) {
		return nil, httpapi.ErrQueryCapacityExhausted
	}
	if err != nil {
		return nil, err
	}
	e := s.active
	e.pins++
	return &pageCapture{epoch: e, reader: r, batches: e.batches, refs: 1, at: time.Now()}, nil
}

func (s *retained) dropPage(c *pageCapture) error {
	c.refs--
	if c.refs != 0 {
		return nil
	}
	c.epoch.pins--
	if c.epoch != s.active {
		s.notifyMaintenance()
	}
	return c.reader.Close()
}

func (s *retained) finishPage(c *pageCapture) (err error) {
	s.owner.Synchronize(func(*wal.Locked) { err = s.dropPage(c) })
	return err
}

func (p *retainedPages) Release(id string) (err error) {
	key, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return err
	}
	s := p.storage
	s.owner.Synchronize(func(*wal.Locked) {
		if pin := s.captures[key]; pin != nil {
			delete(s.captures, key)
			err = s.dropPage(pin)
		}
	})
	return err
}

const pageResultBytes = 4 << 20

// Account for each segment's cursor and seek page, the tail merge heap, and
// the reusable overflow buffer including its smaller initial allocation.
const scanScratch = checkpoint.MaxSegments*(4096+256) + store.MaxBatch + (512 << 10)

func (*retainedPages) QueryVisitScanBytes() uint64 { return scanScratch + store.MaxBatch }

func (p *retainedPages) PageScanBytes(req httpapi.PageRequest) uint64 {
	if req.Limit <= 0 || req.Limit > 1000 {
		return math.MaxUint64
	}
	return scanScratch + pageResultBytes + uint64(req.Limit)*256
}

func (p *retainedPages) VisitQuery(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork, visit func(store.Row) (bool, error)) (finish func() error, err error) {
	if err := work.Charge(1); err != nil {
		return nil, err
	}
	s := p.storage
	var c *pageCapture
	if err := s.owner.WithLock(func(*wal.Locked) error {
		var err error
		c, err = s.acquirePage(ctx)
		return err
	}); err != nil {
		return nil, err
	}
	defer func() {
		if finish == nil {
			err = errors.Join(err, s.finishPage(c))
		}
	}()
	remaining := req.Limit
	emit := func(row store.Row) error {
		if err := work.Charge(1); err != nil {
			return err
		}
		ok, err := visit(row)
		if err != nil {
			return err
		}
		remaining--
		if !ok || remaining == 0 {
			return pageFull
		}
		return nil
	}
	err = scanMatches(ctx, c, req.Filter, nil, work,
		func(row checkpoint.BlockRow) error { return emit(row.Row) },
		func(pos history.Position, line []byte) error {
			return emit(store.Row{TS: pos.Time, Line: string(line)})
		})
	if err != nil && err != pageFull {
		return nil, err
	}
	return func() error { return s.finishPage(c) }, nil
}

func (p *retainedPages) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (_ httpapi.PageResult, err error) {
	if err := work.Charge(1); err != nil {
		return httpapi.PageResult{}, err
	}
	after, err := decodePosition(req.After)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	s := p.storage
	var c *pageCapture
	var key uint64
	opening := req.Snapshot && req.Capture == ""
	if req.Capture != "" {
		key, err = strconv.ParseUint(req.Capture, 10, 64)
		if err != nil {
			return httpapi.PageResult{}, httpapi.ErrCaptureExpired
		}
		s.owner.Synchronize(func(*wal.Locked) {
			c = s.captures[key]
			if c != nil {
				c.refs++
			}
		})
		if c == nil {
			return httpapi.PageResult{}, httpapi.ErrCaptureExpired
		}
	} else if err := s.owner.WithLock(func(*wal.Locked) error {
		var err error
		c, err = s.acquirePage(ctx)
		return err
	}); err != nil {
		return httpapi.PageResult{}, err
	}
	transferred := false
	defer func() {
		if !transferred {
			err = errors.Join(err, s.finishPage(c))
		}
	}()

	result, err := p.scan(ctx, c, req, after, work)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	if opening {
		s.owner.Synchronize(func(*wal.Locked) {
			s.nextCapture++
			key = s.nextCapture
			s.captures[key] = c
		})
		result.Capture = strconv.FormatUint(key, 10)
		transferred = true
	} else if !req.Snapshot {
		result.Finish = func() error { return s.finishPage(c) }
		transferred = true
	}
	return result, nil
}

var pageFull = errors.New("page has a further match")

func (p *retainedPages) scan(ctx context.Context, c *pageCapture, req httpapi.PageRequest, after *history.Position, work *store.QueryWork) (httpapi.PageResult, error) {
	var result httpapi.PageResult
	var size int
	admit := func(n int) error {
		if len(result.Rows) == req.Limit || (len(result.Rows) > 0 && n > pageResultBytes-size) {
			return pageFull
		}
		return work.Charge(1)
	}
	add := func(row checkpoint.BlockRow) {
		out := httpapi.PageRow{Row: row.Row}
		if req.Snapshot {
			out.Position = encodePosition(row)
		}
		result.Rows = append(result.Rows, out)
		size += len(row.Row.Line)
	}
	err := scanMatches(ctx, c, req.Filter, after, work, func(row checkpoint.BlockRow) error {
		if err := admit(len(row.Row.Line)); err != nil {
			return err
		}
		add(row)
		return nil
	}, func(pos history.Position, line []byte) error {
		if err := admit(len(line)); err != nil {
			return err
		}
		add(checkpoint.BlockRow{Row: store.Row{TS: pos.Time, Line: string(line)}, Ordinal: pos.Ordinal})
		return nil
	})
	if err == pageFull {
		result.More = req.Snapshot
		return result, nil
	}
	if err != nil {
		return httpapi.PageResult{}, err
	}
	return result, nil
}

func scanMatches(ctx context.Context, c *pageCapture, f httpapi.PageFilter, after *history.Position, work *store.QueryWork,
	visitTail func(checkpoint.BlockRow) error, visitHistory func(history.Position, []byte) error) error {
	q := history.Query{Start: f.Start, End: f.End, Contains: f.Contains, After: after}
	tail, err := newTailScan(c.batches, q, work)
	if err != nil {
		return err
	}
	emitTail := func(before *checkpoint.BlockRow) error {
		for len(tail) > 0 && (before == nil || compareRows(tail[0].row(), *before) < 0) {
			if err := visitTail(tail[0].row()); err != nil {
				return err
			}
			if err := tail.advance(q, work); err != nil {
				return err
			}
		}
		return nil
	}
	err = c.reader.ScanBorrowedWithWork(ctx, q, func(pos history.Position, line []byte) error {
		key := checkpoint.BlockRow{Row: store.Row{TS: pos.Time}, Ordinal: pos.Ordinal}
		if err := emitTail(&key); err != nil {
			return err
		}
		return visitHistory(pos, line)
	}, work)
	if err != nil {
		return err
	}
	return emitTail(nil)
}

type tailCursor struct {
	rows []checkpoint.BlockRow
	i    int
}

func (c tailCursor) row() checkpoint.BlockRow { return c.rows[c.i] }

func (c *tailCursor) seekMatch(q history.Query, work *store.QueryWork) (bool, error) {
	for ; c.i < len(c.rows); c.i++ {
		row := c.row().Row
		if !row.TS.Before(q.End) {
			break
		}
		units := uint64(1)
		if q.Contains != "" {
			units += uint64(len(row.Line))
		}
		if err := work.Charge(units); err != nil {
			return false, err
		}
		if strings.Contains(row.Line, q.Contains) {
			return true, nil
		}
	}
	return false, nil
}

type tailScan []tailCursor

func newTailScan(batches []*tailBatch, q history.Query, work *store.QueryWork) (tailScan, error) {
	h := make(tailScan, 0, len(batches))
	for _, batch := range batches {
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
		i, _ := slices.BinarySearchFunc(rows, q, func(row checkpoint.BlockRow, q history.Query) int {
			if row.Row.TS.Before(q.Start) {
				return -1
			}
			if q.After == nil {
				return 1
			}
			if order := row.Row.TS.Compare(q.After.Time); order != 0 {
				return order
			}
			if row.Ordinal <= q.After.Ordinal {
				return -1
			}
			return 1
		})
		c := tailCursor{rows: rows, i: i}
		if ok, err := c.seekMatch(q, work); err != nil {
			return nil, err
		} else if ok {
			h = append(h, c)
		}
	}
	heap.Init(&h)
	return h, nil
}

func (h *tailScan) advance(q history.Query, work *store.QueryWork) error {
	c := &(*h)[0]
	c.i++
	ok, err := c.seekMatch(q, work)
	if err != nil {
		return err
	}
	if ok {
		heap.Fix(h, 0)
	} else {
		heap.Pop(h)
	}
	return nil
}

func (h tailScan) Len() int           { return len(h) }
func (h tailScan) Less(i, j int) bool { return compareRows(h[i].row(), h[j].row()) < 0 }
func (h tailScan) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *tailScan) Push(x any)        { *h = append(*h, x.(tailCursor)) }
func (h *tailScan) Pop() any {
	i := len(*h) - 1
	c := (*h)[i]
	(*h)[i] = tailCursor{}
	*h = (*h)[:i]
	return c
}

func encodePosition(row checkpoint.BlockRow) httpapi.PagePosition {
	var b [20]byte
	binary.BigEndian.PutUint64(b[:8], uint64(row.Row.TS.Unix()))
	binary.BigEndian.PutUint32(b[8:12], uint32(row.Row.TS.Nanosecond()))
	binary.BigEndian.PutUint64(b[12:], row.Ordinal)
	return httpapi.PagePosition(base64.RawURLEncoding.EncodeToString(b[:]))
}

func decodePosition(value httpapi.PagePosition) (*history.Position, error) {
	if value == "" {
		return nil, nil
	}
	var b [20]byte
	if len(value) != base64.RawURLEncoding.EncodedLen(len(b)) {
		return nil, store.ErrInvalidQueryCursor
	}
	n, err := base64.RawURLEncoding.Strict().Decode(b[:], []byte(value))
	ns := binary.BigEndian.Uint32(b[8:12])
	if err != nil || n != len(b) || cmp.Compare(ns, 1_000_000_000) >= 0 {
		return nil, store.ErrInvalidQueryCursor
	}
	return &history.Position{Time: time.Unix(int64(binary.BigEndian.Uint64(b[:8])), int64(ns)).UTC(),
		Ordinal: binary.BigEndian.Uint64(b[12:])}, nil
}
