package history

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"math/bits"
	"os"
	"sort"
	"time"
	"unicode/utf8"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

const readPageSize = 4 << 10

// Position is a row's timestamp and zero-based append ordinal.
type Position struct {
	Time    time.Time
	Ordinal uint64
}

func (p Position) less(q Position) bool {
	if p.Time.Equal(q.Time) {
		return p.Ordinal < q.Ordinal
	}
	return p.Time.Before(q.Time)
}

// Query specifies a half-open interval [Start, End) and an exact, case-sensitive
// substring match. After, if set, excludes keys up to and including that key.
// After is a lower bound. Snapshot and cursor validation belongs to the caller.
type Query struct {
	Start, End time.Time
	Contains   string
	After      *Position
}

// Scan visits matching rows in timestamp and append-ordinal order. Lines passed
// to visit are safe to retain. A visit or I/O error stops the scan.
func (r *Reader) Scan(ctx context.Context, q Query, visit func(checkpoint.BlockRow) error) error {
	return r.ScanWithWork(ctx, q, visit, nil)
}

// ScanWithWork has the same ordered visitor contract as Scan. It charges one
// unit per segment considered, a worst-case binary seek cost per overlapping
// segment, one unit per row header examined, and one unit per byte of a line
// eligible for literal matching. This includes nonmatches and any lookahead
// the visitor requests by continuing the scan. A nil meter allows unlimited
// work. On error, the caller decides whether to discard rows already visited.
func (r *Reader) ScanWithWork(ctx context.Context, q Query, visit func(checkpoint.BlockRow) error, work *store.QueryWork) error {
	var borrowed func(Position, []byte) error
	if visit != nil {
		borrowed = func(key Position, line []byte) error {
			return visit(checkpoint.BlockRow{Row: store.Row{TS: key.Time, Line: string(line)}, Ordinal: key.Ordinal})
		}
	}
	return r.ScanBytesWithWork(ctx, q, borrowed, work)
}

// ScanBytesWithWork visits the same ordered matches using line bytes borrowed
// only for the duration of the callback.
func (r *Reader) ScanBytesWithWork(ctx context.Context, q Query, visit func(Position, []byte) error, work *store.QueryWork) (err error) {
	r.mu.Lock()
	if !r.deadline.IsZero() && !time.Now().Before(r.deadline) {
		r.mu.Unlock()
		return &ExpiredError{ID: r.info.ID}
	}
	g := r.gen
	if g == nil {
		r.mu.Unlock()
		return os.ErrClosed
	}
	var charge *checkpoint.Reservation
	if r.charge != nil {
		charge = r.charge.Retain()
	}
	g.refs.Add(1)
	r.mu.Unlock()
	if charge != nil {
		defer charge.Release()
	}
	defer func() {
		if closeErr := g.release(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := work.Charge(); err != nil {
		return err
	}
	if !q.Start.Before(q.End) || len(q.Contains) > checkpoint.MaxBlockSize-headerSize-rowHeaderSize {
		return nil
	}
	if visit == nil {
		return errors.New("nil history visitor")
	}
	return g.scan(ctx, q, visit, work)
}

func (g *generation) scan(ctx context.Context, q Query, visit func(Position, []byte) error, work *store.QueryWork) error {
	lower := Position{Time: q.Start.UTC()}
	strict := false
	if q.After != nil {
		after := Position{Time: q.After.Time.UTC(), Ordinal: q.After.Ordinal}
		if !after.less(lower) {
			lower, strict = after, true
		}
	}
	end := q.End.UTC()
	if !lower.Time.Before(end) {
		return nil
	}

	var h scanHeap
	for i := range g.segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := work.Charge(1); err != nil {
			return err
		}
		s := &g.segments[i]
		if s.hi.Before(lower.Time) || !s.lo.Before(end) {
			continue
		}
		if err := work.Charge(uint64(bits.Len(uint(len(s.points))))); err != nil {
			return err
		}
		j := sort.Search(len(s.points), func(i int) bool {
			return !s.points[i].key.less(lower)
		})
		c := &cursor{seg: s, offset: s.points[max(j-1, 0)].offset}
		if err := c.seek(ctx, g.dir, lower, strict, end, work); err != nil {
			return err
		}
		if c.ready {
			h = append(h, c)
		}
	}
	heap.Init(&h)
	needle := []byte(q.Contains)
	var overflow []byte
	for len(h) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := h[0]
		if err := work.Charge(uint64(c.lineLen)); err != nil {
			return err
		}
		line, err := c.line(g.dir, &overflow)
		if err != nil {
			return err
		}
		if bytes.Contains(line, needle) {
			if err := visit(c.key, line); err != nil {
				return err
			}
		}
		c.offset += rowHeaderSize + c.lineLen
		if err := c.advance(ctx, g.dir, end, work); err != nil {
			return err
		}
		if c.ready {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return work.Charge()
}

type cursor struct {
	seg     *segment
	offset  int64
	key     Position
	lineLen int64
	ready   bool

	pageOffset int64
	page       []byte
}

func (c *cursor) seek(ctx context.Context, dir *os.File, lower Position, strict bool, end time.Time, work *store.QueryWork) error {
	for {
		if err := c.advance(ctx, dir, end, work); err != nil {
			return err
		}
		if !c.ready || lower.less(c.key) || (!strict && !c.key.less(lower)) {
			return nil
		}
		c.offset += rowHeaderSize + c.lineLen
	}
}

func (c *cursor) advance(ctx context.Context, dir *os.File, end time.Time, work *store.QueryWork) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.ready = false
	size := c.seg.stat.Size()
	if c.offset == size {
		return nil
	}
	if err := work.Charge(1); err != nil {
		return err
	}
	if c.offset < headerSize || size-c.offset < rowHeaderSize {
		return corrupt(c.seg.name, "row header outside segment")
	}
	if !c.covers(c.offset, rowHeaderSize) {
		if err := c.fill(dir, c.offset); err != nil {
			return err
		}
	}
	key, n, ok := parseRow(c.page[c.offset-c.pageOffset:], c.offset, size)
	if !ok {
		return corrupt(c.seg.name, "invalid row header")
	}
	c.key, c.lineLen, c.ready = key, n, key.Time.Before(end)
	return nil
}

func (c *cursor) covers(offset, length int64) bool {
	return offset >= c.pageOffset && offset-c.pageOffset+length <= int64(len(c.page))
}

func (c *cursor) fill(dir *os.File, offset int64) error {
	n := int(min(readPageSize, c.seg.stat.Size()-offset))
	if cap(c.page) < n {
		c.page = make([]byte, n)
	} else {
		c.page = c.page[:n]
	}
	c.pageOffset = offset
	return c.seg.readAt(dir, c.page, offset)
}

func (c *cursor) line(dir *os.File, overflow *[]byte) ([]byte, error) {
	offset := c.offset + rowHeaderSize
	var b []byte
	if c.covers(offset, c.lineLen) {
		p := offset - c.pageOffset
		b = c.page[p : p+c.lineLen]
	} else {
		n := int(c.lineLen)
		if cap(*overflow) < n {
			*overflow = make([]byte, n)
		}
		b = (*overflow)[:n]
		if err := c.seg.readAt(dir, b, offset); err != nil {
			return nil, err
		}
	}
	if !utf8.Valid(b) {
		return nil, corrupt(c.seg.name, "invalid line encoding")
	}
	return b, nil
}

func (s *segment) readAt(dir *os.File, b []byte, offset int64) (err error) {
	if offset < 0 || int64(len(b)) > s.stat.Size()-offset {
		return corrupt(s.name, "read outside segment")
	}
	f, info, err := openRegular(dir, s.name)
	if err != nil {
		return segmentError(s.name, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	if !sameSegment(s.stat, info) {
		return corrupt(s.name, "segment file changed")
	}
	if err := readExactAt(f, b, offset); err != nil {
		return segmentError(s.name, err)
	}
	return nil
}

type scanHeap []*cursor

func (h scanHeap) Len() int           { return len(h) }
func (h scanHeap) Less(i, j int) bool { return h[i].key.less(h[j].key) }
func (h scanHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scanHeap) Push(x any)        { *h = append(*h, x.(*cursor)) }

func (h *scanHeap) Pop() any {
	last := len(*h) - 1
	c := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return c
}
