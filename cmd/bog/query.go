package main

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
)

type pinnedView struct {
	point    checkpoint.RecoveryPoint
	reader   *history.Reader
	manifest string
	tail     *tailFile
	batches  []*tailBatch
	refs     int
}

func (r *retained) pinLocked(ctx context.Context, query bool) (*pinnedView, error) {
	reader, err := r.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		if errors.Is(err, checkpoint.ErrExhausted) {
			err = errors.Join(httpapi.ErrQueryCapacityExhausted, err)
		}
		return nil, err
	}
	p := &pinnedView{point: r.point, reader: reader, manifest: r.manifest, tail: r.tail, refs: 1}
	p.tail.refs++
	r.pins[p.point.Generation]++
	if query {
		p.batches = append([]*tailBatch(nil), r.batches...)
		for _, b := range p.batches {
			b.refs++
		}
	}
	return p, nil
}

func (r *retained) dropViewLocked(p *pinnedView) error {
	p.refs--
	if p.refs != 0 {
		return nil
	}
	for _, b := range p.batches {
		b.release()
	}
	id := p.point.Generation
	if r.pins[id]--; r.pins[id] == 0 {
		delete(r.pins, id)
	}
	err := errors.Join(p.reader.Close(), r.dropTailLocked(p.tail))
	if _, retired := r.dead[id]; retired && r.busy == nil && r.fenced == nil && r.pins[id] == 0 {
		err = errors.Join(err, r.reclaimLocked())
	}
	return err
}

type retainedPages struct{ *retained }

func (retainedPages) ChunkOrdinaryQueries() {}

var (
	_           httpapi.PageSource          = retainedPages{}
	_           httpapi.PageSourceScanBound = retainedPages{}
	errPageFull                             = errors.New("retained page has another row")
)

func (s retainedPages) PageScanBytes(req httpapi.PageRequest) uint64 {
	if req.Limit <= 0 || req.Limit > 1000 {
		return math.MaxUint64
	}
	// One 4 KiB page plus metadata per checkpoint run, tail heap and
	// capture tables, the reusable overflow line buffer and its preceding
	// backing during growth. Borrow disk lines before copying into the result.
	const scratch = (5 << 20) + 2*store.MaxBatch
	return scratch + store.MaxBatch +
		uint64(req.Limit)*256
}

func (s retainedPages) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (result httpapi.PageResult, err error) {
	if err := work.Charge(1); err != nil {
		return result, err
	}
	id := req.Capture
	s.mu.Lock()
	var p *pinnedView
	if id != "" {
		p = s.views[id]
		if p == nil {
			err = httpapi.ErrCaptureExpired
		} else {
			p.refs++
		}
	} else if s.fenced != nil {
		err = httpapi.ErrQueryCapacityExhausted
	} else {
		p, err = s.pinLocked(ctx, true)
	}
	s.mu.Unlock()
	if err != nil {
		return result, err
	}
	defer func() {
		s.mu.Lock()
		s.recordRelease(s.dropViewLocked(p))
		s.mu.Unlock()
	}()
	result, err = s.scan(ctx, p, req, work)
	if err == nil && req.Snapshot && id == "" {
		s.mu.Lock()
		s.nextView++
		id = strconv.FormatUint(s.nextView, 10)
		p.refs++
		s.views[id] = p
		s.mu.Unlock()
	}
	result.Capture = id
	return result, err
}

func (s retainedPages) Release(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.views[id]
	if p == nil {
		return nil
	}
	delete(s.views, id)
	err := s.dropViewLocked(p)
	s.recordRelease(err)
	return err
}

func lessPosition(a, b history.Position) bool {
	cmp := a.Time.Compare(b.Time)
	return cmp < 0 || cmp == 0 && a.Ordinal < b.Ordinal
}

type tailCursor struct {
	b *tailBatch
	i int
}

type tailHeap []tailCursor

func (h tailHeap) Len() int { return len(h) }
func (h tailHeap) Less(i, j int) bool {
	return lessPosition(h[i].b.position(h[i].i), h[j].b.position(h[j].i))
}
func (h tailHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *tailHeap) Push(v any)   { *h = append(*h, v.(tailCursor)) }
func (h *tailHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

func (s retainedPages) scan(ctx context.Context, p *pinnedView, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	f := req.Filter
	q := history.Query{Start: f.Start, End: f.End, Contains: f.Contains}
	if req.After != "" {
		stamp, ordinal, _ := strings.Cut(string(req.After), "/")
		t, timeErr := time.Parse(time.RFC3339Nano, stamp)
		n, ordErr := strconv.ParseUint(ordinal, 10, 64)
		if timeErr != nil || ordErr != nil {
			return httpapi.PageResult{}, store.ErrInvalidQueryCursor
		}
		q.After = &history.Position{Time: t, Ordinal: n}
	}
	h := make(tailHeap, 0, len(p.batches))
	for _, b := range p.batches {
		if err := work.Charge(1); err != nil {
			return httpapi.PageResult{}, err
		}
		var probes uint64
		i := sort.Search(len(b.order), func(i int) bool {
			probes++
			k := b.position(i)
			return !k.Time.Before(f.Start) && (q.After == nil || lessPosition(*q.After, k))
		})
		if err := work.Charge(probes); err != nil {
			return httpapi.PageResult{}, err
		}
		if i < len(b.order) && b.position(i).Time.Before(f.End) {
			h = append(h, tailCursor{b, i})
		}
	}
	heap.Init(&h)
	result := httpapi.PageResult{Rows: make([]httpapi.PageRow, 0, req.Limit)}
	remaining := store.MaxBatch
	emit := func(row store.Row, k history.Position, raw []byte) error {
		n := len(row.Line)
		if raw != nil {
			n = len(raw)
		}
		if len(result.Rows) == req.Limit || (len(result.Rows) > 0 && n > remaining) {
			result.More = req.Snapshot
			return errPageFull
		}
		remaining -= n
		if raw != nil {
			row.Line = string(raw)
		} else {
			row.Line = strings.Clone(row.Line)
		}
		var pos httpapi.PagePosition
		if req.Snapshot {
			pos = httpapi.PagePosition(k.Time.Format(time.RFC3339Nano) + "/" + strconv.FormatUint(k.Ordinal, 10))
		}
		result.Rows = append(result.Rows, httpapi.PageRow{Row: row, Position: pos})
		if !req.Snapshot && len(result.Rows) == req.Limit {
			return errPageFull
		}
		return nil
	}
	drain := func(before *history.Position) error {
		for len(h) != 0 {
			c := h[0]
			k := c.b.position(c.i)
			if before != nil && !lessPosition(k, *before) {
				break
			}
			row := c.b.rows[c.b.order[c.i]]
			if err := work.Charge(1, uint64(len(row.Line))); err != nil {
				return err
			}
			if strings.Contains(row.Line, f.Contains) {
				if err := emit(row, k, nil); err != nil {
					return err
				}
			}
			c.i++
			if c.i == len(c.b.order) || !c.b.position(c.i).Time.Before(f.End) {
				heap.Pop(&h)
			} else {
				h[0] = c
				heap.Fix(&h, 0)
			}
		}
		return nil
	}
	err := p.reader.ScanBytesWithWork(ctx, q, func(k history.Position, raw []byte) error {
		if err := drain(&k); err != nil {
			return err
		}
		return emit(store.Row{TS: k.Time}, k, raw)
	}, work)
	if err == nil {
		err = drain(nil)
	}
	if err != nil && err != errPageFull {
		return httpapi.PageResult{}, err
	}
	return result, nil
}
