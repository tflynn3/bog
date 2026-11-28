package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
)

const rawPageBytes = 1 << 20

var stopPage = errors.New("retained page has lookahead")

type retainedPages struct{ storage *retained }

type queryView struct {
	epoch      *epoch
	generation *history.Reader
	tail       *tailSnapshot
	refs       int
	bareSlot   *checkpoint.Reservation
}

type tailSnapshot struct {
	rows    *store.PositionedSnapshot
	charges []*checkpoint.Reservation
	refs    int
}

func (r *retained) capture(ctx context.Context) (*queryView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fenced {
		return nil, httpapi.ErrQueryCapacityExhausted
	}
	e := r.current
	v := &queryView{epoch: e, refs: 1}
	var err error
	if e.first != 0 || e.sequence != 0 {
		v.generation, err = r.catalog.AcquireCurrentUntil(ctx, time.Time{})
	} else {
		var slot *checkpoint.Reservation
		slot, err = r.budget.Reserve(ctx, checkpoint.LiveCaptures, 1)
		if err == nil {
			v.bareSlot = slot
		}
	}
	if err != nil {
		return nil, err
	}
	t := r.tailSnapshot
	if t == nil {
		// Captures of the same committed end share the independent row
		// headers and references. Appends create a new snapshot on demand.
		charge, err := r.budget.Reserve(ctx, checkpoint.CaptureBytes, uint64(r.rows.PublishedCount())*80+uint64(len(r.batches))*64+1024)
		if err != nil {
			return nil, errors.Join(err, v.closeGeneration())
		}
		t = &tailSnapshot{rows: r.rows.CaptureDetachedPositionedSnapshot(), charges: make([]*checkpoint.Reservation, 0, len(r.batches)+1)}
		t.charges = append(t.charges, charge)
		for _, b := range r.batches {
			t.charges = append(t.charges, b.Reservation.Retain())
		}
		r.tailSnapshot = t
	}
	t.refs++
	v.tail = t
	e.pins++
	return v, nil
}

func (v *queryView) closeGeneration() error {
	if v.bareSlot != nil {
		v.bareSlot.Release()
	}
	if v.generation != nil {
		return v.generation.Close()
	}
	return nil
}

func (r *retained) release(v *queryView) error {
	r.mu.Lock()
	v.refs--
	if v.refs != 0 {
		r.mu.Unlock()
		return nil
	}
	t := v.tail
	t.refs--
	if t.refs == 0 {
		if r.tailSnapshot == t {
			r.tailSnapshot = nil
		}
		for _, q := range t.charges {
			q.Release()
		}
	}
	r.mu.Unlock()

	err := v.closeGeneration()
	r.mu.Lock()
	v.epoch.pins--
	if v.epoch.retired && v.epoch.pins == 0 {
		r.notify()
	}
	r.mu.Unlock()
	return err
}

func (s *retainedPages) Release(id string) error {
	r := s.storage
	r.mu.Lock()
	v := r.captures[id]
	delete(r.captures, id)
	r.mu.Unlock()
	if v == nil {
		return nil
	}
	return r.release(v)
}

// Include all segment seek pages and cursor state, overflow scratch and its
// growth, filter backing, tail results, and result lines and position arrays.
func (*retainedPages) PageScanBytes(req httpapi.PageRequest) uint64 {
	raw := min(uint64(req.Limit)*store.MaxBatch, rawPageBytes+store.MaxBatch)
	return (12 << 20) + raw + uint64(req.Limit)*320
}

func (s *retainedPages) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (_ httpapi.PageResult, err error) {
	r := s.storage
	if req.Capture != "" {
		r.mu.Lock()
		v := r.captures[req.Capture]
		if v != nil {
			v.refs++
		}
		r.mu.Unlock()
		if v == nil {
			return httpapi.PageResult{}, httpapi.ErrCaptureExpired
		}
		defer func() { err = errors.Join(err, r.release(v)) }()
		return scanPage(ctx, v, req, work)
	}
	v, err := r.capture(ctx)
	if err != nil {
		if errors.Is(err, checkpoint.ErrExhausted) {
			err = errors.Join(httpapi.ErrQueryCapacityExhausted, err)
		}
		return httpapi.PageResult{}, err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, r.release(v))
		}
	}()
	result, err := scanPage(ctx, v, req, work)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	if req.Snapshot || result.More {
		r.mu.Lock()
		r.nextCapture++
		result.Capture = strconv.FormatUint(r.nextCapture, 10)
		r.captures[result.Capture] = v
		r.mu.Unlock()
		owned = false
	}
	return result, nil
}

func scanPage(ctx context.Context, v *queryView, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	q := history.Query{Start: req.Filter.Start, End: req.Filter.End, Contains: req.Filter.Contains}
	var after *store.TailPosition
	if req.After != "" {
		ts, ordinal, ok := strings.Cut(string(req.After), "/")
		t, terr := time.Parse(time.RFC3339Nano, ts)
		o, oerr := strconv.ParseUint(ordinal, 16, 64)
		if !ok || terr != nil || oerr != nil {
			return httpapi.PageResult{}, store.ErrInvalidQueryCursor
		}
		after = &store.TailPosition{Time: t, Ordinal: o}
		q.After = &history.Position{Time: t, Ordinal: o}
	}
	tail, more, err := v.tail.rows.Page(q.Start, q.End, q.Contains, req.Limit, v.epoch.first, after, work)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	result := httpapi.PageResult{Rows: make([]httpapi.PageRow, 0, req.Limit)}
	var raw uint64
	full := func() bool { return len(result.Rows) == req.Limit || raw >= rawPageBytes }
	push := func(row store.Row, ordinal uint64) {
		position := row.TS.AppendFormat(make([]byte, 0, 48), time.RFC3339Nano)
		position = strconv.AppendUint(append(position, '/'), ordinal, 16)
		result.Rows = append(result.Rows, httpapi.PageRow{Row: row, Position: httpapi.PagePosition(position)})
		raw += uint64(len(row.Line))
	}
	i := 0
	if v.generation != nil {
		err = v.generation.ScanBytesWithWork(ctx, q, func(pos history.Position, line []byte) error {
			// Every covered ordinal precedes every tail ordinal on a tie.
			for i < len(tail) && tail[i].Row.TS.Before(pos.Time) {
				if full() {
					return stopPage
				}
				push(tail[i].Row, tail[i].Position.Ordinal)
				i++
			}
			if full() {
				return stopPage
			}
			push(store.Row{TS: pos.Time, Line: string(line)}, pos.Ordinal)
			if !req.Snapshot && len(result.Rows) == req.Limit {
				return stopPage
			}
			return nil
		}, work)
	}
	for err == nil && i < len(tail) {
		if full() {
			err = stopPage
			break
		}
		push(tail[i].Row, tail[i].Position.Ordinal)
		i++
	}
	if err != nil && err != stopPage {
		return httpapi.PageResult{}, err
	}
	result.More = (req.Snapshot || len(result.Rows) < req.Limit) && (err == stopPage || more)
	return result, work.Charge()
}
