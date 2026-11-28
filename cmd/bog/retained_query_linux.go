package main

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

const pageLineBytes = 1 << 20

type retainedView struct {
	owner    *retainedRuntime
	reader   *history.Reader
	wal      *wal.Capture
	tail     *store.PositionedSnapshot
	point    wal.Authority
	manifest string
	at       time.Time
	// The map owns one reference. An admitted page keeps its reference if
	// retirement and release race the scan.
	mu    sync.Mutex
	users int
}

func (v *retainedView) close() error {
	v.mu.Lock()
	v.users--
	last := v.users == 0
	v.mu.Unlock()
	if last {
		defer v.owner.releaseView()
		return errors.Join(v.reader.Close(), v.wal.Release())
	}
	return nil
}

func (r *retainedRuntime) releaseView() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.views--
	if r.views == 0 && r.stopped.Load() {
		close(r.viewsDone)
	}
}

func (r *retainedRuntime) acquireView(ctx context.Context) (*retainedView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acquireViewLocked(ctx)
}

func (r *retainedRuntime) acquireViewLocked(ctx context.Context) (*retainedView, error) {
	if r.stopped.Load() {
		return nil, os.ErrClosed
	}
	reader, err := r.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return nil, err
	}
	capture, tail, point, err := r.wal.View(ctx)
	if err != nil {
		return nil, errors.Join(err, reader.Close())
	}
	gen := reader.Generation()
	if point.Covered != gen.Boundary.CoveredOffset || point.First != gen.Boundary.NextOrdinal ||
		(point.Sequence != 0 && point.Generation != gen.ID) {
		return nil, errors.Join(wal.ErrUncertain, reader.Close(), capture.Release())
	}
	r.views++
	return &retainedView{owner: r, reader: reader, wal: capture, tail: tail, point: point, manifest: r.manifest, at: time.Now(), users: 1}, nil
}

func (*retainedRuntime) PageScanBytes(req httpapi.PageRequest) uint64 {
	// One 4 KiB seek page per segment, a long-line read buffer, the
	// separately allocated lookahead line, and copied returned lines.
	return checkpoint.MaxSegments*(4096+256) + 3*store.MaxBatch + pageLineBytes + uint64(req.Limit+1)*256
}

func (r *retainedRuntime) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (_ httpapi.PageResult, err error) {
	if err := work.Charge(1); err != nil {
		return httpapi.PageResult{}, err
	}
	var view *retainedView
	id := req.Capture
	r.mu.Lock()
	if id != "" {
		view = r.captures[id]
		if view == nil {
			err = httpapi.ErrCaptureExpired
		} else {
			view.mu.Lock()
			view.users++
			view.mu.Unlock()
		}
	} else {
		view, err = r.acquireViewLocked(ctx)
	}
	r.mu.Unlock()
	if errors.Is(err, checkpoint.ErrExhausted) {
		err = errors.Join(httpapi.ErrQueryCapacityExhausted, err)
	}
	if err != nil {
		return httpapi.PageResult{}, err
	}
	acquired := id == ""
	defer func() {
		if !acquired || err != nil {
			err = errors.Join(err, view.close())
		}
	}()
	result, err := view.page(ctx, req, work)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	if acquired {
		r.mu.Lock()
		r.nextCapture++
		id = strconv.FormatUint(r.nextCapture, 10)
		r.captures[id] = view
		r.mu.Unlock()
		result.Capture = id
	}
	return result, nil
}

func (r *retainedRuntime) Release(id string) error {
	r.mu.Lock()
	view := r.captures[id]
	delete(r.captures, id)
	r.mu.Unlock()
	if view == nil {
		return nil
	}
	err := view.close()
	r.notify()
	return err
}

var errPageFull = errors.New("retained page is full")

func (v *retainedView) page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	var result httpapi.PageResult
	if req.Limit <= 0 {
		return result, errors.New("invalid retained page limit")
	}
	f := req.Filter
	hq := history.Query{Start: f.Start, End: f.End, Contains: f.Contains}
	var after *store.TailPosition
	if req.After != "" {
		ts, ordinal, ok := strings.Cut(string(req.After), "/")
		t, tsErr := store.ParseTime(ts)
		n, ordErr := strconv.ParseUint(ordinal, 10, 64)
		if !ok || tsErr != nil || ordErr != nil {
			return result, store.ErrInvalidQueryCursor
		}
		after = &store.TailPosition{Time: t, Ordinal: n}
		hq.After = &history.Position{Time: t, Ordinal: n}
	}
	tail, tailMore, err := v.tail.Page(f.Start, f.End, f.Contains, req.Limit+1, v.point.First, after, work)
	if err != nil {
		return result, err
	}
	var bytes int
	emit := func(row store.Row, ordinal uint64) error {
		if len(result.Rows) == req.Limit || len(result.Rows) > 0 && len(row.Line) > pageLineBytes-bytes {
			return errPageFull
		}
		pos := row.TS.AppendFormat(nil, time.RFC3339Nano)
		pos = strconv.AppendUint(append(pos, '/'), ordinal, 10)
		result.Rows = append(result.Rows, httpapi.PageRow{Row: row, Position: httpapi.PagePosition(pos)})
		bytes += len(row.Line)
		return nil
	}
	err = v.reader.ScanWithWork(ctx, hq, func(row checkpoint.BlockRow) error {
		for len(tail) > 0 && (tail[0].Row.TS.Before(row.Row.TS) ||
			tail[0].Row.TS.Equal(row.Row.TS) && tail[0].Position.Ordinal < row.Ordinal) {
			if err := emit(tail[0].Row, tail[0].Position.Ordinal); err != nil {
				return err
			}
			tail = tail[1:]
		}
		return emit(row.Row, row.Ordinal)
	}, work)
	if err == nil {
		for _, row := range tail {
			if err = emit(row.Row, row.Position.Ordinal); err != nil {
				break
			}
		}
	}
	switch {
	case errors.Is(err, errPageFull):
		result.More = true
	case err != nil:
		return httpapi.PageResult{}, err
	default:
		result.More = tailMore
	}
	return result, nil
}
