package main

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
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

const pageLineBytes = 4 << 20
const aggregateScanBytes = 72 << 20

// A frame reservation includes the encoded payload, JSON decoder scratch,
// and temporary line copies. The remaining charge covers history seek pages,
// both merge prefixes (including a row considered for lookahead), and positions.
func scanBound(limit, frame, line int) uint64 {
	frame = 1 << bits.Len(uint(max(64<<10, frame)-1))
	line = 1 << bits.Len(uint(max(256, line)-1))
	lines := min(uint64(pageLineBytes), uint64(limit)*uint64(line))
	return (5 << 20) + uint64(frame)*8 + lines*4 + uint64(line)*3 + uint64(limit)*256
}

type retainedView struct {
	recoveryView
	reader   *history.Reader
	refs     int
	released bool
	id       string
}

func (v *retainedView) close() error { return errors.Join(v.reader.Close(), v.tail.Release()) }

func (s *retained) pinLocked(ctx context.Context) (view *retainedView, err error) {
	err = s.wal.WithCapture(ctx, func(tail *wal.Capture, a wal.Authority) error {
		point := checkpoint.RecoveryPoint{
			Generation: [32]byte(s.current.ID), CoveredOffset: s.current.Boundary.CoveredOffset,
			NextOrdinal: s.current.Boundary.NextOrdinal, CommittedEnd: tail.CommittedEnd(),
			CommittedOrdinal: tail.NextOrdinal(),
		}
		if a.Version != 0 {
			selected, err := checkpointPoint(a)
			point.SuffixSHA256 = selected.SuffixSHA256
			if err != nil || selected != point {
				return errors.Join(errors.New("retained capture authority changed"), err, tail.Release())
			}
		}
		reader, err := s.catalog.AcquireCurrentUntil(ctx, time.Time{})
		if err != nil {
			return errors.Join(err, tail.Release())
		}
		view = &retainedView{recoveryView: recoveryView{s: s, tail: tail, point: point, name: s.manifest}, reader: reader}
		return nil
	})
	return view, err
}

func (s *retained) PageScanBytes(req httpapi.PageRequest) uint64 {
	if req.Limit < 1 || req.Limit > 1000 {
		return aggregateScanBytes
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Capture != "" {
		if v := s.views[req.Capture]; v != nil {
			return v.scanBound(req.Limit)
		}
		return scanBound(req.Limit, 0, 0)
	}
	frame, line := s.wal.ScanBounds()
	return scanBound(req.Limit, frame, max(line, s.current.MaxLineBytes))
}

func (v *retainedView) scanBound(limit int) uint64 {
	frame, line := v.tail.ScanBounds()
	return scanBound(limit, frame, max(line, v.reader.Generation().MaxLineBytes))
}

func (s *retained) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (result httpapi.PageResult, err error) {
	if err := work.Charge(1); err != nil {
		return result, err
	}
	after, err := parsePagePosition(req.After)
	if err != nil {
		return result, err
	}
	s.mu.Lock()
	var v *retainedView
	if req.Capture == "" {
		v, err = s.pinLocked(ctx)
	} else {
		v = s.views[req.Capture]
		if v == nil || v.released {
			err = httpapi.ErrCaptureExpired
		} else {
			v.refs++
		}
	}
	s.mu.Unlock()
	if err != nil {
		if errors.Is(err, checkpoint.ErrExhausted) {
			err = errors.Join(httpapi.ErrQueryCaptureCapacityExhausted, err)
		}
		return result, err
	}
	if req.Capture != "" {
		defer func() { err = errors.Join(err, s.donePage(v)) }()
	} else {
		defer func() {
			if result.Capture == "" {
				err = errors.Join(err, v.close())
				s.signalMaintenance()
			}
		}()
	}
	if req.Limit < 1 || req.Limit > 1000 {
		return result, errors.New("invalid retained page limit")
	}
	// The view may have advanced since HTTP computed the reservation. Check
	// the pinned view before allocating any frame, merge or result backing.
	if req.ScanBytes != 0 && v.scanBound(req.Limit) > req.ScanBytes {
		return result, httpapi.ErrQueryScanCapacityExhausted
	}
	result, err = v.page(ctx, req, after, work)
	if err != nil || req.Capture != "" {
		return result, err
	}
	s.mu.Lock()
	s.nextID++
	v.id = strconv.FormatUint(s.nextID, 10)
	s.views[v.id] = v
	s.mu.Unlock()
	result.Capture = v.id
	return result, nil
}

func (s *retained) donePage(v *retainedView) error {
	s.mu.Lock()
	v.refs--
	closeView := v.released && v.refs == 0
	if closeView {
		delete(s.views, v.id)
	}
	s.mu.Unlock()
	if closeView {
		err := v.close()
		s.signalMaintenance()
		return err
	}
	return nil
}

func (s *retained) Release(id string) error {
	s.mu.Lock()
	v := s.views[id]
	if v == nil || v.released {
		s.mu.Unlock()
		return nil
	}
	v.released = true
	closeView := v.refs == 0
	if closeView {
		delete(s.views, v.id)
	}
	s.mu.Unlock()
	if closeView {
		err := v.close()
		s.signalMaintenance()
		return err
	}
	return nil
}

func parsePagePosition(raw httpapi.PagePosition) (*history.Position, error) {
	if raw == "" {
		return nil, nil
	}
	stamp, ordinal, ok := strings.Cut(string(raw), "/")
	t, timeErr := time.Parse(time.RFC3339Nano, stamp)
	n, ordinalErr := strconv.ParseUint(ordinal, 10, 64)
	if !ok || timeErr != nil || ordinalErr != nil {
		return nil, store.ErrInvalidQueryCursor
	}
	return &history.Position{Time: t, Ordinal: n}, nil
}

func compareRows(a, b checkpoint.BlockRow) int {
	return cmp.Or(a.Row.TS.Compare(b.Row.TS), cmp.Compare(a.Ordinal, b.Ordinal))
}

type tailPrefix struct {
	rows     []checkpoint.BlockRow
	bytes    int
	more     bool
	excluded history.Position
}

func (t tailPrefix) Len() int           { return len(t.rows) }
func (t tailPrefix) Less(i, j int) bool { return compareRows(t.rows[i], t.rows[j]) > 0 }
func (t tailPrefix) Swap(i, j int)      { t.rows[i], t.rows[j] = t.rows[j], t.rows[i] }
func (t *tailPrefix) Push(row any)      { t.rows = append(t.rows, row.(checkpoint.BlockRow)) }
func (t *tailPrefix) Pop() any {
	last := len(t.rows) - 1
	row := t.rows[last]
	t.rows[last] = checkpoint.BlockRow{}
	t.rows = t.rows[:last]
	return row
}

func (t *tailPrefix) omit(row checkpoint.BlockRow) {
	ordered := row.Row.TS.Compare(t.excluded.Time)
	if !t.more || ordered < 0 || ordered == 0 && row.Ordinal < t.excluded.Ordinal {
		t.excluded = history.Position{Time: row.Row.TS, Ordinal: row.Ordinal}
	}
	t.more = true
}

func (t *tailPrefix) add(row checkpoint.BlockRow, limit int) {
	if t.more {
		ordered := row.Row.TS.Compare(t.excluded.Time)
		if ordered > 0 || ordered == 0 && row.Ordinal >= t.excluded.Ordinal {
			return
		}
	}
	if len(t.rows) == limit && compareRows(t.rows[0], row) <= 0 {
		t.omit(row)
		return
	}
	heap.Push(t, row)
	t.bytes += len(row.Row.Line)
	for len(t.rows) > limit || len(t.rows) > 1 && t.bytes > pageLineBytes {
		last := heap.Pop(t).(checkpoint.BlockRow)
		t.bytes -= len(last.Row.Line)
		t.omit(last)
	}
}

func (v *retainedView) page(ctx context.Context, req httpapi.PageRequest, after *history.Position, work *store.QueryWork) (httpapi.PageResult, error) {
	q := req.Filter
	if !q.Start.Before(q.End) {
		return httpapi.PageResult{}, nil
	}
	if err := work.Charge(v.tail.SeekWork()); err != nil {
		return httpapi.PageResult{}, err
	}
	lower := q.Start
	if after != nil && after.Time.After(lower) {
		lower = after.Time
	}
	var tail tailPrefix
	err := v.tail.Scan(lower, q.End, func(frame wal.Frame) (bool, error) {
		if err := work.Charge(1); err != nil {
			return false, err
		}
		if frame.MaxTime.Before(q.Start) || !frame.MinTime.Before(q.End) ||
			after != nil && (frame.MaxTime.Before(after.Time) ||
				frame.MaxTime.Equal(after.Time) && frame.NextOrdinal-1 <= after.Ordinal) {
			return false, nil
		}
		return true, work.Charge(uint64(frame.End-frame.Start) / 4096)
	}, func(ordinal uint64, row store.Row) error {
		if row.TS.Before(q.Start) || !row.TS.Before(q.End) ||
			after != nil && (row.TS.Before(after.Time) || row.TS.Equal(after.Time) && ordinal <= after.Ordinal) {
			return nil
		}
		if err := work.Charge(1); err != nil {
			return err
		}
		if q.Contains != "" {
			if err := work.Charge(uint64(len(row.Line))); err != nil {
				return err
			}
			if !strings.Contains(row.Line, q.Contains) {
				return nil
			}
		}
		tail.add(checkpoint.BlockRow{Row: row, Ordinal: ordinal}, req.Limit)
		if tail.more && v.tail.Ordered() {
			return io.EOF
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return httpapi.PageResult{}, err
	}
	slices.SortFunc(tail.rows, compareRows)
	result := httpapi.PageResult{Rows: make([]httpapi.PageRow, 0, req.Limit)}
	bytes, i := 0, 0
	add := func(row checkpoint.BlockRow) error {
		if len(result.Rows) == req.Limit || len(result.Rows) > 0 && len(row.Row.Line) > pageLineBytes-bytes {
			return io.EOF // This known additional match proves More.
		}
		if err := work.Charge(1); err != nil {
			return err
		}
		stamp := row.Row.TS.AppendFormat(make([]byte, 0, 52), time.RFC3339Nano)
		position := httpapi.PagePosition(strconv.AppendUint(append(stamp, '/'), row.Ordinal, 10))
		result.Rows = append(result.Rows, httpapi.PageRow{Row: row.Row, Position: position})
		bytes += len(row.Row.Line)
		return nil
	}
	err = v.reader.ScanWithWork(ctx, history.Query{Start: q.Start, End: q.End, Contains: q.Contains, After: after}, func(row checkpoint.BlockRow) error {
		for i < len(tail.rows) && compareRows(tail.rows[i], row) < 0 {
			if err := add(tail.rows[i]); err != nil {
				return err
			}
			i++
		}
		if i == len(tail.rows) && tail.more {
			ordered := row.Row.TS.Compare(tail.excluded.Time)
			if ordered > 0 || ordered == 0 && row.Ordinal > tail.excluded.Ordinal {
				return io.EOF
			}
		}
		return add(row)
	}, work)
	if err == nil {
		for ; i < len(tail.rows); i++ {
			if err = add(tail.rows[i]); err != nil {
				break
			}
		}
	}
	if err != nil && err != io.EOF {
		return httpapi.PageResult{}, fmt.Errorf("scan retained history: %w", err)
	}
	result.More = err == io.EOF || tail.more
	return result, nil
}
