package main

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
)

const pageLineBytes = 8 << 20

type pageSource struct {
	owner *storageOwner
	mu    sync.Mutex
	next  uint64
	live  map[string]*pageCapture
}

type pageCapture struct {
	view *matchedView
	refs int // Registry and admitted Page calls, under pageSource.mu.
}

var (
	_ httpapi.PageSource          = (*pageSource)(nil)
	_ httpapi.PageSourceScanBound = (*pageSource)(nil)
	_ httpapi.PinnedPageSource    = (*pageSource)(nil)
)

// The history scan can have one 4 KiB page and cursor per segment, one
// max-sized spill buffer and a copied filter. The merge retains up to
// pageLineBytes of returned lines and can see one max-sized lookahead.
// Tail strings borrow already charged storage. Include its block/run
// scratch and both backing arrays, row keys, and formatted positions.
func (s *pageSource) PageScanBytes(req httpapi.PageRequest) uint64 {
	if req.Limit < 1 || req.Limit > 1000 {
		return math.MaxUint64
	}
	segments, maxLine := checkpoint.MaxSegments, store.MaxBatch
	s.mu.Lock()
	if c := s.live[req.Capture]; c != nil {
		segments, maxLine = len(c.view.manifest.Segments), c.view.maxLine
	}
	s.mu.Unlock()
	lines := min(uint64(pageLineBytes), uint64(req.Limit)*uint64(maxLine))
	return history.ScanScratchBytes(segments, maxLine, len(req.Filter.Contains)) +
		lines + min(lines, 8192*uint64(req.Limit)) + uint64(maxLine) + (1 << 20) + 384*uint64(req.Limit+1)
}

func (s *pageSource) CapturePage(ctx context.Context) (string, error) {
	c, id, err := s.open(ctx, false)
	if err != nil {
		return "", err
	}
	return id, s.drop(c) // Transfer the registry's reference to HTTP.
}

func (s *pageSource) open(ctx context.Context, ordinary bool) (*pageCapture, string, error) {
	view, err := s.owner.acquire(ctx)
	if errors.Is(err, checkpoint.ErrExhausted) {
		return nil, "", httpapi.ErrQueryCaptureCapacityExhausted
	}
	if err != nil {
		return nil, "", err
	}
	c := &pageCapture{view: view, refs: 1}
	if ordinary {
		return c, "", nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	id := strconv.FormatUint(s.next, 10)
	c.refs++
	if s.live == nil {
		s.live = make(map[string]*pageCapture)
	}
	s.live[id] = c
	return c, id, nil
}

func (s *pageSource) drop(c *pageCapture) error {
	s.mu.Lock()
	c.refs--
	last := c.refs == 0
	s.mu.Unlock()
	if last {
		err := c.view.close()
		s.owner.schedule()
		return err
	}
	return nil
}

func (s *pageSource) Release(id string) error {
	s.mu.Lock()
	c := s.live[id]
	delete(s.live, id)
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	return s.drop(c)
}

func (s *pageSource) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (result httpapi.PageResult, err error) {
	if req.Limit < 1 || req.Limit > 1000 {
		return result, store.ErrInvalidQueryCursor
	}
	after, err := parsePosition(req.After)
	if err != nil {
		return result, err
	}
	var c *pageCapture
	id := req.Capture
	if id != "" {
		s.mu.Lock()
		c = s.live[id]
		if c != nil {
			c.refs++
		}
		s.mu.Unlock()
		if c == nil {
			return result, httpapi.ErrCaptureExpired
		}
	} else {
		c, id, err = s.open(ctx, !req.Snapshot)
		if err != nil {
			return result, err
		}
	}
	defer func() {
		if err != nil && req.Capture == "" && id != "" {
			err = errors.Join(err, s.Release(id))
		}
		err = errors.Join(err, s.drop(c))
	}()
	rows, more, err := c.view.page(ctx, req, after, work)
	return httpapi.PageResult{Capture: id, Rows: rows, More: more}, err
}

func parsePosition(raw httpapi.PagePosition) (*store.TailPosition, error) {
	if raw == "" {
		return nil, nil
	}
	stamp, ordinal, ok := strings.Cut(string(raw), "/")
	if !ok {
		return nil, store.ErrInvalidQueryCursor
	}
	t, timeErr := store.ParseTime(stamp)
	n, numErr := strconv.ParseUint(ordinal, 10, 64)
	if timeErr != nil || numErr != nil {
		return nil, store.ErrInvalidQueryCursor
	}
	return &store.TailPosition{Time: t, Ordinal: n}, nil
}

var errPageFull = errors.New("retained page has lookahead")

func (v *matchedView) page(ctx context.Context, req httpapi.PageRequest, after *store.TailPosition,
	work *store.QueryWork) ([]httpapi.PageRow, bool, error) {
	f := req.Filter
	tail, _, err := v.tail.Page(f.Start, f.End, f.Contains, req.Limit+1, v.point.NextOrdinal, after, work)
	if err != nil {
		return nil, false, err
	}
	result := make([]httpapi.PageRow, 0, req.Limit)
	bytes := 0
	add := func(row store.Row, ordinal uint64) error {
		if len(result) == req.Limit || len(result) != 0 && len(row.Line) > pageLineBytes-bytes {
			return errPageFull
		}
		key := row.TS.UTC().Format(time.RFC3339Nano) + "/" + strconv.FormatUint(ordinal, 10)
		result = append(result, httpapi.PageRow{Row: row, Position: httpapi.PagePosition(key)})
		bytes += len(row.Line)
		return nil
	}
	query := history.Query{Start: f.Start, End: f.End, Contains: f.Contains}
	if after != nil {
		query.After = &history.Position{Time: after.Time, Ordinal: after.Ordinal}
	}
	err = v.generation.ScanWithWork(ctx, query, func(row checkpoint.BlockRow) error {
		// Every tail ordinal is greater than every checkpoint ordinal.
		for len(tail) > 0 && tail[0].Row.TS.Before(row.Row.TS) {
			if err := add(tail[0].Row, tail[0].Position.Ordinal); err != nil {
				return err
			}
			tail = tail[1:]
		}
		return add(row.Row, row.Ordinal)
	}, work)
	if errors.Is(err, errPageFull) {
		return result, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	for _, r := range tail {
		if err := add(r.Row, r.Position.Ordinal); err != nil {
			return result, true, nil
		}
	}
	return result, false, work.Charge()
}
