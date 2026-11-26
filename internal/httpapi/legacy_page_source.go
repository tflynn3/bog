package httpapi

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"bog/internal/store"
)

type legacyPageSource struct {
	store    *store.Store
	mu       sync.Mutex
	next     uint64
	captures map[string]store.PositionedSnapshot
}

func newLegacyPageSource(s *store.Store) *legacyPageSource {
	return &legacyPageSource{store: s, captures: make(map[string]store.PositionedSnapshot)}
}

func (s *legacyPageSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	if err := ctx.Err(); err != nil {
		return PageResult{}, err
	}
	filter := req.Filter
	if !req.Snapshot {
		rows, err := s.store.QueryWithWork(filter.Start, filter.End, filter.Contains, req.Limit, work)
		if err != nil {
			return PageResult{}, err
		}
		result := PageResult{Rows: make([]PageRow, len(rows))}
		for i, row := range rows {
			result.Rows[i].Row = row
		}
		return result, nil
	}

	var view store.PositionedSnapshot
	var after *store.TailPosition
	if req.Capture == "" {
		var err error
		view, err = s.store.CapturePositionedSnapshot(0)
		if err != nil {
			return PageResult{}, err
		}
	} else {
		s.mu.Lock()
		var ok bool
		view, ok = s.captures[req.Capture]
		s.mu.Unlock()
		if !ok {
			return PageResult{}, ErrCaptureExpired
		}
		position, err := decodeLegacyPosition(req.After)
		if err != nil {
			return PageResult{}, err
		}
		after = &position
	}
	rows, more, err := view.Page(filter.Start, filter.End, filter.Contains, req.Limit, after, work)
	if err != nil {
		return PageResult{}, err
	}
	result := PageResult{Capture: req.Capture, More: more, Rows: make([]PageRow, len(rows))}
	for i, row := range rows {
		position := row.Position.Time.Format(time.RFC3339Nano) + "/" + strconv.FormatUint(row.Position.Ordinal, 36)
		result.Rows[i] = PageRow{Row: row.Row, Position: PagePosition(position)}
	}
	if err := ctx.Err(); err != nil {
		return PageResult{}, err
	}
	if req.Capture == "" {
		s.mu.Lock()
		s.next++
		result.Capture = strconv.FormatUint(s.next, 36)
		s.captures[result.Capture] = view
		s.mu.Unlock()
	}
	return result, nil
}

func decodeLegacyPosition(p PagePosition) (store.TailPosition, error) {
	stamp, ordinal, ok := strings.Cut(string(p), "/")
	t, timeErr := store.ParseTime(stamp)
	n, ordinalErr := strconv.ParseUint(ordinal, 36, 64)
	if !ok || timeErr != nil || ordinalErr != nil {
		return store.TailPosition{}, store.ErrInvalidQueryCursor
	}
	return store.TailPosition{Time: t, Ordinal: n}, nil
}

func (s *legacyPageSource) Release(capture string) error {
	s.mu.Lock()
	delete(s.captures, capture)
	s.mu.Unlock()
	return nil
}
