package httpapi

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"bog/internal/store"
)

type legacyPageSource struct {
	store *store.Store
	mu    sync.Mutex
	next  uint64
	views map[string]legacyQueryView
}

type legacyQueryView struct {
	snapshot  *store.PositionedSnapshot
	batchScan bool
}

const positionsPerUnit = 32

func (s *legacyPageSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
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
	id := req.Capture
	var view legacyQueryView
	var after *store.TailPosition
	// One adapter unit covers capture or lookup and the fixed-size
	// positions for the first block of rows.
	if err := work.Charge(1); err != nil {
		return PageResult{}, err
	}
	if id == "" {
		view.snapshot = s.store.CapturePositionedSnapshot()
		// Use a bounded batch for compact literal searches, where the cost
		// of checking many short nonmatches dominates. Keep the selection
		// with the captured view so later appends cannot change replay cost.
		count := s.store.PublishedCount()
		view.batchScan = filter.Contains != "" && count > positionsPerUnit && count <= 8*positionsPerUnit
	} else {
		s.mu.Lock()
		view = s.views[id]
		s.mu.Unlock()
		if view.snapshot == nil {
			return PageResult{}, ErrCaptureExpired
		}
	}
	if req.After != "" {
		if err := work.Charge(1); err != nil {
			return PageResult{}, err
		}
		position, err := parseLegacyPosition(req.After)
		if err != nil {
			return PageResult{}, err
		}
		after = &position
	}

	rows, more, err := view.page(ctx, filter, req.Limit, after, work)
	if err != nil {
		return PageResult{}, err
	}
	if err := work.Charge(uint64(max(0, len(rows)-1) / positionsPerUnit)); err != nil {
		return PageResult{}, err
	}
	result := PageResult{Rows: make([]PageRow, len(rows)), More: more}
	for i, row := range rows {
		result.Rows[i] = PageRow{Row: row.Row, Position: legacyPosition(row.Position)}
	}
	if id == "" {
		s.mu.Lock()
		s.next++
		id = strconv.FormatUint(s.next, 10)
		if s.views == nil {
			s.views = make(map[string]legacyQueryView)
		}
		s.views[id] = view
		s.mu.Unlock()
	}
	result.Capture = id
	return result, nil
}

func (v legacyQueryView) page(ctx context.Context, filter PageFilter, limit int, after *store.TailPosition, work *store.QueryWork) ([]store.PositionedRow, bool, error) {
	if v.batchScan && after == nil {
		// Pay before scanning and bound the actual Store work, including
		// seeks, candidate bytes, and trailing exhaustion. A heavier scan
		// can use the regular meter after paying for the entire attempt.
		const batches, unitsPerBatch = 16, 128
		if err := work.Charge(batches); err != nil {
			return nil, false, err
		}
		batchWork := store.NewQueryWork(ctx, batches*unitsPerBatch)
		rows, more, err := v.snapshot.Page(filter.Start, filter.End, filter.Contains, limit, 0, after, batchWork)
		if !errors.Is(err, store.ErrQueryWorkExhausted) {
			return rows, more, err
		}
	}
	return v.snapshot.Page(filter.Start, filter.End, filter.Contains, limit, 0, after, work)
}

func (s *legacyPageSource) Release(id string) error {
	s.mu.Lock()
	delete(s.views, id)
	s.mu.Unlock()
	return nil
}

func legacyPosition(p store.TailPosition) PagePosition {
	return PagePosition(p.Time.UTC().Format(time.RFC3339Nano) + "/" + strconv.FormatUint(p.Ordinal, 10))
}

func parseLegacyPosition(p PagePosition) (store.TailPosition, error) {
	stamp, ordinal, _ := strings.Cut(string(p), "/")
	t, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return store.TailPosition{}, fmt.Errorf("legacy page position timestamp: %w", err)
	}
	n, err := strconv.ParseUint(ordinal, 10, 64)
	if err != nil {
		return store.TailPosition{}, fmt.Errorf("legacy page position ordinal: %w", err)
	}
	return store.TailPosition{Time: t, Ordinal: n}, nil
}
