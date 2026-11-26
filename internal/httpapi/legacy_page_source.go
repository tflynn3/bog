package httpapi

import (
	"context"
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
	views map[string]*store.PositionedSnapshot
}

// Legacy single-row pages can reserve a bounded batch of work to certify
// and replay the suffix. Each block covers 256 of the store and encoder's
// primitive units, which are still metered individually inside the batch.
const (
	legacyReplayBlocks = 5
	legacyReplayUnits  = legacyReplayBlocks * 256
)

func (s *legacyPageSource) Page(_ context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	return s.page(req, work, false)
}

// On an unfiltered walk the store still reserves a unit per candidate and per
// line byte. The candidate reservation covers fixed-size position conversion,
// and its byte reservation covers encoding the same line. The HTTP planner
// separately meters delivery and checks cancellation while encoding.
func (s *legacyPageSource) page(req PageRequest, work *store.QueryWork, prepaid bool) (PageResult, error) {
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
	var view *store.PositionedSnapshot
	var after *store.TailPosition
	if err := work.Charge(snapshotBookkeepingWork(prepaid)); err != nil {
		return PageResult{}, err
	}
	if id == "" {
		view = s.store.CapturePositionedSnapshot()
	} else {
		s.mu.Lock()
		view = s.views[id]
		s.mu.Unlock()
		if view == nil {
			return PageResult{}, ErrCaptureExpired
		}
		if req.After != "" {
			position, err := parseLegacyPosition(req.After)
			if err != nil {
				return PageResult{}, err
			}
			after = &position
		}
	}

	rows, more, err := view.Page(filter.Start, filter.End, filter.Contains, req.Limit, 0, after, work)
	if err != nil {
		return PageResult{}, err
	}
	if !prepaid {
		if err := work.Charge(uint64(len(rows))); err != nil {
			return PageResult{}, err
		}
	}
	result := PageResult{Rows: make([]PageRow, len(rows)), More: more}
	for i, row := range rows {
		if err := work.Charge(); err != nil {
			return PageResult{}, err
		}
		result.Rows[i] = PageRow{Row: row.Row, Position: legacyPosition(row.Position)}
	}
	if id == "" {
		s.mu.Lock()
		s.next++
		id = strconv.FormatUint(s.next, 10)
		if s.views == nil {
			s.views = make(map[string]*store.PositionedSnapshot)
		}
		s.views[id] = view
		s.mu.Unlock()
	}
	result.Capture = id
	return result, nil
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
