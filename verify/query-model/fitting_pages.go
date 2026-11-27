package querymodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

var errOversizedBoundary = errors.New("reached oversized record at the next cursor")

// VerifyFittingPages tests a fitting prefix followed by one oversized match
// and a later fitting match. The blocked continuation must reproducibly stop
// at the oversized record. The same oracle compares the held and fresh
// prefixes after backfill and, when provided, generation publication.
func VerifyFittingPages(ctx context.Context, epoch time.Time, limit WireLimits, firstLimit int, f CaptureDeliveryFixture) error {
	walk := WireWalk{
		Filter:    Filter{Start: epoch, End: epoch.Add(time.Second)},
		RowLimits: []int{firstLimit, 10},
		Limits:    limit,
	}
	if err := walk.validate(); err != nil {
		return err
	}
	if f.Acknowledge == nil || f.Fetch == nil || f.Refuse == nil || limit.MaxPageBytes != 256 ||
		(firstLimit != 1 && firstLimit != 10) || limit.MaxPages < 3 {
		return errors.New("fitting-page fixture requires acknowledgement, fetch, refusal, a 256-byte cap, and opening limit 1 or 10")
	}
	ctx, cancel := context.WithTimeout(ctx, limit.Timeout)
	defer cancel()

	tie := epoch.Add(time.Millisecond)
	oversized := struct {
		Time time.Time `json:"ts"`
		Line string    `json:"line"`
	}{tie.UTC(), strings.Repeat("<\n界", 64)}
	record, err := json.Marshal(oversized)
	if err != nil {
		return fmt.Errorf("encode diagnostic fixture: %w", err)
	}
	refusal := fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)\n", limit.MaxPageBytes, len(record)+1)
	seed := []Row{
		{Time: tie, Line: "ok tie"},
		{Time: epoch, Line: "ok first"},
		{Time: tie, Line: "ok z"},
		{Time: tie, Line: "ok tie"},
		{Time: tie, Line: oversized.Line},
		{Time: tie, Line: "ok after oversized"},
	}
	backfill := []Row{
		{Time: epoch.Add(-time.Millisecond), Line: "outside query"},
		{Time: epoch, Line: "ok backfill"},
		{Time: tie, Line: "ok new tie after barrier"},
	}
	var ledger Log
	ackAndPublish := func(rows []Row) error {
		if err := f.Acknowledge(ctx, rows); err != nil {
			return err
		}
		cutoff := ledger.Append(rows)
		if f.Publish != nil {
			return f.Publish(ctx, cutoff)
		}
		return nil
	}
	if err := ackAndPublish(seed); err != nil {
		return fmt.Errorf("seed oversized boundary: %w", err)
	}

	boundaryWalk := func(name string, prefix, pages int, after AfterCapture) error {
		rows, calls := 0, 0
		stats, err := VerifyWireWalk(ctx, &ledger, walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
			calls++
			if rows != prefix {
				page, err := f.Fetch(ctx, req)
				if err != nil {
					return page, fmt.Errorf("fitting page %d: %w", calls, err)
				}
				rows += len(page.Rows)
				return page, nil
			}
			for attempt := 1; attempt <= 2; attempt++ {
				if err := f.Refuse(ctx, req, http.StatusUnprocessableEntity, refusal); err != nil {
					return WirePage{}, fmt.Errorf("oversized continuation attempt %d: %w", attempt, err)
				}
			}
			return WirePage{}, errOversizedBoundary
		}, after)
		if err == nil {
			return fmt.Errorf("%s exhausted without reaching the oversized record (accepted %+v)", name, stats)
		}
		if !errors.Is(err, errOversizedBoundary) {
			return fmt.Errorf("%s did not reach the oversized continuation (accepted %+v): %w", name, stats, err)
		}
		if stats.Pages != pages || stats.Rows != prefix || stats.ByteTrimmedPages != 1 || calls != pages+1 {
			return fmt.Errorf("%s fitting prefix %+v, want %d rows over %d pages with one byte cut", name, stats, prefix, pages)
		}
		return nil
	}
	walk.FirstPageRows = min(firstLimit, 4)
	pages := 1
	if firstLimit == 1 {
		pages = 2
	}
	if err := boundaryWalk("held", 4, pages, func(context.Context, string) error {
		return ackAndPublish(backfill)
	}); err != nil {
		return err
	}
	walk.FirstPageRows = min(firstLimit, 5)
	if err := boundaryWalk("fresh", 5, pages, nil); err != nil {
		return err
	}

	// Independently prove that the rows beyond the barrier were committed,
	// with duplicate ties and new backfill ordered by global append ordinal.
	walk.Filter.Contains = "ok "
	walk.FirstPageRows = 0
	if _, err := VerifyWireWalk(ctx, &ledger, walk, f.Fetch, nil); err != nil {
		return fmt.Errorf("fitting-only walk after publication: %w", err)
	}
	return nil
}
