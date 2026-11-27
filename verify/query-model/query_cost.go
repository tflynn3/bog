package querymodel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// QueryScanBytes is a direct measurement of query scan input. Source names
// the measuring boundary and the bytes it counts. Zero bytes can be observed.
type QueryScanBytes struct {
	Source string `json:"source"`
	Bytes  uint64 `json:"bytes"`
}

// QueryCostSample records adjacent per-request budget settings and the accepted
// walk at the issuing setting. The replay and refusal bodies do not contribute
// to the walk totals.
type QueryCostSample struct {
	Query                         string            `json:"query"`
	Contains                      string            `json:"contains"`
	MinimumIssuingWorkBudgetUnits int               `json:"minimum_issuing_work_budget_units"`
	RejectedWorkBudgetUnits       int               `json:"rejected_work_budget_units"`
	Pages                         int               `json:"pages"`
	ReturnedRows                  int               `json:"returned_rows"`
	ResponseBytes                 int64             `json:"response_bytes"`
	ScannedBytes                  *QueryScanBytes   `json:"scanned_bytes,omitempty"`
	ScannedBytesPerReturnedRow    *float64          `json:"scanned_bytes_per_returned_row,omitempty"`
	Unavailable                   map[string]string `json:"unavailable"`
}

// QueryCostReport identifies the inputs shared by all query cost samples.
type QueryCostReport struct {
	Seed             int64             `json:"seed"`
	Start            time.Time         `json:"start_inclusive"`
	End              time.Time         `json:"end_exclusive"`
	AcknowledgedRows int               `json:"acknowledged_rows"`
	RowLimits        []int             `json:"row_limits"`
	MaxWorkBudget    int               `json:"max_work_budget_units"`
	ResponseByteCap  int64             `json:"response_byte_cap"`
	Samples          []QueryCostSample `json:"samples"`
}

// VerifyQueryCosts compares an absent literal, the sparse trace literal, and
// an unfiltered query over the same timestamp range and seed. Every budget
// probe gets a fresh endpoint and acknowledges the identical batches in order.
// Retained adapters publish the seed at its exact cutoff. The trial runs no
// lifecycle controls and uses only Seed, Epoch, Limits and MaxWorkBudget.
func VerifyQueryCosts(ctx context.Context, cfg QueryTraceConfig, launch LaunchQueryTrace) (QueryCostReport, error) {
	var report QueryCostReport
	if launch == nil || cfg.MaxWorkBudget < 2 || cfg.Limits.MaxPageBytes != seededTrimCap {
		return report, errors.New("query costs require a launcher, a work ceiling of at least two and a 256-byte response cap")
	}
	data := makeSeededWireData(cfg.Seed, cfg.Epoch)
	walk := WireWalk{Seed: cfg.Seed, Filter: data.filter, RowLimits: []int{4, 1, 7}, Limits: cfg.Limits}
	if err := walk.validate(); err != nil {
		return report, err
	}
	report = QueryCostReport{
		Seed: cfg.Seed, Start: data.filter.Start.UTC(), End: data.filter.End.UTC(),
		RowLimits: walk.RowLimits, MaxWorkBudget: cfg.MaxWorkBudget, ResponseByteCap: cfg.Limits.MaxPageBytes,
	}
	for _, batch := range data.initial {
		report.AcknowledgedRows += len(batch)
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Limits.Timeout)
	defer cancel()

	for _, query := range []struct {
		name, literal string
		firstPageRows int
	}{
		{name: "empty", literal: "[no fixture row]"},
		{name: "sparse", literal: data.filter.Contains, firstPageRows: 2},
		{name: "broad"},
	} {
		walk.Filter.Contains, walk.FirstPageRows = query.literal, query.firstPageRows
		issuing, rejected, sample, err := queryBudgetBoundary(cfg.MaxWorkBudget, func(budget int) (bool, QueryCostSample, error) {
			var refused bool
			var sample QueryCostSample
			err := launchQueryTrial(ctx, budget, launch, func(adapter QueryTraceAdapter) error {
				var trialErr error
				refused, sample, trialErr = runQueryCostTrial(ctx, walk, data.initial, adapter)
				return trialErr
			})
			return refused, sample, err
		})
		if err != nil {
			return report, fmt.Errorf("%s query: %w", query.name, err)
		}
		sample.Query, sample.Contains = query.name, query.literal
		sample.MinimumIssuingWorkBudgetUnits, sample.RejectedWorkBudgetUnits = issuing, rejected
		report.Samples = append(report.Samples, sample)
	}
	return report, nil
}

func runQueryCostTrial(ctx context.Context, walk WireWalk, seed [][]Row, adapter QueryTraceAdapter) (bool, QueryCostSample, error) {
	var sample QueryCostSample
	if adapter.Acknowledge == nil || adapter.Fetch == nil {
		return false, sample, errors.New("query costs require acknowledgement and fetch adapters")
	}
	var log Log
	if err := acknowledgeTraceBatches(ctx, &log, adapter.Acknowledge, seed); err != nil {
		return false, sample, fmt.Errorf("seed: %w", err)
	}
	if err := publishTraceCutoff(ctx, adapter.Publish, log.HighWater()); err != nil {
		return false, sample, fmt.Errorf("seed: %w", err)
	}
	var rejection error
	var terminal terminalReplay
	stats, err := VerifyWireWalk(ctx, &log, walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
		page, err := adapter.Fetch(ctx, req)
		if req.Cursor == "" && isCleanOpeningRejection(page, err) {
			rejection = err
		}
		if err == nil && page.NextCursor == "" {
			terminal.record(req, page)
		}
		return page, err
	}, nil)
	if rejection != nil && errors.Is(err, rejection) && ctx.Err() == nil {
		return true, sample, nil
	}
	if err != nil {
		return false, sample, fmt.Errorf("ordered walk: %w", err)
	}

	sample.Pages, sample.ReturnedRows, sample.ResponseBytes = stats.Pages, stats.Rows, stats.BodyBytes
	sample.Unavailable = map[string]string{"charged_work_units": "the query wire response exposes no charged-work counter"}
	if adapter.ObserveScanBytes == nil {
		sample.Unavailable["scanned_bytes"] = "the adapter exposes no query scan byte meter"
	} else {
		observed, err := adapter.ObserveScanBytes(ctx)
		if err != nil {
			return false, sample, fmt.Errorf("observe scanned bytes: %w", err)
		}
		if strings.TrimSpace(observed.Source) == "" {
			return false, sample, errors.New("scan byte observation requires a source")
		}
		sample.ScannedBytes = &observed
		if stats.Rows > 0 {
			ratio := float64(observed.Bytes) / float64(stats.Rows)
			sample.ScannedBytesPerReturnedRow = &ratio
		}
	}
	matches, err := log.Match(log.HighWater(), walk.Filter)
	if err != nil {
		return false, sample, err
	}
	if err := terminal.verify(ctx, adapter.Fetch, matches, stats.Rows, true); err != nil {
		return false, sample, fmt.Errorf("terminal replay: %w", err)
	}
	return false, sample, nil
}
