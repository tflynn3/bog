package querymodel

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// QueryCostCounter is a direct observation at Source. A nil counter means
// unavailable. A non-nil counter can report an observed zero.
type QueryCostCounter struct {
	Source string `json:"source"`
	Value  uint64 `json:"value"`
}

// QueryScanObservation covers all scans serving one request, including any
// opening certification, and counts only successful scan-work charges.
type QueryScanObservation struct {
	ScannedBytes     *QueryCostCounter `json:"scanned_bytes,omitempty"`
	ChargedWorkUnits *QueryCostCounter `json:"charged_work_units,omitempty"`
}

// QueryCostPage describes one accepted page in request order, starting at one.
type QueryCostPage struct {
	Page          int   `json:"page"`
	RowLimit      int   `json:"row_limit"`
	ReturnedRows  int   `json:"returned_rows"`
	ResponseBytes int64 `json:"response_bytes"`
	QueryScanObservation
	Unavailable map[string]string `json:"unavailable"`
}

// QueryCostWalk counts the accepted pages through exhaustion. Its response
// byte counts, including those on each page, use ResponseBytesSource.
type QueryCostWalk struct {
	Pages               int             `json:"pages"`
	ReturnedRows        int             `json:"returned_rows"`
	ResponseBytes       int64           `json:"response_bytes"`
	ResponseBytesSource string          `json:"response_bytes_source"`
	PageObservations    []QueryCostPage `json:"page_observations"`
	QueryScanObservation
	ScannedBytesPerReturnedRow *float64          `json:"scanned_bytes_per_returned_row,omitempty"`
	Unavailable                map[string]string `json:"unavailable"`
}

// QueryCostReplacement records held and fresh walks at a shared budget large
// enough for the augmented view. A publication cutoff is present for retained
// adapters that observe activation after the first backfill.
type QueryCostReplacement struct {
	WorkBudgetUnits            int           `json:"work_budget_units"`
	PublishedReplacementCutoff int           `json:"published_replacement_cutoff,omitempty"`
	AcknowledgedRows           int           `json:"acknowledged_rows"`
	Held                       QueryCostWalk `json:"held"`
	Fresh                      QueryCostWalk `json:"fresh"`
}

// QueryCostSample reports the adjacent request budget settings separately from
// the seed walk's observed work. Refusals and terminal replays contribute no
// cost to any walk.
type QueryCostSample struct {
	Query                         string `json:"query"`
	Contains                      string `json:"contains"`
	MinimumIssuingWorkBudgetUnits int    `json:"minimum_issuing_work_budget_units"`
	RejectedWorkBudgetUnits       int    `json:"rejected_work_budget_units"`
	QueryCostWalk
	Replacement *QueryCostReplacement `json:"replacement,omitempty"`
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

const denseCostLiteral = "[all]"

// VerifyQueryCosts compares absent, rare and dense literals over equivalent
// issuers. For nonempty queries it also checks held and fresh walks across
// backfill at the work ceiling. Retained adapters publish the seed and replace
// it at the first acknowledged backfill cutoff. Lifecycle controls are unused.
func VerifyQueryCosts(ctx context.Context, cfg QueryTraceConfig, launch LaunchQueryTrace) (QueryCostReport, error) {
	var report QueryCostReport
	if launch == nil || cfg.MaxWorkBudget < 2 || cfg.Limits.MaxPageBytes != seededTrimCap {
		return report, errors.New("query costs require a launcher, a work ceiling of at least two and a 256-byte response cap")
	}
	data := makeSeededWireData(cfg.Seed, cfg.Epoch)
	for _, batches := range [][][]Row{data.initial, data.later} {
		for _, batch := range batches {
			for i := range batch {
				batch[i].Line = denseCostLiteral + " " + batch[i].Line
			}
		}
	}
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
		{name: "broad", literal: denseCostLiteral},
	} {
		walk.Filter.Contains, walk.FirstPageRows = query.literal, query.firstPageRows
		issuing, rejected, cost, err := queryBudgetBoundary(cfg.MaxWorkBudget, func(budget int) (bool, QueryCostWalk, error) {
			var refused bool
			var cost QueryCostWalk
			err := launchQueryTrial(ctx, budget, launch, func(adapter QueryTraceAdapter) error {
				log, err := seedQueryCostTrial(ctx, data.initial, adapter)
				if err != nil {
					return err
				}
				refused, cost, err = measureQueryCostWalk(ctx, log, walk, adapter, nil)
				return err
			})
			return refused, cost, err
		})
		if err != nil {
			return report, fmt.Errorf("%s query: %w", query.name, err)
		}
		sample := QueryCostSample{
			Query: query.name, Contains: query.literal, QueryCostWalk: cost,
			MinimumIssuingWorkBudgetUnits: issuing, RejectedWorkBudgetUnits: rejected,
		}
		if cost.ReturnedRows != 0 {
			sample.Replacement = &QueryCostReplacement{WorkBudgetUnits: cfg.MaxWorkBudget}
			err := launchQueryTrial(ctx, cfg.MaxWorkBudget, launch, func(adapter QueryTraceAdapter) error {
				return measureQueryCostReplacement(ctx, data, walk, adapter, sample.Replacement)
			})
			if err != nil {
				return report, fmt.Errorf("%s query backfill trial: %w", query.name, err)
			}
		}
		report.Samples = append(report.Samples, sample)
	}
	return report, nil
}

func seedQueryCostTrial(ctx context.Context, seed [][]Row, adapter QueryTraceAdapter) (*Log, error) {
	if adapter.Acknowledge == nil || adapter.Fetch == nil {
		return nil, errors.New("query costs require acknowledgement and fetch adapters")
	}
	log := new(Log)
	if err := acknowledgeTraceBatches(ctx, log, adapter.Acknowledge, seed); err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	if err := publishTraceCutoff(ctx, adapter.Publish, log.HighWater()); err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	return log, nil
}

func measureQueryCostReplacement(ctx context.Context, data seededWireData, walk WireWalk, adapter QueryTraceAdapter, result *QueryCostReplacement) error {
	log, err := seedQueryCostTrial(ctx, data.initial, adapter)
	if err != nil {
		return err
	}
	refused, held, err := measureQueryCostWalk(ctx, log, walk, adapter, func(ctx context.Context, _ string) error {
		cutoff, err := backfillQueryCapture(ctx, log, data.later, adapter)
		result.PublishedReplacementCutoff = cutoff
		return err
	})
	if err != nil {
		return fmt.Errorf("held walk: %w", err)
	}
	if refused {
		return errors.New("held opening refused at the issuing work ceiling")
	}
	result.Held, result.AcknowledgedRows = held, log.HighWater()
	walk.FirstPageRows = 0
	refused, result.Fresh, err = measureQueryCostWalk(ctx, log, walk, adapter, nil)
	if err != nil {
		return fmt.Errorf("fresh walk: %w", err)
	}
	if refused {
		return errors.New("fresh opening refused at the work ceiling")
	}
	return nil
}

func measureQueryCostWalk(ctx context.Context, log *Log, walk WireWalk, adapter QueryTraceAdapter, afterCapture AfterCapture) (bool, QueryCostWalk, error) {
	var cost QueryCostWalk
	highWater := log.HighWater()
	var rejection error
	var terminal terminalReplay
	var pages []QueryCostPage
	stats, err := VerifyWireWalk(ctx, log, walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
		var page WirePage
		var scan QueryScanObservation
		var err error
		if adapter.FetchObserved != nil {
			page, scan, err = adapter.FetchObserved(ctx, req)
		} else {
			page, err = adapter.Fetch(ctx, req)
		}
		if err != nil {
			if req.Cursor == "" && isCleanOpeningRejection(page, err) {
				rejection = err
			}
			return page, err
		}
		if page.NextCursor == "" {
			terminal.record(req, page)
		}
		pages = append(pages, QueryCostPage{
			Page: len(pages) + 1, RowLimit: req.Limit, ReturnedRows: len(page.Rows), ResponseBytes: page.BodyBytes,
			QueryScanObservation: QueryScanObservation{
				ScannedBytes: copyCostCounter(scan.ScannedBytes), ChargedWorkUnits: copyCostCounter(scan.ChargedWorkUnits),
			},
		})
		return page, nil
	}, afterCapture)
	if rejection != nil && errors.Is(err, rejection) && ctx.Err() == nil {
		return true, cost, nil
	}
	if err != nil {
		return false, cost, fmt.Errorf("ordered walk: %w", err)
	}
	if err := cost.summarize(pages, stats); err != nil {
		return false, QueryCostWalk{}, err
	}
	matches, err := log.Match(highWater, walk.Filter)
	if err != nil {
		return false, QueryCostWalk{}, err
	}
	if err := terminal.verify(ctx, adapter.Fetch, matches, stats.Rows, true); err != nil {
		return false, QueryCostWalk{}, fmt.Errorf("terminal replay: %w", err)
	}
	return false, cost, nil
}

func copyCostCounter(counter *QueryCostCounter) *QueryCostCounter {
	if counter == nil {
		return nil
	}
	copy := *counter
	return &copy
}

func (cost *QueryCostWalk) summarize(pages []QueryCostPage, stats WireStats) error {
	cost.Pages, cost.ReturnedRows, cost.ResponseBytes = stats.Pages, stats.Rows, stats.BodyBytes
	cost.PageObservations = pages
	cost.ResponseBytesSource = "accepted query wire body bytes"
	cost.Unavailable = make(map[string]string)
	for i := range pages {
		pages[i].Unavailable = make(map[string]string)
	}
	for _, metric := range []struct {
		name  string
		page  func(QueryCostPage) *QueryCostCounter
		total **QueryCostCounter
	}{
		{"scanned_bytes", func(p QueryCostPage) *QueryCostCounter { return p.ScannedBytes }, &cost.ScannedBytes},
		{"charged_work_units", func(p QueryCostPage) *QueryCostCounter { return p.ChargedWorkUnits }, &cost.ChargedWorkUnits},
	} {
		var total QueryCostCounter
		for i, page := range pages {
			counter := metric.page(page)
			if counter == nil {
				page.Unavailable[metric.name] = "adapter supplied no counter for this page"
				cost.Unavailable[metric.name] = "one or more accepted pages have no counter"
				continue
			}
			if strings.TrimSpace(counter.Source) == "" {
				return fmt.Errorf("page %d %s observation requires a source", i+1, metric.name)
			}
			if total.Source == "" {
				total.Source = counter.Source
			}
			if total.Source != counter.Source {
				return fmt.Errorf("page %d %s observation changed source", i+1, metric.name)
			}
			if counter.Value > math.MaxUint64-total.Value {
				return fmt.Errorf("page %d %s observation overflows walk total", i+1, metric.name)
			}
			total.Value += counter.Value
		}
		if cost.Unavailable[metric.name] == "" {
			*metric.total = &total
		}
	}
	if cost.ScannedBytes != nil && stats.Rows != 0 {
		ratio := float64(cost.ScannedBytes.Value) / float64(stats.Rows)
		cost.ScannedBytesPerReturnedRow = &ratio
	}
	return nil
}
