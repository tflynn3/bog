package querymodel

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// QueryTraceAdapter binds an initially empty, exclusively owned endpoint to a
// trace. Acknowledge and Fetch use the existing wire contracts. A retained
// fixture supplies Publish, which must observe activation at the exact
// acknowledged cutoff before returning. The runner invokes it after the seed
// and again with the opening cursor held, after the first backfill.
type QueryTraceAdapter struct {
	Acknowledge AcknowledgeBatch
	Fetch       FetchWirePage
	Publish     func(ctx context.Context, cutoff int) error

	// Lifecycle is optional. If supplied, the runner exercises the complete
	// lifecycle at the configured maximum work budget.
	Lifecycle *QueryTraceLifecycle
}

// QueryTraceLifecycle supplies observations from the fixture's failure and
// recovery controls. Callbacks honor the trace deadline. After Restart returns
// Fetch targets the new issuer serving the same durable state.
type QueryTraceLifecycle struct {
	// RejectAppend must verify the whole batch was refused without an
	// acknowledgement. The runner excludes the batch from its ledger.
	RejectAppend AcknowledgeBatch
	// Retire is required for retained lifecycle trials. It bars new use of
	// the held capture. Fetch then reports the clean expired response.
	Retire  func(context.Context, string) error
	Restart func(context.Context) (QueryRecovery, error)
}

// QueryRecovery reports startup observations in fixture-defined work units.
// PeakBytes is the peak memory measured by the fixture's recovery meter.
type QueryRecovery struct {
	CommittedRows int
	Work          uint64
	PeakBytes     uint64
}

// LaunchQueryTrace configures one fixed-budget trial and calls run once,
// synchronously, with its adapter. It owns setup and cleanup and propagates
// failures from both. No callback may outlive its trial.
type LaunchQueryTrace func(ctx context.Context, workBudget int, run func(QueryTraceAdapter) error) error

type QueryTraceConfig struct {
	Seed          int64
	Epoch         time.Time
	MaxWorkBudget int
	Limits        WireLimits

	// Set both bounds when a lifecycle adapter is supplied. The work bound
	// uses the same units as QueryRecovery.Work.
	MaxRecoveryWork  uint64
	MaxRecoveryBytes uint64
}

// QueryTraceResult records the two adjacent boundary observations and the
// independently verified walks. Only accepted pages contribute to the stats.
type QueryTraceResult struct {
	MinimumWorkBudget int
	RejectedBudget    int
	BoundaryHeld      WireStats
	Held              WireStats
	Fresh             WireStats
	Restarted         WireStats
	Recovery          *QueryRecovery
}

// VerifyQueryTrace runs the same seeded operations against either query path.
// Work-budget rejection is monotonic for an identical opening request on an
// empty trial seeded in the same order. Each issuing probe must complete
// its held walk and exact replay at that budget. The lifecycle trial uses
// MaxWorkBudget so a fresh capture can certify the augmented history.
func VerifyQueryTrace(ctx context.Context, cfg QueryTraceConfig, launch LaunchQueryTrace) (QueryTraceResult, error) {
	var result QueryTraceResult
	data := makeSeededWireData(cfg.Seed, cfg.Epoch)
	walk := WireWalk{
		Seed: cfg.Seed, Filter: data.filter, RowLimits: []int{4, 1, 7},
		Limits: cfg.Limits, FirstPageRows: 2,
	}
	if launch == nil || cfg.MaxWorkBudget < 2 || cfg.Limits.MaxPages < 3 ||
		cfg.Limits.MaxPageBytes != seededTrimCap {
		return result, errors.New("trace requires a launcher, a work ceiling of at least two, at least three pages and a 256-byte response cap")
	}
	if err := walk.validate(); err != nil {
		return result, err
	}
	if (cfg.MaxRecoveryWork == 0) != (cfg.MaxRecoveryBytes == 0) {
		return result, errors.New("set both recovery bounds")
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Limits.Timeout)
	defer cancel()

	probe := func(budget int, lifecycle bool) (rejected bool, held WireStats, err error) {
		if err := ctx.Err(); err != nil {
			return false, held, err
		}
		calls := 0
		var runErr error
		launchErr := launch(ctx, budget, func(adapter QueryTraceAdapter) error {
			calls++
			if calls != 1 {
				runErr = errors.New("launcher called the trial more than once")
				return runErr
			}
			rejected, held, runErr = runQueryTrace(ctx, cfg, walk, data, adapter, lifecycle, &result)
			return runErr
		})
		if calls == 0 && launchErr == nil {
			launchErr = errors.New("launcher did not run the trial")
		}
		if launchErr != nil {
			if runErr == nil || errors.Is(launchErr, runErr) {
				runErr = launchErr
			} else {
				runErr = errors.Join(runErr, launchErr)
			}
		}
		if runErr != nil {
			return false, held, fmt.Errorf("work budget %d: %w", budget, runErr)
		}
		return rejected, held, ctx.Err()
	}

	// Probe the configured ceiling first so rejection there cannot be
	// mistaken for the upper edge of a successful interval.
	rejected, held, err := probe(cfg.MaxWorkBudget, false)
	if err != nil {
		return result, err
	}
	if rejected {
		return result, fmt.Errorf("no issuing opening at work budget %d", cfg.MaxWorkBudget)
	}
	low, high := 0, cfg.MaxWorkBudget
	for high-low > 1 {
		budget := low + (high-low)/2
		rejected, stats, err := probe(budget, false)
		if err != nil {
			return result, err
		}
		if rejected {
			low = budget
		} else {
			high, held = budget, stats
		}
	}
	if low == 0 {
		return result, errors.New("minimum issuing budget has no positive adjacent rejection")
	}
	result.MinimumWorkBudget, result.RejectedBudget, result.BoundaryHeld = high, low, held
	rejected, result.Held, err = probe(cfg.MaxWorkBudget, true)
	if err == nil && rejected {
		err = errors.New("lifecycle trial rejected an opening at the previously issuing work ceiling")
	}
	return result, err
}

func runQueryTrace(ctx context.Context, cfg QueryTraceConfig, walk WireWalk, data seededWireData, adapter QueryTraceAdapter, lifecycle bool, result *QueryTraceResult) (bool, WireStats, error) {
	var held WireStats
	if adapter.Acknowledge == nil || adapter.Fetch == nil {
		return false, held, errors.New("trace requires acknowledgement and fetch adapters")
	}
	control := adapter.Lifecycle
	if (control != nil) != (cfg.MaxRecoveryWork > 0) ||
		control != nil && (control.RejectAppend == nil || control.Restart == nil || adapter.Publish != nil && control.Retire == nil) {
		return false, held, errors.New("recovery bounds and complete lifecycle controls must be supplied together")
	}
	var log Log
	acknowledge := func(ctx context.Context, batches [][]Row) error {
		for i, rows := range batches {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := adapter.Acknowledge(ctx, rows); err != nil {
				return fmt.Errorf("batch %d: %w", i+1, err)
			}
			log.Append(rows)
		}
		return ctx.Err()
	}
	publish := func(ctx context.Context) error {
		if adapter.Publish == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := adapter.Publish(ctx, log.HighWater()); err != nil {
			return fmt.Errorf("publish through ordinal %d: %w", log.HighWater(), err)
		}
		return ctx.Err()
	}
	if err := acknowledge(ctx, data.initial); err != nil {
		return false, held, fmt.Errorf("seed: %w", err)
	}
	if err := publish(ctx); err != nil {
		return false, held, fmt.Errorf("seed: %w", err)
	}
	if lifecycle && control != nil {
		forbidden := Row{Time: walk.Filter.Start, Line: sparseLiteral + " refused batch"}
		if err := control.RejectAppend(ctx, []Row{forbidden, data.later[0][0]}); err != nil {
			return false, held, fmt.Errorf("refuse append: %w", err)
		}
	}
	highWater := log.HighWater()
	var openingRejection error
	var terminal terminalReplay
	held, err := VerifyWireWalk(ctx, &log, walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
		page, err := adapter.Fetch(ctx, req)
		if req.Cursor == "" && isCleanOpeningRejection(page, err) {
			openingRejection = err
		}
		if err == nil && page.NextCursor == "" {
			terminal.record(req, page)
		}
		return page, err
	}, func(ctx context.Context, _ string) error {
		if err := acknowledge(ctx, data.later[:1]); err != nil {
			return fmt.Errorf("first backfill: %w", err)
		}
		if err := publish(ctx); err != nil {
			return fmt.Errorf("replacement: %w", err)
		}
		if err := acknowledge(ctx, data.later[1:]); err != nil {
			return fmt.Errorf("tail backfill: %w", err)
		}
		return nil
	})
	if openingRejection != nil && errors.Is(err, openingRejection) && ctx.Err() == nil {
		return true, held, nil
	}
	if err != nil {
		return false, held, fmt.Errorf("held walk: %w", err)
	}
	matches, err := log.Match(highWater, walk.Filter)
	if err != nil {
		return false, held, err
	}
	if err := terminal.verify(ctx, adapter.Fetch, matches, held.Rows, true); err != nil {
		return false, held, fmt.Errorf("held terminal replay: %w", err)
	}
	if !lifecycle {
		return false, held, nil
	}

	fresh := walk
	fresh.FirstPageRows = 0
	fresh.RowLimits = []int{1, 4, 2}
	result.Fresh, err = VerifyWireWalk(ctx, &log, fresh, adapter.Fetch, nil)
	if err != nil {
		return false, held, fmt.Errorf("fresh walk: %w", err)
	}
	if control == nil {
		return false, held, nil
	}
	if control.Retire != nil {
		if err := control.Retire(ctx, terminal.req.Cursor); err != nil {
			return false, held, redactWireFailure(fmt.Errorf("retire capture: %w", err), terminal.req.Cursor)
		}
		if err := requireUnavailableCapture(ctx, adapter.Fetch, terminal.req, http.StatusGone); err != nil {
			return false, held, fmt.Errorf("retired capture: %w", err)
		}
	}
	recovery, err := control.Restart(ctx)
	if err != nil {
		return false, held, fmt.Errorf("restart: %w", err)
	}
	result.Recovery = &recovery
	if recovery.CommittedRows != log.HighWater() || recovery.Work == 0 || recovery.Work > cfg.MaxRecoveryWork ||
		recovery.PeakBytes == 0 || recovery.PeakBytes > cfg.MaxRecoveryBytes {
		return false, held, fmt.Errorf("recovery %+v outside expected rows=%d work=1..%d peak_bytes=1..%d",
			recovery, log.HighWater(), cfg.MaxRecoveryWork, cfg.MaxRecoveryBytes)
	}
	if err := requireUnavailableCapture(ctx, adapter.Fetch, terminal.req, http.StatusBadRequest); err != nil {
		return false, held, fmt.Errorf("previous issuer: %w", err)
	}
	// Verify all in-range recovered rows, including the sparse filter's misses.
	fresh.Filter.Contains = ""
	result.Restarted, err = VerifyWireWalk(ctx, &log, fresh, adapter.Fetch, nil)
	if err != nil {
		return false, held, fmt.Errorf("restarted walk: %w", err)
	}
	return false, held, nil
}

func requireUnavailableCapture(ctx context.Context, fetch FetchWirePage, req WireRequest, status int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	page, err := fetch(ctx, req)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if observed, ok := err.(*FixedBudgetHTTPError); ok && observed != nil && observed.StatusCode == status &&
		observed.Cause == nil && len(page.Rows) == 0 && page.NextCursor == "" && page.BodyBytes == 0 && page.EncodedBody == nil {
		return nil
	}
	if err != nil {
		return redactWireFailure(fmt.Errorf("expected clean HTTP %d: %w", status, err), req.Cursor, page.NextCursor)
	}
	return fmt.Errorf("expected clean HTTP %d, got a successful page", status)
}
