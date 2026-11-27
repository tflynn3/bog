package querymodel

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"
)

// SelectiveAdmissionScenario selects a legacy pagination scan-budget probe.
type SelectiveAdmissionScenario string

const (
	SelectiveAlongsideCursor  SelectiveAdmissionScenario = "alongside-cursor"
	SelectiveScanExhaustion   SelectiveAdmissionScenario = "scan-exhaustion"
	SelectiveScanCancellation SelectiveAdmissionScenario = "scan-cancellation"
	SelectiveScanExpiry       SelectiveAdmissionScenario = "scan-expiry"
)

// SelectiveScanCapacityBody is the exact aggregate scan-byte refusal on the wire.
const SelectiveScanCapacityBody = "query scan capacity exhausted\n"

const (
	selectiveMissing   = "missing-needle"
	selectiveLiteral   = "needle"
	selectiveBatchRows = 4096
	// Even the final attempted opening leaves at least one capture slot free.
	maxSelectiveBroadAttempts = 63
)

// ErrSelectiveScanCapacity is returned by a fetch adapter only after it
// observes a complete plain-text 503 with SelectiveScanCapacityBody, no NDJSON
// and no cursor header or trailer. The accompanying page must be zero.
var ErrSelectiveScanCapacity = errors.New("clean query scan capacity refusal")

// SelectiveAdmissionConfig records the issuer settings and bounds the probe.
// ScanBytes is the configured or documented default aggregate scan budget.
type SelectiveAdmissionConfig struct {
	Epoch            time.Time
	SeedRows         int
	WorkBudget       uint64
	ScanBytes        uint64
	QueryConcurrency int
	Limits           WireLimits
	CursorLifetime   time.Duration
	ExpiryGrace      time.Duration
}

// SelectiveAdmissionAdapter runs the workload against an initially empty issuer.
type SelectiveAdmissionAdapter struct {
	Acknowledge AcknowledgeBatch
	Fetch       FetchWirePage
	Refuse      func(context.Context, WireRequest, int, string) error
	// CancelScan calls whileScanning synchronously once the opening has begun
	// allocating scan buffers. It then cancels and joins the handler, checking
	// the clean cancellation response before returning.
	CancelScan func(ctx context.Context, req WireRequest, whileScanning func() error) error
}

// SelectiveAdmissionReport records completed checks without cursor contents.
type SelectiveAdmissionReport struct {
	Scenario                  SelectiveAdmissionScenario
	Settings                  SelectiveAdmissionConfig
	AcknowledgedRows          int
	BroadOpens                int
	BroadResumes              int
	Empty, Sparse             WireStats
	SparseTerminalReplay      bool
	ScanRefusals              int
	Cancelled, Reused         bool
	LiveBeforeExpiry, Expired bool
	ExpiryRecoveryAfter       time.Duration
}

// VerifySelectiveAdmission seeds an empty issuer. The alongside trial
// overlaps selective walks and a replay of an idle broad cursor. The other
// trials exercise scan exhaustion and, when requested, release of its charge.
func VerifySelectiveAdmission(ctx context.Context, cfg SelectiveAdmissionConfig, scenario SelectiveAdmissionScenario, f SelectiveAdmissionAdapter) (SelectiveAdmissionReport, error) {
	report := SelectiveAdmissionReport{Scenario: scenario, Settings: cfg}
	broad := Filter{Start: cfg.Epoch, End: cfg.Epoch.Add(time.Second)}
	walk := WireWalk{Filter: broad, RowLimits: []int{1, 2}, Limits: cfg.Limits}
	if err := walk.validate(); err != nil {
		return report, err
	}
	if cfg.Epoch.IsZero() || cfg.SeedRows < 16 || cfg.SeedRows > 400_000 ||
		cfg.QueryConcurrency < 4 || cfg.WorkBudget == 0 || cfg.ScanBytes == 0 ||
		cfg.Limits.MaxPages < 3 || f.Acknowledge == nil || f.Fetch == nil {
		return report, errors.New("invalid selective admission fixture, issuer settings or adapter")
	}
	switch scenario {
	case SelectiveAlongsideCursor, SelectiveScanExhaustion:
	case SelectiveScanCancellation:
		if f.CancelScan == nil {
			return report, errors.New("scan cancellation requires a paused opening and joined cancellation witness")
		}
	case SelectiveScanExpiry:
		if f.Refuse == nil || cfg.ExpiryGrace <= 0 || cfg.CursorLifetime <= cfg.ExpiryGrace ||
			cfg.CursorLifetime >= cfg.Limits.Timeout || cfg.ExpiryGrace >= cfg.Limits.Timeout-cfg.CursorLifetime {
			return report, errors.New("expiry requires refusal observation and time for the full cursor lifetime and grace")
		}
	default:
		return report, fmt.Errorf("unknown selective admission scenario %q", scenario)
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Limits.Timeout)
	defer cancel()
	trial := selectiveAdmissionTrial{
		f: f, walk: walk, report: &report, seen: make(map[string]int),
		open: WireRequest{Filter: broad, Limit: 1, MaxPageBytes: cfg.Limits.MaxPageBytes},
	}
	err := trial.run(ctx)
	return report, errors.Join(err, ctx.Err())
}

func (t *selectiveAdmissionTrial) run(ctx context.Context) error {
	if err := t.seed(ctx); err != nil {
		return err
	}
	issuedAfter := time.Now()
	first, err := t.openBroad(ctx)
	if err != nil {
		return fmt.Errorf("uncontended broad opening: %w", err)
	}
	issuedBefore := time.Now()
	switch t.report.Scenario {
	case SelectiveAlongsideCursor:
		err = t.selective(ctx, first)
	case SelectiveScanCancellation:
		err = t.cancelScan(ctx, first)
	case SelectiveScanExhaustion, SelectiveScanExpiry:
		err = t.saturate(ctx, first)
		if err == nil && t.report.Scenario == SelectiveScanExpiry {
			err = t.expire(ctx, first, issuedAfter, issuedBefore)
		}
	}
	return err
}

type selectiveAdmissionTrial struct {
	f       SelectiveAdmissionAdapter
	walk    WireWalk
	report  *SelectiveAdmissionReport
	ledger  Log
	matches []Entry
	seen    map[string]int
	open    WireRequest
}

func (t *selectiveAdmissionTrial) seed(ctx context.Context) error {
	count, start := t.report.Settings.SeedRows, t.walk.Filter.Start
	tie := start.Add(2 * time.Millisecond)
	dup := Row{Time: tie, Line: "needle duplicate"}
	rows := make([]Row, count)
	for i := range rows {
		rows[i] = Row{Time: start.Add(time.Duration(i%8) * time.Millisecond), Line: "noise NEEDLE"}
	}
	rows[0], rows[8] = Row{Time: start, Line: "broad first"}, Row{Time: start, Line: "broad second"}
	rows[1], rows[count/2], rows[count-2] = dup, Row{Time: start, Line: "needle earlier"}, dup
	// Ensure the upper bound is exclusive even for a matching row.
	rows[count-1] = Row{Time: t.walk.Filter.End, Line: "needle excluded"}
	for batch := range slices.Chunk(rows, selectiveBatchRows) {
		if err := t.f.Acknowledge(ctx, batch); err != nil {
			return fmt.Errorf("acknowledge seed at ordinal %d: %w", t.ledger.HighWater(), err)
		}
		t.report.AcknowledgedRows = t.ledger.Append(batch)
	}
	var err error
	t.matches, err = t.ledger.Match(t.ledger.HighWater(), t.open.Filter)
	return err
}

func (t *selectiveAdmissionTrial) openBroad(ctx context.Context) (string, error) {
	page, err := t.f.Fetch(ctx, t.open)
	if err != nil {
		// Forward only a witnessed, empty refusal to the saturation loop.
		if err == ErrSelectiveScanCapacity && noSelectiveWirePage(page) {
			return "", err
		}
		return "", redactWireFailure(fmt.Errorf("broad opening: %w", err), page.NextCursor)
	}
	if err := checkSelectivePage(t.open, page, t.matches, 0, t.seen); err != nil {
		return "", err
	}
	t.report.BroadOpens++
	t.seen[page.NextCursor] = t.report.BroadOpens
	return page.NextCursor, nil
}

func noSelectiveWirePage(page WirePage) bool {
	return len(page.Rows) == 0 && page.NextCursor == "" && page.BodyBytes == 0 && page.EncodedBody == nil
}

func checkSelectivePage(req WireRequest, page WirePage, matches []Entry, offset int, seen map[string]int) error {
	if page.EncodedBody == nil {
		return errors.New("missing encoded page witness")
	}
	if err := checkWirePage(req, page, matches, offset, seen); err != nil {
		return err
	}
	if len(page.Rows) != 1 || page.NextCursor == "" {
		return errors.New("broad probe must return one row and a continuation")
	}
	return nil
}

func (t *selectiveAdmissionTrial) resumeBroad(ctx context.Context, cursor string) error {
	req := t.open
	req.Cursor = cursor
	page, err := t.f.Fetch(ctx, req)
	if err != nil {
		return redactWireFailure(err, cursor, page.NextCursor)
	}
	if err := checkSelectivePage(req, page, t.matches[1:], 1, t.seen); err != nil {
		return err
	}
	t.report.BroadResumes++
	return nil
}

func (t *selectiveAdmissionTrial) selective(ctx context.Context, first string) error {
	var wg sync.WaitGroup
	start := make(chan struct{})
	var emptyErr, sparseErr, broadErr error
	wg.Go(func() {
		<-start
		t.report.Empty, emptyErr = t.walkSelective(ctx, selectiveMissing, 0)
	})
	wg.Go(func() {
		<-start
		t.report.Sparse, sparseErr = t.walkSelective(ctx, selectiveLiteral, 3)
	})
	wg.Go(func() {
		<-start
		broadErr = t.resumeBroad(ctx, first)
	})
	close(start)
	wg.Wait()
	return errors.Join(emptyErr, sparseErr, broadErr)
}

func (t *selectiveAdmissionTrial) walkSelective(ctx context.Context, literal string, wantRows int) (WireStats, error) {
	walk := t.walk
	walk.Filter.Contains = literal
	matches, err := t.ledger.Match(t.ledger.HighWater(), walk.Filter)
	if err != nil {
		return WireStats{}, err
	}
	if len(matches) != wantRows {
		return WireStats{}, fmt.Errorf("fixture literal %q matched %d rows, want %d", literal, len(matches), wantRows)
	}
	var terminal terminalReplay
	stats, err := VerifyWireWalk(ctx, &t.ledger, walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
		page, err := t.f.Fetch(ctx, req)
		if err != nil {
			return page, err
		}
		if page.EncodedBody == nil {
			return page, errors.New("missing encoded page witness")
		}
		if req.Cursor != "" && page.NextCursor == "" {
			terminal.record(req, page)
		}
		return page, nil
	}, nil)
	if err != nil {
		return stats, fmt.Errorf("selective walk %q: %w", literal, err)
	}
	if wantRows == 0 {
		return stats, nil
	}
	if terminal.req.Cursor == "" {
		return stats, errors.New("sparse fixture did not exercise a terminal continuation")
	}
	if err := terminal.verify(ctx, t.f.Fetch, matches, stats.Rows, true); err != nil {
		return stats, fmt.Errorf("sparse terminal replay: %w", err)
	}
	t.report.SparseTerminalReplay = true
	return stats, nil
}

func (t *selectiveAdmissionTrial) requireScanRefusal(ctx context.Context) error {
	_, err := t.openBroad(ctx)
	switch err {
	case ErrSelectiveScanCapacity:
		t.report.ScanRefusals++
		return nil
	case nil:
		return errors.New("expected aggregate scan-byte refusal, got a successful broad opening")
	default:
		return fmt.Errorf("expected clean aggregate scan-byte refusal: %w", err)
	}
}

func (t *selectiveAdmissionTrial) saturate(ctx context.Context, first string) error {
	for t.report.BroadOpens < maxSelectiveBroadAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := t.openBroad(ctx); err != nil {
			if err != ErrSelectiveScanCapacity {
				return fmt.Errorf("opening %d before scan exhaustion: %w", t.report.BroadOpens+1, err)
			}
			t.report.ScanRefusals++
			if err := t.requireScanRefusal(ctx); err != nil {
				return fmt.Errorf("confirm exhaustion on a sequential retry: %w", err)
			}
			return t.resumeBroad(ctx, first)
		}
	}
	return errors.New("scan-byte budget did not saturate with capture slots still available")
}

func (t *selectiveAdmissionTrial) cancelScan(ctx context.Context, first string) error {
	var entered bool
	var probeErr error
	err := t.f.CancelScan(ctx, t.open, func() error {
		if entered {
			probeErr = errors.New("scan barrier invoked more than once")
			return probeErr
		}
		entered = true
		probeErr = t.probeDuringScan(ctx, first)
		return probeErr
	})
	if !entered && err == nil {
		err = errors.New("scan cancellation did not reach the allocation barrier")
	}
	if probeErr != nil && !errors.Is(err, probeErr) {
		err = errors.Join(err, probeErr)
	}
	if err != nil {
		return fmt.Errorf("cancel concurrent opening: %w", err)
	}
	t.report.Cancelled = true
	if _, err := t.openBroad(ctx); err != nil {
		return fmt.Errorf("reuse scan capacity after cancellation: %w", err)
	}
	t.report.Reused = true
	return nil
}

func (t *selectiveAdmissionTrial) probeDuringScan(ctx context.Context, first string) error {
	if err := t.requireScanRefusal(ctx); err != nil {
		return err
	}
	// One selective preparation fits beside the paused allocation.
	// Each probe overlaps that same scan without competing for setup bytes.
	var err error
	t.report.Empty, err = t.walkSelective(ctx, selectiveMissing, 0)
	if err != nil {
		return err
	}
	t.report.Sparse, err = t.walkSelective(ctx, selectiveLiteral, 3)
	if err != nil {
		return err
	}
	return t.resumeBroad(ctx, first)
}

func (t *selectiveAdmissionTrial) expire(ctx context.Context, first string, issuedAfter, issuedBefore time.Time) error {
	cfg := t.report.Settings
	if err := waitSelectiveDeadline(ctx, issuedAfter.Add(cfg.CursorLifetime-cfg.ExpiryGrace)); err != nil {
		return err
	}
	if !time.Now().Before(issuedAfter.Add(cfg.CursorLifetime)) {
		return errors.New("missed the last pre-expiry observation window")
	}
	if err := t.resumeBroad(ctx, first); err != nil {
		return fmt.Errorf("resume shortly before expiry: %w", err)
	}
	t.report.LiveBeforeExpiry = true
	if err := t.requireScanRefusal(ctx); err != nil {
		return fmt.Errorf("scan budget shortly before expiry: %w", err)
	}
	if err := waitSelectiveDeadline(ctx, issuedBefore.Add(cfg.CursorLifetime+cfg.ExpiryGrace)); err != nil {
		return err
	}
	// A stale resume may itself retire an expired capture. Require a
	// successful fresh opening first to witness release without that lookup.
	if _, err := t.openBroad(ctx); err != nil {
		return fmt.Errorf("reuse scan capacity after expiry: %w", err)
	}
	t.report.Reused = true
	t.report.ExpiryRecoveryAfter = time.Since(issuedAfter)
	req := t.open
	req.Cursor = first
	if err := t.f.Refuse(ctx, req, http.StatusGone, expiredCaptureBody); err != nil {
		return redactWireFailure(fmt.Errorf("authenticated resume after expiry: %w", err), first)
	}
	t.report.Expired = true
	return nil
}

func waitSelectiveDeadline(ctx context.Context, deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
