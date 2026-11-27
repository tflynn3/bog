package querymodel

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"
)

// QueryHoldPoint identifies a server-side barrier. A held operation occupies
// its query admission slot until the handler and transport have both drained.
type QueryHoldPoint uint8

const (
	PreparedDelivery QueryHoldPoint = iota + 1
	ScanBufferReady
)

// ConcurrentQueryAdapter extends the replacement fixture with deterministic
// holds and resource observations. Hold calls its callback once, at the
// specified barrier. A true decision cancels the request and requires
// server-observed cancellation and no accepted response. In either case Hold
// joins the request and handler before returning, including on callback error.
type ConcurrentQueryAdapter struct {
	GenerationReplacementAdapter
	Hold    func(context.Context, WireRequest, QueryHoldPoint, func() (cancel bool, err error)) (WirePage, error)
	Observe func(context.Context) (QueryDemand, []QueryCaptureLifetime, error)
}

// QueryDemand accounts for prepared NDJSON at the delivery barrier, positioned
// row buffer capacity at the source barrier, and source capture ownership.
// All times are monotonic elapsed durations encoded as nanoseconds.
type QueryDemand struct {
	PreparedBytes       int64         `json:"prepared_bytes"`
	ScanBufferBytes     int64         `json:"scan_buffer_bytes"`
	PreparedResponses   int           `json:"prepared_responses"`
	ScanBuffers         int           `json:"scan_buffers"`
	LiveCaptures        int           `json:"live_captures"`
	PeakPreparedBytes   int64         `json:"peak_prepared_bytes"`
	PeakScanBufferBytes int64         `json:"peak_scan_buffer_bytes"`
	PeakCaptures        int           `json:"peak_captures"`
	ReleasedCaptures    int           `json:"released_captures"`
	MaxCaptureLifetime  time.Duration `json:"max_capture_lifetime_ns"`
}

// QueryDemandSample pairs demand with the independently observed count of
// predecessor pins at a named barrier or after a joined request.
type QueryDemandSample struct {
	Point       string      `json:"point"`
	Demand      QueryDemand `json:"demand"`
	RetiredPins uint64      `json:"retired_pins"`
}

// QueryCaptureLifetime identifies a capture by its registration order, without
// exposing source IDs or cursor tokens. Durations end at source Release.
type QueryCaptureLifetime struct {
	Acquisition int           `json:"acquisition"`
	Duration    time.Duration `json:"duration_ns"`
}

type ConcurrentQueryReport struct {
	WorkBudget          int                      `json:"work_budget"`
	Limits              WireLimits               `json:"wire_limits"`
	PreparedObservation string                   `json:"prepared_observation"`
	ScanObservation     string                   `json:"scan_observation"`
	CaptureObservation  string                   `json:"capture_observation"`
	Initial             ReplacementGeneration    `json:"initial"`
	Replacement         ReplacementGeneration    `json:"replacement"`
	Held                [2]WireStats             `json:"held_dense_sparse"`
	Fresh               [2]WireStats             `json:"fresh_dense_sparse"`
	Samples             []QueryDemandSample      `json:"samples"`
	CaptureLifetimes    []QueryCaptureLifetime   `json:"capture_lifetimes"`
	ReleaseAfterCancel  QueryCancellationRelease `json:"release_after_cancel"`
	ReleaseAfterDrain   time.Duration            `json:"release_after_drain_ns"`
	ByteIdenticalRetry  [2]bool                  `json:"byte_identical_retry_dense_sparse"`
	AdmissionRefused    bool                     `json:"admission_refused_at_four_holds"`
}

// QueryCancellationRelease times from just before signaling transport
// cancellation through a joined handler and a confirmed resource drop.
type QueryCancellationRelease struct {
	SparseScan    time.Duration `json:"sparse_scan_ns"`
	DenseScan     time.Duration `json:"dense_scan_ns"`
	DenseDelivery time.Duration `json:"dense_delivery_ns"`
}

// ConcurrentQuerySeed supplies the precursor checkpoint and a nonempty
// committed suffix. The opening filters are dense (unfiltered) and sparse.
func ConcurrentQuerySeed(epoch time.Time) GenerationReplacementSeed {
	tie := epoch.Add(time.Millisecond)
	duplicate := Row{Time: tie, Line: "match duplicate"}
	seed := GenerationReplacementSeed{
		Checkpoint: []Row{duplicate, {Time: epoch, Line: "match opening"}, duplicate},
		Tail: []Row{
			{Time: tie.In(fixtureOffsets[2]), Line: "match later tie"},
			{Time: epoch.Add(18 * time.Millisecond), Line: "match late"},
		},
	}
	for i := 16; i >= 1; i-- {
		seed.Checkpoint = append(seed.Checkpoint, Row{Time: epoch.Add(time.Duration(i) * time.Millisecond), Line: fmt.Sprintf("noise %02d", i)})
	}
	return seed
}

type concurrentWalk struct {
	walk     WireWalk
	opening  string
	matches  []Entry
	stats    WireStats
	terminal terminalReplay
}

func (w *concurrentWalk) read(ctx context.Context, log *Log, fetch FetchWirePage) error {
	*w = concurrentWalk{walk: w.walk}
	var err error
	w.matches, err = log.Match(log.HighWater(), w.walk.Filter)
	if err != nil {
		return err
	}
	w.stats, err = VerifyWireWalk(ctx, log, w.walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
		page, err := fetch(ctx, req)
		if err == nil && req.Cursor != "" && page.NextCursor == "" {
			w.terminal.record(req, page)
		}
		return page, err
	}, func(_ context.Context, cursor string) error {
		w.opening = cursor
		return nil
	})
	if err == nil && (w.terminal.req.Cursor == "" || w.terminal.body == nil) {
		err = errors.New("missing terminal continuation byte witness")
	}
	return err
}

func (w *concurrentWalk) replay(ctx context.Context, fetch FetchWirePage) error {
	return w.terminal.verify(ctx, fetch, w.matches, w.stats.Rows, true)
}

// VerifyConcurrentQueryDemand runs against a fresh retained fixture initialized
// with ConcurrentQuerySeed. It holds two prepared resumes and two opening
// scans simultaneously across a backfilled replacement, then drains them in
// a prescribed order. Configure the adapter with cfg.WorkBudget and four
// query slots before calling. A single deadline bounds the workload.
func VerifyConcurrentQueryDemand(ctx context.Context, cfg GenerationReplacementConfig, f ConcurrentQueryAdapter) (ConcurrentQueryReport, error) {
	report := ConcurrentQueryReport{
		WorkBudget:          cfg.WorkBudget,
		Limits:              cfg.Limits,
		PreparedObservation: "encoded NDJSON Content-Length at HTTP 200 WriteHeader entry, through handler return",
		ScanObservation:     "positioned row backing array capacity after the store scan, through PageSource return",
		CaptureObservation:  "successful source capture registration through source Release, including retired captures with admitted users",
		Initial:             f.Initial,
	}
	if f.Acknowledge == nil || f.Fetch == nil || f.Activate == nil || f.Refuse == nil || f.Retire == nil ||
		f.RetiredPins == nil || f.Hold == nil || f.Observe == nil || cfg.Epoch.IsZero() || cfg.WorkBudget <= 0 {
		return report, errors.New("concurrent query requires a complete retained fixture, an epoch and a positive work budget")
	}
	walk := WireWalk{Filter: Filter{Start: cfg.Epoch, End: cfg.Epoch.Add(time.Second)}, RowLimits: []int{1, 4}, Limits: cfg.Limits}
	if err := walk.validate(); err != nil {
		return report, err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Limits.Timeout)
	defer cancel()
	seed := ConcurrentQuerySeed(cfg.Epoch)
	var log Log
	covered := log.Append(seed.Checkpoint)
	if err := checkReplacementPoint(f.Initial, covered, log.Append(seed.Tail)); err != nil {
		return report, err
	}
	trial := concurrentQueryTrial{f: f, report: &report, log: log}
	trial.walks[0].walk = walk
	walk.Filter.Contains = "match"
	trial.walks[1].walk = walk
	if err := trial.run(ctx); err != nil {
		return report, err
	}
	return report, ctx.Err()
}

type concurrentQueryTrial struct {
	f      ConcurrentQueryAdapter
	report *ConcurrentQueryReport
	log    Log
	walks  [2]concurrentWalk
}

// hold verifies the callback contract before counting any drain as evidence.
func (t *concurrentQueryTrial) hold(ctx context.Context, req WireRequest, point QueryHoldPoint, callback func() (bool, error)) (WirePage, error) {
	var called, cancelled bool
	var callbackErr error
	page, err := t.f.Hold(ctx, req, point, func() (bool, error) {
		if called {
			callbackErr = errors.Join(callbackErr, errors.New("delivery or scan barrier called back more than once"))
			return false, callbackErr
		}
		called = true
		cancelled, callbackErr = callback()
		return cancelled, callbackErr
	})
	if !called && err == nil {
		err = errors.New("delivery or scan barrier was not reached")
	}
	if cancelled && (page.EncodedBody != nil || page.NextCursor != "" || page.BodyBytes != 0 || len(page.Rows) != 0) {
		err = errors.Join(err, errors.New("cancelled request returned an accepted page"))
	}
	if callbackErr != nil && !errors.Is(err, callbackErr) {
		err = errors.Join(err, callbackErr)
	}
	return page, err
}

func (t *concurrentQueryTrial) sample(ctx context.Context, point string, prepared, scans, captures, released int, pins uint64) error {
	d, lifetimes, err := t.f.Observe(ctx)
	if err != nil {
		return fmt.Errorf("%s demand: %w", point, err)
	}
	retired, err := t.f.RetiredPins(ctx)
	if err != nil {
		return fmt.Errorf("%s retired pins: %w", point, err)
	}
	t.report.Samples = append(t.report.Samples, QueryDemandSample{point, d, retired})
	t.report.CaptureLifetimes = slices.Clone(lifetimes)
	var preparedBytes int64
	if prepared > 0 {
		preparedBytes = t.walks[1].terminal.bodyBytes
	}
	if prepared == 2 {
		preparedBytes += t.walks[0].terminal.bodyBytes
	}
	if d.PreparedResponses != prepared || d.ScanBuffers != scans || d.LiveCaptures != captures ||
		d.ReleasedCaptures != released || len(lifetimes) != released || retired != pins ||
		d.PreparedBytes != preparedBytes ||
		d.ScanBufferBytes < int64(scans) || (scans == 0 && d.ScanBufferBytes != 0) ||
		d.PeakPreparedBytes < d.PreparedBytes || d.PeakScanBufferBytes < d.ScanBufferBytes ||
		d.PeakCaptures < captures || (released > 0 && d.MaxCaptureLifetime <= 0) {
		return fmt.Errorf("%s: demand %+v and %d retired pins, want prepared=%d scans=%d live=%d released=%d retired=%d",
			point, d, retired, prepared, scans, captures, released, pins)
	}
	return ctx.Err()
}

func (t *concurrentQueryTrial) run(ctx context.Context) error {
	if err := t.sample(ctx, "initial", 0, 0, 0, 0, 0); err != nil {
		return err
	}
	for i := range t.walks {
		if err := t.walks[i].read(ctx, &t.log, t.f.Fetch); err != nil {
			return fmt.Errorf("predecessor walk %d: %w", i, err)
		}
		t.report.Held[i] = t.walks[i].stats
	}

	// Keep sparse delivery last so it witnesses final drain after the
	// cancelled dense delivery and both cancelled opening scans.
	var drain time.Time
	sparse := &t.walks[1]
	page, err := t.hold(ctx, sparse.terminal.req, PreparedDelivery, func() (bool, error) {
		err := t.cancelDense(ctx)
		drain = time.Now()
		return false, err
	})
	if err != nil {
		return redactWireFailure(fmt.Errorf("hold sparse delivery: %w", err), sparse.terminal.req.Cursor, page.NextCursor)
	}
	if err := t.sample(ctx, "after_final_drain", 0, 0, 0, 4, 0); err != nil {
		return err
	}
	t.report.ReleaseAfterDrain = time.Since(drain)
	if err := sparse.replay(ctx, func(context.Context, WireRequest) (WirePage, error) { return page, nil }); err != nil {
		return fmt.Errorf("admitted sparse delivery: %w", err)
	}
	for i := range t.walks {
		w := &t.walks[i]
		if err := w.read(ctx, &t.log, t.f.Fetch); err != nil {
			return fmt.Errorf("successor walk %d: %w", i, err)
		}
		t.report.Fresh[i] = w.stats
		if err := t.f.Retire(ctx, w.walk.Filter, w.opening); err != nil {
			return redactWireFailure(err, w.opening)
		}
	}
	return t.sample(ctx, "after_fresh_retirement", 0, 0, 0, 6, 0)
}

func (t *concurrentQueryTrial) cancelDense(ctx context.Context) error {
	dense := &t.walks[0]
	var cancelled time.Time
	_, err := t.hold(ctx, dense.terminal.req, PreparedDelivery, func() (bool, error) {
		if err := t.cancelScans(ctx, 0); err != nil {
			return false, err
		}
		for _, w := range &t.walks {
			req := w.terminal.req
			if err := t.f.Retire(ctx, req.Filter, req.Cursor); err != nil {
				return false, redactWireFailure(err, req.Cursor)
			}
			if err := t.f.Refuse(ctx, req, http.StatusGone, expiredCaptureBody); err != nil {
				return false, redactWireFailure(err, req.Cursor)
			}
		}
		if err := t.sample(ctx, "retirement_with_two_deliveries", 2, 0, 2, 2, 2); err != nil {
			return false, err
		}
		cancelled = time.Now()
		return true, nil
	})
	if err != nil {
		return redactWireFailure(fmt.Errorf("hold dense delivery: %w", err), dense.terminal.req.Cursor)
	}
	if err := t.sample(ctx, "after_dense_delivery_cancel", 1, 0, 1, 3, 1); err != nil {
		return err
	}
	t.report.ReleaseAfterCancel.DenseDelivery = time.Since(cancelled)
	return nil
}

func (t *concurrentQueryTrial) cancelScans(ctx context.Context, i int) error {
	open := WireRequest{Filter: t.walks[i].walk.Filter, Limit: 12, MaxPageBytes: t.walks[i].walk.Limits.MaxPageBytes}
	var cancelled time.Time
	_, err := t.hold(ctx, open, ScanBufferReady, func() (bool, error) {
		var err error
		if i == 0 {
			err = t.cancelScans(ctx, 1)
		} else {
			err = t.replace(ctx)
		}
		if err != nil {
			return false, err
		}
		cancelled = time.Now()
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("hold scan %d: %w", i, err)
	}
	if i == 1 {
		if err := t.sample(ctx, "after_sparse_scan_cancel", 2, 1, 3, 1, 3); err != nil {
			return err
		}
		t.report.ReleaseAfterCancel.SparseScan = time.Since(cancelled)
		return nil
	}
	if err := t.sample(ctx, "after_dense_scan_cancel", 2, 0, 2, 2, 2); err != nil {
		return err
	}
	t.report.ReleaseAfterCancel.DenseScan = time.Since(cancelled)
	for j := range t.walks {
		if err := t.walks[j].replay(ctx, t.f.Fetch); err != nil {
			return fmt.Errorf("terminal retry %d after replacement: %w", j, err)
		}
		t.report.ByteIdenticalRetry[j] = true
	}
	return nil
}

func (t *concurrentQueryTrial) replace(ctx context.Context) error {
	if err := t.sample(ctx, "overlapping_preparation_and_scans", 2, 2, 4, 0, 0); err != nil {
		return err
	}
	walk := t.walks[0].walk
	probe := WireRequest{Filter: walk.Filter, Limit: 1, MaxPageBytes: walk.Limits.MaxPageBytes}
	if err := t.f.Refuse(ctx, probe, http.StatusServiceUnavailable, captureCapacityBody); err != nil {
		return fmt.Errorf("query admission at four occupied slots: %w", err)
	}
	t.report.AdmissionRefused = true
	start := walk.Filter.Start
	batch := []Row{{Time: start, Line: "match backfill"}, {Time: start.Add(time.Millisecond), Line: "match appended tie"}}
	if err := t.f.Acknowledge(ctx, batch); err != nil {
		return fmt.Errorf("acknowledge backfill: %w", err)
	}
	cutoff := t.log.Append(batch)
	batch = []Row{{Time: start.Add(2 * time.Millisecond), Line: "match carried suffix"}}
	if err := t.f.Acknowledge(ctx, batch); err != nil {
		return fmt.Errorf("acknowledge carried suffix: %w", err)
	}
	committed := t.log.Append(batch)
	g, err := t.f.Activate(ctx, cutoff)
	if err != nil {
		return fmt.Errorf("activate cutoff %d: %w", cutoff, err)
	}
	if err := checkReplacementPoint(g, cutoff, committed); err != nil {
		return err
	}
	if g.Generation <= t.report.Initial.Generation {
		return errors.New("replacement generation did not advance")
	}
	t.report.Replacement = g
	return t.sample(ctx, "after_replacement_with_four_holds", 2, 2, 4, 0, 4)
}
