package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"bog/verify/capacityevidence"
)

const (
	compactionPollInterval = 30 * time.Millisecond
	timelineSampleInterval = 100 * time.Millisecond
	timelineSampleLimit    = 64
	liveMemorySampleLimit  = capacityevidence.MemorySampleLimit // per timeline, including attempted reads
	drainBudget            = capacityevidence.DrainBudget
	diagnosticRequestLimit = 250 * time.Millisecond
)

type filesystemSample struct {
	Elapsed      *time.Duration    `json:"observed_ns"`
	StagingBytes *int64            `json:"checkpoint_staging_apparent_bytes"`
	PinRecords   *int              `json:"published_pin_records"`
	OldestRecord *time.Duration    `json:"oldest_pin_record_mtime_age_ns"`
	Unavailable  map[string]string `json:"unavailable,omitempty"`
}

// The timeline and aggregate diagnostics use the same filesystem definitions.
// Each scan seals its own observations even if one measurement fails.
func observeStorage(dir string, now time.Time) (sample filesystemSample, err error) {
	if dir == "" {
		err = errors.New("fixture directory unavailable")
		return sample, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return sample, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	bytes, stagingErr := stagingBytes(root)
	if stagingErr == nil {
		sample.StagingBytes = &bytes
	}
	count, oldest, pinErr := pinRecords(root, now)
	if pinErr == nil {
		sample.PinRecords, sample.OldestRecord = &count, oldest
	}
	sample.Unavailable = make(map[string]string)
	if stagingErr != nil {
		sample.Unavailable["checkpoint_staging_apparent_bytes"] = diagnosticError(stagingErr)
	}
	if pinErr != nil {
		sample.Unavailable["published_pin_records"] = diagnosticError(pinErr)
		sample.Unavailable["oldest_pin_record_mtime_age_ns"] = diagnosticError(pinErr)
	} else if oldest == nil {
		sample.Unavailable["oldest_pin_record_mtime_age_ns"] = "no published pin records"
	}
	return sample, errors.Join(stagingErr, pinErr)
}

type phaseSample struct {
	Event       string               `json:"event"`
	Elapsed     time.Duration        `json:"elapsed_ns"`
	RequestSent *time.Duration       `json:"request_sent_ns"`
	Status      *compaction          `json:"status"`
	StatusError string               `json:"status_error,omitempty"`
	Candidate   runtimeSample        `json:"candidate_status_metrics"`
	Filesystem  filesystemSample     `json:"filesystem"`
	Memory      *memoryCurrentSample `json:"cgroup_memory_current,omitempty"`
}

func (s phaseSample) replaceable() bool {
	switch s.Event {
	case "status_poll":
		return s.Memory == nil
	case "drain_poll":
		return s.Candidate.RetiredPins == nil || *s.Candidate.RetiredPins > 0
	case "reclamation_poll":
		return s.Candidate.ReclaimedBytes == nil
	default:
		return false
	}
}

type memoryCurrentSample struct {
	ReadStart time.Duration `json:"read_start_ns"`
	ReadEnd   time.Duration `json:"read_end_ns"`
	Bytes     *uint64       `json:"bytes"`
	Error     string        `json:"error,omitempty"`
}

type memoryObservation struct {
	Samples int           `json:"sample_count"`
	Span    time.Duration `json:"observation_span_ns"`
	Min     uint64        `json:"min_bytes"`
	Max     uint64        `json:"max_bytes"`
}

func (t *backfillTimeline) summarizeMemory() (summary memoryObservation) {
	var first time.Duration
	for _, sample := range t.Samples {
		m := sample.Memory
		if m == nil || m.Error != "" || m.Bytes == nil {
			continue
		}
		if summary.Samples == 0 {
			first, summary.Min, summary.Max = m.ReadStart, *m.Bytes, *m.Bytes
		}
		summary.Samples++
		summary.Span = m.ReadEnd - first
		summary.Min = min(summary.Min, *m.Bytes)
		summary.Max = max(summary.Max, *m.Bytes)
	}
	return summary
}

type publicationProof struct {
	Generation uint64        `json:"generation"`
	LowerBound time.Duration `json:"observed_lag_lower_bound_ns"`
	UpperBound time.Duration `json:"observed_lag_upper_bound_ns"`
}

type pinDrainProof struct {
	Status string  `json:"status"`
	Held   *uint64 `json:"retired_pins_while_held"`
	After  *uint64 `json:"retired_pins_after_exhaustion"`
	Reason string  `json:"reason,omitempty"`
}

type backfillAcknowledgements struct {
	Rows       int           `json:"rows"`
	Batches    int           `json:"batches"`
	First      time.Duration `json:"first_ack_ns"`
	Last       time.Duration `json:"last_ack_ns"`
	EarliestTS time.Time     `json:"earliest_row_ts"`
	LatestTS   time.Time     `json:"latest_row_ts"`
}

type backfillTimeline struct {
	Acknowledged       *backfillAcknowledgements          `json:"acknowledged_backfill"`
	Pattern            string                             `json:"pattern"`
	HeldCutoff         int                                `json:"held_cutoff"`
	RequestedCutoff    int                                `json:"requested_cutoff"`
	FinalCutoff        int                                `json:"expected_committed_rows"`
	HeldRows           int                                `json:"held_rows_checked"`
	HeldExhausted      bool                               `json:"held_cursor_exhausted"`
	TerminalRetried    bool                               `json:"terminal_page_retried"`
	TerminalBytesEqual bool                               `json:"terminal_page_bytes_equal"`
	CaptureRetired     bool                               `json:"capture_retired"`
	FreshVisible       bool                               `json:"fresh_backfill_visible"`
	FreshExact         bool                               `json:"fresh_walk_exact"`
	FreshRows          int                                `json:"fresh_rows_checked"`
	Publication        *publicationProof                  `json:"publication"`
	PinDrain           pinDrainProof                      `json:"pin_drain"`
	Correlation        *capacityevidence.CycleCorrelation `json:"capacity_correlation,omitempty"`
	Memory             *memoryObservation                 `json:"live_memory_current,omitempty"`
	Samples            []phaseSample                      `json:"samples"`
	Dropped            int                                `json:"dropped_samples"`
	Status             string                             `json:"status"`
	Failure            string                             `json:"failure,omitempty"`
}

func newBackfillTimelines(f fixture) []*backfillTimeline {
	return []*backfillTimeline{
		newBackfillTimeline("clustered", f.SeedRows-2*f.BatchRows, f.SeedRows-f.BatchRows, f.SeedRows),
		newBackfillTimeline("scattered", f.SeedRows, f.SeedRows+f.BatchRows, f.SeedRows+f.TailRows),
	}
}

func newBackfillTimeline(pattern string, held, cutoff, final int) *backfillTimeline {
	return &backfillTimeline{
		Pattern: pattern, HeldCutoff: held, RequestedCutoff: cutoff, FinalCutoff: final,
		Status: "not_started", Samples: []phaseSample{},
		PinDrain: pinDrainProof{Status: "unavailable", Reason: "phase did not reach explicit capture retirement"},
	}
}

func (t *backfillTimeline) add(sample phaseSample) {
	if len(t.Samples) == timelineSampleLimit {
		// Keep named boundaries, memory evidence, and the initial half of
		// the timeline. Rotate the other poll samples.
		for i := timelineSampleLimit / 2; i < len(t.Samples); i++ {
			if t.Samples[i].replaceable() {
				copy(t.Samples[i:], t.Samples[i+1:])
				t.Samples = t.Samples[:len(t.Samples)-1]
				break
			}
		}
		t.Dropped++
		if len(t.Samples) == timelineSampleLimit {
			return
		}
	}
	t.Samples = append(t.Samples, sample)
}

type backfillProbe struct {
	*backfillTimeline
	service
	control, dir string
	origin       time.Time
	startSent    time.Time
	lastRunning  time.Time
	lastPoll     time.Time
	memoryReads  int
}

func (p *backfillProbe) recordAcknowledgements(f fixture, writes []ingestSample) {
	if len(writes) == 0 {
		return
	}
	rows := min(len(writes)*f.BatchRows, p.FinalCutoff-p.HeldCutoff)
	lo, hi := f.slot(p.HeldCutoff), f.slot(p.HeldCutoff)
	for ordinal := p.HeldCutoff + 1; ordinal < p.HeldCutoff+rows; ordinal++ {
		slot := f.slot(ordinal)
		lo, hi = min(lo, slot), max(hi, slot)
	}
	p.Acknowledged = &backfillAcknowledgements{
		Rows: rows, Batches: len(writes),
		First: writes[0].ack.Sub(p.origin), Last: writes[len(writes)-1].ack.Sub(p.origin),
		EarliestTS: epoch.Add(time.Duration(lo) * time.Millisecond),
		LatestTS:   epoch.Add(time.Duration(hi) * time.Millisecond),
	}
}

func (p *backfillProbe) record(event string, sent, received time.Time, c compactionReply, err error) runtimeSample {
	sample := phaseSample{Event: event, Elapsed: received.Sub(p.origin)}
	if !sent.IsZero() {
		offset := sent.Sub(p.origin)
		sample.RequestSent = &offset
	}
	if c.State != "" {
		sample.Status = &c.compaction
	}
	if err != nil {
		sample.StatusError = diagnosticError(err)
	} else if sent.IsZero() {
		err = errors.New("no candidate status requested at this boundary")
	} else if c.State == "running" {
		sample.Memory = c.memory
	}
	sample.Candidate = readRuntimeMetrics(c.Metrics, err)
	var storageErr error
	storageStart := time.Now()
	sample.Filesystem, storageErr = observeStorage(p.dir, storageStart)
	elapsed := storageStart.Sub(p.origin)
	sample.Filesystem.Elapsed = &elapsed
	if storageErr != nil {
		if sample.Filesystem.Unavailable == nil {
			sample.Filesystem.Unavailable = make(map[string]string)
		}
		sample.Filesystem.Unavailable["sample_error"] = diagnosticError(storageErr)
	}
	p.add(sample)
	return sample.Candidate
}

func (p *backfillProbe) boundary(event string) time.Time {
	observed := time.Now()
	p.record(event, time.Time{}, observed, compactionReply{}, nil)
	return observed
}

func (p *backfillProbe) request(ctx context.Context, action string, forceSample bool) (compactionReply, time.Time, time.Time, error) {
	if err := ctx.Err(); err != nil {
		now := time.Now()
		return compactionReply{}, now, now, err
	}
	var memory *memoryCurrentSample
	if p.readMemoryCurrent != nil && action == "status" && !p.startSent.IsZero() && p.Publication == nil &&
		p.memoryReads < liveMemorySampleLimit && (forceSample || time.Since(p.lastPoll) >= timelineSampleInterval) {
		// A running start reply has already been processed. Read before the
		// next status request so a running reply brackets the whole read.
		p.memoryReads++
		start := time.Now()
		value, err := p.readMemoryCurrent(ctx)
		memory = &memoryCurrentSample{ReadStart: start.Sub(p.origin), ReadEnd: time.Since(p.origin)}
		if err != nil {
			memory.Error = diagnosticError(err)
		} else {
			memory.Bytes = &value
		}
	}
	sent := time.Now()
	c, err := p.compact(ctx, p.control, action, p.RequestedCutoff)
	c.memory = memory
	return c, sent, time.Now(), err
}

func (p *backfillProbe) pollPublication(ctx context.Context, previous uint64, afterWrites bool) (compaction, error) {
	c, sent, received, err := p.request(ctx, "status", false)
	event := "status_poll"
	if err == nil {
		switch {
		case c.State == "complete":
			err = c.validatePublication(p.RequestedCutoff, previous)
			if err == nil && p.Publication != nil && c.Generation != p.Publication.Generation {
				err = fmt.Errorf("compaction generation changed after publication: %+v", c.compaction)
			}
			if err == nil && (c.CommittedRows > p.FinalCutoff || (afterWrites && c.CommittedRows != p.FinalCutoff)) {
				err = fmt.Errorf("unexpected uncovered tail: %+v", c.compaction)
			}
			switch {
			case p.Publication == nil:
				event = "publication"
			case afterWrites:
				event = "final_cutoff"
			}
		case p.Publication != nil:
			err = fmt.Errorf("compaction returned to running after publication: %+v", c.compaction)
		default:
			committed := min(p.RequestedCutoff+(p.RequestedCutoff-p.HeldCutoff), p.FinalCutoff)
			if afterWrites {
				committed = p.FinalCutoff
			}
			err = p.validateRunning(c.compaction, previous, committed)
		}
	}
	if err == nil && c.State == "running" {
		p.lastRunning = sent
	}
	if event != "status_poll" || err != nil || c.memory != nil || received.Sub(p.lastPoll) >= timelineSampleInterval {
		metrics := p.record(event, sent, received, c, err)
		p.lastPoll = received
		if event == "publication" && err == nil {
			p.Publication = &publicationProof{c.Generation, p.lastRunning.Sub(p.startSent), received.Sub(p.startSent)}
		}
		if err == nil && p.Publication != nil && metrics.RetiredPins != nil && *metrics.RetiredPins > 0 {
			p.PinDrain.Held = metrics.RetiredPins
		}
	}
	return c.compaction, err
}

func (p *backfillProbe) waitPublication(ctx context.Context, previous uint64) (compaction, error) {
	ticker := time.NewTicker(compactionPollInterval)
	defer ticker.Stop()
	for {
		c, err := p.pollPublication(ctx, previous, true)
		if err != nil || c.State == "complete" {
			return c, err
		}
		select {
		case <-ctx.Done():
			return c, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (t *backfillTimeline) validateRunning(c compaction, previous uint64, committed int) error {
	if c.State != "running" || c.Generation != previous || c.CoveredRows != t.HeldCutoff ||
		c.CommittedRows < committed || c.CommittedRows > t.FinalCutoff {
		return fmt.Errorf("unexpected running compaction state: %+v", c)
	}
	return nil
}

func (p *backfillProbe) observePublishedPins(ctx context.Context, event string) (runtimeSample, time.Time, error) {
	c, sent, received, err := p.request(ctx, "status", false)
	if err == nil && (c.State != "complete" || c.Generation != p.Publication.Generation ||
		c.CoveredRows != p.RequestedCutoff || c.CommittedRows != p.FinalCutoff) {
		err = fmt.Errorf("published state changed during pin observation: %+v", c.compaction)
	}
	metrics := p.record(event, sent, received, c, err)
	return metrics, received, err
}

func (p *backfillProbe) observeHeldPin(ctx context.Context) error {
	metrics, received, err := p.observePublishedPins(ctx, "before_retirement")
	count := metrics.RetiredPins
	p.PinDrain.Held = count
	switch {
	case err != nil:
		p.PinDrain.Reason = "held pin status unavailable: " + diagnosticError(err)
		return err
	case count == nil || *count == 0:
		p.PinDrain.Reason = "candidate did not expose a positive predecessor pin count while the capture was held"
		return errors.New(p.PinDrain.Reason)
	default:
		p.PinDrain.Status, p.PinDrain.Reason = "pending", "capture retirement and drain have yet to be observed"
		return p.observeHeldCapacity(metrics, received)
	}
}

func (p *backfillProbe) observeDrain(ctx context.Context, retiredAt time.Time, limit time.Duration) error {
	if p.PinDrain.Held == nil || *p.PinDrain.Held == 0 {
		p.PinDrain.Reason = "candidate did not expose a positive predecessor pin count while the capture was held"
		return errors.New(p.PinDrain.Reason)
	}
	p.PinDrain.Status, p.PinDrain.Reason = "pending", "no pin status observed within the drain budget"
	deadline := retiredAt.Add(limit)
	budget, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	ticker := time.NewTicker(timelineSampleInterval)
	defer ticker.Stop()
poll:
	for event := "after_retirement"; budget.Err() == nil; event = "drain_poll" {
		request, stop := context.WithTimeout(budget, diagnosticRequestLimit)
		metrics, received, err := p.observePublishedPins(request, event)
		stop()
		count := metrics.RetiredPins
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil && !errors.Is(err, context.DeadlineExceeded):
			p.PinDrain.Reason = "drain status unavailable: " + diagnosticError(err)
			return err
		case received.After(deadline):
			// Use the recorded reply time. The filesystem scan runs afterward.
			p.PinDrain.Reason = "pin observation exceeded the drain budget"
			break poll
		case err != nil:
			p.PinDrain.Reason = "drain status unavailable: " + diagnosticError(err)
		case count == nil:
			p.PinDrain.Reason = "drain status did not include a valid retired_generation_pins count"
		case *count == 0:
			p.PinDrain.After = count
			p.PinDrain.Status, p.PinDrain.Reason = "observed", ""
			return p.observeReclamation(ctx, metrics, received)
		default:
			p.PinDrain.After = count
			p.PinDrain.Reason = "predecessor pins still present at the last drain sample"
		}
		select {
		case <-budget.Done():
			break poll
		case <-ticker.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("predecessor pin drain unconfirmed after %s: %s", limit, p.PinDrain.Reason)
}
