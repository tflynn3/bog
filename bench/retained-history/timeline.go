package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	compactionPollInterval = 30 * time.Millisecond
	timelineSampleInterval = 100 * time.Millisecond
	timelineSampleLimit    = 64
	drainBudget            = 2 * time.Second
	diagnosticRequestLimit = 250 * time.Millisecond
)

type runtimeSample struct {
	PublicationLag *uint64           `json:"publication_lag_ns"`
	Reserved       *uint64           `json:"staging_reserved_bytes"`
	Used           *uint64           `json:"staging_used_bytes"`
	OldestPin      *uint64           `json:"oldest_pin_age_ns"`
	Reclaimable    *uint64           `json:"reclaimable_bytes"`
	RetiredPins    *uint64           `json:"retired_generation_pins"`
	Unavailable    map[string]string `json:"unavailable,omitempty"`
}

func readRuntimeMetrics(raw json.RawMessage, statusErr error) runtimeSample {
	var m runtimeSample
	var fields map[string]json.RawMessage
	var absent string
	switch {
	case statusErr != nil:
		absent = "compaction status unavailable: " + diagnosticError(statusErr)
	case len(raw) == 0 || string(raw) == "null":
		absent = "candidate did not expose metrics"
	case json.Unmarshal(raw, &fields) != nil:
		absent = "candidate metrics must be a JSON object"
	}
	for _, field := range []struct {
		name string
		dest **uint64
	}{
		{"publication_lag_ns", &m.PublicationLag},
		{"staging_reserved_bytes", &m.Reserved},
		{"staging_used_bytes", &m.Used},
		{"oldest_pin_age_ns", &m.OldestPin},
		{"reclaimable_bytes", &m.Reclaimable},
		{"retired_generation_pins", &m.RetiredPins},
	} {
		reason := absent
		if reason == "" {
			if err := json.Unmarshal(fields[field.name], field.dest); err != nil || *field.dest == nil {
				*field.dest = nil
				reason = "candidate did not supply a nonnegative integer"
			}
		}
		if reason != "" {
			if m.Unavailable == nil {
				m.Unavailable = make(map[string]string)
			}
			m.Unavailable[field.name] = reason
		}
	}
	return m
}

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
	Event       string           `json:"event"`
	Elapsed     time.Duration    `json:"elapsed_ns"`
	RequestSent *time.Duration   `json:"request_sent_ns"`
	Status      *compaction      `json:"status"`
	StatusError string           `json:"status_error,omitempty"`
	Candidate   runtimeSample    `json:"candidate_status_metrics"`
	Filesystem  filesystemSample `json:"filesystem"`
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
	Acknowledged    *backfillAcknowledgements `json:"acknowledged_backfill"`
	Pattern         string                    `json:"pattern"`
	HeldCutoff      int                       `json:"held_cutoff"`
	RequestedCutoff int                       `json:"requested_cutoff"`
	FinalCutoff     int                       `json:"expected_committed_rows"`
	HeldRows        int                       `json:"held_rows_checked"`
	HeldExhausted   bool                      `json:"held_cursor_exhausted"`
	FreshVisible    bool                      `json:"fresh_backfill_visible"`
	Publication     *publicationProof         `json:"publication"`
	PinDrain        pinDrainProof             `json:"pin_drain"`
	Samples         []phaseSample             `json:"samples"`
	Dropped         int                       `json:"dropped_samples"`
	Status          string                    `json:"status"`
	Failure         string                    `json:"failure,omitempty"`
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
		PinDrain: pinDrainProof{Status: "unavailable", Reason: "phase did not reach held cursor exhaustion"},
	}
}

func (t *backfillTimeline) add(sample phaseSample) {
	if len(t.Samples) == timelineSampleLimit {
		// There are only a fixed number of named boundary samples. Retain
		// them and the initial half of the timeline, then rotate poll samples.
		for i := timelineSampleLimit / 2; i < len(t.Samples); i++ {
			if t.Samples[i].Event == "status_poll" || t.Samples[i].Event == "drain_poll" {
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

func (p *backfillProbe) boundary(event string) {
	p.record(event, time.Time{}, time.Now(), compactionReply{}, nil)
}

func (p *backfillProbe) request(ctx context.Context, action string) (compactionReply, time.Time, time.Time, error) {
	sent := time.Now()
	c, err := p.compact(ctx, p.control, action, p.RequestedCutoff)
	return c, sent, time.Now(), err
}

func (p *backfillProbe) pollPublication(ctx context.Context, previous uint64, afterWrites bool) (compaction, error) {
	c, sent, received, err := p.request(ctx, "status")
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
	if event != "status_poll" || err != nil || received.Sub(p.lastPoll) >= timelineSampleInterval {
		metrics := p.record(event, sent, received, c, err)
		p.lastPoll = received
		if event == "publication" && err == nil {
			p.Publication = &publicationProof{c.Generation, p.lastRunning.Sub(p.startSent), received.Sub(p.startSent)}
		}
		if err == nil && p.Publication != nil && metrics.RetiredPins != nil &&
			(p.PinDrain.Held == nil || *p.PinDrain.Held == 0) {
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

func (p *backfillProbe) validateRunning(c compaction, previous uint64, committed int) error {
	if c.State != "running" || c.Generation != previous || c.CoveredRows != p.HeldCutoff ||
		c.CommittedRows < committed || c.CommittedRows > p.FinalCutoff {
		return fmt.Errorf("unexpected running compaction state: %+v", c)
	}
	return nil
}

func (p *backfillProbe) observeDrain(ctx context.Context, limit time.Duration) error {
	p.PinDrain.Reason = "candidate did not expose a positive predecessor pin count while the cursor was held"
	positiveHeld := p.PinDrain.Held != nil && *p.PinDrain.Held > 0
	budget, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	ticker := time.NewTicker(timelineSampleInterval)
	defer ticker.Stop()
	for event := "after_exhaustion"; budget.Err() == nil; event = "drain_poll" {
		request, stop := context.WithTimeout(budget, diagnosticRequestLimit)
		c, sent, received, err := p.request(request, "status")
		stop()
		if err == nil && (c.State != "complete" || c.Generation != p.Publication.Generation ||
			c.CoveredRows != p.RequestedCutoff || c.CommittedRows != p.FinalCutoff) {
			err = fmt.Errorf("published state changed during pin drain: %+v", c.compaction)
			p.record(event, sent, received, c, err)
			return err
		}
		metrics := p.record(event, sent, received, c, err)
		if metrics.RetiredPins != nil {
			p.PinDrain.After = metrics.RetiredPins
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && budget.Err() != nil && p.PinDrain.Status == "pending" {
				break
			}
			p.PinDrain.Reason = "drain status unavailable: " + diagnosticError(err)
			if errors.Is(err, context.DeadlineExceeded) && p.PinDrain.Status != "pending" {
				return nil
			}
			return err
		}
		switch {
		case metrics.RetiredPins == nil:
			// A gap in instrumentation cannot clear the last positive sample.
			if p.PinDrain.Status != "pending" {
				p.PinDrain.Reason = metrics.Unavailable["retired_generation_pins"]
			}
			if !positiveHeld {
				return nil
			}
		case !positiveHeld:
			return nil
		case *metrics.RetiredPins == 0:
			p.PinDrain.Status, p.PinDrain.Reason = "observed", ""
			return nil
		default:
			p.PinDrain.Status = "pending"
			p.PinDrain.Reason = "predecessor pins still present at the last available drain sample"
		}
		select {
		case <-budget.Done():
		case <-ticker.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.PinDrain.Status == "pending" {
		return fmt.Errorf("predecessor pins did not drain within %s: %w", limit, budget.Err())
	}
	return nil
}
