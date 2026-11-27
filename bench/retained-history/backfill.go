package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (f fixture) runBackfill(ctx context.Context, s service, o options, r *result, dir string, timeline *backfillTimeline, held *walk, previous uint64) (_ compaction, err error) {
	p := backfillProbe{backfillTimeline: timeline, service: s, control: o.control, dir: dir, origin: time.Now()}
	p.Status = "running"
	p.boundary("phase_start")
	defer func() {
		p.HeldRows = held.offset
		if summary := p.summarizeMemory(); summary.Samples > 0 {
			p.Memory = &summary
		}
		if err != nil {
			p.Status, p.Failure = "fail", diagnosticError(err)
			p.boundary("phase_failure")
		} else {
			p.Status = "pass"
		}
	}()
	var writes []ingestSample
	var reads []time.Duration
	defer func() { p.recordAcknowledgements(f, writes) }()
	scattered := p.Pattern == "scattered"
	if scattered {
		defer func() { r.recordMixed(writes, reads, p.lastRunning) }()
	}
	setPhase := func(phase string) {
		if !scattered {
			phase = "clustered_" + phase
		}
		r.Phase, held.phase = phase, phase
	}
	setPhase("seed_capture")
	if _, err := held.page(ctx, s, f); err != nil {
		return compaction{}, fmt.Errorf("capture cutoff %d: %w", p.HeldCutoff, err)
	}
	if held.cursor == "" {
		return compaction{}, errors.New("held capture did not issue a cursor")
	}
	p.boundary("held_capture")
	if err := f.appendRange(ctx, s, p.HeldCutoff, p.RequestedCutoff, &writes); err != nil {
		return compaction{}, err
	}
	p.boundary("cutoff_acknowledged")
	setPhase("mixed_compaction")
	c, sent, received, err := p.request(ctx, "start", false)
	p.startSent = sent
	if scattered && err == nil {
		r.Diagnostics.OpeningCompaction = &c.compaction
	}
	if err == nil {
		err = p.validateRunning(c.compaction, previous, p.RequestedCutoff)
		if err == nil && c.CommittedRows != p.RequestedCutoff {
			err = fmt.Errorf("unexpected committed rows at start: %+v", c.compaction)
		}
	}
	p.record("compaction_start", sent, received, c, err)
	if err != nil {
		return compaction{}, err
	}

	// Take the first bracket immediately. The overlap status takes a second
	// sample even if the continuation and first batch finish within one poll interval.
	if p.readMemoryCurrent != nil {
		c, sent, received, err = p.request(ctx, "status", true)
		if err == nil {
			err = p.validateRunning(c.compaction, previous, p.RequestedCutoff)
		}
		p.record("status_poll", sent, received, c, err)
		if err != nil {
			return compaction{}, fmt.Errorf("observe opening compaction: %w", err)
		}
		p.lastRunning, p.lastPoll = sent, received
	}

	setPhase("mixed_continuation")
	if err := f.overlapBackfill(ctx, &p, held, r, previous, &writes, &reads); err != nil {
		return compaction{}, err
	}
	p.boundary("backfill_acknowledged")
	setPhase("publication")
	published, err := p.waitPublication(ctx, previous)
	if scattered {
		r.Compaction = &published
	}
	if err != nil {
		return published, err
	}

	setPhase("after_publication_continuation")
	terminal, err := held.exhaust(ctx, s, f)
	if err != nil {
		return published, fmt.Errorf("resume after compaction: %w", err)
	}
	p.HeldExhausted = true
	p.boundary("held_exhausted")
	setPhase("terminal_retry")
	cursor := terminal.cursor
	terminal.phase = r.Phase
	terminal.seen = map[string]bool{cursor: true}
	if _, err := terminal.page(ctx, s, f); err != nil {
		return published, fmt.Errorf("retry terminal page: %w", err)
	}
	if terminal.cursor != "" || terminal.offset != held.offset {
		return published, errors.New("retry of terminal page returned a different page boundary")
	}
	p.TerminalRetried = true
	p.boundary("terminal_retried")
	setPhase("capture_retirement")
	if err := p.observeHeldPin(ctx); err != nil {
		return published, err
	}
	if err := s.retireCapture(ctx, o.control, cursor); err != nil {
		return published, fmt.Errorf("explicitly retire held capture: %w", err)
	}
	p.CaptureRetired = true
	p.boundary("capture_retired")
	setPhase("pin_drain")
	if err := p.observeDrain(ctx, drainBudget); err != nil {
		return published, err
	}
	setPhase("fresh_backfill")
	// The third row of the overlapping batch is distinct from the tied
	// duplicate pair and was absent from the held cutoff.
	ordinal := p.RequestedCutoff + 2
	row := f.row(ordinal)
	fresh := walk{
		filter: filter{row.TS, row.TS.Add(time.Millisecond), row.Line},
		want:   []int{ordinal}, phase: r.Phase,
	}
	if _, err := fresh.page(ctx, s, f); err != nil {
		return published, fmt.Errorf("fresh acknowledged backfill at ordinal %d: %w", ordinal, err)
	}
	p.FreshVisible = true
	p.boundary("fresh_visible")
	setPhase("fresh_walk")
	var exact *walk
	if scattered {
		exact = f.newWalk(p.FinalCutoff, false)
	} else {
		exact = f.walkFor(p.FinalCutoff, held.filter)
	}
	exact.phase, exact.pageLimit = r.Phase, 1000
	_, err = exact.exhaust(ctx, s, f)
	p.FreshRows = exact.offset
	if err != nil {
		return published, fmt.Errorf("fresh exact walk: %w", err)
	}
	p.FreshExact = true
	p.boundary("fresh_walk_complete")
	return published, nil
}

func (f fixture) overlapBackfill(ctx context.Context, p *backfillProbe, held *walk, r *result, previous uint64, writes *[]ingestSample, reads *[]time.Duration) error {
	// Only the writer modifies writes. Join it before inspecting samples
	// or starting the publication wait, including on continuation failure.
	mixed, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := make(chan error, 1)
	firstWrite := make(chan struct{})
	next := min(p.RequestedCutoff+f.BatchRows, p.FinalCutoff)
	go func() {
		err := f.appendRange(mixed, p.service, p.RequestedCutoff, next, writes)
		close(firstWrite)
		if err == nil {
			err = f.appendRange(mixed, p.service, next, p.FinalCutoff, writes)
		}
		if err != nil {
			cancel()
		}
		writer <- err
	}()
	walkErr := func() error {
		d, err := held.page(mixed, p.service, f)
		if err != nil {
			return fmt.Errorf("walk captured cutoff %d: %w", p.HeldCutoff, err)
		}
		*reads = append(*reads, d)
		if held.cursor == "" {
			return errors.New("overlapping continuation exhausted the held cursor before publication")
		}
		select {
		case <-mixed.Done():
			return mixed.Err()
		case <-firstWrite:
		}
		c, sent, received, err := p.request(mixed, "status", true)
		if p.Pattern == "scattered" && err == nil {
			r.Diagnostics.OverlapCompaction = &c.compaction
		}
		if err == nil {
			err = p.validateRunning(c.compaction, previous, next)
		}
		p.record("overlap", sent, received, c, err)
		if err != nil {
			return fmt.Errorf("compaction did not span a continuation and an acknowledged batch: %w", err)
		}
		p.lastRunning, p.lastPoll = sent, received
		if p.Pattern == "scattered" {
			// Preserve the mixed-load latency series while holding at least
			// one more page across the durable replacement.
			for held.cursor != "" && len(held.want)-held.offset > 509 {
				d, err := held.page(mixed, p.service, f)
				if err != nil {
					return fmt.Errorf("walk captured cutoff %d: %w", p.HeldCutoff, err)
				}
				*reads = append(*reads, d)
				if time.Since(p.lastPoll) >= timelineSampleInterval && p.Publication == nil {
					if _, err := p.pollPublication(mixed, previous, false); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}()
	if walkErr != nil {
		cancel()
	}
	return errors.Join(<-writer, walkErr)
}
