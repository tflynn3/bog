package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"bog/verify/capacityevidence"
)

func (f fixture) runBackfill(ctx context.Context, s service, o options, r *result, dir string, timeline *backfillTimeline, held *walk, previous uint64) (_ compaction, err error) {
	p := backfillProbe{backfillTimeline: timeline, service: s, control: o.control, dir: dir, origin: time.Now()}
	p.Status = "running"
	if o.requireCorrelation {
		p.Correlation = &capacityevidence.CycleCorrelation{PredecessorGeneration: &previous}
	}
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
	var slow *slowScenario
	if scattered && o.requireSlowOverlap {
		e := r.Diagnostics.SlowResponses
		if e == nil {
			e = newSlowResponseEvidence()
			r.Diagnostics.SlowResponses = e
		}
		e.Status, e.origin = "running", p.origin
		slow = &slowScenario{slowResponseEvidence: e, probe: &p, fixture: f}
		defer func() {
			err = errors.Join(err, slow.close())
			if err != nil {
				e.fail(err)
			}
		}()
	}
	measure := func(name string, run func() error) error {
		if slow == nil {
			return run()
		}
		return slow.measurePhase(ctx, s, name, run)
	}
	if scattered {
		defer func() { r.recordMixed(writes, reads, p.lastRunning) }()
	}
	setPhase := func(phase string) {
		if !scattered {
			phase = "clustered_" + phase
		}
		r.Phase, held.phase = phase, phase
	}
	var published compaction
	if err := measure("replacement_overlap", func() error {
		setPhase("seed_capture")
		if _, err := held.page(ctx, s, f); err != nil {
			return fmt.Errorf("capture cutoff %d: %w", p.HeldCutoff, err)
		}
		if held.cursor == "" {
			return errors.New("held capture did not issue a cursor")
		}
		p.boundary("held_capture")
		if slow != nil {
			if err := slow.open(ctx, held); err != nil {
				return err
			}
		}
		if err := f.appendRange(ctx, s, p.HeldCutoff, p.RequestedCutoff, &writes); err != nil {
			return err
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
			return err
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
				return fmt.Errorf("observe opening compaction: %w", err)
			}
			p.lastRunning, p.lastPoll = sent, received
		}

		setPhase("mixed_continuation")
		if err := f.overlapBackfill(ctx, &p, held, r, previous, &writes, &reads, slow); err != nil {
			return err
		}
		p.boundary("backfill_acknowledged")
		setPhase("publication")
		published, err = p.waitPublication(ctx, previous)
		if scattered {
			r.Compaction = &published
		}
		if err != nil {
			return err
		}
		if slow != nil {
			return slow.replace(ctx, published)
		}
		return nil
	}); err != nil {
		return published, err
	}
	if slow != nil {
		if err := measure("cancellation_and_retry", func() error { return slow.cancelAndRetry(ctx) }); err != nil {
			return published, err
		}
	}

	var terminal terminalPage
	if err := measure("held_parity", func() error {
		setPhase("after_publication_continuation")
		if slow != nil {
			held.pageLimit = slowPageLimit
		}
		var err error
		terminal, err = held.exhaust(ctx, s, f)
		if err != nil {
			return fmt.Errorf("resume after compaction: %w", err)
		}
		p.HeldExhausted = true
		p.boundary("held_exhausted")
		setPhase("terminal_retry")
		if err := terminal.replay(ctx, s, f, r.Phase, held.offset); err != nil {
			return fmt.Errorf("retry terminal page: %w", err)
		}
		p.TerminalRetried, p.TerminalBytesEqual = true, true
		p.boundary("terminal_retried")
		if slow != nil {
			slow.record(slowEvent{Name: "held_broad_exact", Rows: held.offset})
			return slow.verifySelectiveHeld(ctx)
		}
		return nil
	}); err != nil {
		return published, err
	}

	if err := measure("retirement_and_reclamation", func() error {
		setPhase("capture_retirement")
		if slow != nil {
			return slow.retire(ctx, terminal)
		}
		if err := p.observeHeldPin(ctx); err != nil {
			return err
		}
		if err := s.retireCapture(ctx, o.control, terminal.request.cursor); err != nil {
			return fmt.Errorf("explicitly retire held capture: %w", err)
		}
		p.CaptureRetired = true
		retiredAt := p.boundary("capture_retired")
		setPhase("pin_drain")
		if err := p.observeDrain(ctx, retiredAt, drainBudget); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return published, err
	}

	err = measure("fresh_parity", func() error {
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
			return fmt.Errorf("fresh acknowledged backfill at ordinal %d: %w", ordinal, err)
		}
		p.FreshVisible = true
		p.boundary("fresh_visible")
		setPhase("fresh_walk")
		var exact *walk
		if scattered {
			cutoff := p.FinalCutoff
			if slow != nil {
				cutoff = p.acknowledged.rows()
				if cutoff != p.FinalCutoff {
					return fmt.Errorf("fresh fixture cutoff %d disagrees with acknowledged cutoff %d", p.FinalCutoff, cutoff)
				}
			}
			exact = f.newWalk(cutoff, false)
		} else {
			exact = f.walkFor(p.FinalCutoff, held.filter)
		}
		exact.phase, exact.pageLimit = r.Phase, 1000
		_, err := exact.exhaust(ctx, s, f)
		p.FreshRows = exact.offset
		if err != nil {
			return fmt.Errorf("fresh exact walk: %w", err)
		}
		if slow != nil {
			slow.record(slowEvent{Name: "fresh_exact", Rows: exact.offset})
			narrow := p.acknowledged.selectiveWalk(f, "slow_selective_fresh", slowPageLimit)
			if _, err := narrow.exhaust(ctx, s, f); err != nil {
				return fmt.Errorf("fresh selective ledger parity: %w", err)
			}
			if narrow.offset <= slow.selective.offset {
				return errors.New("selective fresh walk did not include any acknowledged backfill")
			}
			slow.record(slowEvent{Name: "fresh_selective_exact", Rows: narrow.offset})
		}
		p.FreshExact = true
		p.boundary("fresh_walk_complete")
		return p.observePeak(ctx)
	})
	return published, err
}

func (f fixture) overlapBackfill(ctx context.Context, p *backfillProbe, held *walk, r *result, previous uint64, writes *[]ingestSample, reads *[]time.Duration, slow *slowScenario) error {
	// Only the writer modifies writes. Join it before inspecting samples
	// or starting the publication wait, including on continuation failure.
	mixed, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := make(chan error, 1)
	firstWrite := make(chan struct{})
	next := min(p.RequestedCutoff+f.BatchRows, p.FinalCutoff)
	go func() {
		err := f.appendRange(mixed, p.service, p.RequestedCutoff, next, writes)
		if err == nil {
			close(firstWrite)
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
		if slow != nil {
			slow.record(slowEvent{Name: "ingest_acknowledged_during_maintenance", Compaction: &c.compaction, Rows: c.CommittedRows - p.RequestedCutoff})
		}
		if p.Pattern == "scattered" && slow == nil {
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
