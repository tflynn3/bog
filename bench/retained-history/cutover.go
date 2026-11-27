package main

import (
	"context"
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
)

// The native gate runs this check from its own checkout. A report from an
// older candidate can pass the historical validator without cutover proof.
func validateCutoverFile(path string) (err error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	const maxReport = 16 << 20
	if !info.Mode().IsRegular() || info.Size() > maxReport {
		return errors.New("capacity report must be a regular file of at most 16 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxReport+1))
	if err != nil {
		return err
	}
	if len(data) > maxReport {
		return errors.New("capacity report exceeds 16 MiB")
	}
	// The producer always writes a boolean. Track presence when reading
	// strict evidence so missing or null cannot pass as false.
	decodeResult := json.UnmarshalFromFunc(func(d *jsontext.Decoder, r *result) error {
		wire := struct {
			*result
			TimedOut *bool `json:"timed_out"`
		}{result: r}
		if err := json.UnmarshalDecode(d, &wire); err != nil {
			return err
		}
		if wire.TimedOut == nil {
			return errors.New("missing or null timed_out verdict")
		}
		r.TimedOut = *wire.TimedOut
		return nil
	})
	var report report
	if err := json.Unmarshal(data, &report, jsonv1.FormatDurationAsNano(true), json.WithUnmarshalers(decodeResult)); err != nil {
		return fmt.Errorf("decode capacity report: %w", err)
	}
	return report.validateCutover()
}

func (r report) validateCutover() error {
	if r.SchemaVersion != 1 || r.Fixture != "linux-release-v1" || r.Mode != "live" {
		return errors.New("cutover capacity requires a schema-v1 live linux-release-v1 report")
	}
	c := r.Cases[capacityCase]
	sizes := fixtures("live")
	if c.Status != "pass" || len(c.Evidence) != len(sizes) {
		return errors.New("cutover capacity requires a passing record at each live scale")
	}
	for _, f := range sizes {
		found := false
		for _, record := range c.Evidence {
			if record.Fixture.SeedRows != f.SeedRows {
				continue
			}
			if err := record.validateCutover(f); err != nil {
				return fmt.Errorf("capacity fixture %d: %w", f.SeedRows, err)
			}
			found = true
			break
		}
		if !found {
			return fmt.Errorf("missing capacity fixture %d", f.SeedRows)
		}
	}
	return nil
}

func (r result) validateCutover(f fixture) error {
	recorded := r.Fixture
	recorded.scatteredTail = f.scatteredTail // fixture placement is implicit in live reports
	if r.TimedOut {
		return errors.New("require timed_out to be false")
	}
	if recorded != f || r.Phase != "complete" || r.Status != "pass" || r.Workload != "pass" ||
		r.RecoveredRows != f.SeedRows+f.TailRows {
		return errors.New("missing completed fixture with exact restart parity")
	}
	if r.CgroupLimit == nil || *r.CgroupLimit != f.MemoryBytes || r.CgroupSwap == nil || *r.CgroupSwap != 0 ||
		r.CgroupPeak == nil || *r.CgroupPeak == 0 || *r.CgroupPeak > f.MemoryBytes {
		return errors.New("missing cgroup limit readback, disabled swap readback, or bounded memory peak")
	}
	if r.Diagnostics == nil {
		return errors.New("missing cutover diagnostics")
	}
	oom, observed := r.Diagnostics.MemoryEvents["oom_kill"]
	if !observed || oom != 0 || r.CgroupOOM == nil || *r.CgroupOOM != 0 {
		return errors.New("require an observed zero cgroup OOM-kill count")
	}
	c := r.Cleanup
	if c.Status != "pass" || !c.CandidateReaped || !c.SubtreeEmpty || !c.SubtreeRemoved || !c.StoreRemoved || c.Detail != "" {
		return errors.New("missing candidate, subtree, or fixture store cleanup")
	}
	if len(r.Diagnostics.Candidates) != 2 {
		return errors.New("missing startup and restart lifecycle evidence")
	}
	for i, lifetime := range []string{"startup", "restart"} {
		p := r.Diagnostics.Candidates[i]
		if p == nil || p.Lifetime != lifetime || p.Ready == nil || *p.Ready <= 0 ||
			!p.Reaped || p.ExitedBeforeCleanup || p.ExitCode == nil || *p.ExitCode != 0 ||
			p.Signal != "" || p.WaitError != "" || p.StartupError != "" || p.LogDrainError != "" {
			return fmt.Errorf("missing clean %s lifecycle", lifetime)
		}
	}
	expected := newBackfillTimelines(f)
	if len(r.Diagnostics.Backfills) != len(expected) {
		return errors.New("missing clustered and scattered backfill evidence")
	}
	for i, want := range expected {
		t := r.Diagnostics.Backfills[i]
		if err := t.validateCutover(want, f); err != nil {
			return fmt.Errorf("%s backfill: %w", want.Pattern, err)
		}
		if err := t.validateLiveMemory(*r.CgroupPeak); err != nil {
			return fmt.Errorf("%s backfill: %w", want.Pattern, err)
		}
	}
	scattered := r.Diagnostics.Backfills[1]
	if r.Compaction == nil || r.Compaction.State != "complete" ||
		r.Diagnostics.Backfills[0].Publication.Generation >= scattered.Publication.Generation ||
		r.Compaction.Generation != scattered.Publication.Generation || r.Compaction.CoveredRows != scattered.RequestedCutoff ||
		r.Compaction.CommittedRows != scattered.FinalCutoff {
		return errors.New("scattered publication disagrees with the fixture compaction")
	}
	return nil
}

func (t *backfillTimeline) validateCutover(want *backfillTimeline, f fixture) error {
	if t == nil || t.Pattern != want.Pattern || t.Status != "pass" || t.Failure != "" || t.HeldCutoff != want.HeldCutoff ||
		t.RequestedCutoff != want.RequestedCutoff || t.FinalCutoff != want.FinalCutoff ||
		t.Publication == nil || t.Publication.Generation == 0 ||
		t.Publication.LowerBound < 0 || t.Publication.UpperBound <= t.Publication.LowerBound {
		return errors.New("missing exact durable publication")
	}
	rows := t.FinalCutoff - t.HeldCutoff
	if t.Acknowledged == nil || t.Acknowledged.Rows != rows || t.Acknowledged.Batches != rows/f.BatchRows ||
		t.Acknowledged.First <= 0 || t.Acknowledged.Last < t.Acknowledged.First ||
		t.Acknowledged.EarliestTS.IsZero() || t.Acknowledged.LatestTS.Before(t.Acknowledged.EarliestTS) {
		return errors.New("missing acknowledged backfill")
	}
	if !t.HeldExhausted || !t.TerminalRetried || !t.CaptureRetired || t.HeldRows <= 0 || t.HeldRows > t.HeldCutoff ||
		!t.FreshVisible || !t.FreshExact || t.FreshRows <= t.HeldRows || t.FreshRows > t.FinalCutoff ||
		(t.Pattern == "scattered" && t.FreshRows != t.FinalCutoff) {
		return errors.New("missing held, terminal retry, retirement, or exact fresh parity")
	}
	d := t.PinDrain
	if d.Status != "observed" || d.Held == nil || *d.Held == 0 || d.After == nil || *d.After != 0 || d.Reason != "" {
		return errors.New("require a positive held predecessor pin count and an observed zero after retirement")
	}
	return t.validateStatusSamples()
}

func (t *backfillTimeline) validateLiveMemory(peak uint64) error {
	var lastRunning time.Duration
	var predecessor uint64
	var committed, samples int
	var overlap, published bool
	for _, s := range t.Samples {
		m, c := s.Memory, s.Status
		switch s.Event {
		case "compaction_start", "overlap", "status_poll":
			if published || s.StatusError != "" || s.RequestSent == nil || *s.RequestSent <= 0 ||
				*s.RequestSent < lastRunning || s.Elapsed <= *s.RequestSent || c == nil {
				return fmt.Errorf("invalid running status timing or reply for %s", s.Event)
			}
		case "publication":
			if m != nil || published || s.StatusError != "" || c == nil ||
				s.RequestSent == nil || *s.RequestSent < lastRunning || s.Elapsed <= *s.RequestSent ||
				c.CommittedRows < committed {
				return errors.New("invalid publication after running memory observations")
			}
			if !overlap || samples < 2 {
				return errors.New("require multiple bracketed memory.current samples including overlap before publication")
			}
			published = true
			continue
		default:
			if m != nil {
				return errors.New("memory.current sample outside running status polls")
			}
			continue
		}
		if s.Event == "compaction_start" {
			if lastRunning != 0 || m != nil || c.Generation >= t.Publication.Generation ||
				c.CommittedRows != t.RequestedCutoff {
				return errors.New("memory evidence requires a valid running compaction start")
			}
			predecessor, committed = c.Generation, t.RequestedCutoff
		} else if lastRunning == 0 {
			return errors.New("memory evidence requires a prior running compaction start")
		}
		if t.validateRunning(*c, predecessor, committed) != nil {
			return errors.New("memory evidence requires running status for the same compaction")
		}
		if s.Event == "overlap" {
			next := min(t.RequestedCutoff+(t.RequestedCutoff-t.HeldCutoff), t.FinalCutoff)
			if m == nil || overlap || c.CommittedRows < next {
				return errors.New("require a bracketed memory.current overlap sample after backfill")
			}
			overlap = true
		}
		if m != nil {
			if samples == liveMemorySampleLimit || m.ReadStart < lastRunning ||
				m.ReadEnd <= m.ReadStart || m.ReadEnd > *s.RequestSent {
				return errors.New("mis-timed or excessive memory.current samples")
			}
			if m.Error != "" {
				return fmt.Errorf("live memory.current sampling failed: %s", m.Error)
			}
			if m.Bytes == nil || *m.Bytes == 0 || *m.Bytes > peak {
				return errors.New("missing, zero, or above-final-memory.peak memory.current bytes")
			}
			samples++
		}
		lastRunning, committed = s.Elapsed, c.CommittedRows
	}
	if !published || t.Memory == nil || *t.Memory != t.summarizeMemory() {
		return errors.New("missing or inconsistent memory.current observation span or range")
	}
	return nil
}

func (b *backfillTimeline) validateStatusSamples() error {
	var elapsed, startSent, overlapSent, lastRunningSent, exhaustedAt, retriedAt, retiredAt, drainStarted time.Duration
	var published, finalObserved, held, drained, fresh bool
	var predecessor uint64
	for _, sample := range b.Samples {
		if sample.Elapsed < elapsed {
			return errors.New("cutover samples must be in elapsed time order")
		}
		previousElapsed := elapsed
		elapsed = sample.Elapsed
		switch sample.Event {
		case "held_exhausted":
			if !finalObserved || exhaustedAt != 0 {
				return errors.New("held cursor exhaustion requires a prior final cutoff observation")
			}
			exhaustedAt = elapsed
			continue
		case "terminal_retried":
			if exhaustedAt == 0 || retriedAt != 0 {
				return errors.New("terminal page replay must follow held cursor exhaustion")
			}
			retriedAt = elapsed
			continue
		case "capture_retired":
			if !held || retiredAt != 0 {
				return errors.New("capture retirement requires a prior before_retirement pin observation")
			}
			retiredAt = elapsed
			continue
		case "fresh_visible":
			if !drained || fresh {
				return errors.New("fresh visibility sample must follow observed pin drain")
			}
			fresh = true
			continue
		case "compaction_start", "overlap", "status_poll", "publication", "final_cutoff", "before_retirement", "after_retirement", "drain_poll":
		default:
			continue
		}
		drain := sample.Event == "after_retirement" || sample.Event == "drain_poll"
		// The producer records a request deadline as a failed status sample
		// and keeps polling within the drain budget.
		retry := drain && sample.StatusError == context.DeadlineExceeded.Error()
		if sample.StatusError != "" && !retry {
			return fmt.Errorf("%s sample failed: %s", sample.Event, sample.StatusError)
		}
		if sample.RequestSent == nil || *sample.RequestSent <= 0 || *sample.RequestSent > elapsed {
			return fmt.Errorf("missing or invalid request timing for %s sample", sample.Event)
		}
		if sample.Event == "status_poll" {
			lastRunningSent = *sample.RequestSent
			continue
		}
		if drain {
			if retiredAt == 0 || *sample.RequestSent < retiredAt {
				return errors.New("pin drain sample did not follow explicit capture retirement")
			}
			if *sample.RequestSent < previousElapsed {
				return errors.New("pin drain status request overlapped a previous observation")
			}
			if drained || (sample.Event == "after_retirement") != (drainStarted == 0) {
				return errors.New("pin drain must start with after_retirement and end at the first observed zero")
			}
			if drainStarted == 0 {
				drainStarted = *sample.RequestSent
			}
			if elapsed-drainStarted > drainBudget {
				return errors.New("pin drain sample exceeded the drain budget")
			}
			if retry {
				continue
			}
		}
		if sample.Status == nil {
			return fmt.Errorf("missing request timing or compaction state for %s sample", sample.Event)
		}
		c := *sample.Status
		switch sample.Event {
		case "compaction_start":
			if startSent != 0 || c.State != "running" || c.Generation >= b.Publication.Generation ||
				c.CoveredRows != b.HeldCutoff || c.CommittedRows != b.RequestedCutoff {
				return errors.New("compaction start did not observe the predecessor at the requested cutoff")
			}
			startSent, predecessor = *sample.RequestSent, c.Generation
			continue
		case "overlap":
			if startSent == 0 || overlapSent != 0 || *sample.RequestSent <= startSent ||
				c.State != "running" || c.Generation != predecessor || c.CoveredRows != b.HeldCutoff ||
				c.CommittedRows < b.RequestedCutoff+(b.RequestedCutoff-b.HeldCutoff) || c.CommittedRows > b.FinalCutoff {
				return errors.New("overlap did not observe backfill on the predecessor generation")
			}
			overlapSent = *sample.RequestSent
			continue
		}
		if overlapSent == 0 || c.State != "complete" || c.Generation != b.Publication.Generation ||
			c.CoveredRows != b.RequestedCutoff || c.CommittedRows < b.RequestedCutoff || c.CommittedRows > b.FinalCutoff {
			return fmt.Errorf("%s sample did not observe publication at the requested cutoff", sample.Event)
		}
		if sample.Event == "publication" {
			if published {
				return errors.New("duplicate publication sample")
			}
			pub := b.Publication
			if pub.LowerBound < max(overlapSent, lastRunningSent)-startSent || pub.LowerBound > *sample.RequestSent-startSent ||
				pub.UpperBound != elapsed-startSent {
				return errors.New("publication lag bounds disagree with observed request timing")
			}
			published = true
		}
		if !published {
			return fmt.Errorf("%s sample preceded publication", sample.Event)
		}
		pins := sample.Candidate.RetiredPins
		if pins != nil && sample.Candidate.Unavailable["retired_generation_pins"] != "" {
			return errors.New("retired pin count contradicts unavailable status")
		}
		switch sample.Event {
		case "publication", "final_cutoff":
			if exhaustedAt != 0 || (sample.Event == "final_cutoff" && c.CommittedRows != b.FinalCutoff) {
				return fmt.Errorf("%s sample must confirm its cutoff before held cursor exhaustion", sample.Event)
			}
			finalObserved = finalObserved || c.CommittedRows == b.FinalCutoff
		case "before_retirement":
			if retriedAt == 0 || *sample.RequestSent < retriedAt || held || c.CommittedRows != b.FinalCutoff ||
				pins == nil || *pins != *b.PinDrain.Held {
				return errors.New("before_retirement must observe the positive held pin count at the final cutoff after terminal page replay")
			}
			held = true
		case "after_retirement", "drain_poll":
			if c.CommittedRows != b.FinalCutoff {
				return errors.New("pin drain sample did not observe the final cutoff")
			}
			if pins != nil && *pins == 0 {
				drained = true
			}
		}
	}
	if !published || !finalObserved || !held || !drained || !fresh {
		return errors.New("missing runtime samples for publication, final cutoff, held pin, pin drain, or fresh visibility")
	}
	return nil
}
