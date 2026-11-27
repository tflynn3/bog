package main

import (
	jsonv1 "encoding/json"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
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
	var report report
	if err := json.Unmarshal(data, &report, jsonv1.FormatDurationAsNano(true)); err != nil {
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
	if recorded != f || r.Phase != "complete" || r.Status != "pass" || r.Workload != "pass" || r.TimedOut ||
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
		if err := r.Diagnostics.Backfills[i].validateCutover(want, f); err != nil {
			return fmt.Errorf("%s backfill: %w", want.Pattern, err)
		}
	}
	scattered := r.Diagnostics.Backfills[1]
	if r.Compaction == nil || r.Compaction.State != "complete" ||
		r.Compaction.Generation != scattered.Publication.Generation || r.Compaction.CoveredRows != scattered.RequestedCutoff ||
		r.Compaction.CommittedRows != scattered.FinalCutoff {
		return errors.New("scattered publication disagrees with the fixture compaction")
	}
	return nil
}

func (t *backfillTimeline) validateCutover(want *backfillTimeline, f fixture) error {
	if t == nil || t.Pattern != want.Pattern || t.Status != "pass" || t.HeldCutoff != want.HeldCutoff ||
		t.RequestedCutoff != want.RequestedCutoff || t.FinalCutoff != want.FinalCutoff ||
		t.Publication == nil || t.Publication.Generation == 0 ||
		t.Publication.LowerBound < 0 || t.Publication.UpperBound <= t.Publication.LowerBound {
		return errors.New("missing exact durable publication")
	}
	rows := t.FinalCutoff - t.HeldCutoff
	if t.Acknowledged == nil || t.Acknowledged.Rows != rows || t.Acknowledged.Batches != rows/f.BatchRows {
		return errors.New("missing acknowledged backfill")
	}
	if !t.HeldExhausted || !t.TerminalRetried || !t.CaptureRetired || t.HeldRows <= 0 ||
		!t.FreshVisible || !t.FreshExact || t.FreshRows <= t.HeldRows ||
		(t.Pattern == "scattered" && t.FreshRows != t.FinalCutoff) {
		return errors.New("missing held, terminal retry, retirement, or exact fresh parity")
	}
	d := t.PinDrain
	if d.Status != "observed" || d.Held == nil || *d.Held == 0 || d.After == nil || *d.After != 0 || d.Reason != "" {
		return errors.New("require a positive held predecessor pin count and an observed zero after retirement")
	}
	return t.validateStatusSamples()
}

func (t *backfillTimeline) validateStatusSamples() error {
	published, held, drained := false, false, false
	for _, s := range t.Samples {
		c := s.Status
		if s.StatusError != "" || s.RequestSent == nil || c == nil || c.State != "complete" ||
			c.Generation != t.Publication.Generation || c.CoveredRows != t.RequestedCutoff {
			continue
		}
		pins := s.Candidate.RetiredPins
		switch s.Event {
		case "publication":
			published = c.CommittedRows >= t.RequestedCutoff && c.CommittedRows <= t.FinalCutoff
		case "before_retirement":
			held = published && c.CommittedRows == t.FinalCutoff && pins != nil && *pins == *t.PinDrain.Held
		case "after_retirement", "drain_poll":
			drained = drained || held && c.CommittedRows == t.FinalCutoff && pins != nil && *pins == 0
		}
	}
	if !published || !held || !drained {
		return errors.New("missing matching status samples for publication, held pins, and drain")
	}
	return nil
}
