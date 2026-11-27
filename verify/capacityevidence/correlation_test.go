package capacityevidence

import (
	"bytes"
	json "encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func correlatedFixture(t *testing.T, seed int, edit func(*backfillEvidence, *CycleCorrelation)) []byte {
	t.Helper()
	var r record
	if err := json.Unmarshal(observationRecord(t, seed), &r); err != nil {
		t.Fatal(err)
	}
	for i := range r.Diagnostics.Timelines {
		b := &r.Diagnostics.Timelines[i]
		start, held, released := event(b, "compaction_start"), event(b, "before_retirement"), event(b, "drain_poll")
		predecessor, replacement := start.Compaction.Generation, b.Publication.Generation
		held.Metrics.ReplayTailBytes = new(uint64(0))
		held.Metrics.PredecessorPinAge = new(uint64(0))
		held.Metrics.PredecessorPinGeneration = &predecessor
		credited := released
		if i == 1 {
			late := *released
			late.Event = "reclamation_poll"
			late.SentNS = new(released.ElapsedNS + 1)
			late.ElapsedNS = released.ElapsedNS + 2
			at := len(b.Samples) - 1 // before fresh_visible
			b.Samples = slices.Insert(b.Samples, at, late)
			credited = &b.Samples[at]
			held = event(b, "before_retirement")
			held.Metrics.ReplayTailBytes = new(uint64(500))
			held.Metrics.PredecessorPinAge = new(uint64(900))
		}
		credited.Metrics.ReclaimedBytes = new(uint64(i * 1200))
		credited.Metrics.ReclaimedPredecessor = &predecessor
		credited.Metrics.ReclaimedReplacement = &replacement
		walkAt := b.Samples[len(b.Samples)-1].ElapsedNS + 2
		b.Samples = append(b.Samples, backfillSample{Event: "fresh_walk_complete", ElapsedNS: walkAt})
		proof := CycleCorrelation{
			PredecessorGeneration: new(predecessor), ReplacementGeneration: new(replacement),
			ReplayTailBytes: new(*held.Metrics.ReplayTailBytes), PredecessorPinAge: new(*held.Metrics.PredecessorPinAge),
			HeldObservedNS: new(held.ElapsedNS), FinalPinReleaseNS: new(released.ElapsedNS),
			ReclaimedBytes: new(*credited.Metrics.ReclaimedBytes), ReclaimedObservedNS: new(credited.ElapsedNS),
			CgroupPeak: &PeakObservation{ReadStartNS: new(walkAt + 1), ReadEndNS: new(walkAt + 2), Bytes: new(uint64(1500 + i))},
		}
		if edit != nil && i == 1 {
			edit(b, &proof)
		}
		if b.Correlation == nil {
			raw, err := json.Marshal(proof)
			if err != nil {
				t.Fatal(err)
			}
			b.Correlation = raw
		}
	}
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func event(b *backfillEvidence, name string) *backfillSample {
	index := slices.IndexFunc(b.Samples, func(s backfillSample) bool { return s.Event == name })
	return &b.Samples[index]
}

func correlatedEnvelope(small, large []byte) []byte {
	return fmt.Appendf(nil, `{"schema_version":1,"fixture":%q,"mode":"live","cases":{%q:{"status":"pass","evidence":[%s,%s]}}}`,
		FixtureID, Case, small, large)
}

func TestValidateCorrelatedCapacity(t *testing.T) {
	t.Parallel()
	large := correlatedFixture(t, LargeScale, nil)
	for _, tc := range []struct {
		name, problem string
		edit          func(*backfillEvidence, *CycleCorrelation)
	}{
		{name: "zero and nonzero measurements with immediate and delayed credit"},
		{"null proof", "missing capacity correlation", func(b *backfillEvidence, _ *CycleCorrelation) { b.Correlation = []byte("null") }},
		{"malformed proof", "decode capacity correlation", func(b *backfillEvidence, _ *CycleCorrelation) { b.Correlation = []byte("[]") }},
		{"wrong predecessor in summary", "generations", func(_ *backfillEvidence, p *CycleCorrelation) { *p.PredecessorGeneration++ }},
		{"wrong replacement in summary", "generations", func(_ *backfillEvidence, p *CycleCorrelation) { *p.ReplacementGeneration++ }},
		{"missing replay measurement", "missing replay_tail_bytes", func(b *backfillEvidence, _ *CycleCorrelation) {
			event(b, "before_retirement").Metrics.ReplayTailBytes = nil
		}},
		{"missing predecessor pin age", "missing oldest predecessor pin age", func(b *backfillEvidence, _ *CycleCorrelation) {
			event(b, "before_retirement").Metrics.PredecessorPinAge = nil
		}},
		{"age from successor", "oldest predecessor pin age belongs to a different generation", func(b *backfillEvidence, _ *CycleCorrelation) {
			event(b, "before_retirement").Metrics.PredecessorPinGeneration = new(b.Publication.Generation)
		}},
		{"held time disagrees", "disagrees with held replay", func(_ *backfillEvidence, p *CycleCorrelation) { *p.HeldObservedNS++ }},
		{"replay summary disagrees", "disagrees with held replay", func(_ *backfillEvidence, p *CycleCorrelation) { *p.ReplayTailBytes++ }},
		{"release timestamp omitted", "disagrees with final pin release", func(_ *backfillEvidence, p *CycleCorrelation) { p.FinalPinReleaseNS = nil }},
		{"release claimed at retirement", "disagrees with final pin release", func(b *backfillEvidence, p *CycleCorrelation) {
			p.FinalPinReleaseNS = new(event(b, "capture_retired").ElapsedNS)
		}},
		{"wrong replacement in runtime credit", "attributed to a different predecessor or replacement", func(b *backfillEvidence, _ *CycleCorrelation) {
			event(b, "reclamation_poll").Metrics.ReclaimedReplacement = new(b.Publication.Generation + 1)
		}},
		{"wrong predecessor in runtime credit", "attributed to a different predecessor or replacement", func(b *backfillEvidence, _ *CycleCorrelation) {
			event(b, "reclamation_poll").Metrics.ReclaimedPredecessor = new(b.Publication.Generation)
		}},
		{"incomplete credit tuple", "incomplete reclamation measurement", func(b *backfillEvidence, _ *CycleCorrelation) {
			event(b, "reclamation_poll").Metrics.ReclaimedReplacement = nil
		}},
		{"credit omitted", "fresh visibility preceded", func(b *backfillEvidence, _ *CycleCorrelation) {
			m := &event(b, "reclamation_poll").Metrics
			m.ReclaimedBytes, m.ReclaimedReplacement, m.ReclaimedPredecessor = nil, nil, nil
		}},
		{"zero credit before drain", "preceded final predecessor pin release", func(b *backfillEvidence, _ *CycleCorrelation) {
			valid := event(b, "reclamation_poll").Metrics
			early := &event(b, "after_retirement").Metrics
			early.ReclaimedBytes = new(uint64(0))
			early.ReclaimedPredecessor, early.ReclaimedReplacement = valid.ReclaimedPredecessor, valid.ReclaimedReplacement
		}},
		{"pins return after drain", "observed zero predecessor pin count", func(b *backfillEvidence, _ *CycleCorrelation) {
			event(b, "reclamation_poll").Metrics.RetiredPins = new(uint64(1))
		}},
		{"reclamation outside budget", "outside the observation window", func(b *backfillEvidence, p *CycleCorrelation) {
			poll := event(b, "reclamation_poll")
			poll.ElapsedNS += int64(ReclamationBudget)
			event(b, "fresh_visible").ElapsedNS = poll.ElapsedNS + 1
			event(b, "fresh_walk_complete").ElapsedNS = poll.ElapsedNS + 2
			p.CgroupPeak.ReadStartNS, p.CgroupPeak.ReadEndNS = new(poll.ElapsedNS+3), new(poll.ElapsedNS+4)
		}},
		{"unexpected observation after exact walk", "fresh walk completion must end", func(b *backfillEvidence, _ *CycleCorrelation) {
			at := event(b, "fresh_walk_complete").ElapsedNS
			b.Samples = append(b.Samples, backfillSample{Event: "phase_failure", ElapsedNS: at + 1})
		}},
		{"credit summary omitted", "inconsistent reclaimed capacity", func(_ *backfillEvidence, p *CycleCorrelation) { p.ReclaimedBytes = nil }},
		{"credit timestamp disagrees", "inconsistent reclaimed capacity", func(_ *backfillEvidence, p *CycleCorrelation) { *p.ReclaimedObservedNS++ }},
		{"peak omitted", "cgroup memory.peak", func(_ *backfillEvidence, p *CycleCorrelation) { p.CgroupPeak = nil }},
		{"peak bytes omitted", "cgroup memory.peak", func(_ *backfillEvidence, p *CycleCorrelation) { p.CgroupPeak.Bytes = nil }},
		{"peak read failed", "cgroup memory.peak", func(_ *backfillEvidence, p *CycleCorrelation) { p.CgroupPeak.Error = "read failed" }},
		{"peak read before fresh walk", "mis-timed cgroup memory.peak", func(b *backfillEvidence, p *CycleCorrelation) {
			p.CgroupPeak.ReadStartNS = new(event(b, "fresh_visible").ElapsedNS)
		}},
		{"peak below current", "disagrees with live memory", func(_ *backfillEvidence, p *CycleCorrelation) { *p.CgroupPeak.Bytes = 1023 }},
		{"peak decreased across workloads", "cumulative cgroup peaks", func(_ *backfillEvidence, p *CycleCorrelation) { *p.CgroupPeak.Bytes = 1499 }},
		{"peak above final high-water mark", "cumulative cgroup peaks", func(_ *backfillEvidence, p *CycleCorrelation) { *p.CgroupPeak.Bytes = 2049 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			small := correlatedFixture(t, SmallScale, tc.edit)
			envelope := correlatedEnvelope(small, large)
			for _, err := range []error{
				ValidateRecord(small), ValidateReport(envelope),
				ValidateCorrelatedRecord(small), ValidateCorrelatedReport(envelope),
			} {
				if tc.problem == "" && err != nil ||
					tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
					t.Errorf("error = %v, want %q", err, tc.problem)
				}
			}
		})
	}
}

func TestCorrelatedCapacityCannotMixHistoricalScales(t *testing.T) {
	t.Parallel()
	small := observationRecord(t, SmallScale)
	large := observationRecord(t, LargeScale)
	if err := ValidateReport(correlatedEnvelope(small, large)); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		ValidateCorrelatedRecord(small),
		ValidateCorrelatedReport(correlatedEnvelope(small, large)),
		ValidateReport(correlatedEnvelope(small, correlatedFixture(t, LargeScale, nil))),
	} {
		if err == nil || !strings.Contains(err.Error(), "missing capacity correlation") {
			t.Errorf("error = %v, want missing capacity correlation", err)
		}
	}
}

func TestCorrelatedCapacityRejectsDuplicateWireMembers(t *testing.T) {
	t.Parallel()
	small := correlatedFixture(t, SmallScale, nil)
	for _, key := range []string{ReclaimedBytesMetric, ReplayTailBytesMetric} {
		field := fmt.Appendf(nil, "%q:0", key)
		duplicate := bytes.Replace(small, field, fmt.Appendf(nil, "%s,%s", field, field), 1)
		if bytes.Equal(duplicate, small) {
			t.Fatalf("fixture did not contain %s", field)
		}
		if err := ValidateCorrelatedRecord(duplicate); err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("duplicate field %s error = %v", key, err)
		}
	}
}
