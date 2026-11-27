package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func passingCutoverReport() report {
	var records []result
	for _, f := range fixtures("live") {
		backfills := newBackfillTimelines(f)
		for i, b := range backfills {
			b.Status = "pass"
			b.HeldRows, b.FreshRows = b.HeldCutoff/2, b.FinalCutoff
			b.HeldExhausted, b.TerminalRetried, b.CaptureRetired = true, true, true
			b.FreshVisible, b.FreshExact = true, true
			b.Publication = &publicationProof{Generation: uint64(i + 1), LowerBound: 4 * time.Millisecond, UpperBound: 7 * time.Millisecond}
			rows := b.FinalCutoff - b.HeldCutoff
			b.Acknowledged = &backfillAcknowledgements{Rows: rows, Batches: rows / f.BatchRows}
			b.PinDrain = pinDrainProof{Status: "observed", Held: new(uint64(1)), After: new(uint64)}
			status := &compaction{b.Publication.Generation, b.RequestedCutoff, b.FinalCutoff, "complete"}
			b.Samples = []phaseSample{
				{Event: "compaction_start", Status: &compaction{uint64(i), b.HeldCutoff, b.RequestedCutoff, "running"},
					RequestSent: new(time.Millisecond), Elapsed: 2 * time.Millisecond},
				{Event: "overlap", Status: &compaction{uint64(i), b.HeldCutoff, b.FinalCutoff, "running"},
					RequestSent: new(5 * time.Millisecond), Elapsed: 6 * time.Millisecond,
					Memory: &memoryCurrentSample{ReadStart: 3 * time.Millisecond, ReadEnd: 4 * time.Millisecond, Bytes: new(f.MemoryBytes / 4)}},
				{Event: "publication", Status: status, RequestSent: new(7 * time.Millisecond), Elapsed: 8 * time.Millisecond},
				{Event: "before_retirement", Status: status, RequestSent: new(9 * time.Millisecond), Elapsed: 10 * time.Millisecond,
					Candidate: runtimeSample{RetiredPins: new(uint64(1))}},
				{Event: "after_retirement", Status: status, RequestSent: new(11 * time.Millisecond), Elapsed: 12 * time.Millisecond,
					Candidate: runtimeSample{RetiredPins: new(uint64)}},
			}
		}
		scattered := backfills[1]
		records = append(records, result{
			Fixture: f, Status: "pass", Workload: "pass", Phase: "complete", RecoveredRows: f.SeedRows + f.TailRows,
			Compaction: &compaction{scattered.Publication.Generation, scattered.RequestedCutoff, scattered.FinalCutoff, "complete"},
			Cleanup:    cleanupProof{Status: "pass", CandidateReaped: true, SubtreeEmpty: true, SubtreeRemoved: true, StoreRemoved: true},
			Diagnostics: &capacityDiagnostics{
				Candidates: []*candidateEvidence{
					{Lifetime: "startup", Ready: new(time.Millisecond), Reaped: true, ExitCode: new(int)},
					{Lifetime: "restart", Ready: new(time.Millisecond), Reaped: true, ExitCode: new(int)},
				},
				Backfills: backfills, MemoryEvents: map[string]uint64{"oom_kill": 0},
			},
			resources: resources{CgroupLimit: new(f.MemoryBytes), CgroupSwap: new(uint64),
				CgroupPeak: new(f.MemoryBytes / 2), CgroupOOM: new(uint64)},
		})
	}
	return report{1, strings.Repeat("a", 40), strings.Repeat("b", 64), "linux-release-v1", "live",
		map[string]outcome{capacityCase: {Status: "pass", Evidence: records}}}
}

func TestStrictCutoverReportProof(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		edit       func(*result, *backfillTimeline)
	}{
		{name: "both live scales"},
		{"duplicate scale", "missing capacity fixture", func(r *result, _ *backfillTimeline) { r.Fixture.SeedRows /= 2 }},
		{"missing swap", "swap readback", func(r *result, _ *backfillTimeline) { r.CgroupSwap = nil }},
		{"swap enabled", "swap readback", func(r *result, _ *backfillTimeline) { *r.CgroupSwap = 1 }},
		{"wrong memory limit", "limit readback", func(r *result, _ *backfillTimeline) { *r.CgroupLimit /= 2 }},
		{"missing peak", "memory peak", func(r *result, _ *backfillTimeline) { r.CgroupPeak = nil }},
		{"over cap peak", "memory peak", func(r *result, _ *backfillTimeline) { *r.CgroupPeak = r.Fixture.MemoryBytes + 1 }},
		{"missing OOM", "OOM-kill", func(r *result, _ *backfillTimeline) { r.CgroupOOM = nil }},
		{"reported OOM", "OOM-kill", func(r *result, _ *backfillTimeline) { *r.CgroupOOM = 1 }},
		{"raw OOM count missing", "OOM-kill", func(r *result, _ *backfillTimeline) { delete(r.Diagnostics.MemoryEvents, "oom_kill") }},
		{"raw OOM nonzero", "OOM-kill", func(r *result, _ *backfillTimeline) { r.Diagnostics.MemoryEvents["oom_kill"] = 1 }},
		{"store leftover", "cleanup", func(r *result, _ *backfillTimeline) { r.Cleanup.StoreRemoved = false }},
		{"unexpected exit", "startup lifecycle", func(r *result, _ *backfillTimeline) { r.Diagnostics.Candidates[0].ExitedBeforeCleanup = true }},
		{"wrong restart row count", "restart parity", func(r *result, _ *backfillTimeline) { r.RecoveredRows-- }},
		{"wrong cutoff", "exact durable publication", func(_ *result, b *backfillTimeline) { b.RequestedCutoff++ }},
		{"no replay", "terminal retry", func(_ *result, b *backfillTimeline) { b.TerminalRetried = false }},
		{"no retirement", "retirement", func(_ *result, b *backfillTimeline) { b.CaptureRetired = false }},
		{"inexact fresh walk", "exact fresh parity", func(_ *result, b *backfillTimeline) { b.FreshRows-- }},
		{"missing held pins", "positive held predecessor", func(_ *result, b *backfillTimeline) { b.PinDrain.Held = nil }},
		{"zero held pins", "positive held predecessor", func(_ *result, b *backfillTimeline) { *b.PinDrain.Held = 0 }},
		{"missing drained value", "observed zero", func(_ *result, b *backfillTimeline) { b.PinDrain.After = nil }},
		{"positive drained value", "observed zero", func(_ *result, b *backfillTimeline) { *b.PinDrain.After = 1 }},
		{"lost status samples", "matching status samples", func(_ *result, b *backfillTimeline) { b.Samples = nil }},
		{"different generation sampled", "matching status samples", func(_ *result, b *backfillTimeline) { b.Samples[2].Status.Generation++ }},
		{"drained status before held", "matching status samples", func(_ *result, b *backfillTimeline) { b.Samples[3], b.Samples[4] = b.Samples[4], b.Samples[3] }},
		{"startup-only memory", "live memory evidence", func(_ *result, b *backfillTimeline) {
			b.Samples[0].Memory, b.Samples[1].Memory = b.Samples[1].Memory, nil
		}},
		{"completed-state memory", "running status", func(_ *result, b *backfillTimeline) { b.Samples[1].Status.State = "complete" }},
		{"wrong running generation", "running status", func(_ *result, b *backfillTimeline) { b.Samples[1].Status.Generation++ }},
		{"memory before start reply", "mis-timed", func(_ *result, b *backfillTimeline) { b.Samples[1].Memory.ReadStart = time.Millisecond }},
		{"memory after status send", "mis-timed", func(_ *result, b *backfillTimeline) { b.Samples[1].Memory.ReadEnd = b.Samples[1].Elapsed }},
		{"reversed memory times", "mis-timed", func(_ *result, b *backfillTimeline) { b.Samples[1].Memory.ReadEnd = time.Millisecond }},
		{"overlap after publication", "timed overlap", func(_ *result, b *backfillTimeline) { b.Samples[1].Elapsed = b.Samples[2].Elapsed }},
		{"sampler failure", "sampling failed", func(_ *result, b *backfillTimeline) {
			b.Samples[1].Memory.Bytes, b.Samples[1].Memory.Error = nil, "memory.current: permission denied"
		}},
		{"missing live bytes", "out-of-limit", func(_ *result, b *backfillTimeline) { b.Samples[1].Memory.Bytes = nil }},
		{"zero live bytes", "out-of-limit", func(_ *result, b *backfillTimeline) { *b.Samples[1].Memory.Bytes = 0 }},
		{"over cap live bytes", "out-of-limit", func(r *result, b *backfillTimeline) { *b.Samples[1].Memory.Bytes = r.Fixture.MemoryBytes + 1 }},
		{"live bytes at cap", "", func(r *result, b *backfillTimeline) { *b.Samples[1].Memory.Bytes = r.Fixture.MemoryBytes }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := passingCutoverReport()
			scale := &r.Cases[capacityCase].Evidence[1]
			if tc.edit != nil {
				tc.edit(scale, scale.Diagnostics.Backfills[1])
			}
			path := filepath.Join(t.TempDir(), "capacity.json")
			if err := writeReport(path, r); err != nil {
				t.Fatal(err)
			}
			err := run(t.Context(), []string{"-validate-cutover", path})
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("validation error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCutoverRequiresOverlapMemoryForEveryTimeline(t *testing.T) {
	for scale, f := range fixtures("live") {
		for pattern, b := range newBackfillTimelines(f) {
			t.Run(fmt.Sprintf("%d/%s", f.SeedRows, b.Pattern), func(t *testing.T) {
				r := passingCutoverReport()
				r.Cases[capacityCase].Evidence[scale].Diagnostics.Backfills[pattern].Samples[1].Memory = nil
				if err := r.validateCutover(); err == nil || !strings.Contains(err.Error(), b.Pattern+" backfill: missing live memory.current overlap evidence") {
					t.Fatalf("validation error = %v, want missing overlap rejection for %s", err, b.Pattern)
				}
			})
		}
	}
}

func TestArchivedReportAndStrictValidation(t *testing.T) {
	const archive = "evidence/legacy-linux-arm64.json"
	if err := validateCutoverFile(archive); err == nil || !strings.Contains(err.Error(), "schema-v1 live") {
		t.Fatalf("strict cutover accepted archive: %v", err)
	}
	path := filepath.Join(t.TempDir(), "duplicate.json")
	if err := os.WriteFile(path, []byte(`{"mode":"legacy","mode":"live"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateCutoverFile(path); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("strict cutover accepted duplicate key: %v", err)
	}
}
