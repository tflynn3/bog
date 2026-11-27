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
			b.Acknowledged = &backfillAcknowledgements{
				Rows: rows, Batches: rows / f.BatchRows, First: time.Millisecond / 2, Last: 6500 * time.Microsecond,
				EarliestTS: epoch, LatestTS: epoch.Add(time.Second),
			}
			b.PinDrain = pinDrainProof{Status: "observed", Held: new(uint64(1)), After: new(uint64)}
			status := &compaction{b.Publication.Generation, b.RequestedCutoff, b.FinalCutoff, "complete"}
			b.Samples = []phaseSample{
				{Event: "compaction_start", Status: &compaction{uint64(i), b.HeldCutoff, b.RequestedCutoff, "running"},
					RequestSent: new(time.Millisecond), Elapsed: 2 * time.Millisecond},
				{Event: "status_poll", Status: &compaction{uint64(i), b.HeldCutoff, b.RequestedCutoff, "running"},
					RequestSent: new(3 * time.Millisecond), Elapsed: 3500 * time.Microsecond,
					Memory: &memoryCurrentSample{ReadStart: 2200 * time.Microsecond, ReadEnd: 2700 * time.Microsecond, Bytes: new(f.MemoryBytes / 8)}},
				{Event: "overlap", Status: &compaction{uint64(i), b.HeldCutoff, b.FinalCutoff, "running"},
					RequestSent: new(5 * time.Millisecond), Elapsed: 6 * time.Millisecond,
					Memory: &memoryCurrentSample{ReadStart: 3700 * time.Microsecond, ReadEnd: 4 * time.Millisecond, Bytes: new(f.MemoryBytes / 4)}},
				{Event: "publication", Status: status, RequestSent: new(7 * time.Millisecond), Elapsed: 8 * time.Millisecond},
				{Event: "held_exhausted", Elapsed: 8200 * time.Microsecond},
				{Event: "terminal_retried", Elapsed: 8500 * time.Microsecond},
				{Event: "before_retirement", Status: status, RequestSent: new(9 * time.Millisecond), Elapsed: 10 * time.Millisecond,
					Candidate: runtimeSample{RetiredPins: new(uint64(1))}},
				{Event: "capture_retired", Elapsed: 10500 * time.Microsecond},
				{Event: "after_retirement", Status: status, RequestSent: new(11 * time.Millisecond), Elapsed: 12 * time.Millisecond,
					Candidate: runtimeSample{RetiredPins: new(uint64(i))}},
			}
			if i == 1 {
				b.Samples = append(b.Samples, phaseSample{Event: "drain_poll", Status: status,
					RequestSent: new(111 * time.Millisecond), Elapsed: 112 * time.Millisecond,
					Candidate: runtimeSample{RetiredPins: new(uint64)}})
			}
			b.Samples = append(b.Samples, phaseSample{Event: "fresh_visible", Elapsed: 115 * time.Millisecond})
			b.Memory = new(b.summarizeMemory())
		}
		scattered := backfills[1]
		records = append(records, result{
			StartReady: new(time.Millisecond), RestartReady: new(time.Millisecond),
			IngestP99: new(time.Millisecond), QueryP99: new(time.Millisecond),
			IngestSamples: f.TailRows / f.BatchRows, QuerySamples: 2,
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
				CgroupPeak: new(f.MemoryBytes / 2), CgroupOOM: new(uint64),
				PeakRSS: new(f.MemoryBytes / 3), PeakFDs: new(uint64(4)), InitialFree: new(uint64(1024)), MinFree: new(uint64(512)),
				DeviceIO: map[string]map[string]uint64{"1:2": {"rbytes": 0, "wbytes": 1024}}},
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
		{"duplicate scale", "duplicate capacity fixture", func(r *result, _ *backfillTimeline) { r.Fixture.SeedRows /= 2 }},
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
		{"wrong cutoff", "incorrect cutoffs", func(_ *result, b *backfillTimeline) { b.RequestedCutoff++ }},
		{"no replay", "terminal page replay", func(_ *result, b *backfillTimeline) { b.TerminalRetried = false }},
		{"no retirement", "capture retirement", func(_ *result, b *backfillTimeline) { b.CaptureRetired = false }},
		{"inexact fresh walk", "exact fresh walk parity", func(_ *result, b *backfillTimeline) { b.FreshRows-- }},
		{"missing held pins", "positive held count", func(_ *result, b *backfillTimeline) { b.PinDrain.Held = nil }},
		{"zero held pins", "positive held count", func(_ *result, b *backfillTimeline) { *b.PinDrain.Held = 0 }},
		{"missing drained value", "to zero after retirement", func(_ *result, b *backfillTimeline) { b.PinDrain.After = nil }},
		{"positive drained value", "to zero after retirement", func(_ *result, b *backfillTimeline) { *b.PinDrain.After = 1 }},
		{"lost status samples", "missing runtime samples", func(_ *result, b *backfillTimeline) { b.Samples = nil }},
		{"different generation sampled", "publication at the requested cutoff", func(_ *result, b *backfillTimeline) { b.Samples[3].Status.Generation++ }},
		{"above final peak", "above-final-memory.peak", func(r *result, b *backfillTimeline) { *b.Samples[1].Memory.Bytes = *r.CgroupPeak + 1 }},
		{"at final peak", "", func(r *result, b *backfillTimeline) {
			*b.Samples[1].Memory.Bytes = *r.CgroupPeak
			b.Memory = new(b.summarizeMemory())
		}},
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
				r.Cases[capacityCase].Evidence[scale].Diagnostics.Backfills[pattern].Samples[2].Memory = nil
				if err := r.Cases[capacityCase].Evidence[scale].validateCutover(); err == nil || !strings.Contains(err.Error(), b.Pattern+" backfill: require a bracketed memory.current overlap sample") {
					t.Fatalf("validation error = %v, want missing overlap rejection for %s", err, b.Pattern)
				}
			})
		}
	}
}

func TestStrictCutoverTimeoutVerdict(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "capacity.json")
	if err := writeReport(path, passingCutoverReport()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before, after, ok := strings.Cut(string(data), `"timed_out": false,`)
	if !ok {
		t.Fatal("producer report missing explicit false timeout verdict")
	}

	for _, tc := range []struct {
		name, field, want string
	}{
		{"explicit false", `"timed_out": false,`, ""},
		{"missing", "", "missing or null timed_out"},
		{"null", `"timed_out": null,`, "missing or null timed_out"},
		{"timed out", `"timed_out": true,`, "require timed_out to be false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "capacity.json")
			if err := os.WriteFile(path, []byte(before+tc.field+after), 0600); err != nil {
				t.Fatal(err)
			}
			err := run(t.Context(), []string{"-validate-cutover", path})
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("validation error = %v, want %q", err, tc.want)
			}
		})
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

// The release verifier requests a report encoded by the producer for
// cross-command contract tests, using a path in its own test directory.
func TestWriteCutoverFixture(t *testing.T) {
	path := os.Getenv("BOG_CAPACITY_TEST_REPORT")
	if path == "" {
		t.Skip("release verifier did not request a capacity test report")
	}
	if err := writeReport(path, passingCutoverReport()); err != nil {
		t.Fatal(err)
	}
}
