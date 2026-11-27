package fuzz

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"
)

func TestScanAllocationDelta(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after scanMemoryStats
		want          uint64
		invalid       bool
	}{
		{"fixture and catalog excluded", scanMemoryStats{20, 250}, scanMemoryStats{80, 280}, 30, false},
		{"GC during scan", scanMemoryStats{80, 280}, scanMemoryStats{40, 310}, 30, false},
		{"no allocations", scanMemoryStats{80, 280}, scanMemoryStats{40, 280}, 0, false},
		{"invalid counter", scanMemoryStats{80, 280}, scanMemoryStats{40, 279}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scanAllocationDelta(tc.before, tc.after)
			if got != tc.want || (err != nil) != tc.invalid {
				t.Fatalf("allocation delta = %d, %v", got, err)
			}
		})
	}
}

func TestScanMemoryPhaseBudget(t *testing.T) {
	const readers = 8
	limits := scanMemoryLimits(readers)
	if limits != (scanMemoryBudget{128 << 20, 160 << 20, 192 << 20}) {
		t.Fatalf("eight-reader limits = %+v", limits)
	}
	for _, tc := range []struct {
		name                  string
		live, peak, allocated uint64
		wantBudgetError       bool
		wantOtherError        bool
	}{
		{"at limit", 500 + limits.LiveHeap, 500 + limits.IncrementalHeap, limits.Allocated, false, false},
		{"sampled heap one over", 500 + limits.LiveHeap, 501 + limits.IncrementalHeap, limits.Allocated, true, false},
		{"allocation one over", 500 + limits.LiveHeap, 500 + limits.IncrementalHeap, limits.Allocated + 1, true, false},
		{"live heap one over", 501 + limits.LiveHeap, 500 + limits.IncrementalHeap, limits.Allocated, true, false},
		{"invalid baseline", 499, 499, limits.Allocated, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := scanMemoryPhaseReport{Phase: scanSuccess, Limits: limits, BaselineHeap: 500, PausedHeap: tc.live, PeakHeap: tc.peak, Allocated: tc.allocated}
			err := r.checkBudget()
			if errors.Is(err, errScanWorkerBudget) != tc.wantBudgetError || (err != nil) != (tc.wantBudgetError || tc.wantOtherError) {
				t.Fatalf("checkBudget = %v", err)
			}
		})
	}
}

func validScanMemoryReport() scanMemoryReport {
	const readers = 8
	r := scanMemoryReport{
		Seed: scanMemorySeed, Arch: "arm64", GoVersion: "go1.27.0",
		Concurrency: readers, ConfiguredConcurrency: readers, Fixture: scanMemoryFixture{Rows: 3070, MaxLineBytes: 4 << 20},
		Startup: scanMemoryStartup{GoAllocated: 20 << 20, GoAllocLimit: scanMemoryStartupAllocLimit,
			CatalogHeap: 5 << 20, PeakRSS: 35 << 20, VirtualMemory: 2 << 40},
		PeakRSS: 125 << 20, CgroupUnavailable: "read-only cgroup mount", CatalogFDs: 2, GenerationDrained: true,
	}
	for _, phase := range scanMemoryPhases {
		rows, _ := phase.outcome(r.Fixture.Rows)
		r.Phases = append(r.Phases, scanMemoryPhaseReport{
			Phase: phase, WorkLimit: scanMemoryWorkLimit(phase, r.Fixture.MaxLineBytes), Limits: scanMemoryLimits(readers),
			BaselineHeap: 5 << 20, PeakHeap: 112 << 20, PausedHeap: 72 << 20, PostHeap: 5 << 20,
			Allocated: 114 << 20, HeapSamples: 3, PausedFDs: 2, PostFDs: 2, PausedCaptures: readers,
			PausedPins: 1, PostPins: 1, RowsPerReader: rows, CompletedReaders: readers,
		})
	}
	return r
}

func TestScanMemoryReport(t *testing.T) {
	base := validScanMemoryReport()
	for _, tc := range []struct {
		name   string
		mutate func(*scanMemoryReport)
	}{
		{"missing startup accounting", func(r *scanMemoryReport) { r.Startup.PeakRSS = 0 }},
		{"startup allocation over budget", func(r *scanMemoryReport) { r.Startup.GoAllocated = r.Startup.GoAllocLimit + 1 }},
		{"process peak smaller than startup RSS", func(r *scanMemoryReport) { r.PeakRSS = r.Startup.PeakRSS - 1 }},
		{"missing cgroup evidence", func(r *scanMemoryReport) { r.CgroupUnavailable = "" }},
		{"wrong concurrency", func(r *scanMemoryReport) { r.Concurrency = 3 }},
		{"phase missing", func(r *scanMemoryReport) { r.Phases = r.Phases[:3] }},
		{"phase order", func(r *scanMemoryReport) { r.Phases[0], r.Phases[1] = r.Phases[1], r.Phases[0] }},
		{"short retry", func(r *scanMemoryReport) { r.Phases[3].RowsPerReader-- }},
		{"parser allowed extra row", func(r *scanMemoryReport) { r.Phases[2].RowsPerReader++ }},
		{"parser work budget changed", func(r *scanMemoryReport) { r.Phases[2].WorkLimit++ }},
		{"short cancellation join", func(r *scanMemoryReport) { r.Phases[1].CompletedReaders-- }},
		{"growth beyond budget", func(r *scanMemoryReport) { r.Phases[0].Allocated = r.Phases[0].Limits.Allocated + 1 }},
		{"overflow row unaccounted", func(r *scanMemoryReport) { r.Phases[0].PausedHeap = r.Phases[0].BaselineHeap }},
		{"phase descriptor leak", func(r *scanMemoryReport) { r.Phases[1].PostFDs++ }},
		{"phase capture leak", func(r *scanMemoryReport) { r.Phases[1].PostCaptures++ }},
		{"phase pin leak", func(r *scanMemoryReport) { r.Phases[1].PostPins++ }},
		{"catalog descriptor leak", func(r *scanMemoryReport) { r.FinalFDs++ }},
		{"catalog pin leak", func(r *scanMemoryReport) { r.GenerationDrained = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			r.Phases = slices.Clone(base.Phases)
			tc.mutate(&r)
			if err := r.validate(false); err == nil {
				t.Fatal("accepted invalid worker evidence")
			}
		})
	}
	if err := base.validate(false); err != nil {
		t.Fatal(err)
	}
	peak := uint64(140 << 20)
	base.CgroupPeak, base.CgroupUnavailable = &peak, ""
	if err := base.validate(true); err != nil {
		t.Fatal(err)
	}
	if err := base.validate(false); err == nil {
		t.Fatal("accepted an isolated cgroup peak without an isolated cgroup")
	}
}

func TestScanMemoryWorkerVerdict(t *testing.T) {
	base := validScanMemoryReport()
	line := func(prefix string, data any) string {
		wire, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return prefix + string(wire) + "\n"
	}
	ready := line(scanMemoryReadyPrefix, base.Startup)
	report := line(scanMemoryReportPrefix, base)
	budget := line(scanMemoryBudgetPrefix, "heap exceeded")
	changed := base
	changed.Startup.PeakRSS++
	exit := errors.New("child exit")
	for _, tc := range []struct {
		name, output  string
		process, want error
	}{
		{"clean", "unrelated log\n" + ready + report + "PASS\n", nil, nil},
		{"failed exec", "", exit, errScanWorkerStartup},
		{"startup allocator failure", "runtime: cannot allocate memory\n", exit, errScanWorkerStartup},
		{"malformed readiness", scanMemoryReadyPrefix + `{"bogus":true}`, exit, errScanWorkerStartup},
		{"budget marker without readiness", budget, exit, errScanWorkerStartup},
		{"budget after readiness", ready + budget, exit, errScanWorkerBudget},
		{"budget marker with zero exit", ready + budget, nil, errScanWorkerRun},
		{"unclassified crash after readiness", ready + "cannot allocate memory\n", exit, errScanWorkerRun},
		{"failed process with success report", ready + report, exit, errScanWorkerRun},
		{"duplicate ready", ready + ready + report, nil, errScanWorkerRun},
		{"duplicate report", ready + report + report, nil, errScanWorkerRun},
		{"mixed outcomes", ready + budget + report, exit, errScanWorkerRun},
		{"inconsistent startup", ready + line(scanMemoryReportPrefix, changed), nil, errScanWorkerRun},
		{"incomplete report", ready + report[:len(report)-2], nil, errScanWorkerRun},
		{"trailing JSON", ready + report[:len(report)-1] + " {}", nil, errScanWorkerRun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scanMemoryVerdict([]byte(tc.output), tc.process)
			for _, kind := range []error{errScanWorkerStartup, errScanWorkerBudget, errScanWorkerRun} {
				if errors.Is(err, kind) != (tc.want == kind) {
					t.Fatalf("verdict = %v, want category %v", err, tc.want)
				}
			}
			if tc.process != nil && !errors.Is(err, tc.process) {
				t.Fatalf("verdict %v lost process error %v", err, tc.process)
			}
			if tc.want == nil {
				if err != nil || got.Startup != base.Startup || !slices.Equal(got.Phases, base.Phases) {
					t.Fatalf("verdict = %+v, %v", got, err)
				}
			}
		})
	}
}
