package fuzz

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"
)

const scanMemorySeed = "retained-scan-memory-v2"

type scanMemoryFixture struct {
	Generation      string `json:"generation"`
	Bytes           int64  `json:"fixture_bytes"`
	Files           int    `json:"fixture_files"`
	Rows            int    `json:"rows"`
	Segments        int    `json:"segments"`
	PageBytes       int    `json:"page_bytes"`
	MaxLineBytes    int    `json:"max_line_bytes"`
	CoveredWALBytes int64  `json:"covered_wal_bytes"`
}

const (
	scanMemoryReportPrefix  = "retained_scan_memory "
	scanMemoryStartupPrefix = "retained_scan_startup "
	scanMemoryReaders       = 8
	scanMemoryPageBytes     = 4 << 10
	scanMemoryJoinDeadline  = 12 * time.Second

	// Go heap arenas, race shadow/metadata, and OS thread stacks can
	// reserve more mappings after startup. This is a virtual address
	// space allowance. The workload limits below meter Go allocations.
	scanMemoryAddressGrowth  = 4 << 30
	scanMemoryDeadline       = 30 * time.Second
	scanMemoryWorkPerReader  = 16 << 20
	scanMemoryHeapPerReader  = 16 << 20
	scanMemoryAllocBase      = 48 << 20
	scanMemoryAllocPerReader = 20 << 20
)

type scanMemoryStartup struct {
	AddressBytes uint64 `json:"startup_address_space_bytes"`
	AddressLimit uint64 `json:"address_space_limit_bytes"`
	FileLimit    uint64 `json:"file_size_limit_bytes"`
}

func (s scanMemoryStartup) validate() error {
	if s.AddressBytes == 0 || s.AddressLimit <= s.AddressBytes || s.AddressLimit-s.AddressBytes != scanMemoryAddressGrowth ||
		s.FileLimit == 0 || s.FileLimit > scanMemoryFileLimit {
		return fmt.Errorf("invalid scan startup limits: %+v", s)
	}
	return nil
}

const scanMemoryFileLimit = 1 << 20

type scanMemoryLimits struct {
	WorkPerReader uint64 `json:"work_units_per_reader"`
	HeapGrowth    uint64 `json:"incremental_heap_limit_bytes"`
	Allocated     uint64 `json:"allocation_limit_bytes"`
	DeadlineNS    int64  `json:"deadline_ns"`
}

func scanMemoryBudget(readers int) scanMemoryLimits {
	return scanMemoryLimits{
		WorkPerReader: scanMemoryWorkPerReader,
		HeapGrowth:    uint64(readers) * scanMemoryHeapPerReader,
		Allocated:     scanMemoryAllocBase + uint64(readers)*scanMemoryAllocPerReader,
		DeadlineNS:    int64(scanMemoryDeadline),
	}
}

func checkScanMemoryBytes(resource string, before, after, limit uint64) error {
	if after < before {
		return fmt.Errorf("invalid %s counters: before=%d after=%d", resource, before, after)
	}
	if after-before > limit {
		return fmt.Errorf("%w: %s used %d bytes, limit %d", errReplayLimit, resource, after-before, limit)
	}
	return nil
}

type scanMemoryReport struct {
	Seed                  string            `json:"seed"`
	Arch                  string            `json:"arch"`
	GoVersion             string            `json:"go_version"`
	Fixture               scanMemoryFixture `json:"fixture"`
	Concurrency           int               `json:"concurrency"`
	ConfiguredConcurrency int               `json:"configured_query_concurrency"`
	PeakRSS               uint64            `json:"peak_rss_bytes"`
	CgroupPeak            *uint64           `json:"cgroup_peak_memory_bytes"`
	CgroupUnavailable     string            `json:"cgroup_peak_unavailable,omitempty"`
	Limits                scanMemoryLimits  `json:"workload_limits"`
	AllocatedBytes        uint64            `json:"allocated_bytes"`
	ElapsedNS             int64             `json:"elapsed_ns"`
	WorkRejections        int               `json:"work_budget_rejections"`
	CatalogHeap           uint64            `json:"catalog_heap_bytes"`
	PeakHeap              uint64            `json:"peak_sampled_heap_bytes"`
	PeakIncrementalHeap   uint64            `json:"peak_incremental_live_heap_bytes"`
	PausedHeap            uint64            `json:"paused_post_gc_heap_bytes"`
	PostCancelHeap        uint64            `json:"post_cancel_post_gc_heap_bytes"`
	HeapSamples           int               `json:"heap_samples"`
	CatalogFDs            int               `json:"catalog_fixture_fds"`
	PausedFDs             int               `json:"paused_fixture_fds"`
	PostCancelFDs         int               `json:"post_cancel_fixture_fds"`
	FinalFDs              int               `json:"final_fixture_fds"`
	PausedCaptures        uint64            `json:"paused_capture_reservations"`
	PostCancelCaptures    uint64            `json:"post_cancel_capture_reservations"`
	FinalPins             uint64            `json:"final_generation_pin_reservations"`
	CanceledScans         int               `json:"canceled_scans"`
	RetryRows             int               `json:"exact_retry_rows"`
	GenerationDrained     bool              `json:"generation_drained"`
}

func (r scanMemoryReport) validate(isolatedCgroup bool) error {
	if r.Seed != scanMemorySeed || r.Arch == "" || r.GoVersion == "" || r.ConfiguredConcurrency != scanMemoryReaders ||
		(r.Concurrency != 1 && r.Concurrency != r.ConfiguredConcurrency) {
		return errors.New("invalid scan memory report identity")
	}
	if r.Limits != scanMemoryBudget(r.Concurrency) || r.AllocatedBytes == 0 || r.ElapsedNS <= 0 ||
		r.ElapsedNS > r.Limits.DeadlineNS || r.WorkRejections != 1 {
		return errors.New("invalid scan workload budget evidence")
	}
	if err := checkScanMemoryBytes("workload allocation", 0, r.AllocatedBytes, r.Limits.Allocated); err != nil {
		return err
	}
	if err := checkScanMemoryBytes("sampled scan heap", r.CatalogHeap, r.PeakHeap, r.Limits.HeapGrowth); err != nil {
		return err
	}
	if r.PeakRSS == 0 || r.CatalogHeap == 0 || r.PausedHeap == 0 || r.PostCancelHeap == 0 || r.HeapSamples < 2 ||
		r.PeakHeap < max(r.CatalogHeap, r.PausedHeap) || r.PeakHeap-r.CatalogHeap != r.PeakIncrementalHeap {
		return errors.New("invalid scan memory samples")
	}
	if r.Fixture.MaxLineBytes <= r.Fixture.PageBytes || r.Fixture.PageBytes != scanMemoryPageBytes ||
		r.PausedHeap < r.CatalogHeap || (r.PausedHeap-r.CatalogHeap)/uint64(r.Concurrency) < uint64(r.Fixture.MaxLineBytes) {
		return errors.New("paused heap does not account for all retained overflow lines")
	}
	if isolatedCgroup {
		if r.CgroupPeak == nil || *r.CgroupPeak == 0 || r.CgroupUnavailable != "" {
			return errors.New("missing isolated cgroup peak")
		}
	} else if r.CgroupPeak != nil || r.CgroupUnavailable == "" {
		return errors.New("invalid cgroup unavailability evidence")
	}
	if r.CatalogFDs != 2 || r.PausedFDs != r.CatalogFDs || r.PostCancelFDs != r.CatalogFDs || r.FinalFDs != 0 ||
		r.PausedCaptures != uint64(r.Concurrency) || r.PostCancelCaptures != 0 || r.FinalPins != 0 || r.CanceledScans != r.Concurrency ||
		r.RetryRows != r.Fixture.Rows || !r.GenerationDrained {
		return errors.New("incomplete scan cleanup or retry evidence")
	}
	return nil
}

func decodeScanMemoryReport(output []byte) (scanMemoryReport, error) {
	var report scanMemoryReport
	err := decodeScanMemoryRecord(output, scanMemoryReportPrefix, &report)
	return report, err
}

func decodeScanMemoryRecord(output []byte, prefix string, record any) error {
	if len(output) > replayWorkerLogBytes {
		return fmt.Errorf("%w: scan worker output exceeds %d bytes", errReplayLimit, replayWorkerLogBytes)
	}
	found := false
	for line := range bytes.Lines(output) {
		data, ok := bytes.CutPrefix(bytes.TrimSpace(line), []byte(prefix))
		if !ok {
			continue
		}
		if found {
			return fmt.Errorf("duplicate worker record %q", prefix)
		}
		found = true
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(record); err != nil {
			return fmt.Errorf("decode worker record %q: %w", prefix, err)
		}
		if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
			return fmt.Errorf("unexpected data after worker record %q: %v", prefix, err)
		}
	}
	if !found {
		return fmt.Errorf("missing worker record %q", prefix)
	}
	return nil
}

func TestScanMemoryReport(t *testing.T) {
	cgroupPeak := uint64(64 << 20)
	base := scanMemoryReport{
		Seed: scanMemorySeed, Arch: "arm64", GoVersion: "go1.27.0",
		Concurrency: 8, ConfiguredConcurrency: 8, Fixture: scanMemoryFixture{Rows: 3070, MaxLineBytes: 4 << 20, PageBytes: 4 << 10},
		Limits: scanMemoryBudget(8), AllocatedBytes: 128 << 20, ElapsedNS: int64(time.Second), WorkRejections: 1,
		PeakRSS: 62 << 20, CgroupPeak: &cgroupPeak, CatalogHeap: 5 << 20, PeakHeap: 52 << 20,
		PeakIncrementalHeap: 47 << 20, PausedHeap: 50 << 20, PostCancelHeap: 5 << 20, HeapSamples: 9,
		CatalogFDs: 2, PausedFDs: 2, PostCancelFDs: 2, PausedCaptures: 8, CanceledScans: 8,
		RetryRows: 3070, GenerationDrained: true,
	}
	for _, tc := range []struct {
		name     string
		change   func(*scanMemoryReport)
		isolated bool
		valid    bool
	}{
		{name: "isolated", isolated: true, valid: true},
		{name: "unavailable", valid: true, change: func(r *scanMemoryReport) {
			r.CgroupPeak = nil
			r.CgroupUnavailable = "read-only cgroup mount"
		}},
		{name: "shared peak", isolated: false},
		{name: "missing peak", isolated: true, change: func(r *scanMemoryReport) { r.CgroupPeak = nil }},
		{name: "wrong scan count", isolated: true, change: func(r *scanMemoryReport) { r.Concurrency = 3 }},
		{name: "missing budget", isolated: true, change: func(r *scanMemoryReport) { r.Limits = scanMemoryLimits{} }},
		{name: "allocation overage", isolated: true, change: func(r *scanMemoryReport) { r.AllocatedBytes = r.Limits.Allocated + 1 }},
		{name: "deadline overage", isolated: true, change: func(r *scanMemoryReport) { r.ElapsedNS = r.Limits.DeadlineNS + 1 }},
		{name: "missing clean rejection", isolated: true, change: func(r *scanMemoryReport) { r.WorkRejections = 0 }},
		{name: "heap exceeds limit", isolated: true, change: func(r *scanMemoryReport) {
			r.PeakIncrementalHeap = r.Limits.HeapGrowth + 1
			r.PeakHeap = r.CatalogHeap + r.PeakIncrementalHeap
		}},
		{name: "missing overflow heap", isolated: true, change: func(r *scanMemoryReport) { r.PausedHeap = r.CatalogHeap }},
		{name: "bad delta", isolated: true, change: func(r *scanMemoryReport) { r.PeakIncrementalHeap-- }},
		{name: "short retry", isolated: true, change: func(r *scanMemoryReport) { r.RetryRows-- }},
		{name: "open descriptor", isolated: true, change: func(r *scanMemoryReport) { r.FinalFDs++ }},
		{name: "unreleased reservation", isolated: true, change: func(r *scanMemoryReport) { r.PostCancelCaptures++ }},
		{name: "pin still held", isolated: true, change: func(r *scanMemoryReport) { r.GenerationDrained = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := base
			if tc.change != nil {
				tc.change(&report)
			}
			if err := report.validate(tc.isolated); (err == nil) != tc.valid {
				t.Fatalf("validate = %v, want valid=%v", err, tc.valid)
			}
		})
	}

	data, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	line := scanMemoryReportPrefix + string(data)
	for _, tc := range []struct {
		name, output string
		valid        bool
	}{
		{"test output", "=== RUN worker\n" + line + "\nPASS\n", true},
		{"missing", "PASS\n", false},
		{"duplicate", line + "\n" + line, false},
		{"truncated", line[:len(line)-1], false},
		{"oversized output", line + "\n" + string(bytes.Repeat([]byte{'x'}, replayWorkerLogBytes)), false},
		{"trailing JSON", line + " {}", false},
		{"unknown field", scanMemoryReportPrefix + `{"unknown":true}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeScanMemoryReport([]byte(tc.output))
			if (err == nil) != tc.valid {
				t.Fatalf("decode = %v, want valid=%v", err, tc.valid)
			}
			if tc.valid && (got.CgroupPeak == nil || *got.CgroupPeak != cgroupPeak) {
				t.Fatalf("cgroup peak = %v, want %d", got.CgroupPeak, cgroupPeak)
			}
			got.CgroupPeak = base.CgroupPeak
			if tc.valid && got != base {
				t.Fatalf("round trip = %+v, want %+v", got, base)
			}
		})
	}
}

func TestScanMemoryBudgetAccounting(t *testing.T) {
	for _, tc := range []struct {
		readers          int
		heap, allocation uint64
	}{
		{1, 16 << 20, 68 << 20},
		{8, 128 << 20, 208 << 20},
	} {
		got := scanMemoryBudget(tc.readers)
		want := scanMemoryLimits{16 << 20, tc.heap, tc.allocation, int64(30 * time.Second)}
		if got != want {
			t.Fatalf("%d readers: limits = %+v, want %+v", tc.readers, got, want)
		}
	}
	for _, tc := range []struct {
		name                 string
		before, after, limit uint64
		wantError, exhausted bool
	}{
		{"exact cap", 10, 14, 4, false, false},
		{"over cap", 10, 15, 4, true, true},
		{"counter regressed", 10, 9, 4, true, false},
		{"large baseline", math.MaxUint64 - 1, math.MaxUint64, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkScanMemoryBytes("sample", tc.before, tc.after, tc.limit)
			if (err != nil) != tc.wantError || errors.Is(err, errReplayLimit) != tc.exhausted {
				t.Fatalf("accounting returned %v, want error=%t exhaustion=%t", err, tc.wantError, tc.exhausted)
			}
		})
	}
}
