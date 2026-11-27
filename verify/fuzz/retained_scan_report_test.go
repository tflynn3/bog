package fuzz

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
)

const (
	scanMemorySeed = "retained-scan-memory-v3"
	// Per reader, allow segment pages, overflow scratch and the copied
	// line (about 12 MiB together), plus cursor and allocation headroom.
	scanReaderHeapBytes = 16 << 20
)

func scanHeapLimit(count int) uint64 { return uint64(count) * scanReaderHeapBytes }

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

const scanMemoryReportPrefix = "retained_scan_memory "

type scanMemoryReport struct {
	Seed                  string            `json:"seed"`
	Arch                  string            `json:"arch"`
	GoVersion             string            `json:"go_version"`
	Fixture               scanMemoryFixture `json:"fixture"`
	Concurrency           int               `json:"concurrency"`
	ConfiguredConcurrency int               `json:"configured_query_concurrency"`
	ProcessMemory         scanProcessMemory `json:"workload_process_memory"`
	PeakRSSUnavailable    string            `json:"peak_rss_unavailable,omitempty"`
	PeakRSS               uint64            `json:"peak_rss_bytes"`
	CgroupPeak            *uint64           `json:"cgroup_peak_memory_bytes"`
	CgroupUnavailable     string            `json:"cgroup_peak_unavailable,omitempty"`
	ReaderHeapLimit       uint64            `json:"reader_heap_limit_bytes"`
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
	RejectionChecks       int               `json:"rejection_checks"`
	RetryRows             int               `json:"exact_retry_rows"`
	GenerationDrained     bool              `json:"generation_drained"`
}

func (r scanMemoryReport) validate(isolatedCgroup bool) error {
	if r.Seed != scanMemorySeed || r.Arch == "" || r.GoVersion == "" || r.ConfiguredConcurrency < 1 ||
		(r.Concurrency != 1 && r.Concurrency != r.ConfiguredConcurrency) {
		return errors.New("invalid scan memory report identity")
	}
	if !r.ProcessMemory.valid() || (r.PeakRSS == 0) == (r.PeakRSSUnavailable == "") {
		return errors.New("invalid process memory evidence")
	}
	if r.CatalogHeap == 0 || r.PausedHeap == 0 || r.PostCancelHeap == 0 || r.HeapSamples < 2 ||
		r.PeakHeap < max(r.CatalogHeap, r.PausedHeap) || r.PeakHeap-r.CatalogHeap != r.PeakIncrementalHeap {
		return errors.New("invalid scan memory samples")
	}
	if r.ReaderHeapLimit != scanHeapLimit(r.Concurrency) || r.PeakIncrementalHeap > r.ReaderHeapLimit {
		return fmt.Errorf("%w: incremental heap %d, limit %d", errScanReaderBudget, r.PeakIncrementalHeap, r.ReaderHeapLimit)
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
		r.RetryRows != r.Fixture.Rows || r.RejectionChecks != 3 || !r.GenerationDrained {
		return errors.New("incomplete scan cleanup or retry evidence")
	}
	return nil
}

func decodeScanMemoryReport(output []byte) (scanMemoryReport, error) {
	var report scanMemoryReport
	found := false
	for line := range bytes.Lines(output) {
		data, ok := bytes.CutPrefix(bytes.TrimSpace(line), []byte(scanMemoryReportPrefix))
		if !ok {
			continue
		}
		if found {
			return report, errors.New("duplicate scan memory worker report")
		}
		found = true
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&report); err != nil {
			return report, fmt.Errorf("decode scan memory worker report: %w", err)
		}
		if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
			return report, fmt.Errorf("unexpected data after scan memory worker report: %v", err)
		}
	}
	if !found {
		return report, fmt.Errorf("missing scan memory worker report in output: %s", output)
	}
	return report, nil
}

func TestScanMemoryReport(t *testing.T) {
	cgroupPeak := uint64(64 << 20)
	base := scanMemoryReport{
		Seed: scanMemorySeed, Arch: "arm64", GoVersion: "go1.27.0",
		Concurrency: 8, ConfiguredConcurrency: 8, Fixture: scanMemoryFixture{Rows: 3070},
		PeakRSS: 62 << 20, ProcessMemory: scanProcessMemory{VirtualBytes: 4 << 40, RSSBytes: 60 << 20},
		ReaderHeapLimit: scanHeapLimit(8), CgroupPeak: &cgroupPeak, CatalogHeap: 5 << 20, PeakHeap: 52 << 20,
		PeakIncrementalHeap: 47 << 20, PausedHeap: 50 << 20, PostCancelHeap: 5 << 20, HeapSamples: 9,
		CatalogFDs: 2, PausedFDs: 2, PostCancelFDs: 2, PausedCaptures: 8, CanceledScans: 8,
		RetryRows: 3070, RejectionChecks: 3, GenerationDrained: true,
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
		{name: "unavailable process", isolated: true, valid: true, change: func(r *scanMemoryReport) {
			r.ProcessMemory = scanProcessMemory{Unavailable: "proc status unavailable"}
			r.PeakRSS = 0
			r.PeakRSSUnavailable = "missing VmHWM"
		}},
		{name: "unexplained RSS", isolated: true, change: func(r *scanMemoryReport) { r.PeakRSS = 0 }},
		{name: "heap budget exceeded", isolated: true, change: func(r *scanMemoryReport) {
			r.PeakIncrementalHeap = r.ReaderHeapLimit + 1
			r.PeakHeap = r.CatalogHeap + r.PeakIncrementalHeap
		}},
		{name: "shared peak", isolated: false},
		{name: "missing peak", isolated: true, change: func(r *scanMemoryReport) { r.CgroupPeak = nil }},
		{name: "wrong scan count", isolated: true, change: func(r *scanMemoryReport) { r.Concurrency = 3 }},
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
