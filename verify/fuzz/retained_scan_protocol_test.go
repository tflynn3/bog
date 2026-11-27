package fuzz

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	scanStartupVersion = 1
	scanStartupBytes   = 4096
	scanStartByte      = 'S'
	scanWorkerPrefix   = "retained_scan_worker "
	scanBudgetMarker   = "retained_scan_failure reader_budget"
)

type scanWorkerOutcome string

const (
	scanWorkerPassed    scanWorkerOutcome = "passed"
	scanWorkerStartup   scanWorkerOutcome = "startup_failure"
	scanWorkerSanitizer scanWorkerOutcome = "sanitizer_mapping_failure"
	scanWorkerBudget    scanWorkerOutcome = "reader_budget_failure"
	scanWorkerFailed    scanWorkerOutcome = "reader_failure"
)

type scanProcessMemory struct {
	VirtualBytes uint64 `json:"virtual_bytes"`
	RSSBytes     uint64 `json:"rss_bytes"`
	Unavailable  string `json:"unavailable,omitempty"`
}

func (m scanProcessMemory) valid() bool {
	if m.Unavailable != "" {
		return m.VirtualBytes == 0 && m.RSSBytes == 0
	}
	return m.RSSBytes > 0 && m.VirtualBytes >= m.RSSBytes
}

type scanWorkerReady struct {
	Version   int               `json:"version"`
	PID       int               `json:"pid"`
	Readers   int               `json:"readers"`
	Arch      string            `json:"arch"`
	GoVersion string            `json:"go_version"`
	Race      bool              `json:"race"`
	Memory    scanProcessMemory `json:"startup_process_memory"`
}

type scanWorkerEvidence struct {
	Outcome             scanWorkerOutcome `json:"outcome"`
	Readers             int               `json:"readers"`
	Ready               *scanWorkerReady  `json:"ready"`
	StartupUnavailable  string            `json:"startup_measurement_unavailable,omitempty"`
	Started             bool              `json:"started"`
	WorkloadUnavailable string            `json:"workload_measurement_unavailable,omitempty"`
	Reaped              bool              `json:"reaped"`
	CgroupPeak          *uint64           `json:"cgroup_peak_memory_bytes"`
	CgroupUnavailable   string            `json:"cgroup_peak_unavailable,omitempty"`
}

func readScanWorkerReady(r io.Reader, want scanWorkerReady) (scanWorkerReady, error) {
	var ready scanWorkerReady
	data, err := io.ReadAll(io.LimitReader(r, scanStartupBytes+1))
	if err != nil {
		return ready, fmt.Errorf("read worker readiness: %w", err)
	}
	if len(data) > scanStartupBytes {
		return ready, fmt.Errorf("worker readiness exceeds %d bytes", scanStartupBytes)
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&ready); err != nil {
		return ready, fmt.Errorf("decode worker readiness: %w", err)
	}
	if err := d.Decode(new(json.RawMessage)); err != io.EOF {
		return ready, fmt.Errorf("worker readiness requires exactly one record: trailing decode returned %v", err)
	}
	if ready.Version != scanStartupVersion || ready.PID != want.PID || ready.Readers != want.Readers ||
		ready.Arch != want.Arch || ready.GoVersion != want.GoVersion || ready.Race != want.Race || !ready.Memory.valid() {
		return ready, fmt.Errorf("invalid worker readiness: %+v", ready)
	}
	return ready, nil
}

func scanOutcome(started bool, output []byte, err error) scanWorkerOutcome {
	if err == nil {
		return scanWorkerPassed
	}
	for line := range bytes.Lines(output) {
		line := strings.ToLower(string(line))
		if (strings.Contains(line, "threadsanitizer") &&
			(strings.Contains(line, "unexpected memory mapping") || strings.Contains(line, "failed to mmap") ||
				strings.Contains(line, "failed to allocate") || strings.Contains(line, "can not mmap") ||
				strings.Contains(line, "cannot mmap") || strings.Contains(line, "memory layout is incompatible"))) ||
			(strings.Contains(line, "sanitizer") && strings.Contains(line, "check failed") &&
				(strings.Contains(line, "allocator") || strings.Contains(line, "memory mapping") || strings.Contains(line, "addr_no_randomize"))) {
			return scanWorkerSanitizer
		}
	}
	if !started {
		return scanWorkerStartup
	}
	if errors.Is(err, errScanReaderBudget) {
		return scanWorkerBudget
	}
	for line := range bytes.Lines(output) {
		if string(bytes.TrimSpace(line)) == scanBudgetMarker {
			return scanWorkerBudget
		}
	}
	return scanWorkerFailed
}

var errScanReaderBudget = errors.New("retained reader resource budget exceeded")
