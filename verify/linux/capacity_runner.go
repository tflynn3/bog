package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	capacityCaseName             = "retained_history_capacity"
	smallReleaseRows             = 196608
	largeReleaseRows             = 393216
	committedWorkloadFailureExit = 20

	runnerFailure       = "infrastructure"
	workloadFailure     = "candidate_workload"
	addressSpaceFailure = "verifier_address_space"
	cancelledFailure    = "cancelled"
)

var releaseScales = [...]int{smallReleaseRows, largeReleaseRows}

type runnerReport struct {
	SchemaVersion int             `json:"schema_version"`
	Revision      string          `json:"revision"`
	Fixture       string          `json:"fixture"`
	BinarySHA256  string          `json:"binary_sha256,omitempty"`
	Status        string          `json:"status"`
	FailureKind   string          `json:"failure_kind,omitempty"`
	Detail        string          `json:"detail,omitempty"`
	Probe         runnerProbe     `json:"preflight"`
	Capacity      *runnerCapacity `json:"capacity,omitempty"`
}

type runnerCapacity struct {
	ExitCode        *int              `json:"producer_exit_code"`
	Signal          string            `json:"producer_exit_signal,omitempty"`
	Output          string            `json:"producer_output,omitempty"`
	OutputTruncated bool              `json:"producer_output_truncated,omitempty"`
	ParentOOMDelta  map[string]uint64 `json:"delegation_oom_event_deltas,omitempty"`
	AccountingError string            `json:"accounting_error,omitempty"`
	Scales          [2]scaleProgress  `json:"scales"`
}

type scaleProgress struct {
	SeedRows      int               `json:"seed_rows"`
	Status        string            `json:"status"`
	Phase         string            `json:"phase,omitempty"`
	FailureKind   string            `json:"failure_kind,omitempty"`
	MemoryPeak    *uint64           `json:"memory_peak_bytes"`
	OOMDelta      map[string]uint64 `json:"oom_event_deltas,omitempty"`
	PeakSource    string            `json:"peak_source,omitempty"`
	OOMSource     string            `json:"oom_source,omitempty"`
	IngestSamples *int              `json:"ingest_samples,omitempty"`
	QuerySamples  *int              `json:"query_samples,omitempty"`
	RecoveredRows *int              `json:"recovered_rows,omitempty"`
	TimedOut      *bool             `json:"timed_out,omitempty"`
	Detail        string            `json:"detail,omitempty"`
	Cleanup       *producerCleanup  `json:"cleanup,omitempty"`
}

func newRunnerCapacity() *runnerCapacity {
	evidence := &runnerCapacity{}
	for i, rows := range releaseScales {
		evidence.Scales[i] = scaleProgress{SeedRows: rows, Status: "unobserved"}
	}
	return evidence
}

type producerCleanup struct {
	Status          string `json:"status"`
	Detail          string `json:"detail,omitempty"`
	CandidateReaped *bool  `json:"candidate_reaped,omitempty"`
	SubtreeEmpty    *bool  `json:"subtree_empty,omitempty"`
	SubtreeRemoved  *bool  `json:"subtree_removed,omitempty"`
	StoreRemoved    *bool  `json:"store_removed,omitempty"`
}

type producerRecord struct {
	Fixture struct {
		SeedRows int `json:"seed_rows"`
	} `json:"fixture"`
	Status        string            `json:"status"`
	Workload      string            `json:"workload_status"`
	Phase         string            `json:"phase"`
	Detail        string            `json:"detail"`
	TimedOut      *bool             `json:"timed_out"`
	IngestSamples *int              `json:"ingest_samples"`
	QuerySamples  *int              `json:"query_samples"`
	RecoveredRows *int              `json:"recovered_rows"`
	MemoryPeak    *uint64           `json:"cgroup_peak_memory_bytes"`
	Cleanup       *producerCleanup  `json:"cleanup"`
	Unavailable   map[string]string `json:"unavailable"`
	Diagnostics   struct {
		MemoryEvents map[string]uint64 `json:"cgroup_memory_events"`
	} `json:"diagnostics"`
}

func (r *runnerReport) readCapacity(ctx context.Context, data []byte, exitCode int) (string, error) {
	var wire struct {
		SchemaVersion int    `json:"schema_version"`
		Revision      string `json:"revision"`
		SHA256        string `json:"binary_sha256"`
		Fixture       string `json:"fixture"`
		Mode          string `json:"mode"`
		Cases         map[string]struct {
			Status   string           `json:"status"`
			Detail   string           `json:"detail"`
			Evidence []producerRecord `json:"evidence"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return runnerFailure, fmt.Errorf("decode capacity producer report: %w", err)
	}
	if wire.SchemaVersion != 1 || wire.Revision != r.Revision || wire.SHA256 != r.BinarySHA256 ||
		wire.Fixture != r.Fixture || wire.Mode != "live" {
		return runnerFailure, errors.New("capacity producer report identity does not match the pinned native candidate")
	}
	c, ok := wire.Cases[capacityCaseName]
	if !ok {
		return runnerFailure, errors.New("capacity producer report is missing the live case")
	}
	scales := r.Capacity.Scales
	seen := make(map[int]bool, len(scales))
	haveFailedRecord := false
	haveWorkloadFailure := false
	for _, record := range c.Evidence {
		index := 0
		rows := record.Fixture.SeedRows
		switch rows {
		case smallReleaseRows:
		case largeReleaseRows:
			index = 1
		default:
			return runnerFailure, fmt.Errorf("unexpected capacity scale %d", rows)
		}
		if seen[rows] {
			return runnerFailure, fmt.Errorf("duplicate capacity scale %d", rows)
		}
		seen[rows] = true
		switch record.Workload {
		case "", "pass", "fail", "skip":
		default:
			return runnerFailure, fmt.Errorf("unexpected capacity workload status %q for scale %d", record.Workload, rows)
		}
		switch record.Status {
		case "pass":
			if record.Workload != "" && record.Workload != "pass" ||
				record.Cleanup != nil && record.Cleanup.Status == "fail" {
				return runnerFailure, fmt.Errorf("contradictory passing capacity record for scale %d", rows)
			}
		case "fail":
			if record.Phase == "" {
				return runnerFailure, fmt.Errorf("failed capacity record has no phase for scale %d", rows)
			}
			haveFailedRecord = true
		default:
			return runnerFailure, fmt.Errorf("unexpected capacity record status %q for scale %d", record.Status, rows)
		}
		scales[index].apply(record)
		haveWorkloadFailure = haveWorkloadFailure || scales[index].FailureKind == workloadFailure
	}
	switch exitCode {
	case 0:
		if c.Status != "pass" || len(seen) != len(scales) || haveFailedRecord {
			return runnerFailure, fmt.Errorf("expected passing live capacity evidence (case %q, %d of %d scales): %s",
				c.Status, len(seen), len(scales), c.Detail)
		}
	case committedWorkloadFailureExit:
		if c.Status != "fail" || !haveWorkloadFailure {
			return runnerFailure, fmt.Errorf("capacity producer exit %d requires a failed live case with candidate workload failure: %s",
				exitCode, c.Detail)
		}
	default:
		return runnerFailure, fmt.Errorf("capacity producer exit %d cannot attest to proof publication", exitCode)
	}
	if err := ctx.Err(); err != nil {
		return cancelledFailure, err
	}
	r.Capacity.Scales = scales
	if exitCode == committedWorkloadFailureExit {
		return workloadFailure, fmt.Errorf("live capacity case failed: %s", c.Detail)
	}
	return "", nil
}

func (s *scaleProgress) apply(record producerRecord) {
	s.Phase, s.Status, s.Detail = record.Phase, record.Status, record.Detail
	s.IngestSamples, s.QuerySamples, s.RecoveredRows = record.IngestSamples, record.QuerySamples, record.RecoveredRows
	s.TimedOut = record.TimedOut
	s.Cleanup = record.Cleanup
	if record.MemoryPeak != nil {
		s.MemoryPeak = record.MemoryPeak
		s.PeakSource = "producer"
	}
	// Each fixture creates a fresh kernel cgroup. Its counters start at
	// zero and cover both candidate lifetimes through final teardown.
	delta, err := oomDelta(freshOOMBaseline(), record.Diagnostics.MemoryEvents)
	if err == nil {
		s.OOMDelta, s.OOMSource = delta, "producer"
	}
	if record.Status != "pass" {
		s.FailureKind = workloadFailure
		switch {
		case record.Workload != "" && record.Workload != "fail",
			record.Phase == "provision", record.Phase == "complete":
			s.FailureKind = runnerFailure
		case delta["oom_kill"] > 0:
			// The workload ran out of its own cgroup memory.
		case record.Workload == "" && record.Cleanup != nil && record.Cleanup.Status == "fail",
			record.Phase == "startup" && strings.Contains(record.Detail, "/cgroup.procs:"),
			record.Phase == "measurement" && missingKernelAccounting(record.Unavailable):
			s.FailureKind = runnerFailure
		}
		if record.Cleanup != nil && record.Cleanup.Detail != "" {
			s.Detail = strings.TrimSpace(s.Detail + "\n" + record.Cleanup.Detail)
		}
	}
}

func missingKernelAccounting(unavailable map[string]string) bool {
	for _, name := range []string{"cgroup_peak_memory", "cgroup_oom_kill_count", "device_io"} {
		if unavailable[name] != "" {
			return true
		}
	}
	return false
}

func freshOOMBaseline() map[string]uint64 {
	return map[string]uint64{"oom": 0, "oom_kill": 0, "oom_group_kill": 0}
}

func oomDelta(before, after map[string]uint64) (map[string]uint64, error) {
	out := make(map[string]uint64)
	for _, name := range []string{"oom", "oom_kill", "oom_group_kill"} {
		base, haveBase := before[name]
		end, haveEnd := after[name]
		if name == "oom_group_kill" && (!haveBase || !haveEnd) {
			continue // This counter is kernel-version dependent.
		}
		if !haveBase || !haveEnd || end < base {
			return nil, fmt.Errorf("cannot compute %s delta from the observed counters", name)
		}
		out[name] = end - base
	}
	return out, nil
}

func verifierReserveFailure(output string) bool {
	// Inspect the producer's own output. Candidate stderr is held in the
	// producer's nested diagnostic report and has a different attribution.
	for line := range strings.SplitSeq(output, "\n") {
		switch strings.TrimSpace(line) {
		case "fatal error: failed to reserve page summary memory",
			"fatal error: memory reservation exceeds address space limit",
			"fatal error: runtime: cannot map pages in arena address space",
			"fatal error: runtime: cannot remap pages in address space":
			return true
		}
	}
	return false
}
