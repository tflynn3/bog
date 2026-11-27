package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	capacityCaseName = "retained_history_capacity"
	smallReleaseRows = 196608
	largeReleaseRows = 393216

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
}

func newRunnerCapacity() *runnerCapacity {
	evidence := &runnerCapacity{}
	for i, rows := range releaseScales {
		evidence.Scales[i] = scaleProgress{SeedRows: rows, Status: "unobserved"}
	}
	return evidence
}

type producerRecord struct {
	Fixture struct {
		SeedRows int `json:"seed_rows"`
	} `json:"fixture"`
	Status        string  `json:"status"`
	Phase         string  `json:"phase"`
	Detail        string  `json:"detail"`
	TimedOut      *bool   `json:"timed_out"`
	IngestSamples *int    `json:"ingest_samples"`
	QuerySamples  *int    `json:"query_samples"`
	RecoveredRows *int    `json:"recovered_rows"`
	MemoryPeak    *uint64 `json:"cgroup_peak_memory_bytes"`
	Cleanup       struct {
		Status string `json:"status"`
		Detail string `json:"detail"`
	} `json:"cleanup"`
	Unavailable map[string]string `json:"unavailable"`
	Diagnostics struct {
		MemoryEvents map[string]uint64 `json:"cgroup_memory_events"`
	} `json:"diagnostics"`
}

func (r *runnerReport) readCapacity(data []byte) (kind string, err error) {
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
	seen := make(map[int]bool)
	for _, record := range c.Evidence {
		index := 0
		rows := record.Fixture.SeedRows
		switch rows {
		case smallReleaseRows:
		case largeReleaseRows:
			index = 1
		default:
			err = errors.Join(err, fmt.Errorf("unexpected capacity scale %d", rows))
			continue
		}
		if seen[rows] {
			err = errors.Join(err, fmt.Errorf("duplicate capacity scale %d", rows))
			continue
		}
		seen[rows] = true
		r.Capacity.Scales[index].apply(record)
	}
	if err != nil {
		return runnerFailure, err
	}
	for _, scale := range r.Capacity.Scales {
		if scale.FailureKind != "" && (kind == "" || scale.FailureKind == runnerFailure) {
			kind = scale.FailureKind
		}
	}
	if kind != "" {
		return kind, fmt.Errorf("live capacity case failed: %s", c.Detail)
	}
	if c.Status != "pass" || len(seen) != len(releaseScales) {
		return runnerFailure, fmt.Errorf("incomplete live capacity evidence (case %q, %d of %d scales): %s",
			c.Status, len(seen), len(releaseScales), c.Detail)
	}
	return "", nil
}

func (s *scaleProgress) apply(record producerRecord) {
	s.Phase, s.Status, s.Detail = record.Phase, record.Status, record.Detail
	s.IngestSamples, s.QuerySamples, s.RecoveredRows = record.IngestSamples, record.QuerySamples, record.RecoveredRows
	s.TimedOut = record.TimedOut
	if record.MemoryPeak != nil {
		s.MemoryPeak = record.MemoryPeak
		s.PeakSource = "producer"
	}
	// Each fixture creates a fresh kernel cgroup. Its counters start at
	// zero and cover both candidate lifetimes through final teardown.
	if delta, err := oomDelta(freshOOMBaseline(), record.Diagnostics.MemoryEvents); err == nil {
		s.OOMDelta, s.OOMSource = delta, "producer"
	}
	if record.Status != "pass" {
		s.FailureKind = workloadFailure
		switch {
		case s.OOMDelta["oom_kill"] > 0:
			// The workload ran out of its own cgroup memory.
		case record.Phase == "provision", record.Cleanup.Status == "fail",
			record.Phase == "startup" && strings.Contains(record.Detail, "/cgroup.procs:"),
			record.Phase == "measurement" && missingKernelAccounting(record.Unavailable):
			s.FailureKind = runnerFailure
		case record.Status != "fail":
			s.FailureKind = runnerFailure
		}
		if record.Cleanup.Detail != "" {
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
