// Package capacityevidence validates the schema-v1 live retained-history proof.
package capacityevidence

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
)

const (
	FixtureID   = "linux-release-v1"
	Case        = "retained_history_capacity"
	MemoryLimit = 128 << 20
	TailRows    = 2048
	BatchRows   = 64
	LineBytes   = 1024
	SmallScale  = 196608
	LargeScale  = 393216
)

type fixture struct {
	SeedRows    int    `json:"seed_rows"`
	TailRows    int    `json:"tail_rows"`
	LineBytes   int    `json:"line_bytes"`
	BatchRows   int    `json:"batch_rows"`
	MemoryBytes uint64 `json:"memory_limit_bytes"`
}

// CompactionObservation is a snapshot of the candidate's checkpoint state and row counts.
type CompactionObservation struct {
	State         string `json:"state"`
	Generation    uint64 `json:"generation"`
	CoveredRows   int    `json:"covered_rows"`
	CommittedRows int    `json:"committed_rows"`
}

type compactionEvidence = CompactionObservation

type record struct {
	Fixture       fixture            `json:"fixture"`
	Status        string             `json:"status"`
	Workload      string             `json:"workload_status"`
	Phase         string             `json:"phase"`
	TimedOut      *bool              `json:"timed_out"`
	RecoveredRows int                `json:"recovered_rows"`
	Compaction    compactionEvidence `json:"compaction"`
	Diagnostics   *diagnostics       `json:"diagnostics"`
	Cleanup       cleanupEvidence    `json:"cleanup"`
	CgroupLimit   *uint64            `json:"cgroup_memory_limit_bytes"`
	CgroupSwap    *uint64            `json:"cgroup_swap_max_bytes"`
	CgroupOOM     *uint64            `json:"cgroup_oom_kill_count"`
	CgroupPeak    uint64             `json:"cgroup_peak_memory_bytes"`
}

type cleanupEvidence struct {
	Status          string `json:"status"`
	CandidateReaped bool   `json:"candidate_reaped"`
	SubtreeEmpty    bool   `json:"subtree_empty"`
	SubtreeRemoved  bool   `json:"subtree_removed"`
	StoreRemoved    bool   `json:"store_removed"`
	Detail          string `json:"detail"`
}

type diagnostics struct {
	Timelines []backfillEvidence `json:"backfill_timelines"`
	Restart   *restartEvidence   `json:"restart"`
	// A pointer preserves an explicitly observed zero through JSON decoding.
	MemoryEvents map[string]*uint64  `json:"cgroup_memory_events"`
	Candidates   []lifecycleEvidence `json:"candidates"`
}

type lifecycleEvidence struct {
	Lifetime            string `json:"lifetime"`
	Ready               int64  `json:"start_to_ready_ns"`
	Reaped              bool   `json:"reaped"`
	ExitedBeforeCleanup bool   `json:"exited_before_cleanup"`
	ExitCode            *int   `json:"exit_code"`
	Signal              string `json:"exit_signal"`
	WaitError           string `json:"wait_error"`
	StartupError        string `json:"startup_error"`
	LogDrainError       string `json:"log_drain_error"`
}

type report struct {
	SchemaVersion int                       `json:"schema_version"`
	Fixture       string                    `json:"fixture"`
	Mode          string                    `json:"mode"`
	Cases         map[string]jsontext.Value `json:"cases"`
}

// ValidateReport requires a complete live proof at both scales. Callers
// validate candidate identity and any companion reports at their entry point.
func ValidateReport(data []byte) error {
	return validateReport(data, false)
}

// ValidateCorrelatedReport requires the enriched proof at every scale and
// for both backfill workloads, even if all correlation fields were omitted.
func ValidateCorrelatedReport(data []byte) error {
	return validateReport(data, true)
}

func validateReport(data []byte, requireCorrelation bool) error {
	var envelope report
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode capacity report: %w", err)
	}
	if envelope.SchemaVersion != 1 || envelope.Fixture != FixtureID || envelope.Mode != "live" {
		return errors.New("cutover capacity requires a schema-v1 live linux-release-v1 report")
	}
	raw, present := envelope.Cases[Case]
	if !present {
		return fmt.Errorf("missing required case %q", Case)
	}
	var capacity struct {
		Status   string   `json:"status"`
		Evidence []record `json:"evidence"`
	}
	if err := json.Unmarshal(raw, &capacity); err != nil {
		return fmt.Errorf("decode capacity case: %w", err)
	}
	if capacity.Status != "pass" || len(capacity.Evidence) != 2 {
		return errors.New("cutover capacity requires a passing record at each live scale")
	}
	for _, r := range capacity.Evidence {
		requireCorrelation = requireCorrelation || r.hasCorrelation()
	}
	var seen [2]bool
	for _, r := range capacity.Evidence {
		size := r.Fixture.SeedRows
		index, err := scaleIndex(size)
		if err != nil {
			return err
		}
		if seen[index] {
			return fmt.Errorf("duplicate capacity fixture size %d", size)
		}
		seen[index] = true
		if err := r.validate(requireCorrelation); err != nil {
			return fmt.Errorf("capacity fixture %d: %w", size, err)
		}
	}
	return nil
}

// ValidateRecord checks the same wire proof before a producer reports a
// passing live scale. Presence-sensitive fields must reach this API as JSON.
func ValidateRecord(data []byte) error {
	return validateRecord(data, false)
}

// ValidateCorrelatedRecord requires a complete enriched proof for a scale.
func ValidateCorrelatedRecord(data []byte) error {
	return validateRecord(data, true)
}

func validateRecord(data []byte, requireCorrelation bool) error {
	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return fmt.Errorf("decode capacity record: %w", err)
	}
	if _, err := scaleIndex(r.Fixture.SeedRows); err != nil {
		return err
	}
	return r.validate(requireCorrelation || r.hasCorrelation())
}

func scaleIndex(size int) (int, error) {
	switch size {
	case SmallScale:
		return 0, nil
	case LargeScale:
		return 1, nil
	default:
		return 0, fmt.Errorf("unexpected capacity fixture size %d", size)
	}
}

func (r record) hasCorrelation() bool {
	if r.Diagnostics != nil {
		for _, t := range r.Diagnostics.Timelines {
			if len(t.Correlation) != 0 {
				return true
			}
		}
	}
	return false
}

func (r record) validate(requireCorrelation bool) error {
	f := r.Fixture
	if f.TailRows != TailRows || f.BatchRows != BatchRows || f.LineBytes != LineBytes || f.MemoryBytes != MemoryLimit {
		return errors.New("incorrect fixture dimensions or memory limit")
	}
	if r.TimedOut == nil {
		return errors.New("missing or null timed_out verdict")
	}
	if *r.TimedOut {
		return errors.New("require timed_out to be false")
	}
	if r.Phase != "complete" || r.Status != "pass" || r.Workload != "pass" ||
		r.RecoveredRows != f.SeedRows+TailRows {
		return errors.New("missing completed fixture with exact restart parity")
	}
	d := r.Diagnostics
	if d == nil {
		return errors.New("missing cutover diagnostics")
	}
	if r.CgroupLimit == nil || *r.CgroupLimit != MemoryLimit || r.CgroupSwap == nil || *r.CgroupSwap != 0 ||
		r.CgroupPeak == 0 || r.CgroupPeak > MemoryLimit {
		return errors.New("missing cgroup limit readback, disabled swap readback, or bounded memory peak")
	}
	if oom := d.MemoryEvents["oom_kill"]; oom == nil || *oom != 0 || r.CgroupOOM == nil || *r.CgroupOOM != 0 {
		return errors.New("require an observed numeric zero for raw and summary cgroup OOM-kill counts")
	}
	c := r.Cleanup
	if c.Status != "pass" || !c.CandidateReaped || !c.SubtreeEmpty || !c.SubtreeRemoved || !c.StoreRemoved || c.Detail != "" {
		return errors.New("missing candidate, subtree, or fixture store cleanup")
	}
	if len(d.Candidates) != 2 {
		return errors.New("missing startup and restart lifecycle evidence")
	}
	for i, lifetime := range [...]string{"startup", "restart"} {
		p := d.Candidates[i]
		if p.Lifetime != lifetime || p.Ready <= 0 || !p.Reaped || p.ExitedBeforeCleanup ||
			p.ExitCode == nil || *p.ExitCode != 0 || p.Signal != "" || p.WaitError != "" ||
			p.StartupError != "" || p.LogDrainError != "" {
			return fmt.Errorf("missing clean %s lifecycle", lifetime)
		}
	}
	if err := r.validateTimelines(requireCorrelation); err != nil {
		return err
	}
	return d.Restart.validate(r.Compaction)
}

func (r record) validateTimelines(requireCorrelation bool) error {
	timelines := r.Diagnostics.Timelines
	if len(timelines) != 2 || timelines[0].Pattern != "clustered" || timelines[1].Pattern != "scattered" {
		return errors.New("require clustered followed by scattered backfill timelines")
	}
	seed := r.Fixture.SeedRows
	cutoffs := [...]backfillCutoffs{
		{seed - 2*BatchRows, seed - BatchRows, seed},
		{seed, seed + BatchRows, seed + TailRows},
	}
	var published, previousPeak uint64
	for i, t := range timelines {
		if err := t.validate(cutoffs[i], published, r.CgroupPeak); err != nil {
			return fmt.Errorf("%s backfill: %w", t.Pattern, err)
		}
		if requireCorrelation {
			if len(t.Correlation) == 0 {
				return fmt.Errorf("%s backfill: missing capacity correlation", t.Pattern)
			}
			var err error
			previousPeak, err = t.validateCorrelation(r.CgroupPeak, previousPeak)
			if err != nil {
				return fmt.Errorf("%s backfill: %w", t.Pattern, err)
			}
		}
		published = t.Publication.Generation
	}
	c := r.Compaction
	if c.State != "complete" || c.Generation != published ||
		c.CoveredRows != cutoffs[1].publish || c.CommittedRows != cutoffs[1].final {
		return errors.New("scattered publication disagrees with the completed fixture compaction")
	}
	return nil
}
