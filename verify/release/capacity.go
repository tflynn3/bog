package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	capacityFixtureID = "linux-release-v1"
	capacityMemory    = 128 << 20
	capacityTail      = 2048
	capacityBatch     = 64
)

// This is the release-side contract for the benchmark's schema-v1 live
// evidence. Positive values and explicit booleans make missing and null
// measurements fail closed while permitting additional diagnostic fields.
type capacityEvidence struct {
	Fixture struct {
		SeedRows    int    `json:"seed_rows"`
		TailRows    int    `json:"tail_rows"`
		LineBytes   int    `json:"line_bytes"`
		BatchRows   int    `json:"batch_rows"`
		MemoryBytes uint64 `json:"memory_limit_bytes"`
	} `json:"fixture"`
	Status        string `json:"status"`
	Workload      string `json:"workload_status"`
	Phase         string `json:"phase"`
	TimedOut      *bool  `json:"timed_out"`
	RecoveredRows int    `json:"recovered_rows"`
	Compaction    struct {
		State         string `json:"state"`
		Generation    uint64 `json:"generation"`
		CoveredRows   int    `json:"covered_rows"`
		CommittedRows int    `json:"committed_rows"`
	} `json:"compaction"`
	Cleanup struct {
		Status          string `json:"status"`
		CandidateReaped bool   `json:"candidate_reaped"`
		SubtreeEmpty    bool   `json:"subtree_empty"`
		SubtreeRemoved  bool   `json:"subtree_removed"`
		Detail          string `json:"detail"`
	} `json:"cleanup"`
	StartReady    int64                         `json:"start_to_ready_ns"`
	RestartReady  int64                         `json:"restart_to_ready_ns"`
	IngestP99     int64                         `json:"ingest_p99_ns"`
	QueryP99      int64                         `json:"backfill_query_p99_ns"`
	IngestSamples int                           `json:"ingest_samples"`
	QuerySamples  int                           `json:"backfill_query_samples"`
	CgroupPeak    uint64                        `json:"cgroup_peak_memory_bytes"`
	PeakRSS       uint64                        `json:"peak_rss_bytes"`
	PeakFDs       uint64                        `json:"peak_descriptors"`
	InitialFree   uint64                        `json:"initial_disk_free_bytes"`
	MinFree       uint64                        `json:"minimum_disk_headroom_bytes"`
	DeviceIO      map[string]map[string]*uint64 `json:"device_io"`
	Unavailable   map[string]string             `json:"unavailable"`
}

func validateCapacity(rawMode, outcome jsontext.Value, fixture string) error {
	var mode string
	if err := json.Unmarshal(rawMode, &mode); err != nil || mode != "live" || fixture != capacityFixtureID {
		return errors.New("capacity requires live mode and fixture linux-release-v1")
	}
	var records []capacityEvidence
	if _, err := decodeFields(outcome, map[string]any{"evidence": &records}); err != nil {
		return fmt.Errorf("capacity evidence: %w", err)
	}
	const small, large = 196608, 393216
	if len(records) != 2 {
		return fmt.Errorf("capacity requires exactly one record each for %d and %d seed rows", small, large)
	}
	seen := make(map[int]bool, len(records))
	for _, r := range records {
		size := r.Fixture.SeedRows
		if size != small && size != large || seen[size] {
			return fmt.Errorf("unexpected or duplicate capacity fixture size %d", size)
		}
		seen[size] = true
		if err := r.validate(); err != nil {
			return fmt.Errorf("capacity fixture %d: %w", size, err)
		}
	}
	return nil
}

func (r capacityEvidence) validate() error {
	f := r.Fixture
	if f.TailRows != capacityTail || f.LineBytes != 1024 || f.BatchRows != capacityBatch || f.MemoryBytes != capacityMemory {
		return errors.New("incorrect fixture dimensions or memory limit")
	}
	if r.Status != "pass" || r.Workload != "pass" || r.Phase != "complete" || r.TimedOut == nil || *r.TimedOut {
		return errors.New("require a completed fixture and workload without timeout")
	}
	total := f.SeedRows + capacityTail
	if r.RecoveredRows != total {
		return fmt.Errorf("recovered %d rows, want %d", r.RecoveredRows, total)
	}
	c := r.Compaction
	if c.State != "complete" || c.Generation == 0 || c.CoveredRows != f.SeedRows+capacityBatch || c.CommittedRows != total {
		return errors.New("missing or incorrect completed compaction evidence")
	}
	cleanup := r.Cleanup
	if cleanup.Status != "pass" || !cleanup.CandidateReaped || !cleanup.SubtreeEmpty || !cleanup.SubtreeRemoved || cleanup.Detail != "" {
		return errors.New("cleanup must prove candidate reaping, subtree emptiness, and subtree removal")
	}
	if r.StartReady <= 0 || r.RestartReady <= 0 || r.IngestP99 <= 0 || r.QueryP99 <= 0 ||
		r.IngestSamples != capacityTail/capacityBatch || r.QuerySamples <= 0 {
		return errors.New("missing or invalid timing measurements or sample counts")
	}
	if r.CgroupPeak == 0 || r.CgroupPeak > capacityMemory || r.PeakRSS == 0 || r.PeakFDs == 0 ||
		r.MinFree == 0 || r.InitialFree < r.MinFree || len(r.Unavailable) != 0 {
		return errors.New("missing or invalid resource measurements")
	}
	if len(r.DeviceIO) == 0 {
		return errors.New("missing device I/O measurements")
	}
	for device, counters := range r.DeviceIO {
		major, minor, _ := strings.Cut(device, ":")
		_, majorErr := strconv.ParseUint(major, 10, 32)
		_, minorErr := strconv.ParseUint(minor, 10, 32)
		if majorErr != nil || minorErr != nil || counters["rbytes"] == nil || counters["wbytes"] == nil {
			return fmt.Errorf("invalid I/O measurements for device %q", device)
		}
		for name, value := range counters {
			if name == "" || value == nil {
				return fmt.Errorf("invalid I/O counter %q for device %q", name, device)
			}
		}
	}
	return nil
}
