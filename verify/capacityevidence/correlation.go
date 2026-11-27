package capacityevidence

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"
)

const ReclamationBudget = 2 * time.Second

// CycleCorrelation ties measurements to a single backfill timeline. Times are
// relative to its phase start. The two generation numbers use the compaction
// status sequence, independent of the manifest digest used for the pin set.
type CycleCorrelation struct {
	PredecessorGeneration *uint64          `json:"predecessor_generation"`
	ReplacementGeneration *uint64          `json:"replacement_generation"`
	ReplayTailBytes       *uint64          `json:"replay_tail_bytes"`
	PredecessorPinAge     *uint64          `json:"oldest_predecessor_pin_age_ns"`
	HeldObservedNS        *int64           `json:"held_observed_ns"`
	FinalPinReleaseNS     *int64           `json:"final_pin_release_observed_ns"`
	ReclaimedBytes        *uint64          `json:"reclaimed_bytes"`
	ReclaimedObservedNS   *int64           `json:"reclaimed_observed_ns"`
	CgroupPeak            *PeakObservation `json:"cgroup_memory_peak"`
}

// PeakObservation is a read of the kernel's cumulative cgroup high-water
// mark after the workload's fresh walk, on the same cgroup as memory.current.
type PeakObservation struct {
	ReadStartNS *int64  `json:"read_start_ns"`
	ReadEndNS   *int64  `json:"read_end_ns"`
	Bytes       *uint64 `json:"bytes"`
	Error       string  `json:"error,omitempty"`
}

// HeldCapacity returns the replayable committed suffix size on the selected
// replacement and the oldest live pin age on its predecessor.
func (m RuntimeMetrics) HeldCapacity(predecessor uint64) (replay, age *uint64, err error) {
	if m.ReplayTailBytes == nil {
		return nil, nil, errors.New("missing replay_tail_bytes at the final cutoff")
	}
	if m.PredecessorPinAge == nil || m.PredecessorPinGeneration == nil {
		return m.ReplayTailBytes, nil, errors.New("missing oldest predecessor pin age or its generation")
	}
	if *m.PredecessorPinGeneration != predecessor {
		return m.ReplayTailBytes, nil, errors.New("oldest predecessor pin age belongs to a different generation")
	}
	return m.ReplayTailBytes, m.PredecessorPinAge, nil
}

// Reclamation returns the completed capacity credit for this transition. The
// runtime omits all three members while credit is pending. A complete tuple
// with zero bytes is an observed credit of zero.
func (m RuntimeMetrics) Reclamation(predecessor, replacement uint64) (*uint64, error) {
	if m.ReclaimedBytes == nil && m.ReclaimedPredecessor == nil && m.ReclaimedReplacement == nil {
		for _, name := range [...]string{ReclaimedBytesMetric, ReclaimedPredecessorMetric, ReclaimedReplacementMetric} {
			if reason := m.Unavailable[name]; reason != "" {
				return nil, fmt.Errorf("invalid reclamation metric %s: %s", name, reason)
			}
		}
		return nil, nil
	}
	if m.ReclaimedBytes == nil || m.ReclaimedPredecessor == nil || m.ReclaimedReplacement == nil {
		return nil, errors.New("incomplete reclamation measurement")
	}
	if *m.ReclaimedPredecessor != predecessor || *m.ReclaimedReplacement != replacement {
		return nil, errors.New("reclamation attributed to a different predecessor or replacement")
	}
	return m.ReclaimedBytes, nil
}

func (b backfillEvidence) validateCorrelation(peak, previousPeak uint64) (uint64, error) {
	var proof *CycleCorrelation
	if err := json.Unmarshal(b.Correlation, &proof); err != nil {
		return 0, fmt.Errorf("decode capacity correlation: %w", err)
	}
	if proof == nil {
		return 0, errors.New("missing capacity correlation")
	}
	var predecessor uint64
	var heldAt, releasedAt, reclaimedAt, walkAt int64
	var replay, age, reclaimed *uint64
	var lastObservation int64
	var freshVisible bool
	for i, sample := range b.Samples {
		switch sample.Event {
		case "compaction_start":
			predecessor = sample.Compaction.Generation
			if predecessor == 0 {
				return 0, errors.New("capacity correlation requires a prepared predecessor generation")
			}
		case "publication", "final_cutoff":
			if count := sample.Metrics.RetiredPins; count != nil && *count == 0 {
				return 0, errors.New("predecessor pin count was zero while the capture was held")
			}
		case "before_retirement":
			var err error
			replay, age, err = sample.Metrics.HeldCapacity(predecessor)
			if err != nil {
				return 0, err
			}
			heldAt = sample.ElapsedNS
		case "after_retirement", "drain_poll":
			if count := sample.Metrics.RetiredPins; count != nil && *count == 0 {
				releasedAt = sample.ElapsedNS
			}
		case "reclamation_poll":
			if releasedAt == 0 || reclaimed != nil || *sample.SentNS < releasedAt ||
				sample.ElapsedNS-releasedAt > int64(ReclamationBudget) {
				return 0, errors.New("reclamation poll outside the observation window after final pin release")
			}
			if count := sample.Metrics.RetiredPins; count == nil || *count != 0 {
				return 0, errors.New("reclamation poll requires an observed zero predecessor pin count")
			}
		case "fresh_visible":
			if reclaimed == nil {
				return 0, errors.New("fresh visibility preceded observed capacity reclamation")
			}
			freshVisible = true
		case "fresh_walk_complete":
			if i != len(b.Samples)-1 || !freshVisible || sample.StatusError != "" ||
				sample.Compaction != nil || sample.SentNS != nil {
				return 0, errors.New("fresh walk completion must end the capacity correlation timeline")
			}
			walkAt, lastObservation = sample.ElapsedNS, sample.ElapsedNS
			if fs := sample.Filesystem; fs != nil && fs.ObservedNS != nil {
				lastObservation = max(lastObservation, *fs.ObservedNS)
			}
		}

		if sample.StatusError != "" || sample.Compaction == nil || sample.SentNS == nil {
			continue
		}
		credit, err := sample.Metrics.Reclamation(predecessor, b.Publication.Generation)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", sample.Event, err)
		}
		if credit == nil {
			continue
		}
		if releasedAt == 0 {
			return 0, errors.New("completed reclamation preceded final predecessor pin release")
		}
		switch sample.Event {
		case "after_retirement", "drain_poll", "reclamation_poll":
			reclaimed, reclaimedAt = credit, sample.ElapsedNS
		default:
			return 0, errors.New("reclamation outside pin drain or reclamation status")
		}
	}
	if !matches(proof.PredecessorGeneration, predecessor) || !matches(proof.ReplacementGeneration, b.Publication.Generation) {
		return 0, errors.New("capacity correlation generations do not match the compaction")
	}
	if replay == nil || age == nil || !matches(proof.ReplayTailBytes, *replay) ||
		!matches(proof.PredecessorPinAge, *age) || !matches(proof.HeldObservedNS, heldAt) {
		return 0, errors.New("capacity correlation disagrees with held replay tail or predecessor pin age")
	}
	if releasedAt == 0 || !matches(proof.FinalPinReleaseNS, releasedAt) {
		return 0, errors.New("capacity correlation disagrees with final pin release")
	}
	if reclaimed == nil || !matches(proof.ReclaimedBytes, *reclaimed) || !matches(proof.ReclaimedObservedNS, reclaimedAt) {
		return 0, errors.New("missing or inconsistent reclaimed capacity")
	}
	m := proof.CgroupPeak
	if walkAt == 0 || m == nil || m.Error != "" || m.ReadStartNS == nil || m.ReadEndNS == nil ||
		*m.ReadStartNS < max(walkAt, lastObservation) || *m.ReadEndNS <= *m.ReadStartNS {
		return 0, errors.New("missing or mis-timed cgroup memory.peak read after the fresh walk")
	}
	if m.Bytes == nil || *m.Bytes < max(b.Memory.Max, previousPeak) || *m.Bytes > peak {
		return 0, errors.New("cgroup memory.peak disagrees with live memory samples or cumulative cgroup peaks")
	}
	return *m.Bytes, nil
}

func matches[T comparable](observed *T, want T) bool {
	return observed != nil && *observed == want
}
