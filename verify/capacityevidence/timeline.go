package capacityevidence

import (
	"context"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	DrainBudget       = 2 * time.Second
	MemorySampleLimit = 16
)

type backfillEvidence struct {
	Pattern      string `json:"pattern"`
	Status       string `json:"status"`
	Failure      string `json:"failure"`
	HeldCutoff   int    `json:"held_cutoff"`
	Cutoff       int    `json:"requested_cutoff"`
	FinalCutoff  int    `json:"expected_committed_rows"`
	HeldRows     int    `json:"held_rows_checked"`
	Exhausted    bool   `json:"held_cursor_exhausted"`
	Retried      bool   `json:"terminal_page_retried"`
	Retired      bool   `json:"capture_retired"`
	FreshExact   bool   `json:"fresh_walk_exact"`
	FreshRows    int    `json:"fresh_rows_checked"`
	FreshVisible bool   `json:"fresh_backfill_visible"`
	Acknowledged struct {
		Rows     int       `json:"rows"`
		Batches  int       `json:"batches"`
		First    int64     `json:"first_ack_ns"`
		Last     int64     `json:"last_ack_ns"`
		Earliest time.Time `json:"earliest_row_ts"`
		Latest   time.Time `json:"latest_row_ts"`
	} `json:"acknowledged_backfill"`
	Publication struct {
		Generation uint64 `json:"generation"`
		LowerNS    int64  `json:"observed_lag_lower_bound_ns"`
		UpperNS    int64  `json:"observed_lag_upper_bound_ns"`
	} `json:"publication"`
	PinDrain struct {
		Status string  `json:"status"`
		Reason string  `json:"reason"`
		Held   *uint64 `json:"retired_pins_while_held"`
		After  *uint64 `json:"retired_pins_after_exhaustion"`
	} `json:"pin_drain"`
	Memory  *memoryObservation `json:"live_memory_current"`
	Samples []backfillSample   `json:"samples"`
}

type memoryObservation struct {
	Samples int    `json:"sample_count"`
	Span    int64  `json:"observation_span_ns"`
	Min     uint64 `json:"min_bytes"`
	Max     uint64 `json:"max_bytes"`
}

type memoryCurrentSample struct {
	ReadStart int64   `json:"read_start_ns"`
	ReadEnd   int64   `json:"read_end_ns"`
	Bytes     *uint64 `json:"bytes"`
	Error     string  `json:"error"`
}

type filesystemObservation struct {
	ObservedNS   *int64            `json:"observed_ns"`
	StagingBytes *int64            `json:"checkpoint_staging_apparent_bytes"`
	Unavailable  map[string]string `json:"unavailable,omitempty"`
}

type backfillSample struct {
	Filesystem  *filesystemObservation `json:"filesystem,omitempty"`
	Memory      *memoryCurrentSample   `json:"cgroup_memory_current"`
	Event       string                 `json:"event"`
	ElapsedNS   int64                  `json:"elapsed_ns"`
	SentNS      *int64                 `json:"request_sent_ns"`
	Compaction  *compactionEvidence    `json:"status"`
	StatusError string                 `json:"status_error"`
	Metrics     runtimeObservations    `json:"candidate_status_metrics"`
}

type runtimeObservations struct {
	PublicationLag     *uint64 `json:"publication_lag_ns,omitempty"`
	StagingReserved    *uint64 `json:"staging_reserved_bytes,omitempty"`
	StagingUsed        *uint64 `json:"staging_used_bytes,omitempty"`
	OldestPinAge       *uint64 `json:"oldest_pin_age_ns,omitempty"`
	Reclaimable        *uint64 `json:"reclaimable_bytes,omitempty"`
	RetiredPins        *uint64 `json:"retired_generation_pins,omitempty"`
	PendingCompactions *uint64 `json:"pending_compactions,omitempty"`
	// A raw value preserves an explicit null separately from an omitted member.
	OldestPinnedGeneration jsontext.Value    `json:"oldest_pinned_generation,omitzero"`
	Unavailable            map[string]string `json:"unavailable,omitempty"`
}

func (m runtimeObservations) validate(hasStatus bool) error {
	for _, field := range [...]struct {
		name  string
		value *uint64
	}{
		{"publication_lag_ns", m.PublicationLag},
		{"staging_reserved_bytes", m.StagingReserved},
		{"staging_used_bytes", m.StagingUsed},
		{"oldest_pin_age_ns", m.OldestPinAge},
		{"reclaimable_bytes", m.Reclaimable},
		{"retired_generation_pins", m.RetiredPins},
		{"pending_compactions", m.PendingCompactions},
	} {
		if field.value == nil {
			continue
		}
		if m.Unavailable[field.name] != "" {
			return fmt.Errorf("%s contradicts unavailable status", field.name)
		}
		if !hasStatus {
			return fmt.Errorf("%s requires a successful candidate status snapshot", field.name)
		}
	}

	const oldest = "oldest_pinned_generation"
	raw := m.OldestPinnedGeneration
	if len(raw) == 0 || raw.Kind() == jsontext.KindNull {
		// Omission carries no observation. An explicit null observes an
		// empty set only on a successful snapshot without a reason. Older
		// reports also used null placeholders for skipped snapshots.
		return nil
	}
	var id string
	if err := json.Unmarshal(raw, &id); err != nil || len(id) != 2*sha256.Size || strings.Trim(id, "0123456789abcdef") != "" {
		return fmt.Errorf("%s must be a lowercase hexadecimal SHA-256 digest or null", oldest)
	}
	if m.Unavailable[oldest] != "" {
		return fmt.Errorf("%s contradicts unavailable status", oldest)
	}
	if !hasStatus {
		return fmt.Errorf("%s requires a successful candidate status snapshot", oldest)
	}
	return nil
}

type backfillCutoffs struct {
	held, publish, final int
}

func (b backfillEvidence) validate(want backfillCutoffs, previousPublication, peak uint64) error {
	if b.Status != "pass" || b.Failure != "" {
		return fmt.Errorf("timeline did not pass: status %q, failure %q", b.Status, b.Failure)
	}
	if b.HeldCutoff != want.held || b.Cutoff != want.publish || b.FinalCutoff != want.final {
		return fmt.Errorf("incorrect cutoffs: held %d, publication %d, final %d", b.HeldCutoff, b.Cutoff, b.FinalCutoff)
	}
	if !b.Exhausted || !b.FreshVisible || b.HeldRows <= 0 || b.HeldRows > want.held {
		return errors.New("require held cursor exhaustion with verified rows and fresh backfill visibility")
	}
	if !b.FreshExact || b.FreshRows <= b.HeldRows || b.FreshRows > want.final ||
		(b.Pattern == "scattered" && b.FreshRows != want.final) {
		return errors.New("missing exact fresh walk parity")
	}
	if !b.Retried || !b.Retired {
		return errors.New("require terminal page replay and explicit capture retirement")
	}
	ack := b.Acknowledged
	if ack.Rows != want.final-want.held || ack.Batches != ack.Rows/BatchRows ||
		ack.First <= 0 || ack.Last < ack.First || ack.Earliest.IsZero() || ack.Latest.Before(ack.Earliest) {
		return errors.New("missing or invalid acknowledged backfill evidence")
	}
	pub := b.Publication
	if pub.Generation == 0 || pub.LowerNS <= 0 || pub.UpperNS <= pub.LowerNS {
		return errors.New("missing or invalid publication generation or observation bounds")
	}
	pins := b.PinDrain
	if pins.Status != "observed" || pins.Reason != "" || pins.Held == nil || *pins.Held == 0 || pins.After == nil || *pins.After != 0 {
		return fmt.Errorf("require observed pin drain from a positive held count to zero after retirement (status %q, reason %q)", pins.Status, pins.Reason)
	}
	return b.validateSamples(want, previousPublication, peak)
}

func (b backfillEvidence) validateSamples(want backfillCutoffs, previousPublication, peak uint64) error {
	var elapsed, startSent, lastRunningSent, exhaustedAt, retriedAt, retiredAt, drainStarted int64
	var overlap, published, finalObserved, held, drained, fresh bool
	var predecessor uint64
	var committed int
	var stagingObservedAt, firstRead int64
	var memory memoryObservation
	for _, sample := range b.Samples {
		if sample.ElapsedNS < max(elapsed, stagingObservedAt) {
			return errors.New("cutover samples must be in elapsed time order")
		}
		previousElapsed := elapsed
		elapsed = sample.ElapsedNS
		// Storage is sampled after this event or HTTP reply. Carry its
		// timestamp forward so the next probe cannot start before it.
		earliest := max(previousElapsed, stagingObservedAt)
		if fs := sample.Filesystem; fs != nil && fs.StagingBytes != nil {
			const staging = "checkpoint_staging_apparent_bytes"
			if *fs.StagingBytes < 0 || fs.Unavailable[staging] != "" {
				return fmt.Errorf("%s sample has invalid %s", sample.Event, staging)
			}
			if fs.ObservedNS == nil || *fs.ObservedNS < elapsed {
				return fmt.Errorf("%s sample has invalid filesystem staging observation timing", sample.Event)
			}
			stagingObservedAt = *fs.ObservedNS
		}
		running := sample.Event == "compaction_start" || sample.Event == "overlap" || sample.Event == "status_poll"
		if sample.Memory != nil && (!running || sample.Event == "compaction_start") {
			return errors.New("memory.current sample outside running status polls")
		}
		hasStatus := sample.StatusError == "" && sample.SentNS != nil && sample.Compaction != nil
		if err := sample.Metrics.validate(hasStatus); err != nil {
			return fmt.Errorf("%s sample: %w", sample.Event, err)
		}
		switch sample.Event {
		case "held_exhausted":
			if !finalObserved || exhaustedAt != 0 {
				return errors.New("held cursor exhaustion requires a prior final cutoff observation")
			}
			exhaustedAt = elapsed
			continue
		case "terminal_retried":
			if exhaustedAt == 0 || retriedAt != 0 {
				return errors.New("terminal page replay must follow held cursor exhaustion")
			}
			retriedAt = elapsed
			continue
		case "capture_retired":
			if !held || retiredAt != 0 {
				return errors.New("capture retirement requires a prior before_retirement pin observation")
			}
			retiredAt = elapsed
			continue
		case "fresh_visible":
			if !drained || fresh {
				return errors.New("fresh visibility sample must follow observed pin drain")
			}
			fresh = true
			continue
		case "compaction_start", "overlap", "status_poll", "publication", "final_cutoff", "before_retirement", "after_retirement", "drain_poll":
		default:
			continue
		}
		drain := sample.Event == "after_retirement" || sample.Event == "drain_poll"
		// The producer records a request deadline as a failed status sample
		// and keeps polling within the drain budget.
		retry := drain && sample.StatusError == context.DeadlineExceeded.Error()
		if sample.StatusError != "" && !retry {
			return fmt.Errorf("%s sample failed: %s", sample.Event, sample.StatusError)
		}
		if sample.SentNS == nil || *sample.SentNS <= 0 || *sample.SentNS > elapsed ||
			(!retry && *sample.SentNS == elapsed) {
			return fmt.Errorf("missing or invalid request timing for %s sample", sample.Event)
		}
		sent := *sample.SentNS
		if drain && (retiredAt == 0 || sent < retiredAt) {
			return errors.New("pin drain sample did not follow explicit capture retirement")
		}
		if sample.Event == "before_retirement" && (retriedAt == 0 || sent < retriedAt) {
			return errors.New("before_retirement must observe the positive held pin count at the final cutoff after terminal page replay")
		}
		if sent < earliest {
			return fmt.Errorf("%s sample request overlapped a previous observation", sample.Event)
		}
		if drain {
			if drained || (sample.Event == "after_retirement") != (drainStarted == 0) {
				return errors.New("pin drain must start with after_retirement and end at the first observed zero")
			}
			if drainStarted == 0 {
				drainStarted = sent
			}
			if elapsed-drainStarted > int64(DrainBudget) {
				return errors.New("pin drain sample exceeded the drain budget")
			}
			if retry {
				if sample.Compaction != nil {
					return fmt.Errorf("%s sample has compaction state on a failed request", sample.Event)
				}
				continue
			}
		}
		if sample.Compaction == nil {
			return fmt.Errorf("missing request timing or compaction state for %s sample", sample.Event)
		}
		c := *sample.Compaction
		if running {
			if sample.Event == "compaction_start" {
				if startSent != 0 || c.Generation >= b.Publication.Generation || c.CommittedRows != want.publish {
					return errors.New("compaction start did not observe the predecessor at the requested cutoff")
				}
				if previousPublication != 0 && c.Generation <= previousPublication {
					return errors.New("scattered predecessor must advance past clustered publication at the seed checkpoint")
				}
				startSent, predecessor, committed = sent, c.Generation, want.publish
			}
			if startSent == 0 || published || c.State != "running" || c.Generation != predecessor ||
				c.CoveredRows != want.held || c.CommittedRows < committed || c.CommittedRows > want.final {
				return fmt.Errorf("%s sample must observe the same running compaction at the predecessor cutoff", sample.Event)
			}
			if sample.Event == "overlap" {
				if overlap || c.CommittedRows < want.publish+BatchRows {
					return errors.New("overlap did not observe backfill on the predecessor generation")
				}
				if sample.Memory == nil {
					return errors.New("require a bracketed memory.current overlap sample after backfill")
				}
				overlap = true
			}
			if m := sample.Memory; m != nil {
				if memory.Samples == MemorySampleLimit || m.ReadStart < earliest ||
					m.ReadEnd <= m.ReadStart || m.ReadEnd > sent {
					return errors.New("mis-timed or excessive memory.current samples")
				}
				if m.Error != "" {
					return fmt.Errorf("live memory.current sampling failed: %s", m.Error)
				}
				if m.Bytes == nil || *m.Bytes == 0 || *m.Bytes > peak {
					return errors.New("missing, zero, or above-final-memory.peak memory.current bytes")
				}
				if memory.Samples == 0 {
					firstRead, memory.Min, memory.Max = m.ReadStart, *m.Bytes, *m.Bytes
				}
				memory.Samples++
				memory.Span = m.ReadEnd - firstRead
				memory.Min = min(memory.Min, *m.Bytes)
				memory.Max = max(memory.Max, *m.Bytes)
			}
			lastRunningSent, committed = sent, c.CommittedRows
			continue
		}
		if !overlap || c.State != "complete" || c.Generation != b.Publication.Generation ||
			c.CoveredRows != want.publish || c.CommittedRows < committed || c.CommittedRows > want.final {
			return fmt.Errorf("%s sample did not observe publication at the requested cutoff", sample.Event)
		}
		if sample.Event == "publication" {
			if published {
				return errors.New("duplicate publication sample")
			}
			if memory.Samples < 2 {
				return errors.New("require multiple bracketed memory.current samples including overlap before publication")
			}
			pub := b.Publication
			if pub.LowerNS < lastRunningSent-startSent || pub.LowerNS > sent-startSent ||
				pub.UpperNS != elapsed-startSent {
				return errors.New("publication lag bounds disagree with observed request timing")
			}
			published = true
		}
		if !published {
			return fmt.Errorf("%s sample preceded publication", sample.Event)
		}
		committed = c.CommittedRows
		pins := sample.Metrics.RetiredPins
		switch sample.Event {
		case "publication", "final_cutoff":
			if exhaustedAt != 0 || (sample.Event == "final_cutoff" && c.CommittedRows != want.final) {
				return fmt.Errorf("%s sample must confirm its cutoff before held cursor exhaustion", sample.Event)
			}
			finalObserved = finalObserved || c.CommittedRows == want.final
		case "before_retirement":
			if held || c.CommittedRows != want.final ||
				pins == nil || *pins != *b.PinDrain.Held {
				return errors.New("before_retirement must observe the positive held pin count at the final cutoff after terminal page replay")
			}
			held = true
		case "after_retirement", "drain_poll":
			if c.CommittedRows != want.final {
				return errors.New("pin drain sample did not observe the final cutoff")
			}
			if pins != nil && *pins == 0 {
				drained = true
			}
		}
	}
	if !published || !finalObserved || !held || !drained || !fresh {
		return errors.New("missing runtime samples for publication, final cutoff, held pin, pin drain, or fresh visibility")
	}
	if b.Memory == nil || *b.Memory != memory {
		return errors.New("missing or inconsistent memory.current observation span or range")
	}
	return nil
}
