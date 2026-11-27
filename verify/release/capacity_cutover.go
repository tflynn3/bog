package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Cutover consumes the producer's additive timelines. The base capacity
// verdict continues to cover archived reports with the original schema.
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
	Samples []backfillSample `json:"samples"`
}

type backfillSample struct {
	Event       string              `json:"event"`
	ElapsedNS   int64               `json:"elapsed_ns"`
	SentNS      *int64              `json:"request_sent_ns"`
	Compaction  *compactionEvidence `json:"status"`
	StatusError string              `json:"status_error"`
	Metrics     struct {
		RetiredPins *uint64 `json:"retired_generation_pins"`
	} `json:"candidate_status_metrics"`
}

type backfillCutoffs struct {
	held, publish, final int
}

func (r capacityEvidence) validateCutover() error {
	var timelines []backfillEvidence
	if _, err := decodeFields(r.Diagnostics, map[string]any{"backfill_timelines": &timelines}); err != nil {
		return fmt.Errorf("cutover diagnostics: %w", err)
	}
	if len(timelines) != 2 {
		return errors.New("cutover requires exactly one clustered and one scattered backfill timeline")
	}
	seed := r.Fixture.SeedRows
	cutoffs := map[string]backfillCutoffs{
		"clustered": {seed - 2*capacityBatch, seed - capacityBatch, seed},
		"scattered": {seed, seed + capacityBatch, seed + capacityTail},
	}
	generations := make(map[string]uint64, len(cutoffs))
	for _, timeline := range timelines {
		expected, ok := cutoffs[timeline.Pattern]
		if !ok || generations[timeline.Pattern] != 0 {
			return fmt.Errorf("unexpected or duplicate cutover backfill pattern %q", timeline.Pattern)
		}
		if err := timeline.validate(expected); err != nil {
			return fmt.Errorf("%s cutover backfill: %w", timeline.Pattern, err)
		}
		generations[timeline.Pattern] = timeline.Publication.Generation
	}
	if generations["clustered"] >= generations["scattered"] || generations["scattered"] != r.Compaction.Generation {
		return errors.New("cutover expected advancing generations with scattered publication matching completed compaction")
	}
	return nil
}

func (b backfillEvidence) validate(want backfillCutoffs) error {
	if b.Status != "pass" || b.Failure != "" {
		return fmt.Errorf("timeline did not pass: status %q, failure %q", b.Status, b.Failure)
	}
	if b.HeldCutoff != want.held || b.Cutoff != want.publish || b.FinalCutoff != want.final {
		return fmt.Errorf("incorrect cutoffs: held %d, publication %d, final %d", b.HeldCutoff, b.Cutoff, b.FinalCutoff)
	}
	if !b.Exhausted || !b.FreshVisible || b.HeldRows <= 0 || b.HeldRows > want.held {
		return errors.New("require held cursor exhaustion with verified rows and fresh backfill visibility")
	}
	if !b.Retried || !b.Retired {
		return errors.New("require terminal page replay and explicit capture retirement")
	}
	ack := b.Acknowledged
	if ack.Rows != want.final-want.held || ack.Batches != ack.Rows/capacityBatch ||
		ack.First <= 0 || ack.Last < ack.First || ack.Earliest.IsZero() || ack.Latest.Before(ack.Earliest) {
		return errors.New("missing or invalid acknowledged backfill evidence")
	}
	pub := b.Publication
	if pub.Generation == 0 || pub.LowerNS <= 0 || pub.UpperNS < pub.LowerNS {
		return errors.New("missing or invalid publication generation or observation bounds")
	}
	pins := b.PinDrain
	if pins.Status != "observed" || pins.Reason != "" || pins.Held == nil || *pins.Held == 0 || pins.After == nil || *pins.After != 0 {
		return fmt.Errorf("require observed pin drain from a positive held count to zero after retirement (status %q, reason %q)", pins.Status, pins.Reason)
	}
	return b.validateSamples(want)
}

func (b backfillEvidence) validateSamples(want backfillCutoffs) error {
	const drainBudget = int64(2 * time.Second) // Match the capacity producer's drain budget.
	var elapsed, startSent, overlapSent, exhaustedAt, retriedAt, retiredAt, drainStarted int64
	var published, finalObserved, held, drained, fresh bool
	var predecessor uint64
	for _, sample := range b.Samples {
		if sample.ElapsedNS < elapsed {
			return errors.New("cutover samples must be in elapsed time order")
		}
		elapsed = sample.ElapsedNS
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
		if sample.Event == "status_poll" {
			continue
		}
		if sample.SentNS == nil || *sample.SentNS <= 0 || *sample.SentNS > elapsed {
			return fmt.Errorf("missing or invalid request timing for %s sample", sample.Event)
		}
		if drain {
			if retiredAt == 0 || *sample.SentNS < retiredAt {
				return errors.New("pin drain sample did not follow explicit capture retirement")
			}
			if drained || (sample.Event == "after_retirement") != (drainStarted == 0) {
				return errors.New("pin drain must start with after_retirement and end at the first observed zero")
			}
			if drainStarted == 0 {
				drainStarted = *sample.SentNS
			}
			if elapsed-drainStarted > drainBudget {
				return errors.New("pin drain sample exceeded the drain budget")
			}
			if retry {
				continue
			}
		}
		if sample.Compaction == nil {
			return fmt.Errorf("missing request timing or compaction state for %s sample", sample.Event)
		}
		c := *sample.Compaction
		switch sample.Event {
		case "compaction_start":
			if startSent != 0 || c.State != "running" || c.Generation >= b.Publication.Generation ||
				c.CoveredRows != want.held || c.CommittedRows != want.publish {
				return errors.New("compaction start did not observe the predecessor at the requested cutoff")
			}
			startSent, predecessor = *sample.SentNS, c.Generation
			continue
		case "overlap":
			if startSent == 0 || overlapSent != 0 || *sample.SentNS <= startSent ||
				c.State != "running" || c.Generation != predecessor || c.CoveredRows != want.held ||
				c.CommittedRows < want.publish+capacityBatch || c.CommittedRows > want.final {
				return errors.New("overlap did not observe backfill on the predecessor generation")
			}
			overlapSent = *sample.SentNS
			continue
		}
		if overlapSent == 0 || c.State != "complete" || c.Generation != b.Publication.Generation ||
			c.CoveredRows != want.publish || c.CommittedRows < want.publish || c.CommittedRows > want.final {
			return fmt.Errorf("%s sample did not observe publication at the requested cutoff", sample.Event)
		}
		if sample.Event == "publication" {
			if published {
				return errors.New("duplicate publication sample")
			}
			pub := b.Publication
			if pub.LowerNS < overlapSent-startSent || pub.LowerNS > *sample.SentNS-startSent ||
				pub.UpperNS != elapsed-startSent {
				return errors.New("publication lag bounds disagree with observed request timing")
			}
			published = true
		}
		if !published {
			return fmt.Errorf("%s sample preceded publication", sample.Event)
		}
		pins := sample.Metrics.RetiredPins
		switch sample.Event {
		case "publication", "final_cutoff":
			if exhaustedAt != 0 || (sample.Event == "final_cutoff" && c.CommittedRows != want.final) {
				return fmt.Errorf("%s sample must confirm its cutoff before held cursor exhaustion", sample.Event)
			}
			finalObserved = finalObserved || c.CommittedRows == want.final
		case "before_retirement":
			if retriedAt == 0 || *sample.SentNS < retriedAt || held || c.CommittedRows != want.final ||
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
	return nil
}
