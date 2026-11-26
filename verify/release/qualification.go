package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"slices"
)

func validateRetargets(raw jsontext.Value) error {
	var healthy, full struct {
		ReadinessStatus int    `json:"readiness_status"`
		ReadinessReason string `json:"readiness_reason"`
		IngestStatus    int    `json:"ingest_status"`
		Replayed        bool   `json:"replayed_after_restart"`
		PostReason      string `json:"post_ingest_reason"`
		PostCategory    string `json:"post_ingest_category"`
	}
	if _, err := decodeFields(raw, map[string]any{"healthy_to_full": &healthy, "full_to_healthy": &full}); err != nil {
		return fmt.Errorf("ENOSPC retarget evidence: %w", err)
	}
	if healthy.ReadinessStatus != http.StatusOK || healthy.ReadinessReason != "ready" ||
		healthy.IngestStatus != http.StatusNoContent || !healthy.Replayed {
		return errors.New("healthy_to_full must prove readiness, acknowledged ingest and replay on the original filesystem")
	}
	if full.ReadinessStatus != http.StatusServiceUnavailable || full.ReadinessReason != "low_disk_headroom" ||
		full.IngestStatus != http.StatusServiceUnavailable || full.PostReason != "wal_rollback" || full.PostCategory != "enospc" {
		return errors.New("full_to_healthy must prove low disk headroom and kernel ENOSPC rollback on the original filesystem")
	}
	return nil
}

func validateFixedBudget(raw jsontext.Value) error {
	var control rejection
	var walk budgetWalkProof
	if _, err := decodeFields(raw, map[string]any{
		"http_status": &control.HTTPStatus, "next_cursor": &control.NextCursor, "ndjson_rows": &control.NDJSONRows,
		"fixed_budget_walk": &walk,
	}); err != nil {
		return err
	}
	if control.HTTPStatus != http.StatusServiceUnavailable || control.NextCursor || control.NDJSONRows != 0 {
		return errors.New("missing clean budget-1 rejection")
	}
	if err := validateExhaustion(walk.budgetWalkStats, 3, 3); err != nil {
		return fmt.Errorf("tied walk: %w", err)
	}
	sparse := walk.SparseWalk
	fixture := sparseBudgetFixture()
	if sparse.ResponseByteCap != fixture.byteCap || sparse.FirstLimit != fixture.firstLimit ||
		sparse.FirstRows != fixture.firstRows || sparse.ResumeLimit != fixture.resumeLimit {
		return errors.New("missing byte-trimmed sparse walk with smaller resume limit")
	}
	if err := validateExhaustion(sparse.budgetWalkStats, 3, len(fixture.want)); err != nil {
		return fmt.Errorf("sparse walk: %w", err)
	}
	return nil
}

func validateExhaustion(walk budgetWalkStats, pages, rows int) error {
	threshold := walk.ExactThresholdBudget
	if threshold < 1 || threshold > budgetWalkMax || threshold != walk.FirstCursorBudget ||
		walk.Pages != pages || walk.Rows != rows || walk.FirstFailure != nil || walk.FirstBrokenContinuation != nil {
		return errors.New("missing successful fixed-budget exhaustion at the issuing threshold")
	}
	seen := make(map[int]bool)
	rejections := 0
	for _, probe := range walk.Probes {
		if probe.Budget < 1 || probe.Budget > budgetWalkMax || seen[probe.Budget] || probe.Failure != nil {
			return fmt.Errorf("invalid or repeated probe at budget %d", probe.Budget)
		}
		seen[probe.Budget] = true
		if probe.Budget >= threshold {
			if probe.Outcome != probeExhausted || probe.Pages != pages || probe.Rows != rows {
				return fmt.Errorf("issuing probe at budget %d did not exhaust the walk", probe.Budget)
			}
		} else {
			if probe.Outcome != probeRejected || probe.Pages != 0 || probe.Rows != 0 {
				return fmt.Errorf("expected a clean rejection at budget %d", probe.Budget)
			}
			rejections++
		}
	}
	if !seen[threshold] || (threshold > 1 && !seen[threshold-1]) || rejections != walk.RejectedBudgets {
		return errors.New("missing or inconsistent issuing and adjacent rejection probes")
	}
	return nil
}

const parityCapturedRows = 6

type parityProgress struct {
	CapturedRows       int              `json:"captured_rows"`
	PostRestore204     bool             `json:"post_restore_204"`
	PostAppendRows     int              `json:"post_append_rows"`
	HeldRows           int              `json:"held_rows_after_compaction"`
	PostCompactionRows int              `json:"post_compaction_rows"`
	RestartRows        int              `json:"restart_rows"`
	StaleCursorReject  bool             `json:"stale_cursor_rejected"`
	FixtureRemoved     bool             `json:"fixture_removed"`
	Compaction         parityGeneration `json:"compaction"`
}

type parityGeneration struct {
	PreviousGeneration uint64 `json:"previous_generation"`
	Generation         uint64 `json:"generation"`
	CoveredRows        int    `json:"covered_rows"`
}

func validateParity(raw jsontext.Value) error {
	var proof struct {
		parityProgress
		VerifiedPoint struct {
			// Schema v1 encodes the digest as a JSON array of byte values.
			Generation    [32]uint16
			CoveredOffset int64
			NextOrdinal   uint64
			CommittedEnd  int64
		} `json:"verified_point"`
		FirstFailure jsontext.Value `json:"first_failure"`
		Compaction   jsontext.Value `json:"compaction"`
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		return err
	}
	const augmented = parityCapturedRows + 1
	point := proof.VerifiedPoint
	maxByte := slices.Max(point.Generation[:])
	if maxByte == 0 || maxByte > 255 || point.NextOrdinal != 3 ||
		point.CoveredOffset <= 0 || point.CommittedEnd <= point.CoveredOffset {
		return errors.New("missing verified v1 restore point with checkpoint and committed tail")
	}
	if proof.FirstFailure != nil {
		return errors.New("restored parity report records a walk failure")
	}
	if proof.CapturedRows != parityCapturedRows || !proof.PostRestore204 || proof.PostAppendRows != augmented ||
		proof.HeldRows != parityCapturedRows || proof.PostCompactionRows != augmented ||
		proof.RestartRows != augmented || !proof.StaleCursorReject {
		return errors.New("incomplete restored, held, post-compaction or restart parity")
	}
	var replacement parityGeneration
	if _, err := decodeFields(proof.Compaction, map[string]any{
		"previous_generation": &replacement.PreviousGeneration,
		"generation":          &replacement.Generation, "covered_rows": &replacement.CoveredRows,
	}); err != nil {
		return fmt.Errorf("replacement generation: %w", err)
	}
	if replacement.Generation <= replacement.PreviousGeneration || replacement.CoveredRows != augmented {
		return errors.New("missing replacement generation at the requested compaction cutoff")
	}
	if !proof.FixtureRemoved {
		return errors.New("restored parity fixture cleanup did not complete")
	}
	return nil
}
