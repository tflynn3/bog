package main

import (
	"crypto/sha256"
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"syscall"
)

const (
	ordinaryWALCase         = "ordinary_wal_reopen"
	restoredParityCase      = "restored_parity"
	postRestoreAckCrashCase = "post_restore_ack_crash"

	// Row counts for the linux-release-v1 recovery fixtures.
	paritySeedRows        = 3
	parityFixtureRows     = 6
	parityPostRestoreRows = parityFixtureRows + 1
	postRestoreCrashRows  = parityFixtureRows + 2
)

var cutoverParityCases = []string{restoredParityCase, ordinaryWALCase, postRestoreAckCrashCase, tailOnlyParityCase}

type ordinaryWALProof struct {
	ReopenedRows int `json:"reopened_rows"`
	AppendStatus int `json:"append_status"`
	RestartRows  int `json:"restart_rows"`
}

type postRestoreAckCrashProof struct {
	Point                  verifiedRestorePoint `json:"verified_point"`
	AppendStatus           int                  `json:"append_status"`
	ObservedSignal         syscall.Signal       `json:"observed_signal"`
	ReplayMatched          bool                 `json:"replay_matched"`
	ReplayRows             int                  `json:"replay_rows"`
	CrashCandidateReaped   bool                 `json:"crash_candidate_reaped"`
	RestartCandidateReaped bool                 `json:"restart_candidate_reaped"`
	FixtureRemoved         bool                 `json:"fixture_removed"`
}

// Schema-v1 uses the standalone probe's point keys and numeric digest bytes.
type verifiedRestorePoint struct {
	Generation    [sha256.Size]byte `json:"Generation"`
	CoveredOffset int64             `json:"CoveredOffset"`
	NextOrdinal   uint64            `json:"NextOrdinal"`
	CommittedEnd  int64             `json:"CommittedEnd"`
}

func validatePostRestoreAckCrash(raw jsontext.Value) error {
	var proof postRestoreAckCrashProof
	var point jsontext.Value
	if _, err := decodeFields(raw, map[string]any{
		"verified_point": &point, "append_status": &proof.AppendStatus,
		"observed_signal": &proof.ObservedSignal, "replay_matched": &proof.ReplayMatched,
		"replay_rows": &proof.ReplayRows, "crash_candidate_reaped": &proof.CrashCandidateReaped,
		"restart_candidate_reaped": &proof.RestartCandidateReaped, "fixture_removed": &proof.FixtureRemoved,
	}); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	// A fixed array checks digest length. Pointers also reject null byte elements.
	var generation [sha256.Size]*byte
	if _, err := decodeFields(point, map[string]any{
		"Generation": &generation, "CoveredOffset": &proof.Point.CoveredOffset,
		"NextOrdinal": &proof.Point.NextOrdinal, "CommittedEnd": &proof.Point.CommittedEnd,
	}); err != nil {
		return fmt.Errorf("evidence: verified_point: %w", err)
	}
	for i, b := range generation {
		if b == nil {
			return fmt.Errorf("evidence: verified_point: null Generation byte at index %d", i)
		}
		proof.Point.Generation[i] = *b
	}
	p := proof.Point
	if p.Generation == ([sha256.Size]byte{}) || p.CoveredOffset <= 0 || p.CommittedEnd <= p.CoveredOffset || p.NextOrdinal != paritySeedRows {
		return fmt.Errorf("evidence: verified_point: require a nonzero Generation, 0 < CoveredOffset < CommittedEnd, and NextOrdinal=%d; got %+v", paritySeedRows, p)
	}
	if proof.AppendStatus != http.StatusNoContent || proof.ObservedSignal != syscall.SIGKILL ||
		!proof.ReplayMatched || proof.ReplayRows != postRestoreCrashRows ||
		!proof.CrashCandidateReaped || !proof.RestartCandidateReaped || !proof.FixtureRemoved {
		return fmt.Errorf("evidence: got append_status=%d, observed_signal=%d, replay_matched=%t, replay_rows=%d, crash_candidate_reaped=%t, restart_candidate_reaped=%t, fixture_removed=%t; want %d, %d (SIGKILL), true, %d, true, true, true",
			proof.AppendStatus, proof.ObservedSignal, proof.ReplayMatched, proof.ReplayRows,
			proof.CrashCandidateReaped, proof.RestartCandidateReaped, proof.FixtureRemoved,
			http.StatusNoContent, syscall.SIGKILL, postRestoreCrashRows)
	}
	return nil
}

func validateOrdinaryWAL(raw jsontext.Value) error {
	var proof ordinaryWALProof
	if _, err := decodeFields(raw, map[string]any{
		"reopened_rows": &proof.ReopenedRows, "append_status": &proof.AppendStatus,
		"restart_rows": &proof.RestartRows,
	}); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	if proof.ReopenedRows != paritySeedRows || proof.AppendStatus != http.StatusNoContent || proof.RestartRows != parityFixtureRows {
		return fmt.Errorf("evidence: got reopened_rows=%d, append_status=%d, restart_rows=%d; want %d, %d, %d",
			proof.ReopenedRows, proof.AppendStatus, proof.RestartRows,
			paritySeedRows, http.StatusNoContent, parityFixtureRows)
	}
	return nil
}

func validateRestoredParity(raw jsontext.Value) error {
	var captured, held, postAppend, restarted int
	var acknowledged bool
	if _, err := decodeFields(raw, map[string]any{
		"captured_rows": &captured, "held_capture_rows": &held, "post_restore_204": &acknowledged,
		"post_append_rows": &postAppend, "restart_rows": &restarted,
	}); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	if captured != parityFixtureRows || held != parityFixtureRows || !acknowledged ||
		postAppend != parityPostRestoreRows || restarted != parityPostRestoreRows {
		return fmt.Errorf("evidence: got captured_rows=%d, held_capture_rows=%d, post_restore_204=%t, post_append_rows=%d, restart_rows=%d; want %d, %d, true, %d, %d",
			captured, held, acknowledged, postAppend, restarted,
			parityFixtureRows, parityFixtureRows, parityPostRestoreRows, parityPostRestoreRows)
	}
	return nil
}
