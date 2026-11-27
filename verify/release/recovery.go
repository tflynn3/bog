package main

import (
	"encoding/json/jsontext"
	"fmt"
	"net/http"
)

const (
	ordinaryWALCase    = "ordinary_wal_reopen"
	restoredParityCase = "restored_parity"

	// Row counts for the linux-release-v1 recovery fixtures.
	paritySeedRows        = 3
	parityFixtureRows     = 6
	parityPostRestoreRows = parityFixtureRows + 1
)

type ordinaryWALProof struct {
	ReopenedRows int `json:"reopened_rows"`
	AppendStatus int `json:"append_status"`
	RestartRows  int `json:"restart_rows"`
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
