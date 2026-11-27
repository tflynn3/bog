package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"
)

const (
	restoreDrillCase          = "restore_drill"
	restoreDrillSchemaVersion = 1

	drillBeforeRestart   = "before_restart"
	drillRestartBoundary = "restart_boundary"
	drillAfterRestart    = "after_restart"

	drillMatched = "matched"
	drillFailed  = "failed"
	drillNotRun  = "not_run"

	drillExitOK           = 0
	drillExitVerification = 3
	drillExitIncomplete   = 4
	drillExitMismatch     = 5
)

// The case evidence is the unmodified JSON result of bog-verify drill -json.
// Candidate identity comes from the enclosing release report. The caller
// obtains the expected point from the verified fixture and supplies the
// counts independently of the result, with separate expectations per walk.
type restoreDrillExpectations struct {
	Point         restoreDrillPoint
	Before, After restoreDrillCounts
}

type restoreDrillPoint struct {
	Generation    string
	CoveredOffset int64
	NextOrdinal   uint64
	CommittedEnd  int64
}

type restoreDrillCounts struct {
	Rows           uint64
	ReferencePages int
	ServerPages    int
}

type restoreDrillPhase struct {
	Status string
	Counts *restoreDrillCounts
}

type restoreDrillDiagnostic struct {
	Phase   string
	Code    string
	Message string
}

type restoreDrillResult struct {
	SchemaVersion int
	Parity        bool
	ExitStatus    int
	Point         *restoreDrillPoint
	Before, After restoreDrillPhase
	Diagnostic    *restoreDrillDiagnostic
}

func validateRestoreDrillOutcome(status string, raw jsontext.Value, expected *restoreDrillExpectations) error {
	if status != "pass" && len(raw) == 0 {
		// A failed or skipped producer may never have emitted a JSON result.
		return nil
	}
	result, err := decodeRestoreDrillResult(raw)
	if err != nil {
		return fmt.Errorf("restore drill result: %w", err)
	}

	var proofErr error
	if expected != nil {
		proofErr = validateRestoreDrillObservations(result, *expected)
	} else if status == "pass" {
		proofErr = errors.New("restore drill requires independent recovery point and phase counts")
	}
	return errors.Join(validateRestoreDrillStatus(status, result), proofErr, result.reportedFailure())
}

func decodeRestoreDrillResult(data []byte) (restoreDrillResult, error) {
	var result restoreDrillResult
	var before, after jsontext.Value
	fields, err := decodeFields(data, map[string]any{
		"schema_version": &result.SchemaVersion, "parity": &result.Parity, "exit_status": &result.ExitStatus,
		drillBeforeRestart: &before, drillAfterRestart: &after,
	}, "recovery_point", "diagnostic")
	if err != nil {
		return result, err
	}
	if result.SchemaVersion != restoreDrillSchemaVersion {
		return result, fmt.Errorf("schema_version=%d, expected %d", result.SchemaVersion, restoreDrillSchemaVersion)
	}
	if raw := fields["recovery_point"]; raw.Kind() != 'n' {
		result.Point = new(restoreDrillPoint)
		if _, err := decodeFields(raw, map[string]any{
			"generation": &result.Point.Generation, "covered_offset": &result.Point.CoveredOffset,
			"next_ordinal": &result.Point.NextOrdinal, "committed_end": &result.Point.CommittedEnd,
		}); err != nil {
			return result, fmt.Errorf("recovery_point: %w", err)
		}
	}
	if raw := fields["diagnostic"]; raw.Kind() != 'n' {
		result.Diagnostic = new(restoreDrillDiagnostic)
		if _, err := decodeFields(raw, map[string]any{
			"phase": &result.Diagnostic.Phase, "code": &result.Diagnostic.Code, "message": &result.Diagnostic.Message,
		}); err != nil {
			return result, fmt.Errorf("diagnostic: %w", err)
		}
	}

	result.Before, err = decodeRestoreDrillPhase(before)
	if err != nil {
		return result, fmt.Errorf("%s: %w", drillBeforeRestart, err)
	}
	result.After, err = decodeRestoreDrillPhase(after)
	if err != nil {
		return result, fmt.Errorf("%s: %w", drillAfterRestart, err)
	}
	return result, nil
}

func decodeRestoreDrillPhase(raw jsontext.Value) (restoreDrillPhase, error) {
	var phase restoreDrillPhase
	fields, err := decodeFields(raw, map[string]any{"status": &phase.Status}, "counts")
	if err != nil {
		return phase, err
	}
	if raw := fields["counts"]; raw.Kind() != 'n' {
		phase.Counts = new(restoreDrillCounts)
		if _, err := decodeFields(raw, map[string]any{
			"rows": &phase.Counts.Rows, "reference_pages": &phase.Counts.ReferencePages, "server_pages": &phase.Counts.ServerPages,
		}); err != nil {
			return phase, fmt.Errorf("counts: %w", err)
		}
	}
	return phase, nil
}

func validateRestoreDrillStatus(status string, result restoreDrillResult) error {
	phaseErr := errors.Join(
		validateDrillPhaseCounts(drillBeforeRestart, result.Before),
		validateDrillPhaseCounts(drillAfterRestart, result.After),
	)
	var pointErr error
	if result.Point != nil {
		pointErr = validateDrillPoint(*result.Point)
	}
	var statusErr error
	switch status {
	case "pass":
		statusErr = validateDrillSuccess(result)
	case "fail":
		statusErr = validateDrillFailure(result)
	case "skip":
		statusErr = errors.New("skipped restore drill must not carry an executed result")
	}
	return errors.Join(statusErr, pointErr, phaseErr)
}

func validateDrillPoint(point restoreDrillPoint) error {
	digest, err := hex.DecodeString(point.Generation)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != point.Generation {
		return errors.New("recovery_point.generation must be a 64-character lowercase hexadecimal digest")
	}
	if point.CoveredOffset < 0 || point.CommittedEnd < point.CoveredOffset {
		return fmt.Errorf("recovery_point has invalid WAL bounds (covered_offset=%d, committed_end=%d)",
			point.CoveredOffset, point.CommittedEnd)
	}
	return nil
}

func validateDrillPhaseCounts(name string, phase restoreDrillPhase) error {
	switch phase.Status {
	case drillNotRun:
		if phase.Counts != nil {
			return fmt.Errorf("%s: not_run requires null counts", name)
		}
	case drillMatched:
		if phase.Counts == nil || phase.Counts.ReferencePages <= 0 || phase.Counts.ServerPages <= 0 {
			return fmt.Errorf("%s: matched requires counts with at least one accepted page per stream", name)
		}
	case drillFailed:
		if c := phase.Counts; c != nil && (c.ReferencePages < 0 || c.ServerPages < 0 ||
			c.ServerPages > 0 && c.ReferencePages == 0 || c.Rows > 0 && c.ServerPages == 0) {
			return fmt.Errorf("%s: invalid partial counts (rows=%d, reference_pages=%d, server_pages=%d)",
				name, c.Rows, c.ReferencePages, c.ServerPages)
		}
	default:
		return fmt.Errorf("%s: unknown status %q", name, phase.Status)
	}
	return nil
}

func validateDrillSuccess(result restoreDrillResult) error {
	if !result.Parity || result.ExitStatus != drillExitOK || result.Diagnostic != nil {
		return fmt.Errorf("passing restore drill requires parity=true, exit_status=0, and diagnostic=null (got parity=%t, exit_status=%d)",
			result.Parity, result.ExitStatus)
	}
	if err := checkDrillPhaseOrder(result, drillMatched, drillMatched); err != nil {
		return err
	}
	if result.Point == nil {
		return errors.New("passing restore drill requires a recovery_point")
	}
	return nil
}

func validateDrillFailure(result restoreDrillResult) error {
	if result.Parity || result.Diagnostic == nil {
		return errors.New("failed restore drill requires parity=false and a diagnostic")
	}
	d := result.Diagnostic
	if strings.TrimSpace(d.Message) == "" {
		return errors.New("failed restore drill requires a nonempty diagnostic.message")
	}
	if err := validateDrillFailureCode(result.ExitStatus, d.Code); err != nil {
		return err
	}
	switch d.Phase {
	case drillBeforeRestart:
		if err := checkDrillPhaseOrder(result, drillFailed, drillNotRun); err != nil {
			return err
		}
		if (result.Point == nil) != (result.Before.Counts == nil) {
			return errors.New("before_restart failure must record the recovery_point and counts together once comparison starts")
		}
		if result.ExitStatus == drillExitVerification && result.Point != nil {
			return errors.New("before_restart verification failure requires null recovery_point")
		}
		return validateDrillMismatchCounts(result.ExitStatus, result.Before.Counts)
	case drillRestartBoundary:
		if err := checkDrillPhaseOrder(result, drillMatched, drillNotRun); err != nil {
			return err
		}
		if result.Point == nil {
			return errors.New("restart_boundary failure requires the recovery_point from before_restart")
		}
		switch d.Code {
		case "canceled", "deadline_exceeded", "incomplete":
			return nil
		default:
			return fmt.Errorf("diagnostic.code %q cannot occur at restart_boundary", d.Code)
		}
	case drillAfterRestart:
		if err := checkDrillPhaseOrder(result, drillMatched, drillFailed); err != nil {
			return err
		}
		if result.Point == nil {
			return errors.New("after_restart failure requires the recovery_point from before_restart")
		}
		if result.ExitStatus == drillExitVerification && result.After.Counts != nil {
			return errors.New("after_restart verification failure requires null after_restart.counts")
		}
		return validateDrillMismatchCounts(result.ExitStatus, result.After.Counts)
	default:
		return fmt.Errorf("unknown restore drill diagnostic.phase %q", d.Phase)
	}
}

func checkDrillPhaseOrder(result restoreDrillResult, before, after string) error {
	if result.Before.Status != before || result.After.Status != after {
		return fmt.Errorf("restore drill phase order: %s.status=%q and %s.status=%q, expected %q and %q",
			drillBeforeRestart, result.Before.Status, drillAfterRestart, result.After.Status, before, after)
	}
	return nil
}

func validateDrillFailureCode(status int, code string) error {
	switch code {
	case "backup_verification_failed":
		if status == drillExitVerification {
			return nil
		}
	case "mismatch":
		if status == drillExitMismatch {
			return nil
		}
	case "cursor_expired", "invalid_cursor", "retries_exhausted", "page_budget_exhausted",
		"page_too_large", "canceled", "deadline_exceeded", "incomplete":
		if status == drillExitIncomplete {
			return nil
		}
	default:
		return fmt.Errorf("unknown restore drill diagnostic.code %q", code)
	}
	return fmt.Errorf("restore drill diagnostic.code %q is inconsistent with exit_status=%d", code, status)
}

func validateDrillMismatchCounts(status int, counts *restoreDrillCounts) error {
	if status == drillExitMismatch && (counts == nil || counts.ReferencePages <= 0 || counts.ServerPages <= 0) {
		return errors.New("restore drill mismatch requires counts with accepted pages from both streams")
	}
	return nil
}

func validateRestoreDrillObservations(result restoreDrillResult, expected restoreDrillExpectations) error {
	var pointErr error
	if result.Point != nil && *result.Point != expected.Point {
		pointErr = fmt.Errorf("recovery_point mismatch: got %+v, want %+v", *result.Point, expected.Point)
	}
	return errors.Join(
		pointErr,
		checkDrillCounts(drillBeforeRestart, result.Before, expected.Before),
		checkDrillCounts(drillAfterRestart, result.After, expected.After),
	)
}

func checkDrillCounts(name string, phase restoreDrillPhase, expected restoreDrillCounts) error {
	if phase.Counts == nil {
		return nil // Presence constraints depend on the phase status.
	}
	got := *phase.Counts
	if phase.Status == drillMatched && got != expected {
		return fmt.Errorf("%s counts mismatch: got rows=%d reference_pages=%d server_pages=%d, want rows=%d reference_pages=%d server_pages=%d",
			name, got.Rows, got.ReferencePages, got.ServerPages, expected.Rows, expected.ReferencePages, expected.ServerPages)
	}
	if phase.Status == drillFailed && got.Rows > expected.Rows {
		return fmt.Errorf("%s matched rows=%d exceeds fixture rows=%d", name, got.Rows, expected.Rows)
	}
	return nil
}

func (r restoreDrillResult) reportedFailure() error {
	if d := r.Diagnostic; d != nil {
		return fmt.Errorf("restore drill diagnostic %s (%s, exit_status=%d): %s", d.Phase, d.Code, r.ExitStatus, d.Message)
	}
	return nil
}
