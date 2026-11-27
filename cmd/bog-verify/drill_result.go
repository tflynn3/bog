package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"bog/internal/backup"
	"bog/internal/client"
)

const drillSchemaVersion = 1

type drillPhase string

const (
	phaseBeforeRestart   drillPhase = "before_restart"
	phaseRestartBoundary drillPhase = "restart_boundary"
	phaseAfterRestart    drillPhase = "after_restart"
)

type phaseStatus string

const (
	phaseNotRun  phaseStatus = "not_run"
	phaseMatched phaseStatus = "matched"
	phaseFailed  phaseStatus = "failed"
)

type drillResult struct {
	SchemaVersion int              `json:"schema_version"`
	Parity        bool             `json:"parity"`
	ExitStatus    int              `json:"exit_status"`
	RecoveryPoint *recoveryPoint   `json:"recovery_point"`
	BeforeRestart drillPhaseResult `json:"before_restart"`
	AfterRestart  drillPhaseResult `json:"after_restart"`
	Diagnostic    *drillDiagnostic `json:"diagnostic"`
}

type recoveryPoint struct {
	Generation    string `json:"generation"`
	CoveredOffset int64  `json:"covered_offset"`
	NextOrdinal   uint64 `json:"next_ordinal"`
	CommittedEnd  int64  `json:"committed_end"`
}

func resultPoint(p backup.Point) *recoveryPoint {
	return &recoveryPoint{
		Generation:    hex.EncodeToString(p.Generation[:]),
		CoveredOffset: p.CoveredOffset,
		NextOrdinal:   p.NextOrdinal,
		CommittedEnd:  p.CommittedEnd,
	}
}

type drillPhaseResult struct {
	Status phaseStatus  `json:"status"`
	Counts *drillCounts `json:"counts"`
}

type drillCounts struct {
	Rows           uint64 `json:"rows"`
	ReferencePages int    `json:"reference_pages"`
	ServerPages    int    `json:"server_pages"`
}

func (p *drillPhaseResult) recordCounts(counts *comparison) {
	if counts != nil {
		p.Counts = &drillCounts{counts.matched, counts.referencePages, counts.livePages}
	}
}

type drillDiagnostic struct {
	Phase   drillPhase `json:"phase"`
	Code    string     `json:"code"`
	Message string     `json:"message"`
}

func (r drillResult) fail(phase drillPhase, status int, err error) (drillResult, error) {
	r.ExitStatus = status
	r.Diagnostic = &drillDiagnostic{phase, drillDiagnosticCode(status, err), err.Error()}
	return r, err
}

func (p recoveryPoint) String() string {
	return fmt.Sprintf("generation=%s covered_offset=%d next_ordinal=%d committed_end=%d",
		p.Generation, p.CoveredOffset, p.NextOrdinal, p.CommittedEnd)
}

func (c drillCounts) String() string {
	return fmt.Sprintf("rows=%d reference_pages=%d server_pages=%d", c.Rows, c.ReferencePages, c.ServerPages)
}

func drillDiagnosticCode(status int, err error) string {
	switch {
	case status == exitVerification:
		return "backup_verification_failed"
	case status == exitMismatch:
		return "mismatch"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, client.ErrCaptureExpired):
		return "cursor_expired"
	case errors.Is(err, client.ErrInvalidCursor):
		return "invalid_cursor"
	case errors.Is(err, client.ErrPageRetryExhausted):
		return "retries_exhausted"
	case errors.Is(err, client.ErrPageBudget):
		return "page_budget_exhausted"
	case errors.Is(err, client.ErrPageBytes):
		return "page_too_large"
	default:
		return "incomplete"
	}
}

func writeDrillResult(out io.Writer, result drillResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	data = append(data, '\n')
	n, err := out.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("write result: %w", err)
	}
	return nil
}
