package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"path/filepath"
	"regexp"
	"slices"

	"bog/verify/capacityevidence"
	"bog/verify/reportpublication"
)

const schemaVersion = 1
const capacityCase = capacityevidence.Case
const saturationFixtureCase = "saturation_fixture"

var fullRevision = regexp.MustCompile(`^[0-9a-f]{40}$`)

type identity struct {
	SchemaVersion int    `json:"schema_version"`
	Revision      string `json:"revision"`
	BinarySHA256  string `json:"binary_sha256"`
	Fixture       string `json:"fixture"`
}

type enospcEvidence struct {
	Errno                     string `json:"errno"`
	DisposableLocalFilesystem bool   `json:"disposable_local_filesystem"`
	ObservedENOSPC            bool   `json:"observed_enospc"`
	RecoveredAfterSpaceFreed  bool   `json:"recovered_after_space_freed"`
}

type outcome struct {
	Status   string         `json:"status"`
	Detail   *string        `json:"detail,omitempty"`
	Evidence jsontext.Value `json:"evidence,omitempty"`
}

type report struct {
	identity
	Cases map[string]outcome `json:"cases"`
}

var admissionCases = []string{"one_slot_saturation", "ingest_during_saturation", "cursor_resume", "work_budget"}
var durabilityCases = []string{"sigkill", "recovery", "enospc"}
var capacityCases = []string{capacityCase}

func readIdentity(ctx context.Context, binary, revision, fixture string) (id identity, err error) {
	if !filepath.IsAbs(binary) || !fullRevision.MatchString(revision) || fixture == "" {
		return id, errors.New("require an absolute -binary path, a full lowercase SHA in -revision, and a nonempty -fixture")
	}
	file, info, err := openRegular(ctx, nil, binary, -1)
	if err != nil {
		return id, fmt.Errorf("open candidate %q: %w", binary, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close candidate %q: %w", binary, closeErr))
		}
	}()
	hash := sha256.New()
	copied, err := io.Copy(hash, newArtifactReader(ctx, file, info.Size()))
	if err != nil {
		return id, fmt.Errorf("hash candidate %q: %w", binary, err)
	}
	if copied != info.Size() {
		return id, fmt.Errorf("hash candidate %q: file shrank during read", binary)
	}
	if copied == 0 {
		return id, errors.New("candidate must be nonempty")
	}
	return identity{schemaVersion, revision, hex.EncodeToString(hash.Sum(nil)), fixture}, nil
}

// Decode contract keys by exact spelling while allowing producer extensions.
func decodeFields(data []byte, fields map[string]any, nullable ...string) (map[string]jsontext.Value, error) {
	var object map[string]jsontext.Value
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("%w: malformed JSON object: %w", errArtifactMalformed, err)
	}
	if object == nil {
		return nil, fmt.Errorf("%w: expected JSON object", errArtifactMalformed)
	}
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		raw, exists := object[key]
		if !exists || raw.Kind() == 'n' {
			return nil, fmt.Errorf("%w: missing or null %q", errArtifactMalformed, key)
		}
		if err := json.Unmarshal(raw, fields[key]); err != nil {
			return nil, fmt.Errorf("%w: %s: %w", errArtifactMalformed, key, err)
		}
	}
	for _, key := range nullable {
		if _, exists := object[key]; !exists {
			return nil, fmt.Errorf("%w: missing %q", errArtifactMalformed, key)
		}
	}
	return object, nil
}

type validationOptions struct {
	requireRetarget bool
	requireCutover  bool
	// The release caller supplies the independent fixture observations.
	// Supplying them also requires the restore drill case in this report.
	restoreDrill *restoreDrillExpectations
}

func validateReport(data []byte, expected identity, required []string) error {
	return validateReportWithOptions(data, expected, required, validationOptions{})
}

func validateReportWithOptions(data []byte, expected identity, required []string, opts validationOptions) error {
	var got identity
	var cases map[string]jsontext.Value
	_, err := decodeFields(data, map[string]any{
		"schema_version": &got.SchemaVersion, "revision": &got.Revision,
		"binary_sha256": &got.BinarySHA256, "fixture": &got.Fixture, "cases": &cases,
	})
	if err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf("report identity mismatch: got %+v, want %+v", got, expected)
	}
	if opts.restoreDrill != nil && !slices.Contains(required, restoreDrillCase) {
		required = append(slices.Clone(required), restoreDrillCase)
	}
	// Prefer failed probes and cleanup details to skipped prerequisites.
	// Retain all malformed or failing cases in stable order for diagnosis.
	var failures, skips []error
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		raw := cases[name]
		mandatory := slices.Contains(required, name) || name == saturationFixtureCase || name == restoreDrillCase
		if err := validateOutcome(raw, name, mandatory, opts); err != nil {
			problem := fmt.Errorf("case %q: %w", name, err)
			if errors.Is(err, errSkippedCase) {
				skips = append(skips, problem)
			} else {
				failures = append(failures, problem)
			}
		}
	}
	problems := append(failures, skips...)
	for _, name := range slices.Compact(slices.Sorted(slices.Values(required))) {
		if _, ok := cases[name]; !ok {
			problems = append(problems, fmt.Errorf("missing required case %q", name))
		}
	}
	if err := errors.Join(problems...); err != nil {
		return err
	}
	if slices.Contains(required, capacityCase) {
		return validateCapacityReport(data, opts.requireCutover)
	}
	return nil
}

var errSkippedCase = errors.New("expected pass, got \"skip\"")

func validateOutcome(raw jsontext.Value, name string, mandatory bool, opts validationOptions) error {
	var status string
	fields, err := decodeFields(raw, map[string]any{"status": &status})
	if err != nil {
		return err
	}
	var detail *string
	if rawDetail, ok := fields["detail"]; ok {
		if err := json.Unmarshal(rawDetail, &detail); err != nil || detail == nil {
			return errors.New("detail must be a string")
		}
	}
	var statusErr error
	switch status {
	case "pass":
	case "fail", "skip":
		if mandatory {
			statusErr = errSkippedCase
			if status == "fail" {
				statusErr = errors.New("expected pass, got \"fail\"")
			}
			if detail != nil && *detail != "" {
				statusErr = fmt.Errorf("%w: %s", statusErr, *detail)
			}
		}
	default:
		return fmt.Errorf("unknown status %q", status)
	}
	if name == restoreDrillCase {
		return errors.Join(statusErr, validateRestoreDrillOutcome(status, fields["evidence"], opts.restoreDrill))
	}
	if statusErr != nil {
		return statusErr
	}
	if !mandatory {
		return nil
	}
	switch name {
	case "enospc":
		return validateENOSPC(fields["evidence"], opts.requireRetarget)
	case ordinaryWALCase:
		if opts.requireCutover {
			return validateOrdinaryWAL(fields["evidence"])
		}
	case restoredParityCase:
		if opts.requireCutover {
			return validateRestoredParity(fields["evidence"])
		}
	case tailOnlyParityCase:
		if opts.requireCutover {
			return validateTailOnlyParity(fields["evidence"])
		}
	case postRestoreAckCrashCase:
		if opts.requireCutover {
			return validatePostRestoreAckCrash(fields["evidence"])
		}
	}
	return nil
}

func validateENOSPC(raw jsontext.Value, requireRetarget bool) error {
	var proof enospcEvidence
	evidence, err := decodeFields(raw, map[string]any{
		"errno": &proof.Errno, "disposable_local_filesystem": &proof.DisposableLocalFilesystem,
		"observed_enospc": &proof.ObservedENOSPC, "recovered_after_space_freed": &proof.RecoveredAfterSpaceFreed,
	})
	if err != nil {
		return fmt.Errorf("kernel ENOSPC evidence: %w", err)
	}
	if proof.Errno != "ENOSPC" || !proof.DisposableLocalFilesystem || !proof.ObservedENOSPC || !proof.RecoveredAfterSpaceFreed {
		return errors.New("require observed kernel ENOSPC on disposable local storage and recovery after freeing space")
	}
	_, healthyToFull := evidence["healthy_to_full"]
	_, fullToHealthy := evidence["full_to_healthy"]
	// Historical schema-v1 reports omit both directions.
	if !requireRetarget && !healthyToFull && !fullToHealthy {
		return nil
	}
	return validateRetarget(evidence)
}

func validateRetarget(evidence map[string]jsontext.Value) error {
	var readiness, ingest int
	var reason string
	var replayed bool
	if _, err := decodeFields(evidence["healthy_to_full"], map[string]any{
		"readiness_status": &readiness, "readiness_reason": &reason,
		"ingest_status": &ingest, "replayed_after_restart": &replayed,
	}); err != nil {
		return fmt.Errorf("healthy_to_full: %w", err)
	}
	if readiness != http.StatusOK || reason != "ready" || ingest != http.StatusNoContent || !replayed {
		return errors.New("healthy_to_full: require ready (200), acknowledged ingest (204), and replay after restart")
	}
	var rollback, category string
	if _, err := decodeFields(evidence["full_to_healthy"], map[string]any{
		"readiness_status": &readiness, "readiness_reason": &reason,
		"ingest_status": &ingest, "post_ingest_reason": &rollback, "post_ingest_category": &category,
	}); err != nil {
		return fmt.Errorf("full_to_healthy: %w", err)
	}
	if readiness != http.StatusServiceUnavailable || reason != "low_disk_headroom" ||
		ingest != http.StatusServiceUnavailable || rollback != "wal_rollback" || category != "enospc" {
		return errors.New("full_to_healthy: require low_disk_headroom (503), rejected ingest (503), and wal_rollback (enospc)")
	}
	return nil
}

const maxReportBytes = 16 << 20

func validateFile(ctx context.Context, path string, expected identity, required []string, opts validationOptions) error {
	data, err := readArtifact(ctx, nil, path, maxReportBytes)
	if err != nil {
		return err
	}
	err = errors.Join(validateReportWithOptions(data, expected, required, opts), ctx.Err())
	if err != nil {
		return fmt.Errorf("%w (validate report %q)", err, path)
	}
	return nil
}

func writeReport(path string, results report) error {
	data, err := jsonv1.MarshalIndent(results, "", "  ")
	if err != nil {
		return &reportpublication.Error{Path: path, State: reportpublication.Unpublished, Err: fmt.Errorf("encode report: %w", err)}
	}
	return reportpublication.WriteFile(path, append(data, '\n'), nil)
}
