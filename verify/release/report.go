package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"syscall"
)

const schemaVersion = 1

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
var capacityCases = []string{"retained_history_capacity"}

func readIdentity(binary, revision, fixture string) (id identity, err error) {
	if !filepath.IsAbs(binary) || !fullRevision.MatchString(revision) || fixture == "" {
		return id, errors.New("require an absolute -binary path, a full lowercase SHA in -revision, and a nonempty -fixture")
	}
	file, err := openRegular(binary)
	if err != nil {
		return id, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	hash := sha256.New()
	copied, err := io.Copy(hash, file)
	if err != nil {
		return id, err
	}
	if copied == 0 {
		return id, errors.New("candidate must be nonempty")
	}
	return identity{schemaVersion, revision, hex.EncodeToString(hash.Sum(nil)), fixture}, nil
}

// Decode contract keys by exact spelling while allowing producer extensions.
func decodeFields(data []byte, fields map[string]any) (map[string]jsontext.Value, error) {
	var object map[string]jsontext.Value
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("malformed JSON object: %w", err)
	}
	if object == nil {
		return nil, errors.New("expected JSON object")
	}
	for key, dest := range fields {
		raw, exists := object[key]
		if !exists || raw.Kind() == 'n' {
			return nil, fmt.Errorf("missing or null %q", key)
		}
		if err := json.Unmarshal(raw, dest); err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
	}
	return object, nil
}

func validateReport(data []byte, expected identity, required []string) error {
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
	for _, name := range required {
		if _, ok := cases[name]; !ok {
			return fmt.Errorf("missing required case %q", name)
		}
	}
	for name, raw := range cases {
		mandatory := slices.Contains(required, name)
		if err := validateOutcome(raw, mandatory, mandatory && name == "enospc"); err != nil {
			return fmt.Errorf("case %q: %w", name, err)
		}
	}
	return nil
}

func validateOutcome(raw jsontext.Value, mandatory, requireENOSPC bool) error {
	var status string
	fields, err := decodeFields(raw, map[string]any{"status": &status})
	if err != nil {
		return err
	}
	if detail, ok := fields["detail"]; ok {
		var text *string
		if err := json.Unmarshal(detail, &text); err != nil || text == nil {
			return errors.New("detail must be a string")
		}
	}
	switch status {
	case "pass":
	case "fail", "skip":
		if mandatory {
			return fmt.Errorf("expected pass, got %q", status)
		}
	default:
		return fmt.Errorf("unknown status %q", status)
	}
	if !requireENOSPC {
		return nil
	}
	var proof enospcEvidence
	_, err = decodeFields(fields["evidence"], map[string]any{
		"errno": &proof.Errno, "disposable_local_filesystem": &proof.DisposableLocalFilesystem,
		"observed_enospc": &proof.ObservedENOSPC, "recovered_after_space_freed": &proof.RecoveredAfterSpaceFreed,
	})
	if err != nil {
		return fmt.Errorf("kernel ENOSPC evidence: %w", err)
	}
	if proof.Errno != "ENOSPC" || !proof.DisposableLocalFilesystem || !proof.ObservedENOSPC || !proof.RecoveredAfterSpaceFreed {
		return errors.New("require observed kernel ENOSPC on disposable local storage and recovery after freeing space")
	}
	return nil
}

func validateFile(path string, expected identity, required []string) (err error) {
	file, err := openRegular(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	const maxReport = 16 << 20
	data, err := io.ReadAll(io.LimitReader(file, maxReport+1))
	if err != nil {
		return err
	}
	if len(data) > maxReport {
		return errors.New("report exceeds 16 MiB")
	}
	return validateReport(data, expected, required)
}

func openRegular(path string) (*os.File, error) {
	// Open without blocking on pipes or devices supplied in place of a file.
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("%s is not a regular file", path)
	}
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}
