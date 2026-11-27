//go:build linux || (darwin && cgo)

package fuzz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRetainedFixtureIsolation(t *testing.T) {
	fixture, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	untouched, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	for _, tc := range retainedCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := tc.prepare(fixture)
			b := tc.prepare(untouched)
			if !retainedImagesEqual(a, b) {
				t.Fatal("seed did not reproduce fixture bytes")
			}
			for _, image := range []files{a.main, a.stage} {
				for name, data := range image {
					for i := range data {
						data[i] ^= 0xff
					}
					delete(image, name)
				}
			}
			next := tc.prepare(fixture)
			if !retainedImagesEqual(next, b) {
				t.Fatal("mutation leaked into the seed or another preparation")
			}
		})
	}
	t.Cleanup(func() {
		requireRetainedOK(t, compareReplayFiles(fixture.previous.image, untouched.previous.image))
		requireRetainedOK(t, compareReplayFiles(fixture.successor.image, untouched.successor.image))
	})
}

func TestRetainedWorkerTimeout(t *testing.T) {
	executable, err := os.Executable()
	requireRetainedOK(t, err)
	parent := t.TempDir()
	var cmd *exec.Cmd
	output, err := runRetainedWorker(t.Context(), parent, 2*time.Second, func(ctx context.Context, root string) *exec.Cmd {
		cmd = exec.CommandContext(ctx, executable, "-test.run=^TestRetainedTimeoutWorker$", "-test.v")
		cmd.Env = append(cmd.Environ(), "BOG_FUZZ_RETAINED_TIMEOUT_ROOT="+root)
		return cmd
	})
	var exit *exec.ExitError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &exit) ||
		cmd.ProcessState == nil || !bytes.Contains(output, []byte("retained timeout worker started")) {
		t.Fatalf("wanted a running worker to hit its deadline and be reaped, got %v\n%s", err, output)
	}
	assertRetainedScratchEmpty(t, parent)
}

func TestRetainedTimeoutWorker(t *testing.T) {
	root := os.Getenv("BOG_FUZZ_RETAINED_TIMEOUT_ROOT")
	if root == "" {
		t.Skip("started by the timeout test")
	}
	fixture := filepath.Join(root, "pending")
	requireRetainedOK(t, os.Mkdir(fixture, 0700))
	requireRetainedOK(t, os.WriteFile(filepath.Join(fixture, "artifact"), []byte("partial fixture"), 0600))
	_, err := fmt.Fprintln(os.Stdout, "retained timeout worker started")
	requireRetainedOK(t, err)
	select {
	case <-time.After(30 * time.Second):
		t.Fatal("parent failed to terminate the worker")
	}
}

func TestRetainedCanceledCaseCleanup(t *testing.T) {
	parent := t.TempDir()
	fixture, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := measureRetainedCase(ctx, parent, fixture, retainedCases()[0])
	if !errors.Is(err, context.Canceled) || report != (retainedRecoveryEvidence{}) {
		t.Fatalf("canceled case returned evidence %+v and error %v", report, err)
	}
	assertRetainedScratchEmpty(t, parent)
}

func assertRetainedScratchEmpty(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	requireRetainedOK(t, err)
	if len(entries) != 0 {
		t.Fatalf("scratch directory still contains %v", entries)
	}
}

func TestRetainedEvidenceContract(t *testing.T) {
	fixture, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	tc := findRetainedCase(t, "old-metadata-with-new-generation")
	report, err := measureRetainedCase(t.Context(), t.TempDir(), fixture, tc)
	requireRetainedOK(t, err)
	encoded, err := json.Marshal(report)
	requireRetainedOK(t, err)
	output := []byte("worker log\n" + retainedEvidencePrefix + string(encoded) + "\nPASS\n")
	got, err := decodeRetainedEvidence(output)
	requireRetainedOK(t, err)
	requireRetainedOK(t, validateRetainedEvidence(got, fixture, tc))
	if got != report {
		t.Fatalf("decoded evidence %+v, want %+v", got, report)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"missing", []byte("PASS")},
		{"duplicate", bytes.Repeat(output, 2)},
		{"invalid-json", []byte(retainedEvidencePrefix + `{"seed":`)},
		{"unknown-field", []byte(retainedEvidencePrefix + `{"unknown":1}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeRetainedEvidence(tc.data); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
	for _, tcEdit := range []struct {
		name string
		edit func(*retainedRecoveryEvidence)
	}{
		{"lost-cleanup", func(r *retainedRecoveryEvidence) { r.FixtureRemoved = false }},
		{"deadline", func(r *retainedRecoveryEvidence) { r.ElapsedNS = r.DeadlineNS + 1 }},
		{"allocation", func(r *retainedRecoveryEvidence) { r.AllocatedBytes = r.AllocationLimitBytes + 1 }},
		{"wrong-generation", func(r *retainedRecoveryEvidence) { r.Generation = "wrong" }},
		{"wrong-suffix", func(r *retainedRecoveryEvidence) { r.CommittedEnd++ }},
		{"wrong-rejection", func(r *retainedRecoveryEvidence) { r.Rejection = "wrong" }},
	} {
		t.Run(tcEdit.name, func(t *testing.T) {
			damaged := report
			tcEdit.edit(&damaged)
			if err := validateRetainedEvidence(damaged, fixture, tc); err == nil {
				t.Fatal("inconsistent evidence accepted")
			}
		})
	}
}

func retainedImagesEqual(a, b retainedImages) bool {
	return (a.stage == nil) == (b.stage == nil) && a.truncateIntent == b.truncateIntent &&
		compareReplayFiles(a.main, b.main) == nil && compareReplayFiles(a.stage, b.stage) == nil
}
