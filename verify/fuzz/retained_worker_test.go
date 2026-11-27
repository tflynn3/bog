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
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	retainedWorkerEnv       = "BOG_FUZZ_RETAINED_RECOVERY_CASE"
	retainedEvidencePrefix  = "retained_recovery "
	retainedWorkerDeadline  = 10 * time.Second
	retainedAllocationBytes = 16 << 20
)

type retainedRecoveryEvidence struct {
	Seed                 uint8           `json:"seed"`
	Case                 string          `json:"case"`
	Outcome              retainedOutcome `json:"outcome"`
	Rejection            string          `json:"rejection,omitempty"`
	Generation           string          `json:"validated_generation"`
	PrefixSHA256         string          `json:"prefix_sha256"`
	CoveredOffset        int64           `json:"covered_offset"`
	CommittedEnd         int64           `json:"committed_end"`
	CoveredRows          uint64          `json:"covered_rows"`
	SuffixFrames         int             `json:"suffix_frames"`
	TotalRows            int             `json:"total_rows"`
	ElapsedNS            int64           `json:"elapsed_ns"`
	DeadlineNS           int64           `json:"deadline_ns"`
	AllocatedBytes       uint64          `json:"allocated_bytes"`
	AllocationLimitBytes uint64          `json:"allocation_limit_bytes"`
	ReplayLimitBytes     uint64          `json:"replay_limit_bytes"`
	FixtureRemoved       bool            `json:"fixture_removed"`
}

func measureRetainedCase(ctx context.Context, parent string, f retainedFixture, tc retainedCase) (retainedRecoveryEvidence, error) {
	ctx, cancel := context.WithTimeout(ctx, replayDeadline)
	defer cancel()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	root, err := os.MkdirTemp(parent, "retained-case-")
	if err != nil {
		return retainedRecoveryEvidence{}, err
	}
	g, operationErr := runRetainedOperation(ctx, root, f, tc)
	err = errors.Join(operationErr, os.RemoveAll(root), checkRetainedAbsent(root), ctx.Err())
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if elapsed > replayDeadline {
		err = errors.Join(err, fmt.Errorf("case elapsed %s exceeds %s: %w", elapsed, replayDeadline, context.DeadlineExceeded))
	}
	if allocated > retainedAllocationBytes {
		err = errors.Join(err, fmt.Errorf("%w: case allocated %d bytes, limit %d", errReplayLimit, allocated, retainedAllocationBytes))
	}
	if err != nil {
		return retainedRecoveryEvidence{}, err
	}
	b := g.view.boundary(g.id)
	var rejection string
	if tc.reject != nil {
		rejection = tc.reject.Error()
	}
	return retainedRecoveryEvidence{
		Seed: retainedSeed, Case: tc.name, Outcome: tc.outcome, Rejection: rejection,
		Generation: fmt.Sprintf("%x", b.generation), PrefixSHA256: fmt.Sprintf("%x", b.prefixHash),
		CoveredOffset: b.covered, CommittedEnd: b.end, CoveredRows: b.next,
		SuffixFrames: len(f.cuts) - 1 - slices.Index(f.cuts, g.view.covered), TotalRows: len(g.view.rows),
		ElapsedNS: int64(elapsed), DeadlineNS: int64(replayDeadline),
		AllocatedBytes: allocated, AllocationLimitBytes: retainedAllocationBytes,
		ReplayLimitBytes: retainedReplayBytes, FixtureRemoved: true,
	}, nil
}

func runRetainedWorker(ctx context.Context, parent string, deadline time.Duration, command func(context.Context, string) *exec.Cmd) (output []byte, err error) {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	root, err := os.MkdirTemp(parent, "retained-worker-")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, os.RemoveAll(root), checkRetainedAbsent(root))
	}()
	cmd := command(ctx, root)
	cmd.WaitDelay = time.Second
	output, err = runBoundedReplayWorker(cmd)
	if ctx.Err() != nil {
		err = errors.Join(err, fmt.Errorf("retained worker exceeded its context (%s): %w", deadline, ctx.Err()))
	}
	return output, err
}

func decodeRetainedEvidence(output []byte) (retainedRecoveryEvidence, error) {
	var report retainedRecoveryEvidence
	found := false
	for line := range bytes.Lines(output) {
		data, ok := strings.CutPrefix(strings.TrimSpace(string(line)), retainedEvidencePrefix)
		if !ok {
			continue
		}
		if found {
			return report, errors.New("retained worker reported more than one verdict")
		}
		found = true
		decoder := json.NewDecoder(strings.NewReader(data))
		decoder.DisallowUnknownFields()
		if !json.Valid([]byte(data)) {
			return report, errors.New("invalid retained recovery evidence JSON")
		}
		if err := decoder.Decode(&report); err != nil {
			return report, err
		}
	}
	if !found {
		return report, errors.New("missing retained recovery evidence")
	}
	return report, nil
}

func validateRetainedEvidence(report retainedRecoveryEvidence, f retainedFixture, tc retainedCase) error {
	g := f.previous
	if tc.outcome == retainedAccept {
		g = f.successor
	}
	b := g.view.boundary(g.id)
	var rejection string
	if tc.reject != nil {
		rejection = tc.reject.Error()
	}
	if report.Seed != retainedSeed || report.Case != tc.name || report.Outcome != tc.outcome || report.Rejection != rejection ||
		report.Generation != fmt.Sprintf("%x", b.generation) || report.PrefixSHA256 != fmt.Sprintf("%x", b.prefixHash) ||
		report.CoveredOffset != b.covered || report.CommittedEnd != b.end || report.CoveredRows != b.next ||
		report.TotalRows != len(g.view.rows) || report.SuffixFrames != len(f.cuts)-1-slices.Index(f.cuts, g.view.covered) {
		return fmt.Errorf("unexpected retained recovery verdict: %+v", report)
	}
	if report.DeadlineNS != int64(replayDeadline) || report.ElapsedNS <= 0 || report.ElapsedNS > report.DeadlineNS ||
		report.AllocationLimitBytes != retainedAllocationBytes || report.AllocatedBytes == 0 ||
		report.AllocatedBytes > report.AllocationLimitBytes || report.ReplayLimitBytes != retainedReplayBytes || !report.FixtureRemoved {
		return fmt.Errorf("retained recovery limits or cleanup were not verified: %+v", report)
	}
	return nil
}

func TestRetainedMalformedRecovery(t *testing.T) {
	fixture, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	executable, err := os.Executable()
	requireRetainedOK(t, err)
	parent := t.TempDir()
	for _, tc := range retainedCases() {
		t.Run(tc.name, func(t *testing.T) {
			output, err := runRetainedWorker(t.Context(), parent, retainedWorkerDeadline, func(ctx context.Context, root string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRetainedRecoveryWorker$", "-test.count=1", "-test.v")
				cmd.Env = append(cmd.Environ(), retainedWorkerEnv+"="+tc.name, "TMPDIR="+root)
				return cmd
			})
			if err != nil {
				t.Fatalf("retained recovery worker failed: %v\n%s", err, output)
			}
			report, err := decodeRetainedEvidence(output)
			if err = errors.Join(err, validateRetainedEvidence(report, fixture, tc)); err != nil {
				t.Fatalf("invalid retained recovery evidence: %v\n%s", err, output)
			}
			t.Logf("%s", output)
		})
	}
}

func TestRetainedRecoveryWorker(t *testing.T) {
	name := os.Getenv(retainedWorkerEnv)
	if name == "" {
		t.Skip("started by the retained recovery corpus")
	}
	setRetainedWorkerLimits(t)
	fixture, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	tc := findRetainedCase(t, name)
	report, err := measureRetainedCase(t.Context(), os.TempDir(), fixture, tc)
	requireRetainedOK(t, err)
	requireRetainedOK(t, validateRetainedEvidence(report, fixture, tc))
	encoded, err := json.Marshal(report)
	requireRetainedOK(t, err)
	_, err = fmt.Fprintf(os.Stdout, "%s%s\n", retainedEvidencePrefix, encoded)
	requireRetainedOK(t, err)
}

func requireRetainedOK(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
