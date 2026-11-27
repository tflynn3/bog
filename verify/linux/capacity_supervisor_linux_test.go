package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCapacitySupervisorExitAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, script, kind string
		exit               int
	}{
		{
			name: "address space failure",
			script: `printf 'fatal error: failed to reserve page summary memory\n' >&2
exit 2`,
			kind: addressSpaceFailure, exit: 2,
		},
		{
			name: "unexplained exit without proof",
			script: `printf 'runner stopped\n' >&2
exit 3`,
			kind: runnerFailure, exit: 3,
		},
		{
			name: "cancellation preserves capacity report",
			script: `trap 'cp "$BOG_TEST_PROOF_SOURCE" "$report"; exit 7' TERM
printf ready > "$BOG_TEST_PRODUCER_READY"
while :; do :; done`,
			kind: cancelledFailure, exit: 7,
		},
	} {
		// This test gives each subprocess its own fixture and environment.
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			producer := filepath.Join(dir, "capacity")
			proof := filepath.Join(dir, "proof.json")
			source := filepath.Join(dir, "partial.json")
			ready := filepath.Join(dir, "ready")
			if err := os.WriteFile(producer, []byte(producerReportScript+tc.script+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			partial := capacityFixture("fail", `{"fixture":{"seed_rows":196608},"status":"pass","phase":"complete",
				"cgroup_peak_memory_bytes":4100,"diagnostics":{"cgroup_memory_events":{"oom":0,"oom_kill":0}}}`)
			if err := os.WriteFile(source, partial, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("BOG_TEST_PROOF_SOURCE", source)
			t.Setenv("BOG_TEST_PRODUCER_READY", ready)
			dest, err := openReportDestination(proof)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := dest.Close(); err != nil {
					t.Error(err)
				}
			})
			r := newCapacityTestReport()
			r.Probe.Parent = dir
			reads := 0
			files := controlFiles{read: func(path string) ([]byte, error) {
				if path != filepath.Join(dir, "memory.events") {
					return nil, os.ErrNotExist
				}
				reads++
				if reads == 1 {
					return []byte("oom 3\noom_kill 1"), nil
				}
				return []byte("oom 5\noom_kill 1"), nil
			}}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- r.runCapacity(ctx, files, producer, "/candidate", dir, dest) }()
			if tc.kind == cancelledFailure {
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for {
					if _, err := os.Stat(ready); err == nil {
						break
					} else if !errors.Is(err, os.ErrNotExist) {
						t.Error(err)
						break
					}
					select {
					case <-tick.C:
						continue
					case <-ctx.Done():
						t.Error("producer did not mark startup")
					}
					break
				}
				cancel()
			}
			err = <-result
			if err == nil || r.FailureKind != tc.kind || r.Capacity.ExitCode == nil || *r.Capacity.ExitCode != tc.exit {
				t.Fatalf("kind=%q, exit=%v, error=%v", r.FailureKind, r.Capacity.ExitCode, err)
			}
			if want := (map[string]uint64{"oom": 2, "oom_kill": 0}); !reflect.DeepEqual(r.Capacity.ParentOOMDelta, want) {
				t.Fatalf("delegation counters=%v, want %v", r.Capacity.ParentOOMDelta, want)
			}
			small, large := r.Capacity.Scales[0], r.Capacity.Scales[1]
			if large.Status != "unobserved" || large.MemoryPeak != nil {
				t.Fatalf("unobserved scale=%+v", large)
			}
			if tc.kind == cancelledFailure {
				if !errors.Is(err, context.Canceled) || small.Status != "pass" || small.MemoryPeak == nil || *small.MemoryPeak != 4100 {
					t.Fatalf("partial progress=%+v, error=%v", small, err)
				}
			} else if small.Status != "unobserved" || !strings.Contains(err.Error(), "read capacity producer report") {
				t.Fatalf("unobserved scale=%+v, error=%v", small, err)
			}
		})
	}
}

// Consume the same report argument as the capacity binary. Using a shell
// which starts another process also exercises the inherited descriptor.
const producerReportScript = `#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
	if [ "$1" = "-report" ]; then
		report=$2
	fi
	shift 2
done
`

func TestCapacityReportsStayDistinctAfterParentRetarget(t *testing.T) {
	dir := t.TempDir()
	original, moved, alternate := filepath.Join(dir, "original"), filepath.Join(dir, "moved"), filepath.Join(dir, "alternate")
	for _, path := range []string{original, alternate} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(original, alias); err != nil {
		t.Fatal(err)
	}
	const proofName, runnerName = "capacity.json", "capacity-runner.json"
	for _, name := range []string{proofName, runnerName} {
		if err := os.WriteFile(filepath.Join(alternate, name), []byte("decoy "+name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The producer starts first, moves the opened directory and replaces the
	// alias, then publishes using only its -report argument.
	producer := filepath.Join(dir, "producer")
	script := producerReportScript + `
mv "$BOG_TEST_ORIGINAL" "$BOG_TEST_MOVED"
ln -s "$BOG_TEST_ALTERNATE" "$BOG_TEST_LINK_STAGE"
mv -Tf "$BOG_TEST_LINK_STAGE" "$BOG_TEST_ALIAS"
stage=$(mktemp "${report%/*}/.fixture-XXXXXX")
cp "$BOG_TEST_PROOF_SOURCE" "$stage"
mv "$stage" "$report"
`
	if err := os.WriteFile(producer, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "source.json")
	proof := capacityFixture("pass",
		`{"fixture":{"seed_rows":196608},"status":"pass","phase":"complete","cgroup_peak_memory_bytes":4100},`+
			`{"fixture":{"seed_rows":393216},"status":"pass","phase":"complete","cgroup_peak_memory_bytes":8192}`)
	if err := os.WriteFile(source, proof, 0600); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"BOG_TEST_ORIGINAL": original, "BOG_TEST_MOVED": moved, "BOG_TEST_ALTERNATE": alternate,
		"BOG_TEST_LINK_STAGE": filepath.Join(dir, "new-alias"), "BOG_TEST_ALIAS": alias, "BOG_TEST_PROOF_SOURCE": source,
	} {
		t.Setenv(key, value)
	}
	d, err := openRunnerDestinations(filepath.Join(alias, runnerName), filepath.Join(alias, proofName))
	if err != nil {
		t.Fatal(err)
	}
	r := newCapacityTestReport()
	r.SchemaVersion, r.Status = 1, "pass"
	r.Probe.Parent = dir
	files := controlFiles{read: func(path string) ([]byte, error) {
		if path == filepath.Join(dir, "memory.events") {
			return []byte("oom 0\noom_kill 0\n"), nil
		}
		return nil, os.ErrNotExist
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	runErr := r.runCapacity(ctx, files, producer, "/candidate", dir, d.proof)
	if err := errors.Join(runErr, d.proof.Close()); err != nil {
		t.Fatal(errors.Join(err, d.report.Close()))
	}
	if err := d.report.publish(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Capacity.ExitCode == nil || *r.Capacity.ExitCode != 0 {
		t.Fatalf("producer exit: %+v", r.Capacity)
	}
	for i, want := range []uint64{4100, 8192} {
		scale := r.Capacity.Scales[i]
		if scale.Status != "pass" || scale.PeakSource != "producer" || scale.MemoryPeak == nil || *scale.MemoryPeak != want {
			t.Fatalf("scale %d = %+v", i, scale)
		}
	}
	data, err := os.ReadFile(filepath.Join(moved, proofName))
	if err != nil || string(data) != string(proof) {
		t.Fatalf("producer report = %q, %v", data, err)
	}
	data, err = os.ReadFile(filepath.Join(moved, runnerName))
	if err != nil {
		t.Fatal(err)
	}
	var runner runnerReport
	if err := json.Unmarshal(data, &runner); err != nil || !reflect.DeepEqual(runner, r) {
		t.Fatalf("runner report = %s, %v", data, err)
	}
	for _, name := range []string{proofName, runnerName} {
		if data, err := os.ReadFile(filepath.Join(alias, name)); err != nil || string(data) != "decoy "+name {
			t.Fatalf("retargeted %s = %q, %v", name, data, err)
		}
	}
	for _, parent := range []string{moved, alternate} {
		if entries, err := os.ReadDir(parent); err != nil || len(entries) != 2 {
			t.Fatalf("%s entries = %v, %v", parent, entries, err)
		}
	}
}

func TestCapacityProofRequiresRegularEntry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		make func(string) error
		want error
	}{
		{"symlink", func(path string) error { return os.Symlink("source", path) }, syscall.ELOOP},
		{"fifo", func(path string) error { return syscall.Mkfifo(path, 0600) }, nil},
		{"directory", func(path string) error { return os.Mkdir(path, 0700) }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "proof")
			if err := os.WriteFile(filepath.Join(dir, "source"), []byte("do not follow"), 0600); err != nil {
				t.Fatal(err)
			}
			dest, err := openReportDestination(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := dest.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := tc.make(path); err != nil {
				t.Fatal(err)
			}
			data, err := readProducerProof(dest)
			if data != nil || err == nil {
				t.Fatalf("readProducerProof = %q, %v", data, err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) ||
				tc.want == nil && !strings.Contains(err.Error(), "must be a regular file") {
				t.Fatal(err)
			}
		})
	}
}

func TestDelegatedPreflightRejectsAliasBeforeCandidateCheck(t *testing.T) {
	dir := t.TempDir()
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	args := []string{"-revision", testRevision, "-fixture", fixtureID, "-arch", "unused",
		"-report", "evidence.json", "-capacity-report", "./alias/evidence.json",
		"-binary", filepath.Join(dir, "missing-binary"), "-producer", filepath.Join(dir, "missing-producer"),
		"-work-dir", dir}
	if err := runDelegated(t.Context(), "capacity", args); err == nil || !strings.Contains(err.Error(), "separate destinations") {
		t.Fatalf("aliased destinations: %v", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 || entries[0].Name() != "alias" {
		t.Fatalf("unexpected preflight side effects: %v, %v", entries, err)
	}
}

func TestDelegatedPreflightCancellationPreservesReport(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	report := filepath.Join(dir, "preflight.json")
	if err := os.WriteFile(report, []byte("previous"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := runDelegated(ctx, "preflight",
		[]string{"-revision", testRevision, "-fixture", fixtureID, "-arch", "unused", "-report", report})
	var publication *runnerPublicationError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &publication) || publication.state != reportUnpublished {
		t.Fatalf("canceled preflight: %v", err)
	}
	if data, err := os.ReadFile(report); err != nil || string(data) != "previous" {
		t.Fatalf("prior evidence = %q, %v", data, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatalf("entries = %v, %v", entries, err)
	}
}
