package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
			script: `trap 'cp "$BOG_TEST_PROOF_SOURCE" "$BOG_TEST_PROOF_DEST"; exit 7' TERM
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
			if err := os.WriteFile(producer, []byte("#!/bin/sh\n"+tc.script+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			partial := capacityFixture("fail", `{"fixture":{"seed_rows":196608},"status":"pass","phase":"complete",
				"cgroup_peak_memory_bytes":4100,"diagnostics":{"cgroup_memory_events":{"oom":0,"oom_kill":0}}}`)
			if err := os.WriteFile(source, partial, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("BOG_TEST_PROOF_SOURCE", source)
			t.Setenv("BOG_TEST_PROOF_DEST", proof)
			t.Setenv("BOG_TEST_PRODUCER_READY", ready)
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- r.runCapacity(ctx, files, producer, "/candidate", dir, proof) }()
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
			err := <-result
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
