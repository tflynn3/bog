package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestParsePopulation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input string
		want  bool
		valid bool
	}{
		{"populated 0\nfrozen 0\n", false, true},
		{"frozen 0\npopulated 1\n", true, true},
		{"", false, false},
		{"frozen 0", false, false},
		{"populated", false, false},
		{"populated 1 extra", false, false},
		{"populated 2", false, false},
		{"populated 0\npopulated 1\n", false, false},
	} {
		got, err := parsePopulation(tc.input)
		if got != tc.want || (err == nil) != tc.valid {
			t.Errorf("parsePopulation(%q) = %v, %v", tc.input, got, err)
		}
	}
}

func TestTeardownOutcomes(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("cgroup operation rejected")
	for _, tc := range []struct {
		name           string
		populated      bool
		eventsErr      error
		killErr        error
		removeErr      error
		staysPopulated bool
		wantKill       bool
		wantEmpty      bool
		wantRemoved    bool
		wantErr        error
	}{
		{name: "empty", wantEmpty: true, wantRemoved: true},
		{name: "populated descendant", populated: true, wantKill: true, wantEmpty: true, wantRemoved: true},
		{name: "malformed events", eventsErr: sentinel, wantKill: true, wantErr: sentinel},
		{name: "kill denied", populated: true, killErr: sentinel, staysPopulated: true, wantKill: true, wantErr: sentinel},
		{name: "drain timeout", populated: true, staysPopulated: true, wantKill: true, wantErr: context.DeadlineExceeded},
		{name: "remove denied", removeErr: sentinel, wantEmpty: true, wantErr: sentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			killed, collected := false, false
			ops := cgroupTeardown{
				populated: func() (bool, error) {
					return tc.populated && (!killed || tc.staysPopulated), tc.eventsErr
				},
				kill: func() error { killed = true; return tc.killErr },
				remove: func(ctx context.Context) error {
					if !collected || ctx.Err() != nil {
						t.Error("removal must follow collection within the cleanup deadline")
					}
					if tc.populated && (!killed || tc.staysPopulated) || tc.eventsErr != nil {
						t.Error("removal attempted without proof of emptiness")
					}
					return tc.removeErr
				},
			}
			start := time.Now()
			proof, shutdown, err := ops.run(nil, 0, 60*time.Millisecond, func() { collected = true })
			if !errors.Is(err, tc.wantErr) || shutdown != nil || killed != tc.wantKill || !collected ||
				proof.CandidateReaped != true || proof.SubtreeEmpty != tc.wantEmpty || proof.SubtreeRemoved != tc.wantRemoved ||
				(proof.Status == "pass") != (tc.wantErr == nil) || (proof.Detail == "") != (tc.wantErr == nil) {
				t.Fatalf("proof=%+v killed=%v collected=%v shutdown=%v cleanup=%v", proof, killed, collected, shutdown, err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("cleanup exceeded its bounded deadline")
			}
		})
	}
}

func TestRemoveCgroupTreeBottomUp(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	root := filepath.Join(parent, "owned")
	for _, name := range []string{"owned/a/b", "owned/c", "sibling"} {
		if err := os.MkdirAll(filepath.Join(parent, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeCgroupTree(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned subtree still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(parent, "sibling")); err != nil {
		t.Fatal(err)
	}
}

// Run under a delegated systemd service with memory/io enabled for children.
func TestDelegatedSubtreeTeardown(t *testing.T) {
	parent := os.Getenv("BOG_TEST_CGROUP_PARENT")
	if parent == "" {
		t.Skip("set BOG_TEST_CGROUP_PARENT to a delegated cgroup v2 parent")
	}
	root, err := openGroup(parent, 128<<20, &resources{})
	if root != "" {
		t.Cleanup(func() {
			if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
				return
			}
			_, _, err := teardownGroup(root, nil, func() {})
			if err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	start := func(group, program string) *child {
		t.Helper()
		candidate, err := os.CreateTemp(dir, "candidate-")
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := fmt.Fprintln(candidate, "#!/bin/sh\n"+program)
		if err := errors.Join(writeErr, candidate.Chmod(0700), candidate.Close()); err != nil {
			t.Fatal(err)
		}
		p, _, err := startChild(candidate.Name(), dir, group, "legacy")
		if p != nil {
			t.Cleanup(func() { testReap(t, p) })
		}
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	exited := start(root, "exit 17")
	descendant := start(nested, "exec sleep 30")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if !waitChild(ctx, exited.done) {
		t.Fatal("candidate did not exit")
	}
	raw, err := os.ReadFile(filepath.Join(root, "cgroup.procs"))
	if err != nil || strings.TrimSpace(string(raw)) != "" {
		t.Fatalf("expected empty parent cgroup.procs, got %q, %v", raw, err)
	}
	raw, err = os.ReadFile(filepath.Join(root, "cgroup.events"))
	if err != nil {
		t.Fatal(err)
	}
	if populated, err := parsePopulation(string(raw)); !populated || err != nil {
		t.Fatalf("expected populated subtree, got %q, %v", raw, err)
	}
	proof, shutdown, cleanup := teardownGroup(root, exited, func() {})
	var exit *exec.ExitError
	if cleanup != nil || proof.Status != "pass" || !proof.CandidateReaped || !proof.SubtreeEmpty || !proof.SubtreeRemoved ||
		!errors.As(shutdown, &exit) || exit.ExitCode() != 17 {
		t.Fatalf("cleanup=%+v shutdown=%v error=%v", proof, shutdown, cleanup)
	}
	ctx, done := context.WithTimeout(t.Context(), time.Second)
	defer done()
	if !waitChild(ctx, descendant.done) || !errors.As(descendant.waitErr, &exit) ||
		exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatal("descendant was not killed and reaped")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("subtree remained after teardown: %v", err)
	}
}

func TestDelegatedFixtureCancellation(t *testing.T) {
	parent := os.Getenv("BOG_TEST_CGROUP_PARENT")
	if parent == "" {
		t.Skip("set BOG_TEST_CGROUP_PARENT to a delegated cgroup v2 parent")
	}
	pattern := filepath.Join(parent, "bog-capacity-*")
	before, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	candidate, marker := filepath.Join(dir, "candidate"), filepath.Join(dir, "started")
	t.Setenv("BOG_TEST_START_MARKER", marker)
	if err := os.WriteFile(candidate, []byte("#!/bin/sh\nprintf started > \"$BOG_TEST_START_MARKER\" || exit 1\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	observed := make(chan error, 1)
	go func() {
		defer cancel()
		for {
			_, err := os.Stat(marker)
			if !errors.Is(err, os.ErrNotExist) {
				observed <- err
				return
			}
			if err := waitCleanupPoll(ctx); err != nil {
				observed <- err
				return
			}
		}
	}()
	r := measure(ctx, options{mode: "legacy", cgroupParent: parent}, candidate, filepath.Join(dir, "store"), fixtures("legacy")[0])
	if err := <-observed; err != nil {
		t.Fatal(err)
	}
	if r.Status != "fail" || r.Workload != "fail" || r.TimedOut || !strings.Contains(r.Detail, "context canceled") {
		t.Fatalf("workload verdict after cancellation = %+v", r)
	}
	if cleanup := r.Cleanup; cleanup.Status != "pass" || !cleanup.CandidateReaped || !cleanup.SubtreeEmpty || !cleanup.SubtreeRemoved {
		t.Fatalf("cleanup after cancellation = %+v", cleanup)
	}
	after, err := filepath.Glob(pattern)
	if err != nil || !slices.Equal(before, after) {
		t.Fatalf("subtrees before %v, after %v, error %v", before, after, err)
	}
}
