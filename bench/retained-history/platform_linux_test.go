package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMemoryLimitReadback(t *testing.T) {
	const memoryLimit = 128 << 20
	for _, tc := range []struct {
		name, swapContents string
		swap               *uint64
		wantError          bool
	}{
		{"swap disabled", "9\n", new(uint64), false},
		{"different swap readback", "12\n", new(uint64(2)), true},
		{"missing swap control", "", nil, true},
		{"invalid swap readback", "max\n", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("999999999\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.swapContents != "" {
				if err := os.WriteFile(filepath.Join(dir, "memory.swap.max"), []byte(tc.swapContents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var got resources
			err := got.configureMemory(dir, memoryLimit)
			if (err != nil) != tc.wantError || got.CgroupLimit == nil || *got.CgroupLimit != memoryLimit ||
				(got.CgroupSwap == nil) != (tc.swap == nil) || (tc.swap != nil && *got.CgroupSwap != *tc.swap) {
				t.Fatalf("swap readback = %v, memory limit = %v, error = %v", got.CgroupSwap, got.CgroupLimit, err)
			}
		})
	}
}

func TestReadCandidateMemoryCurrent(t *testing.T) {
	for _, tc := range []struct {
		name, contents, wantError string
		canceled, exited          bool
	}{
		{name: "read current bytes", contents: "4096\n"},
		{name: "kernel file absent", wantError: "memory.current"},
		{name: "invalid value", contents: "max\n", wantError: "read memory.current"},
		{name: "canceled", canceled: true, wantError: "context canceled"},
		{name: "candidate exited", exited: true, wantError: "candidate exited"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := t.TempDir()
			if tc.contents != "" {
				if err := os.WriteFile(filepath.Join(group, "memory.current"), []byte(tc.contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			p := &child{done: make(chan struct{})}
			if tc.exited {
				close(p.done)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			got, err := readCandidateMemoryCurrent(ctx, p, group)
			if tc.wantError == "" {
				if err != nil || got != 4096 {
					t.Fatalf("memory.current = %d, %v, want 4096", got, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) || got != 0 {
				t.Fatalf("memory.current = %d, %v, want error containing %q", got, err, tc.wantError)
			}
		})
	}
}

func TestCollectCgroupOOMCount(t *testing.T) {
	for _, tc := range []struct {
		name, events string
		count        *uint64
	}{
		{"zero", "oom 2\noom_kill 0\n", new(uint64)},
		{"nonzero", "oom 2\noom_kill 1\n", new(uint64(1))},
		{"missing", "oom 2\n", nil},
		{"invalid", "oom_kill -1\n", nil},
		{"missing file", "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			files := map[string]string{"memory.peak": "4096\n", "io.stat": "8:0 rbytes=0 wbytes=12\n"}
			if tc.events != "" {
				files["memory.events"] = tc.events
			}
			for name, value := range files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			r := result{Diagnostics: &capacityDiagnostics{Unavailable: make(map[string]string)},
				resources: resources{Unavailable: make(map[string]string)}}
			r.collect(dir, true)
			if r.CgroupPeak == nil || *r.CgroupPeak != 4096 {
				t.Fatalf("missing cgroup memory peak: %+v", r.resources)
			}
			if (r.CgroupOOM == nil) != (tc.count == nil) ||
				(tc.count != nil && *r.CgroupOOM != *tc.count) ||
				(r.Unavailable["cgroup_oom_kill_count"] != "") != (tc.count == nil) {
				t.Fatalf("OOM count = %v, unavailable = %v", r.CgroupOOM, r.Unavailable)
			}
		})
	}
}

func TestChildOpensFixtureFromRelativeWorkDir(t *testing.T) {
	const workDir = "local disk"
	for _, tc := range []struct {
		name    string
		workDir string
		tempDir string
	}{
		{name: "relative flag", workDir: workDir, tempDir: "."},
		{name: "relative TMPDIR", tempDir: workDir},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cwd := t.TempDir()
			t.Chdir(cwd)
			parent := filepath.Join(cwd, workDir)
			if err := os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("TMPDIR", tc.tempDir)
			root, err := createFixtureRoot(tc.workDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(root); err != nil {
					t.Error(err)
				}
			})
			dir := filepath.Join(root, "store-0")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			const fixture = "intended store\n"
			if err := os.WriteFile(filepath.Join(dir, "fixture"), []byte(fixture), 0600); err != nil {
				t.Fatal(err)
			}
			candidate := filepath.Join(root, "bog")
			const script = `#!/bin/sh
[ "$3" = -data-dir ] && [ "$4" -ef . ] || exit 11
IFS= read -r value < "$4/fixture" || exit 12
printf '%s\n' "$value" > observed
`
			if err := os.WriteFile(candidate, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			// A regular placement file lets this regression run without delegation.
			if err := os.WriteFile(filepath.Join(cwd, "cgroup.procs"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			p, _, err := startChild(candidate, dir, cwd, "legacy")
			if p != nil {
				t.Cleanup(func() { testReap(t, p) })
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if !waitChild(ctx, p.done) {
				t.Fatal("candidate did not exit")
			}
			if p.waitErr != nil {
				t.Fatalf("candidate could not open its fixture: %v", p.waitErr)
			}
			proof, shutdownErr, err := emptyTeardown().run(p, 0, time.Second, func() {})
			if err != nil || proof.Status != "pass" || !proof.CandidateReaped || !proof.SubtreeEmpty || !proof.SubtreeRemoved {
				t.Fatalf("cleanup = %+v, %v", proof, err)
			}
			if shutdownErr == nil || shutdownErr.Error() != "candidate exited before shutdown" {
				t.Fatalf("unexpected shutdown result for completed fixture probe: %v", shutdownErr)
			}
			if filepath.Dir(root) != parent {
				t.Fatalf("fixture root %q was not created under %q", root, parent)
			}
			got, err := os.ReadFile(filepath.Join(dir, "observed"))
			if err != nil || string(got) != fixture {
				t.Fatalf("child read %q from fixture, want %q (%v)", got, fixture, err)
			}
		})
	}
}

func TestChildCapacityControlEnvironment(t *testing.T) {
	t.Setenv("BOG_CAPACITY_TEST_SENTINEL", "preserved")
	for _, tc := range []struct{ mode, inherited, want string }{
		{"live", "0", "1/preserved"},
		{"legacy", "1", "unset/preserved"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			t.Setenv(capacityControlEnv, tc.inherited)
			dir := t.TempDir()
			candidate := filepath.Join(dir, "bog")
			if err := os.WriteFile(candidate, []byte(
				"#!/bin/sh\nprintf '%s/%s' \"${BOG_CAPACITY_CONTROL-unset}\" \"$BOG_CAPACITY_TEST_SENTINEL\" > observed\n"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			p, _, err := startChild(candidate, dir, dir, tc.mode)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				testReap(t, p)
			})
			select {
			case <-p.done:
			case <-time.After(2 * time.Second):
				t.Fatal("candidate did not exit")
			}
			got, err := os.ReadFile(filepath.Join(dir, "observed"))
			if err != nil || string(got) != tc.want {
				t.Fatalf("candidate environment: got %q, want %q (%v)", got, tc.want, err)
			}
		})
	}
}

func TestChildReapedOnFailedCgroupPlacement(t *testing.T) {
	dir := t.TempDir()
	candidate := filepath.Join(dir, "bog")
	if err := os.WriteFile(candidate, []byte("#!/bin/sh\nexit 17\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p, _, err := startChild(candidate, dir, filepath.Join(dir, "missing"), "legacy")
	if p == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed cgroup placement: child %v, error %v", p, err)
	}
	t.Cleanup(func() { testReap(t, p) })
	proof, _, err := emptyTeardown().run(p, 20*time.Millisecond, time.Second, nil)
	if err != nil || !proof.CandidateReaped {
		t.Fatalf("placement failure cleanup = %+v, %v", proof, err)
	}
}

func TestChildExitAndForcedCleanup(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		exitCode     int
	}{
		{"startup failure", "echo failed >&2; exit 17", 17},
		{"ignores SIGTERM", "trap '' TERM\nprintf ready > ready\nexec sleep 30", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			cgroup := filepath.Join(dir, "cgroup.procs")
			if err := os.WriteFile(cgroup, nil, 0600); err != nil {
				t.Fatal(err)
			}
			candidate := filepath.Join(dir, "bog")
			if err := os.WriteFile(candidate, []byte("#!/bin/sh\n"+tc.script+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			p, _, err := startChild(candidate, dir, dir, "legacy")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { testReap(t, p) })
			if tc.exitCode == -1 {
				deadline := time.Now().Add(2 * time.Second)
				for {
					_, err := os.Stat("ready")
					if err == nil {
						break
					}
					if !errors.Is(err, os.ErrNotExist) || time.Now().After(deadline) {
						t.Fatal(err)
					}
					time.Sleep(time.Millisecond)
				}
			} else {
				select {
				case <-p.done:
				case <-time.After(2 * time.Second):
					t.Fatal("startup failure did not exit")
				}
			}
			var exit *exec.ExitError
			proof, err, cleanupErr := emptyTeardown().run(p, 20*time.Millisecond, time.Second, func() {})
			if cleanupErr != nil || proof.Status != "pass" || !proof.CandidateReaped || !proof.SubtreeEmpty || !proof.SubtreeRemoved {
				t.Fatalf("cleanup = %+v, %v", proof, cleanupErr)
			}
			if !errors.As(err, &exit) || exit.ExitCode() != tc.exitCode ||
				(tc.exitCode == -1 && !strings.Contains(err.Error(), "grace period")) {
				t.Fatalf("unexpected child exit: %v", err)
			}
			var evidence candidateEvidence
			evidence.finish(p)
			if !evidence.Reaped || evidence.WaitError == "" ||
				(tc.exitCode == -1 && (evidence.Signal == "" || evidence.ExitCode != nil)) ||
				(tc.exitCode != -1 && (evidence.ExitCode == nil || *evidence.ExitCode != tc.exitCode ||
					evidence.Signal != "" || evidence.LogTail != "failed\n")) {
				t.Fatalf("unexpected candidate evidence: %+v", evidence)
			}
			pid, err := os.ReadFile(cgroup)
			if err != nil || string(pid) != strconv.Itoa(p.cmd.Process.Pid) {
				t.Fatalf("unexpected cgroup placement: %q %v", pid, err)
			}
		})
	}
}

func TestDeviceIOParsing(t *testing.T) {
	for _, input := range []string{"", "253:0", "253:0 wbytes=-1", "253:0 wbytes=18446744073709551616"} {
		if _, err := deviceIO(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	got, err := deviceIO("253:0 rbytes=0 wbytes=10\n7:1 rbytes=3 wbytes=2\n")
	if err != nil || len(got) != 2 || got["253:0"]["wbytes"] != 10 || got["7:1"]["rbytes"] != 3 {
		t.Fatalf("device counters: %v %v", got, err)
	}
}

func emptyTeardown() cgroupTeardown {
	return cgroupTeardown{
		populated: func() (bool, error) { return false, nil },
		kill:      func() error { return errors.New("unexpected kill on empty subtree") },
		remove:    func(context.Context) error { return nil },
	}
}

func testReap(t *testing.T, p *child) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Error(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !waitChild(ctx, p.done) {
		t.Error("child was not reaped")
	}
}

func TestCorrelatedCapacityLinuxFixture(t *testing.T) {
	group := t.TempDir()
	if err := os.WriteFile(filepath.Join(group, "memory.peak"), []byte("768\n"), 0600); err != nil {
		t.Fatal(err)
	}
	candidate := &child{done: make(chan struct{})}
	testMixedLoad(t, "", func(ctx context.Context) (uint64, error) {
		return readCandidateMemory(ctx, candidate, group, "memory.peak")
	})
}
