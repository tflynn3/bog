package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestChildCapacityControlEnvironment(t *testing.T) {
	t.Setenv(capacityControlEnv, "inherited")
	t.Setenv("BOG_CAPACITY_TEST_SENTINEL", "preserved")
	for _, tc := range []struct{ mode, want string }{
		{"live", "1/preserved"},
		{"legacy", "unset/preserved"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			dir := t.TempDir()
			candidate := filepath.Join(dir, "bog")
			if err := os.WriteFile(candidate, []byte(
				"#!/bin/sh\nprintf '%s/%s' \"${BOG_CAPACITY_CONTROL-unset}\" \"$BOG_CAPACITY_TEST_SENTINEL\" > observed\n"), 0700); err != nil {
				t.Fatal(err)
			}
			p, _, err := startChild(candidate, dir, dir, tc.mode)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.stop(); err != nil {
					t.Error(err)
				}
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
	if p, _, err := startChild(candidate, dir, filepath.Join(dir, "missing"), "legacy"); p != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed cgroup placement: child %v, error %v", p, err)
	}
}

func TestChildExitAndForcedCleanup(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		exitCode     int
	}{
		{"startup failure", "exit 17", 17},
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
			t.Cleanup(func() {
				select {
				case <-p.done:
				default:
					t.Errorf("child not reaped: %v", p.stop())
				}
			})
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
			if err := p.stop(); !errors.As(err, &exit) || exit.ExitCode() != tc.exitCode ||
				(tc.exitCode == -1 && !strings.Contains(err.Error(), "grace period")) {
				t.Fatalf("unexpected child exit: %v", err)
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
