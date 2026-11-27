package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNativeRunnerAndDelegatedPath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		arch, machine, expected string
		pass                    bool
	}{
		{"amd64", "x86_64", "amd64", true},
		{"arm64", "aarch64", "arm64", true},
		{"arm64", "aarch64", "amd64", false},
		{"amd64", "aarch64", "amd64", false},
		{"386", "i386", "386", false},
	} {
		if err := requireNativeArch(tc.arch, tc.machine, tc.expected); (err == nil) != tc.pass {
			t.Errorf("%+v: got %v", tc, err)
		}
	}

	const mount = "/sys/fs/cgroup"
	parent, err := delegatedParent(mount, "0::/system.slice/run-one.service/runner\n")
	if err != nil || parent != mount+"/system.slice/run-one.service" {
		t.Fatalf("delegated parent = %q, %v", parent, err)
	}
	for _, raw := range []string{"", "1:memory:/unit/runner", "0::/runner", "0::/unit", "0::/../unit/runner", "0::/unit/runner\n1:memory:/"} {
		if _, err := delegatedParent(mount, raw); err == nil {
			t.Errorf("accepted membership %q", raw)
		}
	}
}

func TestProbeDelegation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, failAt, wantError  string
		created, placed, removed bool
	}{
		{name: "success", created: true, placed: true, removed: true},
		{name: "controller absent", failAt: "controllers", wantError: "memory and io controllers required"},
		{name: "delegation denied", failAt: "cgroup.subtree_control", wantError: "enable delegated memory and io"},
		{name: "subtree readback wrong", failAt: "enabled", wantError: "memory and io controllers required"},
		{name: "create denied", failAt: "mkdir", wantError: "create disposable cgroup"},
		{name: "memory not writable", failAt: "memory.max", wantError: "configure", created: true, removed: true},
		{name: "memory readback wrong", failAt: "memory_readback", wantError: "readback 1, want 134217728", created: true, removed: true},
		{name: "swap unavailable", failAt: "memory.swap.max", wantError: "configure", created: true, removed: true},
		{name: "swap remains enabled", failAt: "swap_readback", wantError: "readback 1, want 0", created: true, removed: true},
		{name: "io unreadable", failAt: "io.stat", wantError: "read delegated io.stat", created: true, removed: true},
		{name: "cannot move child", failAt: "place", wantError: "injected failure", created: true, removed: true},
		{name: "canceled while placing", failAt: "cancel", wantError: "context canceled", created: true, placed: true, removed: true},
		{name: "counters missing", failAt: "events", wantError: "missing counter oom", created: true, placed: true, removed: true},
		{name: "remove denied", failAt: "remove", wantError: "remove disposable cgroup", created: true, placed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("injected failure")
			var writes []string
			var removed bool
			data := map[string]string{
				"cgroup.controllers": "cpu io memory", "cgroup.subtree_control": "memory io",
				"cgroup.events": "populated 0\nfrozen 0", "memory.max": "134217728",
				"memory.swap.max": "0", "memory.peak": "4096", "memory.events": "oom 0\noom_kill 0",
				"io.stat": "",
			}
			switch tc.failAt {
			case "controllers":
				data["cgroup.controllers"] = "cpu memory"
			case "enabled":
				data["cgroup.subtree_control"] = "memory"
			case "memory_readback":
				data["memory.max"] = "1"
			case "swap_readback":
				data["memory.swap.max"] = "1"
			case "events":
				data["memory.events"] = "oom_kill 0"
			}
			group := "/fixture/child"
			ops := probeOps{
				controlFiles: controlFiles{
					read: func(path string) ([]byte, error) {
						name := filepath.Base(path)
						if raw, ok := data[name]; ok && !(tc.failAt == "io.stat" && name == "io.stat") {
							return []byte(raw), nil
						}
						return nil, sentinel
					},
					write: func(path, value string) error {
						name := filepath.Base(path)
						writes = append(writes, name+"="+value)
						if name == tc.failAt {
							return sentinel
						}
						return nil
					},
				},
				mkdir: func(parent, prefix string) (string, error) {
					if parent != "/fixture" || prefix != "bog-preflight-" {
						t.Fatalf("mkdir(%q, %q)", parent, prefix)
					}
					if tc.failAt == "mkdir" {
						return "", sentinel
					}
					return group, nil
				},
				place: func(_ context.Context, path string, r *runnerProbe) error {
					if path != group {
						t.Fatalf("place(%q)", path)
					}
					if tc.failAt == "place" {
						return sentinel
					}
					if tc.failAt == "cancel" {
						cancel()
					}
					r.Placed, r.Reaped = true, true
					return nil
				},
				remove: func(path string) error {
					if path != group {
						t.Fatalf("remove(%q)", path)
					}
					removed = true
					if tc.failAt == "remove" {
						return sentinel
					}
					return nil
				},
			}
			r := runnerProbe{Parent: "/fixture"}
			err := probeDelegation(ctx, &r, ops)
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("probe error = %v, want %q", err, tc.wantError)
			}
			if (r.Child != "") != tc.created || removed != tc.created || r.Empty != tc.created ||
				r.Removed != tc.removed || r.Placed != tc.placed || r.Reaped != tc.placed {
				t.Fatalf("incorrect lifecycle evidence %+v, remove attempted=%v", r, removed)
			}
			if tc.name == "success" {
				wantWrites := []string{"cgroup.subtree_control=+memory +io", "memory.max=134217728", "memory.swap.max=0"}
				if !reflect.DeepEqual(writes, wantWrites) || !r.IOStatRead || *r.MemoryMax != releaseMemoryLimit ||
					*r.SwapMax != 0 || *r.MemoryPeak != 4096 || r.MemoryEvents["oom_kill"] != 0 {
					t.Fatalf("incorrect readback %+v or writes %v", r, writes)
				}
			}
		})
	}
}

func TestCgroupEventsAndCancellation(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", "oom 0\noom 1", "oom x", "oom 1 extra", "oom_kill 0"} {
		f := controlFiles{read: func(string) ([]byte, error) { return []byte(raw), nil }}
		if _, err := f.events("memory.events", "oom"); err == nil {
			t.Errorf("accepted counters %q", raw)
		}
	}
	f := controlFiles{read: func(string) ([]byte, error) { return []byte("populated 1"), nil }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := waitEmpty(ctx, f, "child"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitEmpty = %v", err)
	}
}

func TestRemoveProbeGroupRetriesBusy(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attempts := 0
	err := removeProbeGroup(ctx, "child", func(path string) error {
		if path != "child" {
			t.Fatalf("remove path = %q", path)
		}
		attempts++
		if attempts == 1 {
			return syscall.EBUSY
		}
		return nil
	})
	if err != nil || attempts != 2 {
		t.Fatalf("remove = %v after %d attempts", err, attempts)
	}
}

func TestRunnerReportPublication(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")
	if err := os.WriteFile(path, []byte("previous"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeRunnerReport(path, make(chan int)); err == nil || !strings.Contains(err.Error(), "replacement not published") {
		t.Fatalf("invalid JSON result: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "previous" {
		t.Fatalf("previous report = %q, %v", raw, err)
	}
	if err := writeRunnerReport(dir+"/missing/result.json", "ignored"); err == nil || !strings.Contains(err.Error(), "replacement not published") {
		t.Fatalf("missing directory result: %v", err)
	}
	if err := writeRunnerReport(path, "replacement"); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil || string(raw) != "\"replacement\"\n" {
		t.Fatalf("new report = %q, %v", raw, err)
	}
	if err := writeRunnerReport(dir+"/.", "ignored"); err == nil {
		t.Fatal("accepted a directory target")
	}
	blocked := filepath.Join(dir, "directory")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeRunnerReport(blocked, "cannot replace directory"); err == nil ||
		!strings.Contains(err.Error(), "replacement not published") {
		t.Fatalf("failed rename = %v", err)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "report.json" {
		t.Fatalf("unexpected staging files %v, %v", entries, err)
	}
}
