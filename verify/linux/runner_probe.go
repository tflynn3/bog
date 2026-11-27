package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const releaseMemoryLimit = 128 << 20

type runnerProbe struct {
	Arch          string            `json:"arch"`
	KernelMachine string            `json:"kernel_machine"`
	AddressSpace  string            `json:"address_space_limit,omitempty"`
	Parent        string            `json:"cgroup_parent,omitempty"`
	Controllers   []string          `json:"controllers,omitempty"`
	Child         string            `json:"probe_child,omitempty"`
	MemoryMax     *uint64           `json:"memory_max_bytes"`
	SwapMax       *uint64           `json:"swap_max_bytes"`
	MemoryPeak    *uint64           `json:"memory_peak_bytes"`
	MemoryEvents  map[string]uint64 `json:"memory_events,omitempty"`
	IOStatRead    bool              `json:"io_stat_read"`
	Placed        bool              `json:"child_placed"`
	Reaped        bool              `json:"child_reaped"`
	Empty         bool              `json:"child_empty"`
	Removed       bool              `json:"child_removed"`
}

func requireNativeArch(arch, machine, expected string) error {
	target := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[expected]
	if target == "" || arch != expected || machine != target {
		return fmt.Errorf("require native Linux %s, observed Go arch %s and kernel machine %s", expected, arch, machine)
	}
	return nil
}

func delegatedParent(mount, membership string) (string, error) {
	path, ok := strings.CutPrefix(strings.TrimSpace(membership), "0::")
	if !ok || !filepath.IsAbs(path) || filepath.Clean(path) != path ||
		strings.ContainsAny(path, "\r\n") || filepath.Base(path) != "runner" || filepath.Dir(path) == "/" {
		return "", fmt.Errorf("require unified cgroup v2 membership in a delegated runner subgroup, got %q", membership)
	}
	return filepath.Join(mount, filepath.Dir(path)), nil
}

type controlFiles struct {
	read  func(string) ([]byte, error)
	write func(string, string) error
}

func (f controlFiles) number(path string) (uint64, error) {
	raw, err := f.read(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
}

func (f controlFiles) events(path string, required ...string) (map[string]uint64, error) {
	raw, err := f.read(path)
	if err != nil {
		return nil, err
	}
	events := make(map[string]uint64)
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s: invalid counter %q", path, line)
		}
		value, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if _, duplicate := events[fields[0]]; duplicate || parseErr != nil {
			return nil, fmt.Errorf("%s: invalid or duplicate counter %q", path, line)
		}
		events[fields[0]] = value
	}
	for _, name := range required {
		if _, ok := events[name]; !ok {
			return nil, fmt.Errorf("%s: missing counter %s", path, name)
		}
	}
	return events, nil
}

var oomEvents = []string{"oom", "oom_kill"}

func checkControllers(raw []byte) error {
	fields := strings.Fields(string(raw))
	if !slices.Contains(fields, "memory") || !slices.Contains(fields, "io") {
		return fmt.Errorf("memory and io controllers required, observed %q", strings.TrimSpace(string(raw)))
	}
	return nil
}

type probeOps struct {
	controlFiles
	mkdir  func(string, string) (string, error)
	remove func(string) error
	place  func(context.Context, string, *runnerProbe) error
}

func probeDelegation(ctx context.Context, r *runnerProbe, ops probeOps) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	available, err := ops.read(filepath.Join(r.Parent, "cgroup.controllers"))
	if err != nil {
		return err
	}
	if err := checkControllers(available); err != nil {
		return err
	}
	control := filepath.Join(r.Parent, "cgroup.subtree_control")
	if err := ops.write(control, "+memory +io"); err != nil {
		return fmt.Errorf("enable delegated memory and io: %w", err)
	}
	enabled, err := ops.read(control)
	if err != nil {
		return err
	}
	r.Controllers = strings.Fields(string(enabled))
	if err := checkControllers(enabled); err != nil {
		return err
	}

	group, err := ops.mkdir(r.Parent, "bog-preflight-")
	if err != nil {
		return fmt.Errorf("create disposable cgroup: %w", err)
	}
	r.Child = group
	defer func() {
		// Cleanup remains bounded after the probe deadline or a cancellation.
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if emptyErr := waitEmpty(cleanup, ops.controlFiles, group); emptyErr != nil {
			err = errors.Join(err, fmt.Errorf("drain disposable cgroup: %w", emptyErr))
		} else {
			r.Empty = true
		}
		if removeErr := removeProbeGroup(cleanup, group, ops.remove); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove disposable cgroup: %w", removeErr))
		} else {
			r.Removed = true
		}
	}()

	for _, limit := range []struct {
		name     string
		value    uint64
		observed **uint64
	}{
		{"memory.max", releaseMemoryLimit, &r.MemoryMax},
		{"memory.swap.max", 0, &r.SwapMax},
	} {
		path := filepath.Join(group, limit.name)
		if err := ops.write(path, strconv.FormatUint(limit.value, 10)); err != nil {
			return fmt.Errorf("configure %s: %w", path, err)
		}
		value, err := ops.number(path)
		if err != nil {
			return fmt.Errorf("read back %s: %w", path, err)
		}
		*limit.observed = &value
		if value != limit.value {
			return fmt.Errorf("%s readback %d, want %d", path, value, limit.value)
		}
	}
	if _, err := ops.read(filepath.Join(group, "io.stat")); err != nil {
		return fmt.Errorf("read delegated io.stat: %w", err)
	}
	r.IOStatRead = true
	if err := ctx.Err(); err != nil {
		return err
	}
	placeErr := ops.place(ctx, group, r)
	peak, peakErr := ops.number(filepath.Join(group, "memory.peak"))
	if peakErr == nil {
		r.MemoryPeak = &peak
	}
	r.MemoryEvents, err = ops.events(filepath.Join(group, "memory.events"), oomEvents...)
	return errors.Join(placeErr, peakErr, err, ctx.Err())
}

func removeProbeGroup(ctx context.Context, group string, remove func(string) error) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := remove(group)
		if !errors.Is(err, syscall.EBUSY) && !errors.Is(err, syscall.ENOTEMPTY) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitEmpty(ctx context.Context, files controlFiles, group string) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := files.events(filepath.Join(group, "cgroup.events"), "populated")
		if err != nil {
			return err
		}
		switch events["populated"] {
		case 0:
			return nil
		case 1:
		default:
			return errors.New("invalid cgroup population")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func writeControl(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = f.WriteString(value)
	return errors.Join(err, f.Close())
}
