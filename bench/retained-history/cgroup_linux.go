package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func openGroup(parent string, limit uint64, measured *resources) (string, error) {
	if parent == "" {
		data, err := os.ReadFile("/proc/self/cgroup")
		if err != nil {
			return "", err
		}
		path, ok := strings.CutPrefix(strings.TrimSpace(string(data)), "0::")
		if !ok || !filepath.IsAbs(path) {
			return "", errors.New("expected unified cgroup v2; set -cgroup-parent to a delegated parent")
		}
		parent = filepath.Join("/sys/fs/cgroup", path)
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(parent, &fs); err != nil {
		return "", err
	}
	const cgroup2Magic = 0x63677270
	if fs.Type != cgroup2Magic {
		return "", fmt.Errorf("%s is not on cgroup v2", parent)
	}
	group, err := os.MkdirTemp(parent, "bog-capacity-")
	if err != nil {
		return "", err
	}
	// Return the owned directory even if configuration fails so it goes
	// through the same teardown as a fully provisioned fixture.
	return group, measured.configureMemory(group, limit)
}

func (r *resources) configureMemory(group string, memoryLimit uint64) error {
	for _, limit := range []struct {
		name     string
		value    uint64
		observed **uint64
	}{
		{"memory.max", memoryLimit, &r.CgroupLimit},
		{"memory.swap.max", 0, &r.CgroupSwap},
	} {
		if err := writeCgroupFile(group, limit.name, strconv.FormatUint(limit.value, 10)); err != nil {
			return fmt.Errorf("configure %s: %w", limit.name, err)
		}
		observed, err := readCgroupUint(group, limit.name)
		if err != nil {
			return err
		}
		*limit.observed = &observed
		if observed != limit.value {
			return fmt.Errorf("%s readback is %d, configured %d", limit.name, observed, limit.value)
		}
	}
	return nil
}

func readCgroupUint(group, name string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(group, name))
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", name, err)
	}
	return value, nil
}

func placeInGroup(group string, pid int) error {
	return writeCgroupFile(group, "cgroup.procs", strconv.Itoa(pid))
}

func readMemoryCurrent(group string) (uint64, error) {
	return readCgroupUint(group, "memory.current")
}

func writeCgroupFile(group, name, value string) error {
	file, err := os.OpenFile(filepath.Join(group, name), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = file.WriteString(value)
	return errors.Join(err, file.Close())
}

func deviceIO(data string) (map[string]map[string]uint64, error) {
	stats := make(map[string]map[string]uint64)
	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, errors.New("missing cgroup device I/O counters")
		}
		counters := make(map[string]uint64)
		for _, field := range fields[1:] {
			key, raw, ok := strings.Cut(field, "=")
			n, err := strconv.ParseUint(raw, 10, 64)
			if !ok || err != nil {
				return nil, fmt.Errorf("invalid device I/O counter %q", field)
			}
			counters[key] = n
		}
		if _, ok := counters["rbytes"]; !ok {
			return nil, errors.New("missing device rbytes")
		}
		if _, ok := counters["wbytes"]; !ok {
			return nil, errors.New("missing device wbytes")
		}
		stats[fields[0]] = counters
	}
	return stats, nil
}

func (r *result) collect(group string, live bool) {
	peak, err := readCgroupUint(group, "memory.peak")
	if err == nil {
		r.CgroupPeak = &peak
	} else {
		r.unavailable("cgroup_peak_memory", err)
	}
	raw, err := os.ReadFile(filepath.Join(group, "io.stat"))
	if err == nil {
		r.DeviceIO, err = deviceIO(string(raw))
	}
	if err != nil {
		r.unavailable("device_io", err)
	}
	data, err := os.ReadFile(filepath.Join(group, "memory.events"))
	if err == nil {
		r.Diagnostics.MemoryEvents, err = memoryEvents(string(data))
	}
	if err != nil {
		r.Diagnostics.Unavailable["cgroup_memory_events"] = diagnosticError(err)
		if live {
			r.unavailable("cgroup_oom_kill_count", err)
		}
	} else {
		count := r.Diagnostics.MemoryEvents["oom_kill"]
		r.CgroupOOM = &count
	}
}
