package fuzz

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

const scanProcStatusLimit = 64 << 10

func readScanStatus() (string, error) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(io.LimitReader(f, scanProcStatusLimit+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return "", err
	}
	if len(data) > scanProcStatusLimit {
		return "", errors.New("worker /proc/self/status exceeds size limit")
	}
	return string(data), nil
}

func readScanProcessMemory() scanProcessMemory {
	status, err := readScanStatus()
	if err != nil {
		return scanProcessMemory{Unavailable: err.Error()}
	}
	return parseScanProcessMemory(status)
}

func parseScanProcessMemory(status string) scanProcessMemory {
	virtual, virtualErr := parseScanStatusBytes(status, "VmSize")
	rss, rssErr := parseScanStatusBytes(status, "VmRSS")
	if err := errors.Join(virtualErr, rssErr); err != nil {
		return scanProcessMemory{Unavailable: err.Error()}
	}
	if virtual < rss {
		return scanProcessMemory{Unavailable: "VmSize smaller than VmRSS"}
	}
	return scanProcessMemory{VirtualBytes: virtual, RSSBytes: rss}
}

func parseScanMemoryPeakRSS(status string) (uint64, error) {
	return parseScanStatusBytes(status, "VmHWM")
}

func parseScanStatusBytes(status, field string) (uint64, error) {
	var result uint64
	for line := range strings.SplitSeq(status, "\n") {
		value, ok := strings.CutPrefix(line, field+":")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if result != 0 || len(fields) != 2 || fields[1] != "kB" {
			return 0, fmt.Errorf("invalid or duplicate %s field", field)
		}
		kib, err := strconv.ParseUint(fields[0], 10, 54)
		if err != nil || kib == 0 {
			return 0, fmt.Errorf("invalid %s size", field)
		}
		result = kib * 1024
	}
	if result == 0 {
		return 0, fmt.Errorf("missing %s in worker /proc/self/status", field)
	}
	return result, nil
}

func scanMemoryFixtureFDs(procFD, fixture string) ([]string, error) {
	entries, err := os.ReadDir(procFD)
	if err != nil {
		return nil, err
	}
	var targets []string
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(procFD, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			// The enumeration descriptor itself disappears when ReadDir returns.
			continue
		}
		if err != nil {
			return nil, err
		}
		if target == fixture || target == fixture+" (deleted)" || strings.HasPrefix(target, fixture+"/") {
			targets = append(targets, target)
		}
	}
	slices.Sort(targets)
	return targets, nil
}

func makeScanMemoryCgroup(parent string) (group, unavailable string, err error) {
	explicit := parent != ""
	if !explicit {
		data, err := os.ReadFile("/proc/self/cgroup")
		if err != nil {
			return "", fmt.Sprintf("read worker cgroup membership: %v", err), nil
		}
		parent, err = scanMemoryCgroupParent(string(data))
		if err != nil {
			return "", err.Error(), nil
		}
	}

	group, err = openScanMemoryCgroup(parent)
	if err == nil {
		return group, "", nil
	}
	provisionErr := fmt.Errorf("create isolated scan memory cgroup under %s: %w", parent, err)
	if group != "" {
		if err := os.Remove(group); err != nil {
			return "", "", errors.Join(provisionErr, err)
		}
	}
	if !explicit && (errors.Is(err, os.ErrPermission) || errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, syscall.EROFS) || errors.Is(err, syscall.ENODEV)) {
		return "", provisionErr.Error(), nil
	}
	return "", "", provisionErr
}

func scanMemoryCgroupParent(membership string) (string, error) {
	for line := range strings.SplitSeq(membership, "\n") {
		if path, ok := strings.CutPrefix(line, "0::"); ok && filepath.IsAbs(path) && filepath.Clean(path) == path {
			return filepath.Join("/sys/fs/cgroup", path), nil
		}
	}
	return "", errors.New("no unified cgroup v2 path in /proc/self/cgroup")
}

func openScanMemoryCgroup(parent string) (string, error) {
	parent, err := filepath.Abs(parent)
	if err != nil {
		return "", err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(parent, &fs); err != nil {
		return "", err
	}
	const cgroup2Magic = 0x63677270
	if fs.Type != cgroup2Magic {
		return "", fmt.Errorf("%s is outside cgroup v2: %w", parent, syscall.ENODEV)
	}
	group, err := os.MkdirTemp(parent, "bog-scan-memory-")
	if err != nil {
		return "", err
	}
	// A delegated parent also needs the memory controller and a kernel
	// exposing memory.peak in its child cgroups.
	_, err = readScanMemoryCgroupPeak(group)
	return group, err
}

func enterScanMemoryCgroup(group string, pid int) error {
	path := filepath.Join(group, "cgroup.procs")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = f.WriteString(strconv.Itoa(pid))
	if err := errors.Join(err, f.Close()); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(data)) != strconv.Itoa(pid) {
		return fmt.Errorf("expected only worker %d in %s, got %q", pid, group, data)
	}
	return nil
}

func readScanMemoryCgroupPeak(group string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(group, "memory.peak"))
	if err != nil {
		return 0, err
	}
	peak, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cgroup memory.peak %q: %w", data, err)
	}
	return peak, nil
}

func TestScanMemoryLinuxMetrics(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		want         uint64
	}{
		{"kibibytes", "Name: test\nVmRSS: 1 kB\nVmHWM:\t123 kB\n", 123 * 1024},
		{"missing", "VmRSS: 123 kB\n", 0},
		{"missing unit", "VmHWM: 123", 0},
		{"wrong unit", "VmHWM: 5 MB", 0},
		{"negative", "VmHWM: -1 kB", 0},
		{"overflow", "VmHWM: 18014398509481984 kB", 0},
		{"zero", "VmHWM: 0 kB", 0},
		{"duplicate", "VmHWM: 1 kB\nVmHWM: 2 kB", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseScanMemoryPeakRSS(tc.status)
			if got != tc.want || (err != nil) != (tc.want == 0) {
				t.Fatalf("peak RSS = %d, %v, want %d", got, err, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		status string
		want   scanProcessMemory
	}{
		{"VmSize: 4398046511104 kB\nVmRSS: 16384 kB", scanProcessMemory{VirtualBytes: 4 << 50, RSSBytes: 16 << 20}},
		{"VmSize: 16384 kB", scanProcessMemory{}},
		{"VmSize: 10 kB\nVmRSS: 11 kB", scanProcessMemory{}},
	} {
		got := parseScanProcessMemory(tc.status)
		if !got.valid() || (got.Unavailable == "") != (tc.want.RSSBytes > 0) ||
			got.VirtualBytes != tc.want.VirtualBytes || got.RSSBytes != tc.want.RSSBytes {
			t.Errorf("process memory from %q = %+v, want %+v", tc.status, got, tc.want)
		}
	}

	for _, tc := range []struct{ membership, want string }{
		{"0::/\n", "/sys/fs/cgroup"},
		{"2:cpu:/legacy\n0::/job/session.scope\n", "/sys/fs/cgroup/job/session.scope"},
		{"2:memory:/legacy\n", ""},
		{"0::relative\n", ""},
		{"0::/../outside\n", ""},
	} {
		got, err := scanMemoryCgroupParent(tc.membership)
		if got != tc.want || (err != nil) != (tc.want == "") {
			t.Errorf("cgroup parent from %q = %q, %v, want %q", tc.membership, got, err, tc.want)
		}
	}

	dir := t.TempDir()
	if _, unavailable, err := makeScanMemoryCgroup(dir); err == nil || unavailable != "" {
		t.Fatalf("explicit non-cgroup parent: unavailable=%q err=%v", unavailable, err)
	}
	for _, tc := range []struct {
		name, contents string
		valid          bool
		want           uint64
	}{
		{"bytes", "1048576\n", true, 1048576},
		{"empty group", "0\n", true, 0},
		{"malformed", "42 kB", false, 0},
		{"overflow", "18446744073709551616", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			must(t, os.WriteFile(filepath.Join(dir, "memory.peak"), []byte(tc.contents), 0600))
			got, err := readScanMemoryCgroupPeak(dir)
			if got != tc.want || (err == nil) != tc.valid {
				t.Fatalf("cgroup peak = %d, %v, want %d (valid=%t)", got, err, tc.want, tc.valid)
			}
		})
	}
}

func TestScanMemoryCgroupAfterReap(t *testing.T) {
	dir := t.TempDir()
	procs := filepath.Join(dir, "cgroup.procs")
	must(t, os.WriteFile(filepath.Join(dir, "memory.peak"), []byte("4096\n"), 0600))
	must(t, os.WriteFile(procs, []byte("1024\n"), 0600))
	if _, err := readReapedScanCgroup(dir); err == nil {
		t.Fatal("accepted an occupied cgroup after worker exit")
	}
	must(t, os.WriteFile(procs, nil, 0600))
	peak, err := readReapedScanCgroup(dir)
	if err != nil || peak != 4096 {
		t.Fatalf("peak after reaping = %d, %v", peak, err)
	}
}

func TestScanMemoryFixtureDescriptors(t *testing.T) {
	dir := t.TempDir()
	targets := []string{
		"socket:[1]", "/fixture-extra", "/fixture", "/fixture/block (deleted)", "/fixture (deleted)", "/fixture/block (deleted)",
	}
	for i, target := range targets {
		must(t, os.Symlink(target, filepath.Join(dir, strconv.Itoa(i))))
	}
	got, err := scanMemoryFixtureFDs(dir, "/fixture")
	must(t, err)
	want := []string{"/fixture", "/fixture (deleted)", "/fixture/block (deleted)", "/fixture/block (deleted)"}
	if !slices.Equal(got, want) {
		t.Fatalf("fixture descriptors = %v, want %v", got, want)
	}
}
