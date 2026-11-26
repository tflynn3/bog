package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type child struct {
	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error // read after done closes
}

const capacityControlEnv = "BOG_CAPACITY_CONTROL"

func startChild(binary, dir, group, mode string) (*child, string, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return nil, "", err
	}
	// A gate keeps the candidate from running before it enters the cgroup.
	// This also works on Linux environments that filter clone3.
	cmd := exec.Command("/bin/sh", "-c", `read -r gate && [ "$gate" = start ] && exec "$@"`, "capacity",
		binary, "-addr", addr, "-data-dir", dir)
	cmd.Dir = dir
	env := cmd.Environ()
	cmd.Env = make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, capacityControlEnv+"=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	if mode == "live" {
		cmd.Env = append(cmd.Env, capacityControlEnv+"=1")
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	readGate, gate, err := os.Pipe()
	if err != nil {
		return nil, "", err
	}
	cmd.Stdin = readGate
	if err := cmd.Start(); err != nil {
		return nil, "", errors.Join(err, readGate.Close(), gate.Close())
	}
	p := &child{cmd: cmd, done: make(chan struct{})}
	defer func() {
		// Keep the PID reserved until placement finishes, even if the
		// shell exits before we release the gate.
		go func() {
			p.waitErr = cmd.Wait()
			close(p.done)
		}()
	}()
	if err := readGate.Close(); err != nil {
		return p, "", errors.Join(err, gate.Close())
	}
	err = writeCgroupFile(group, "cgroup.procs", strconv.Itoa(cmd.Process.Pid))
	if err == nil {
		_, err = gate.Write([]byte("start\n"))
	}
	// The caller owns p even when placement or release of the startup gate fails.
	return p, "http://" + addr, errors.Join(err, gate.Close())
}

func openGroup(parent string, limit uint64) (string, error) {
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
	for _, limit := range []struct{ name, value string }{
		{"memory.max", strconv.FormatUint(limit, 10)},
		{"memory.swap.max", "0"},
	} {
		if err := writeCgroupFile(group, limit.name, limit.value); err != nil {
			return group, err
		}
		data, err := os.ReadFile(filepath.Join(group, limit.name))
		if err != nil {
			return group, err
		}
		if strings.TrimSpace(string(data)) != limit.value {
			return group, fmt.Errorf("%s did not retain limit %s: %q", limit.name, limit.value, data)
		}
	}
	return group, nil
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

func (r *resources) unavailable(key string, err error) {
	r.Unavailable[key] = err.Error()
}

func (r *resources) disk(dir string) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(dir, &s); err != nil {
		r.unavailable("disk_headroom", err)
		return
	}
	free := s.Bavail * uint64(s.Bsize)
	if r.InitialFree == nil {
		r.InitialFree = &free
	}
	if r.MinFree == nil || free < *r.MinFree {
		r.MinFree = &free
	}
}

func (r *resources) sample(ctx context.Context, p *child, dir string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-p.done:
				return
			default:
			}
			files, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", p.cmd.Process.Pid))
			if err == nil {
				n := uint64(len(files))
				if r.PeakFDs == nil || n > *r.PeakFDs {
					r.PeakFDs = &n
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				r.unavailable("peak_descriptors", err)
			}
			r.disk(dir)
			select {
			case <-ctx.Done():
				return
			case <-p.done:
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

func (r *resources) collect(group string) {
	peak, err := os.ReadFile(filepath.Join(group, "memory.peak"))
	if err == nil {
		n, parseErr := strconv.ParseUint(strings.TrimSpace(string(peak)), 10, 64)
		err = parseErr
		if err == nil {
			r.CgroupPeak = &n
		}
	}
	if err != nil {
		r.unavailable("cgroup_peak_memory", err)
	}
	raw, err := os.ReadFile(filepath.Join(group, "io.stat"))
	if err == nil {
		r.DeviceIO, err = deviceIO(string(raw))
	}
	if err != nil {
		r.unavailable("device_io", err)
	}
}

func measure(ctx context.Context, o options, binary, dir string, f fixture) (r result) {
	r = result{Fixture: f, Phase: "provision", Status: "fail", Workload: "fail",
		Cleanup: cleanupProof{Status: "not_started"}, resources: resources{Unavailable: make(map[string]string)}}
	err := func() (err error) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Mkdir(dir, 0700); err != nil {
			return err
		}
		defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
		r.disk(dir)
		group, err := openGroup(o.cgroupParent, f.MemoryBytes)
		var p *child
		var earlyCleanupErr error
		if group != "" {
			defer func() {
				var shutdownErr, cleanupErr error
				r.Cleanup, shutdownErr, cleanupErr = teardownGroup(group, p, func() { r.collect(group) })
				cleanupErr = errors.Join(earlyCleanupErr, cleanupErr)
				if cleanupErr != nil {
					r.Cleanup.Status, r.Cleanup.Detail = "fail", cleanupErr.Error()
				}
				r.collectRSS(p)
				err = errors.Join(err, shutdownErr)
			}()
		}
		if err != nil {
			return fmt.Errorf("create delegated cgroup v2 subtree: %w", err)
		}

		p, err = r.exercise(ctx, o, binary, dir, group, f, false)
		if err != nil {
			return err
		}
		// Drain the first lifetime before restarting with the same data and
		// measurement cgroup. Final teardown also removes the subtree.
		var stopped cleanupProof
		stopped, err, earlyCleanupErr = teardownGroup(group, p, nil)
		r.collectRSS(p)
		if stopped.CandidateReaped {
			p = nil
		}
		if err != nil || earlyCleanupErr != nil {
			return err
		}
		p, err = r.exercise(ctx, o, binary, dir, group, f, true)
		return err
	}()
	for key, missing := range map[string]bool{
		"cgroup_peak_memory": r.CgroupPeak == nil, "peak_rss": r.PeakRSS == nil, "peak_descriptors": r.PeakFDs == nil,
		"device_io": len(r.DeviceIO) == 0, "disk_headroom": r.InitialFree == nil || r.MinFree == nil,
		"ingest_p99": r.IngestP99 == nil, "backfill_query_p99": r.QueryP99 == nil,
		"start_to_ready": r.StartReady == nil, "restart_to_ready": r.RestartReady == nil,
	} {
		if _, haveReason := r.Unavailable[key]; missing && !haveReason {
			r.Unavailable[key] = "no sample collected"
		}
	}
	switch {
	case err != nil:
		r.Detail = err.Error()
		r.TimedOut = errors.Is(err, context.DeadlineExceeded)
	case r.RestartReady == nil && r.Cleanup.Status == "fail":
		r.Workload, r.Detail = "skip", "restart blocked by first-lifetime cleanup"
	case len(r.Unavailable) > 0:
		r.Phase, r.Detail = "measurement", "required measurements unavailable"
	case *r.CgroupPeak == 0 || *r.CgroupPeak > f.MemoryBytes || *r.PeakFDs == 0 || *r.MinFree == 0:
		r.Phase, r.Detail = "measurement", "required resource measurements outside fixture bounds"
	default:
		r.Phase, r.Workload = "complete", "pass"
		if r.Cleanup.Status == "pass" {
			r.Status = "pass"
		}
	}
	return r
}

func (r *result) exercise(ctx context.Context, o options, binary, dir, group string, f fixture, restart bool) (*child, error) {
	r.Phase = "startup"
	if restart {
		r.Phase = "restart"
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := time.Now()
	p, base, err := startChild(binary, dir, group, o.mode)
	if err != nil {
		return p, err
	}
	sampling, stopSampling := context.WithCancel(ctx)
	sampled := r.sample(sampling, p, dir)
	defer func() {
		stopSampling()
		<-sampled
	}()
	probe := newService(base)
	defer probe.client.CloseIdleConnections()
	if err := probe.ready(ctx, p.done); err != nil {
		return p, err
	}
	elapsed := time.Since(start)
	if restart {
		r.RestartReady = &elapsed
		r.Phase = "recovery_walk"
		r.RecoveredRows, err = f.recover(ctx, probe)
		return p, err
	}
	r.StartReady = &elapsed
	return p, f.load(ctx, probe, o, r)
}

func (r *result) collectRSS(p *child) {
	if p == nil {
		return
	}
	select {
	case <-p.done:
		usage, ok := p.cmd.ProcessState.SysUsage().(*syscall.Rusage)
		if ok && usage.Maxrss > 0 {
			rss := uint64(usage.Maxrss) * 1024
			if r.PeakRSS == nil || rss > *r.PeakRSS {
				r.PeakRSS = &rss
			}
		}
	default:
	}
}
