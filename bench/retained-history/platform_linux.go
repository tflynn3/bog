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

	"bog/verify/listener"
)

type child struct {
	cmd         *exec.Cmd
	done        chan struct{}
	port        int
	waitErr     error // read after done closes
	logDrainErr error
	logs        candidateLog
}

func (p *child) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *child) ownsListener() (bool, error) {
	if p.exited() {
		return false, errors.New("candidate exited before listener verification")
	}
	return listener.Owns(p.cmd.Process.Pid, p.port)
}

func (p *child) requireListener() error {
	owned, err := p.ownsListener()
	if err != nil {
		return err
	}
	if !owned {
		return errors.New("candidate no longer owns the listener")
	}
	return nil
}

const (
	capacityControlEnv = "BOG_CAPACITY_CONTROL"
	// Certifying the largest walk charges both scanning and encoding its
	// roughly 386 MiB of lines, plus indexes, lookahead and delivery.
	liveQueryWorkBudget = 2 << 30
)

func startChild(binary, dir, group, mode string) (*child, string, error) {
	reserved, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, "", err
	}
	addr := reserved.Addr().(*net.TCPAddr)
	if err := reserved.Close(); err != nil {
		return nil, "", err
	}
	// A gate keeps the candidate from running before it enters the cgroup.
	// This also works on Linux environments that filter clone3.
	args := []string{"-c", `read -r gate && [ "$gate" = start ] && exec "$@"`, "capacity",
		binary, "-addr", addr.String(), "-data-dir", dir}
	if mode == "live" {
		args = append(args, "-query-work-budget", strconv.FormatUint(liveQueryWorkBudget, 10))
	}
	cmd := exec.Command("/bin/sh", args...)
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
	p := &child{cmd: cmd, done: make(chan struct{}), port: addr.Port}
	cmd.Stdout, cmd.Stderr = &p.logs, &p.logs
	cmd.WaitDelay = 500 * time.Millisecond
	readGate, gate, err := os.Pipe()
	if err != nil {
		return nil, "", err
	}
	cmd.Stdin = readGate
	if err := cmd.Start(); err != nil {
		return nil, "", errors.Join(err, readGate.Close(), gate.Close())
	}
	defer func() {
		// Keep the PID reserved until placement finishes, even if the
		// shell exits before we release the gate.
		go func() {
			p.waitErr = cmd.Wait()
			if errors.Is(p.waitErr, exec.ErrWaitDelay) {
				// The leader exited successfully. An inherited writer kept
				// its logging pipe open. Cgroup teardown still owns drainage.
				p.logDrainErr, p.waitErr = p.waitErr, nil
			}
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
	return p, "http://" + addr.String(), errors.Join(err, gate.Close())
}

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

func readCandidateMemoryCurrent(ctx context.Context, p *child, group string) (uint64, error) {
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.exited() {
			return errors.New("candidate exited during memory.current sampling")
		}
		return nil
	}
	if err := check(); err != nil {
		return 0, err
	}
	value, err := readCgroupUint(group, "memory.current")
	if err := errors.Join(err, check()); err != nil {
		return 0, err
	}
	return value, nil
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

func (r *result) sample(ctx context.Context, p *child, dir string, live bool) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var nextStorageSample time.Time
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
			if now := time.Now(); live && !now.Before(nextStorageSample) {
				nextStorageSample = now.Add(100 * time.Millisecond)
				if err := r.Diagnostics.sampleStorage(dir); err != nil {
					r.Diagnostics.Unavailable["storage_sample_error"] = diagnosticError(err)
				}
			}
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

func measure(ctx context.Context, o options, binary, dir string, f fixture) (r result) {
	r = result{Fixture: f, Phase: "provision", Status: "fail", Workload: "fail",
		Cleanup: cleanupProof{Status: "not_started"}, Diagnostics: &capacityDiagnostics{
			Queries: make(map[string]*queryPhase), Unavailable: make(map[string]string),
		}, resources: resources{Unavailable: make(map[string]string)}}
	if o.mode == "live" {
		r.Diagnostics.Backfills = newBackfillTimelines(f)
	}
	err := func() (err error) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Mkdir(dir, 0700); err != nil {
			return err
		}
		defer func() {
			removeErr := os.RemoveAll(dir)
			r.Cleanup.StoreRemoved = removeErr == nil
			if removeErr != nil {
				if r.Cleanup.Detail != "" {
					removeErr = fmt.Errorf("%s: %w", r.Cleanup.Detail, removeErr)
				}
				r.Cleanup.Status = "fail"
				r.Cleanup.Detail = removeErr.Error()
				err = errors.Join(err, removeErr)
			}
		}()
		r.disk(dir)
		group, err := openGroup(o.cgroupParent, f.MemoryBytes, &r.resources)
		var p *child
		var candidate *candidateEvidence
		var earlyCleanupErr error
		if group != "" {
			defer func() {
				if candidate != nil && p != nil {
					candidate.ExitedBeforeCleanup = p.exited()
				}
				var shutdownErr, cleanupErr error
				r.Cleanup, shutdownErr, cleanupErr = teardownGroup(group, p, func() { r.collect(group, o.mode == "live") })
				cleanupErr = errors.Join(earlyCleanupErr, cleanupErr)
				if cleanupErr != nil {
					r.Cleanup.Status, r.Cleanup.Detail = "fail", cleanupErr.Error()
				}
				r.collectRSS(p)
				if candidate != nil {
					candidate.finish(p)
				}
				err = errors.Join(err, shutdownErr)
			}()
		}
		if err != nil {
			return fmt.Errorf("create delegated cgroup v2 subtree: %w", err)
		}

		candidate = &candidateEvidence{Lifetime: "startup"}
		r.Diagnostics.Candidates = append(r.Diagnostics.Candidates, candidate)
		p, err = r.exercise(ctx, o, binary, dir, group, f, candidate)
		if err != nil {
			return err
		}
		// Drain the first lifetime before restarting with the same data and
		// measurement cgroup. Final teardown also removes the subtree.
		var stopped cleanupProof
		candidate.ExitedBeforeCleanup = p.exited()
		stopped, err, earlyCleanupErr = teardownGroup(group, p, nil)
		r.collectRSS(p)
		candidate.finish(p)
		if stopped.CandidateReaped {
			p = nil
		}
		if err != nil || earlyCleanupErr != nil {
			return err
		}
		candidate = &candidateEvidence{Lifetime: "restart"}
		r.Diagnostics.Candidates = append(r.Diagnostics.Candidates, candidate)
		p, err = r.exercise(ctx, o, binary, dir, group, f, candidate)
		return err
	}()
	r.Diagnostics.markMissing(o.mode)
	for key, missing := range map[string]bool{
		"cgroup_peak_memory": r.CgroupPeak == nil, "peak_rss": r.PeakRSS == nil, "peak_descriptors": r.PeakFDs == nil,
		"device_io": len(r.DeviceIO) == 0, "disk_headroom": r.InitialFree == nil || r.MinFree == nil,
		"ingest_p99": r.IngestP99 == nil, "backfill_query_p99": r.QueryP99 == nil,
		"start_to_ready": r.StartReady == nil, "restart_to_ready": r.RestartReady == nil,
		"cgroup_memory_limit_bytes": r.CgroupLimit == nil, "cgroup_swap_max_bytes": r.CgroupSwap == nil,
		"cgroup_oom_kill_count": o.mode == "live" && r.CgroupOOM == nil,
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
	case o.mode == "live" && (r.CgroupOOM == nil || *r.CgroupOOM != 0):
		r.Phase, r.Detail = "measurement", "live fixture requires an observed zero cgroup OOM-kill count"
	default:
		r.Phase, r.Workload = "complete", "pass"
		if r.Cleanup.Status == "pass" {
			r.Status = "pass"
			if o.mode == "live" {
				if err := r.validateCutover(f); err != nil {
					r.Phase, r.Workload, r.Status, r.Detail = "cutover_proof", "fail", "fail", err.Error()
				}
			}
		}
	}
	return r
}

func (r *result) exercise(ctx context.Context, o options, binary, dir, group string, f fixture, candidate *candidateEvidence) (_ *child, err error) {
	r.Phase = candidate.Lifetime
	if err := ctx.Err(); err != nil {
		candidate.StartupError = diagnosticError(err)
		return nil, err
	}
	start := time.Now()
	p, base, err := startChild(binary, dir, group, o.mode)
	if err != nil {
		candidate.StartupError = diagnosticError(err)
		return p, err
	}
	sampling, stopSampling := context.WithCancel(ctx)
	sampled := r.sample(sampling, p, dir, o.mode == "live")
	defer func() {
		stopSampling()
		<-sampled
	}()
	probe := newService(base)
	if o.mode == "live" {
		// First pages certify the complete walk before writing headers.
		probe.client.Timeout = 45 * time.Second
	}
	probe.queryPhases = r.Diagnostics.Queries
	defer probe.client.CloseIdleConnections()
	if err := probe.ready(ctx, p.done, p.ownsListener); err != nil {
		candidate.StartupError = diagnosticError(err)
		return p, err
	}
	// Abort in-flight HTTP calls and join the watcher before teardown.
	work, cancelWork := context.WithCancel(ctx)
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case <-work.Done():
		case <-p.done:
			cancelWork()
		}
	}()
	defer func() {
		cancelWork()
		<-watched
		if listenerErr := p.requireListener(); listenerErr != nil {
			err = errors.Join(err, fmt.Errorf("workload completion listener: %w", listenerErr))
		}
		if p.exited() {
			err = errors.Join(err, errors.New("candidate exited during workload"))
		}
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
			err = errors.Join(err, ctxErr)
		}
	}()
	elapsed := time.Since(start)
	candidate.Ready = &elapsed
	if candidate.Lifetime == "restart" {
		r.RestartReady = &elapsed
		r.Phase = "recovery_walk"
		r.RecoveredRows, err = f.recover(work, probe)
		return p, err
	}
	r.StartReady = &elapsed
	if o.mode == "live" {
		probe.readMemoryCurrent = func(ctx context.Context) (uint64, error) {
			return readCandidateMemoryCurrent(ctx, p, group)
		}
	}
	return p, f.load(work, probe, o, r, dir)
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
