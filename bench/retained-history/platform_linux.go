package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
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

func startChild(binary, dir, group, mode string, queryFlags ...string) (*child, string, error) {
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
	dataDir := dir
	if mode == "live" {
		// Resolve the store from the same working directory whose kernel
		// identity the runner checks after each successful readiness probe.
		dataDir = "."
	}
	args := []string{"-c", `read -r gate && [ "$gate" = start ] && exec "$@"`, "capacity",
		binary, "-addr", addr.String(), "-data-dir", dataDir}
	if mode == "live" {
		args = append(args, "-query-work-budget", strconv.FormatUint(liveQueryWorkBudget, 10))
	}
	args = append(args, queryFlags...)
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
	err = placeInGroup(group, cmd.Process.Pid)
	if err == nil {
		_, err = gate.Write([]byte("start\n"))
	}
	// The caller owns p even when placement or release of the startup gate fails.
	return p, "http://" + addr.String(), errors.Join(err, gate.Close())
}

func readCandidateMemoryCurrent(ctx context.Context, p *child, group string) (uint64, error) {
	return readCandidateMemory(ctx, p, group, "memory.current")
}

func readCandidateMemory(ctx context.Context, p *child, group, name string) (uint64, error) {
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.exited() {
			return fmt.Errorf("candidate exited during %s sampling", name)
		}
		return nil
	}
	if err := check(); err != nil {
		return 0, err
	}
	value, err := readCgroupUint(group, name)
	if err := errors.Join(err, check()); err != nil {
		return 0, err
	}
	return value, nil
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

func measure(ctx context.Context, o options, binary, dir string, f fixture) (r result) {
	r = result{Fixture: f, Phase: "provision", Status: "fail", Workload: "fail",
		Cleanup: cleanupProof{Status: "not_started"}, Diagnostics: &capacityDiagnostics{
			Queries: make(map[string]*queryPhase), Unavailable: make(map[string]string),
		}, resources: resources{Unavailable: make(map[string]string)}}
	if o.mode == "live" {
		r.Diagnostics.Backfills = newBackfillTimelines(f)
		r.Diagnostics.Restart = newRestartEvidence()
		if o.requireSlowOverlap {
			r.Diagnostics.SlowResponses = newSlowResponseEvidence()
		}
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
		var trial *restartTrial
		if e := r.Diagnostics.Restart; e != nil {
			trial, err = pinRestartTrial(dir, e)
			if err != nil {
				e.Phase = "pin_directory"
				e.finish(err)
				return fmt.Errorf("pin generation-replacement trial directory: %w", err)
			}
			defer func() {
				if closeErr := trial.handle.Close(); closeErr != nil {
					if err == nil {
						e.Phase = "close_directory"
					}
					err = errors.Join(err, fmt.Errorf("close trial directory: %w", closeErr))
				}
				if e.Status == "running" {
					e.finish(err)
				}
			}()
		}
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
		p, err = r.exercise(ctx, o, binary, dir, group, f, candidate, trial)
		if err != nil {
			return err
		}
		if trial != nil {
			trial.evidence.Status = "running"
			r.Phase = "restart_prepare"
			if err := trial.prepare(f, r.Compaction); err != nil {
				return err
			}
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
		if trial != nil {
			if err := trial.beforeStart(stopped, errors.Join(err, earlyCleanupErr)); err != nil {
				r.Phase = trial.evidence.Phase
				return fmt.Errorf("before restart: %w", err)
			}
			trial.evidence.Phase = "restart_startup"
		}
		if err != nil || earlyCleanupErr != nil {
			return err
		}
		candidate = &candidateEvidence{Lifetime: "restart"}
		r.Diagnostics.Candidates = append(r.Diagnostics.Candidates, candidate)
		p, err = r.exercise(ctx, o, binary, dir, group, f, candidate, trial)
		return err
	}()
	if e := r.Diagnostics.SlowResponses; e != nil && e.Status == "running" && err != nil {
		e.fail(err)
	}
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
				if err := r.validateCutover(); err != nil {
					r.Phase, r.Workload, r.Status, r.Detail = "cutover_proof", "fail", "fail", err.Error()
				}
			}
		}
	}
	return r
}

func (r *result) exercise(ctx context.Context, o options, binary, dir, group string, f fixture,
	candidate *candidateEvidence, trial *restartTrial) (_ *child, err error) {
	r.Phase = candidate.Lifetime
	if err := ctx.Err(); err != nil {
		candidate.StartupError = diagnosticError(err)
		return nil, err
	}
	start := time.Now()
	var queryFlags []string
	if o.requireSlowOverlap {
		queryFlags = []string{"-query-concurrency=2"}
	}
	p, base, err := startChild(binary, dir, group, o.mode, queryFlags...)
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
	if trial != nil {
		info, statErr := os.Stat(fmt.Sprintf("/proc/%d/cwd", p.cmd.Process.Pid))
		var identity *directoryIdentity
		if statErr == nil {
			identity, statErr = trial.compareDirectory(info)
		}
		if candidate.Lifetime == "restart" {
			trial.evidence.Restarted = identity
			trial.evidence.Phase = "restart_directory"
		}
		if statErr != nil {
			return p, fmt.Errorf("%s candidate directory: %w", candidate.Lifetime, statErr)
		}
	}
	if o.mode == "live" {
		probe.readMemoryCurrent = func(ctx context.Context) (uint64, error) {
			return readCandidateMemoryCurrent(ctx, p, group)
		}
		probe.readMemoryPeak = func(ctx context.Context) (uint64, error) {
			return readCandidateMemory(ctx, p, group, "memory.peak")
		}
	}
	if o.requireSlowOverlap {
		probe.openPaused = func(ctx context.Context, path, phase string, framing pageFraming) (pausedResponse, error) {
			return openLinuxPaused(ctx, probe, p.cmd.Process.Pid, path, phase, framing)
		}
	}
	if candidate.Lifetime == "restart" {
		r.RestartReady = &elapsed
		r.Phase = "recovery_walk"
		if trial != nil {
			if e := r.Diagnostics.SlowResponses; e != nil {
				err = e.verifyRestart(work, probe, o.control, f, trial)
			} else {
				err = trial.verify(work, probe, o.control, f)
			}
			r.RecoveredRows = trial.evidence.Ledger.Matched
			if err == nil {
				trial.evidence.Phase = "restart_completion"
			}
			return p, err
		}
		r.RecoveredRows, err = f.recover(work, probe)
		return p, err
	}
	r.StartReady = &elapsed
	if trial != nil {
		probe.acknowledged = &trial.ledger
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
