package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"bog/verify/capacityevidence"
)

type linuxMixedLoad struct {
	binary, group string
	trial         *restartTrial
	process       *child
	service       service
}

type linuxMixedLoadReport struct {
	mixedLoadReport
	Revision         string       `json:"revision"`
	Architecture     string       `json:"architecture"`
	BinarySHA256     string       `json:"binary_sha256"`
	Cleanup          cleanupProof `json:"cleanup"`
	MemoryLimitBytes *uint64      `json:"cgroup_memory_limit_bytes,omitempty"`
	SwapMaxBytes     *uint64      `json:"cgroup_swap_max_bytes,omitempty"`
	OOMKills         *uint64      `json:"cgroup_oom_kills,omitempty"`
}

// runLinuxMixedLoad runs a reviewed #108 binary and returns standalone evidence.
func runLinuxMixedLoad(ctx context.Context, o options, f fixture, expectedDigest string) (report linuxMixedLoadReport, err error) {
	report.Revision, report.Architecture = o.revision, runtime.GOARCH
	phase := "configuration"
	defer func() {
		if err != nil && report.FailurePhase == "" {
			report.FailurePhase, report.Failure = phase, diagnosticError(err)
		}
	}()
	if f.MemoryBytes != capacityevidence.MemoryLimit || expectedDigest == "" {
		return report, errors.New("live mixed load requires a 128 MiB cap and a pinned binary digest")
	}
	if o.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.timeout)
		defer cancel()
	}
	phase = "provision"
	root, err := createFixtureRoot(o.workDir)
	if err != nil {
		return report, err
	}
	defer func() {
		removeErr := os.RemoveAll(root)
		report.Cleanup.StoreRemoved = removeErr == nil
		err = errors.Join(err, removeErr)
	}()
	binary, dir := filepath.Join(root, "candidate"), filepath.Join(root, "store")
	report.BinarySHA256, err = pinBinary(o.binary, binary, o.revision)
	if err != nil {
		return report, err
	}
	if report.BinarySHA256 != expectedDigest {
		return report, errors.New("candidate SHA-256 differs from the reviewed binary digest")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return report, err
	}
	trial, err := pinRestartTrial(dir, newRestartEvidence())
	if err != nil {
		return report, err
	}
	defer func() { err = errors.Join(err, trial.handle.Close()) }()
	measured := new(resources)
	group, err := openGroup(o.cgroupParent, f.MemoryBytes, measured)
	report.MemoryLimitBytes, report.SwapMaxBytes = measured.CgroupLimit, measured.CgroupSwap
	if group != "" {
		live := &linuxMixedLoad{binary: binary, group: group, trial: trial}
		defer func() {
			var eventsErr error
			proof, shutdownErr, cleanupErr := teardownGroup(group, live.process, func() {
				raw, err := os.ReadFile(filepath.Join(group, "memory.events"))
				if err != nil {
					eventsErr = err
					return
				}
				events, err := memoryEvents(string(raw))
				if err == nil {
					kills := events["oom_kill"]
					report.OOMKills = &kills
					if kills != 0 {
						err = fmt.Errorf("isolated cgroup recorded %d OOM kills", kills)
					}
				}
				eventsErr = err
			})
			report.Cleanup = proof
			err = errors.Join(err, shutdownErr, cleanupErr, eventsErr)
		}()
		if err == nil {
			phase = "startup"
			if err = live.start(ctx); err == nil {
				report.mixedLoadReport, err = runMixedLoad(ctx, live, live, f, o.control)
				if live.process != nil {
					phase = "listener"
					err = errors.Join(err, live.process.requireListener())
				}
				if err == nil {
					phase = "cleanup"
				}
			}
		}
	}
	return report, err
}

func (l *linuxMixedLoad) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, base, err := startChild(l.binary, l.trial.dir, l.group, "live")
	l.process = p // The launcher can return a child after a placement error.
	if err != nil {
		return err
	}
	s := newService(base)
	s.client.Timeout = 45 * time.Second
	if err := s.ready(ctx, p.done, p.ownsListener); err != nil {
		s.client.CloseIdleConnections()
		return err
	}
	s.client.CloseIdleConnections()
	pid := p.cmd.Process.Pid
	procs, err := os.ReadFile(filepath.Join(l.group, "cgroup.procs"))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(procs)) != strconv.Itoa(pid) {
		return errors.New("candidate must be the sole process in its measurement cgroup")
	}
	info, err := os.Stat(fmt.Sprintf("/proc/%d/cwd", pid))
	if err != nil {
		return err
	}
	if _, err := l.trial.compareDirectory(info); err != nil {
		return err
	}
	l.service = s
	return p.requireListener()
}

func (l *linuxMixedLoad) Service() service { return l.service }

func (l *linuxMixedLoad) Restart(ctx context.Context) (service, error) {
	proof, stopErr, cleanupErr := teardownGroup(l.group, l.process, nil)
	if proof.CandidateReaped {
		l.process = nil
	}
	if err := l.trial.beforeStart(proof, errors.Join(stopErr, cleanupErr)); err != nil {
		return service{}, err
	}
	if err := l.start(ctx); err != nil {
		return service{}, err
	}
	return l.service, nil
}

func (l *linuxMixedLoad) Sample(ctx context.Context) (mixedLoadSample, error) {
	var sample mixedLoadSample
	p := l.process
	if p == nil || p.exited() {
		return sample, errors.New("candidate exited before memory observation")
	}
	proc := fmt.Sprintf("/proc/%d", p.cmd.Process.Pid)
	status, err := os.ReadFile(filepath.Join(proc, "status"))
	if err != nil {
		return sample, err
	}
	_, rss, ok := strings.Cut("\n"+string(status), "\nVmRSS:")
	line, _, _ := strings.Cut(rss, "\n")
	fields := strings.Fields(line)
	if !ok || len(fields) != 2 || fields[1] != "kB" {
		return sample, errors.New("missing or malformed candidate VmRSS")
	}
	kib, err := strconv.ParseUint(fields[0], 10, 54)
	if err != nil {
		return sample, fmt.Errorf("candidate VmRSS: %w", err)
	}
	files, err := os.ReadDir(filepath.Join(proc, "fd"))
	if err != nil {
		return sample, err
	}
	sample.RSSBytes, sample.Descriptors = kib<<10, uint64(len(files))
	current, err := readCandidateMemoryCurrent(ctx, p, l.group)
	if err != nil {
		return sample, err
	}
	sample.CgroupCurrent = &current
	peak, err := readCandidateMemory(ctx, p, l.group, "memory.peak")
	if err == nil {
		sample.CgroupPeak = &peak
	} else if !errors.Is(err, os.ErrNotExist) || p.exited() {
		return sample, err
	}
	return sample, ctx.Err()
}

// Linux reports timer 04 while the peer advertises a zero window and the
// candidate still has queued bytes. Match the exact connection in both directions.
func (l *linuxMixedLoad) Stalled(ctx context.Context, conn *net.TCPConn) (bool, error) {
	addr := func(a net.Addr) string { return fmt.Sprintf("0100007F:%04X", a.(*net.TCPAddr).Port) }
	if l.process == nil || l.process.exited() || conn.RemoteAddr().(*net.TCPAddr).Port != l.process.port {
		return false, errors.New("slow response does not belong to the running candidate")
	}
	table, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return false, err
	}
	return mixedLoadZeroWindow(ctx, string(table), addr(conn.RemoteAddr()), addr(conn.LocalAddr()))
}
