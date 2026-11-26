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
	gate, err := cmd.StdinPipe()
	if err != nil {
		return nil, "", err
	}
	if err := cmd.Start(); err != nil {
		return nil, "", errors.Join(err, gate.Close())
	}
	p := &child{cmd: cmd, done: make(chan struct{})}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()
	err = os.WriteFile(filepath.Join(group, "cgroup.procs"), []byte(strconv.Itoa(cmd.Process.Pid)), 0)
	if err == nil {
		_, err = gate.Write([]byte("start\n"))
	}
	if err := errors.Join(err, gate.Close()); err != nil {
		return nil, "", errors.Join(err, p.stop())
	}
	return p, "http://" + addr, nil
}

func (p *child) stop() error {
	select {
	case <-p.done:
		return p.waitErr
	default:
	}
	err := p.cmd.Process.Signal(syscall.SIGTERM)
	if errors.Is(err, os.ErrProcessDone) {
		err = nil
	}
	select {
	case <-p.done:
		return errors.Join(err, p.waitErr)
	case <-time.After(2 * time.Second):
	}
	killErr := p.cmd.Process.Kill()
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	select {
	case <-p.done:
		return errors.Join(err, killErr, p.waitErr, errors.New("child shutdown exceeded grace period"))
	case <-time.After(2 * time.Second):
		return errors.Join(err, killErr, errors.New("timed out reaping child"))
	}
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
	group, err := os.MkdirTemp(parent, "bog-capacity-")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(group, "memory.max"), []byte(strconv.FormatUint(limit, 10)), 0); err != nil {
		return "", errors.Join(err, os.Remove(group))
	}
	// Disable swap so memory.max is the working-set limit.
	if err := os.WriteFile(filepath.Join(group, "memory.swap.max"), []byte("0"), 0); err != nil {
		return "", errors.Join(err, os.Remove(group))
	}
	return group, nil
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

func closeGroup(group string) error {
	populated, err := os.ReadFile(filepath.Join(group, "cgroup.procs"))
	if err == nil && len(strings.Fields(string(populated))) != 0 {
		err = errors.Join(errors.New("processes left in capacity cgroup"),
			os.WriteFile(filepath.Join(group, "cgroup.kill"), []byte("1"), 0))
	}
	return errors.Join(err, os.Remove(group))
}

func measure(ctx context.Context, o options, binary, dir string, f fixture) (r result) {
	r = result{Fixture: f, Phase: "provision", Status: "fail", resources: resources{Unavailable: make(map[string]string)}}
	err := func() (err error) {
		if err := os.Mkdir(dir, 0700); err != nil {
			return err
		}
		defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
		r.disk(dir)
		group, err := openGroup(o.cgroupParent, f.MemoryBytes)
		if err != nil {
			return fmt.Errorf("create delegated cgroup v2 leaf: %w", err)
		}
		defer func() { r.collect(group); err = errors.Join(err, closeGroup(group)) }()
		for _, restart := range []bool{false, true} {
			err := func() (err error) {
				r.Phase = "startup"
				if restart {
					r.Phase = "restart"
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				start := time.Now()
				p, base, err := startChild(binary, dir, group, o.mode)
				if err != nil {
					return err
				}
				sampling, stopSampling := context.WithCancel(ctx)
				sampled := r.sample(sampling, p, dir)
				defer func() {
					stopSampling()
					<-sampled
					err = errors.Join(err, p.stop())
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
				}()
				probe := newService(base)
				defer probe.client.CloseIdleConnections()
				if err := probe.ready(ctx, p.done); err != nil {
					return err
				}
				elapsed := time.Since(start)
				if restart {
					r.RestartReady = &elapsed
					r.Phase = "recovery_walk"
					n, err := f.recover(ctx, probe)
					r.RecoveredRows = n
					return err
				}
				r.StartReady = &elapsed
				return f.load(ctx, probe, o, &r)
			}()
			if err != nil {
				return err
			}
		}
		return nil
	}()
	for key, missing := range map[string]bool{
		"cgroup_peak_memory": r.CgroupPeak == nil, "peak_rss": r.PeakRSS == nil, "peak_descriptors": r.PeakFDs == nil,
		"device_io": len(r.DeviceIO) == 0, "disk_headroom": r.MinFree == nil,
		"ingest_p99": r.IngestP99 == nil, "backfill_query_p99": r.QueryP99 == nil,
		"start_to_ready": r.StartReady == nil, "restart_to_ready": r.RestartReady == nil,
	} {
		if _, haveReason := r.Unavailable[key]; missing && !haveReason {
			r.Unavailable[key] = "no sample collected"
		}
	}
	if err != nil {
		r.Detail = err.Error()
		r.TimedOut = errors.Is(err, context.DeadlineExceeded)
	} else if len(r.Unavailable) > 0 {
		r.Phase, r.Detail = "measurement", "required measurements unavailable"
	} else {
		r.Phase, r.Status = "complete", "pass"
	}
	return r
}
