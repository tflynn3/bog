package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func (r *runnerReport) runCapacity(ctx context.Context, files controlFiles, producer, binary, work string, proof *reportDestination) error {
	work, err := filepath.EvalSymlinks(work)
	if err != nil {
		return fmt.Errorf("resolve capacity work directory: %w", err)
	}
	eventPath := filepath.Join(r.Probe.Parent, "memory.events")
	before, err := files.events(eventPath, oomEvents...)
	if err != nil {
		return fmt.Errorf("read delegation baseline: %w", err)
	}
	// Keep this supervisor and the producer in the runner subgroup. Only
	// the benchmark's two candidate fixtures belong under the 128 MiB cap.
	// ExtraFiles maps the validated proof directory to descriptor 3 in
	// the producer. It opens this parent itself before staging its report.
	proofPath := "/proc/self/fd/3/" + proof.name
	cmd := exec.CommandContext(ctx, producer, "-binary", binary, "-revision", r.Revision,
		"-fixture", r.Fixture, "-mode", "live", "-cgroup-parent", r.Probe.Parent,
		"-work-dir", work, "-report", proofPath)
	cmd.ExtraFiles = []*os.File{proof.dir}
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 15 * time.Second
	var output limitedLog
	log := io.MultiWriter(&output, os.Stderr)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start capacity producer: %w", err)
	}

	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	waitErr := r.Capacity.observeUntilExit(files, r.Probe.Parent, work, wait)
	if state := cmd.ProcessState; state != nil {
		if exit := state.ExitCode(); exit != -1 {
			r.Capacity.ExitCode = &exit
		}
		if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			r.Capacity.Signal = status.Signal().String()
		}
	}
	r.Capacity.Output, r.Capacity.OutputTruncated = output.snapshot()

	after, accountingErr := files.events(eventPath, oomEvents...)
	if accountingErr == nil {
		r.Capacity.ParentOOMDelta, accountingErr = oomDelta(before, after)
	}
	if accountingErr != nil {
		r.Capacity.AccountingError = accountingErr.Error()
	}
	kind := runnerFailure
	data, reportErr := readProducerProof(proof)
	if reportErr == nil {
		kind, reportErr = r.readCapacity(data)
	} else {
		reportErr = fmt.Errorf("read capacity producer report: %w", reportErr)
	}
	switch {
	case waitErr != nil && verifierReserveFailure(r.Capacity.Output):
		r.FailureKind = addressSpaceFailure
	case ctx.Err() != nil:
		r.FailureKind = cancelledFailure
		waitErr = errors.Join(waitErr, ctx.Err())
	case reportErr != nil:
		r.FailureKind = kind
	}
	return errors.Join(waitErr, reportErr, accountingErr)
}

// readProducerProof opens the published entry relative to the same directory
// passed to the producer. Require its regular file rather than following a
// substituted symlink or blocking on a FIFO.
func readProducerProof(proof *reportDestination) (_ []byte, err error) {
	fd, err := syscall.Openat(int(proof.dir.Fd()), proof.name,
		syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open capacity proof", Path: proof.path, Err: err}
	}
	f := os.NewFile(uintptr(fd), proof.path)
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("capacity proof %q must be a regular file", proof.path)
	}
	return io.ReadAll(f)
}

func (r *runnerCapacity) observeUntilExit(files controlFiles, parent, work string, wait <-chan error) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	// Only link a cgroup to a scale when a member's working directory
	// identifies the owned producer fixture. A rapid failure can leave the
	// scale unobserved. The final producer report wins over these samples.
	groups := make(map[string]int)
	for {
		select {
		case err := <-wait:
			r.sampleGroups(files, parent, work, groups)
			return err
		case <-ticker.C:
			r.sampleGroups(files, parent, work, groups)
		}
	}
}

func (r *runnerCapacity) sampleGroups(files controlFiles, parent, work string, groups map[string]int) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return // The final report and parent counters are collected separately.
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "bog-capacity-") {
			continue
		}
		group := filepath.Join(parent, entry.Name())
		index, known := groups[group]
		if !known {
			index = identifyScale(files, group, work)
			if index < 0 {
				continue
			}
			groups[group] = index
		}
		scale := &r.Scales[index]
		scale.Status, scale.Phase = "observed", "candidate_in_cgroup"
		peak, err := files.number(filepath.Join(group, "memory.peak"))
		if err == nil {
			scale.MemoryPeak, scale.PeakSource = &peak, "live_sample"
		}
		if events, err := files.events(filepath.Join(group, "memory.events"), oomEvents...); err == nil {
			if delta, err := oomDelta(freshOOMBaseline(), events); err == nil {
				scale.OOMDelta, scale.OOMSource = delta, "live_sample"
			}
		}
	}
}

func identifyScale(files controlFiles, group, work string) int {
	data, err := files.read(filepath.Join(group, "cgroup.procs"))
	if err != nil {
		return -1
	}
	for _, pid := range strings.Fields(string(data)) {
		if value, err := strconv.Atoi(pid); err != nil || value <= 0 {
			continue
		}
		cwd, err := os.Readlink(filepath.Join("/proc", pid, "cwd"))
		if err != nil {
			continue // Candidate may have exited between cgroupfs and procfs.
		}
		storeParent := filepath.Dir(cwd)
		if filepath.Dir(storeParent) != work || !strings.HasPrefix(filepath.Base(storeParent), "bog-capacity-") {
			continue
		}
		switch filepath.Base(cwd) {
		case "store-0":
			return 0
		case "store-1":
			return 1
		}
	}
	return -1
}
