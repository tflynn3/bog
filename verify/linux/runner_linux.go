package main

import (
	"context"
	"debug/elf"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const cgroupMount = "/sys/fs/cgroup"

func runDelegated(ctx context.Context, mode string, args []string) (err error) {
	f := flag.NewFlagSet(mode, flag.ContinueOnError)
	arch := f.String("arch", "", "expected native Linux architecture")
	revision := f.String("revision", "", "full candidate commit SHA")
	fixture := f.String("fixture", "", "release fixture identifier")
	out := f.String("report", "", "runner evidence report")
	binary := f.String("binary", "", "absolute path to pinned candidate")
	digest := f.String("sha256", "", "pinned candidate SHA-256 from the build")
	producer := f.String("producer", "", "absolute path to capacity producer")
	work := f.String("work-dir", "", "disposable disk-backed work directory")
	proof := f.String("capacity-report", "", "new schema-v1 capacity producer report")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !revisionPattern.MatchString(*revision) || *fixture != fixtureID || !filepath.IsAbs(*out) {
		return fmt.Errorf("require -arch, -revision, -fixture %s and absolute -report", fixtureID)
	}
	if mode == "capacity" {
		for _, path := range []string{*binary, *producer, *work, *proof} {
			if !filepath.IsAbs(path) {
				return errors.New("capacity requires absolute -binary, -producer, -work-dir and -capacity-report")
			}
		}
		if err := checkRunnerDestinations(*out, *proof, *binary, *producer); err != nil {
			return err
		}
	} else if *binary != "" || *digest != "" || *producer != "" || *work != "" || *proof != "" {
		return errors.New("capacity flags require the capacity subcommand")
	}
	r := runnerReport{SchemaVersion: 1, Revision: *revision, Fixture: *fixture, Status: "pass",
		Probe: runnerProbe{Arch: runtime.GOARCH}}
	if mode == "capacity" {
		r.Capacity = newRunnerCapacity()
	}
	defer func() {
		if err != nil {
			r.Status, r.Detail = "fail", err.Error()
			if r.FailureKind == "" {
				r.FailureKind = runnerFailure
				if ctx.Err() != nil {
					r.FailureKind = cancelledFailure
				}
			}
		}
		err = errors.Join(err, writeRunnerReport(*out, r))
	}()

	if mode == "capacity" {
		identity, err := newReport(*binary, *revision, *fixture)
		if err != nil {
			return fmt.Errorf("identify native candidate: %w", err)
		}
		r.BinarySHA256 = identity.BinarySHA256
		if r.BinarySHA256 != *digest {
			return fmt.Errorf("candidate SHA-256 %s does not match the build pin %q", r.BinarySHA256, *digest)
		}
		if err := checkCandidateELF(*binary, *arch); err != nil {
			return err
		}
	}
	files := controlFiles{os.ReadFile, writeControl}
	probeCtx, stopProbe := context.WithTimeout(ctx, 10*time.Second)
	defer stopProbe()
	if err := inspectRunner(&r.Probe, *arch); err != nil {
		return err
	}
	ops := probeOps{files, os.MkdirTemp, syscall.Rmdir, placeProbe}
	if err := probeDelegation(probeCtx, &r.Probe, ops); err != nil {
		return err
	}
	if mode == "preflight" {
		return nil
	}
	return r.runCapacity(ctx, files, *producer, *binary, *work, *proof)
}

func checkRunnerDestinations(report, proof, binary, producer string) error {
	for _, path := range []string{proof, binary, producer} {
		if filepath.Clean(report) == filepath.Clean(path) {
			return errors.New("runner report must have a separate destination")
		}
	}
	for _, path := range []string{report, proof} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return fmt.Errorf("evidence destination already exists: %s", path)
			}
			return err
		}
	}
	return nil
}

func checkCandidateELF(binary, arch string) (err error) {
	want := map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}[arch]
	file, err := elf.Open(binary)
	if err != nil {
		return fmt.Errorf("read native candidate ELF: %w", err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if want == elf.EM_NONE || file.Machine != want || file.Class != elf.ELFCLASS64 ||
		file.Data != elf.ELFDATA2LSB || (file.Type != elf.ET_EXEC && file.Type != elf.ET_DYN) {
		return fmt.Errorf("require Linux %s executable ELF, observed %v %v %v %v",
			arch, file.Machine, file.Class, file.Data, file.Type)
	}
	return nil
}

func inspectRunner(r *runnerProbe, expected string) error {
	var uts syscall.Utsname
	if err := syscall.Uname(&uts); err != nil {
		return fmt.Errorf("read kernel architecture: %w", err)
	}
	for _, b := range uts.Machine {
		if b == 0 {
			break
		}
		r.KernelMachine += string(byte(b))
	}
	if err := requireNativeArch(r.Arch, r.KernelMachine, expected); err != nil {
		return err
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_AS, &limit); err != nil {
		return fmt.Errorf("read verifier RLIMIT_AS: %w", err)
	}
	r.AddressSpace = strconv.FormatUint(limit.Cur, 10)
	if limit.Cur == ^uint64(0) {
		r.AddressSpace = "unlimited"
	}
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return err
	}
	r.Parent, err = delegatedParent(cgroupMount, string(membership))
	if err != nil {
		return err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(r.Parent, &fs); err != nil {
		return err
	}
	if fs.Type != 0x63677270 {
		return fmt.Errorf("%s is not on cgroup v2", r.Parent)
	}
	return nil
}

func placeProbe(ctx context.Context, group string, r *runnerProbe) (err error) {
	// Hold a process before any workload starts, as the capacity producer
	// does. This exercises migration permission at the common ancestor.
	read, gate, err := os.Pipe()
	if err != nil {
		return err
	}
	cmd := exec.Command("/bin/sh", "-c", "read -r gate")
	cmd.Stdin = read
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		return errors.Join(err, read.Close(), gate.Close())
	}
	defer func() {
		// Keep the gate closed to further execution even if migration or
		// cgroup.kill failed. The shell has no child processes.
		killErr := cmd.Process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}
		closeErr := gate.Close()
		waitErr := cmd.Wait()
		r.Reaped = cmd.ProcessState != nil
		if r.Reaped {
			if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signal() == syscall.SIGKILL {
				waitErr = nil
			}
		}
		err = errors.Join(err, killErr, closeErr, waitErr)
	}()
	if err := read.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pid := strconv.Itoa(cmd.Process.Pid)
	path := filepath.Join(group, "cgroup.procs")
	if err := writeControl(path, pid); err != nil {
		return fmt.Errorf("place probe in disposable cgroup: %w", err)
	}
	procs, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !slices.Contains(strings.Fields(string(procs)), pid) {
		return errors.New("disposable cgroup does not contain the probe PID")
	}
	r.Placed = true
	if err := writeControl(filepath.Join(group, "cgroup.kill"), "1"); err != nil {
		return fmt.Errorf("kill disposable cgroup: %w", err)
	}
	return nil
}
