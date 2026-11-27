package fuzz

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"
)

const (
	interruptionEnv      = "BOG_FUZZ_INTERRUPT"
	sourceEnv            = "BOG_FUZZ_INTERRUPT_SOURCE"
	destEnv              = "BOG_FUZZ_INTERRUPT_DEST"
	interruptionDeadline = 12 * time.Second
)

func TestInterruptionWorker(t *testing.T) {
	mode := os.Getenv(interruptionEnv)
	if mode == "" {
		t.Skip("started by the interruption tests")
	}
	setWorkerLimits(t)
	ready := os.NewFile(3, "phase barrier")
	if ready == nil {
		t.Fatal("missing phase barrier descriptor")
	}
	defer func() { must(t, ready.Close()) }()
	park := func() {
		n, err := ready.Write([]byte{1})
		must(t, err)
		if n != 1 {
			t.Fatal("short write to phase barrier")
		}
		time.Sleep(interruptionDeadline)
		t.Fatal("parent did not kill worker at phase barrier")
	}
	source, dest := os.Getenv(sourceEnv), os.Getenv(destEnv)
	switch mode {
	case "checkpoint", "checkpoint-staged":
		interruptCheckpoint(t, mode, source, park)
	case "export", "restore", "export-umask":
		interruptBackup(t, mode, source, dest, park)
	default:
		t.Fatalf("unknown interruption mode %q", mode)
	}
}

func startInterrupted(t *testing.T, mode, source, dest string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), interruptionDeadline)
	t.Cleanup(cancel)
	reader, writer, err := os.Pipe()
	must(t, err)
	defer func() { must(t, reader.Close()) }()

	cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestInterruptionWorker$", "-test.count=1")
	cmd.Env = append(cmd.Environ(), interruptionEnv+"="+mode, sourceEnv+"="+source, destEnv+"="+dest)
	cmd.ExtraFiles = []*os.File{writer}
	output := new(replayWorkerLog)
	cmd.Stdout, cmd.Stderr = output, output
	cmd.WaitDelay = time.Second
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	// Linux associates the parent-death signal with the launching thread.
	// Keep that thread alive until the worker is reaped.
	started, stopped := false, false
	stop := func() {
		t.Helper()
		if !started || stopped {
			return
		}
		stopped = true
		defer runtime.UnlockOSThread()
		cancelled := ctx.Err()
		killErr := cmd.Process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}
		waitErr := cmd.Wait()
		must(t, errors.Join(killErr, output.limitError()))
		var exit *exec.ExitError
		if cancelled != nil || !errors.As(waitErr, &exit) || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("interrupt %s worker: wait=%v context=%v, want SIGKILL\n%s", mode, waitErr, cancelled, output.buffer.Bytes())
		}
	}
	t.Cleanup(stop)
	runtime.LockOSThread()
	if err := cmd.Start(); err != nil {
		runtime.UnlockOSThread()
		t.Fatalf("start %s worker: %v", mode, errors.Join(err, writer.Close()))
	}
	started = true
	must(t, writer.Close())
	deadline, _ := ctx.Deadline()
	must(t, reader.SetReadDeadline(deadline))
	var event [1]byte
	if _, err := io.ReadFull(reader, event[:]); err != nil || event[0] != 1 {
		t.Fatalf("%s worker did not reach the phase barrier: event=%v error=%v", mode, event, err)
	}
	return stop
}
