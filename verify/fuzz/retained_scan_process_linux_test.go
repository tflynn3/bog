package fuzz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"
)

const scanStartupDeadline = 10 * time.Second

// Closing a parent copy immediately after Start lets EOF detect a worker
// that exits anywhere before readiness. CommandContext owns the process
// deadline and Wait reaps the child even when the handshake fails.
func startScanWorker(ctx context.Context, cmd *exec.Cmd, startup time.Duration, readers int,
	admit func(int) error) (evidence scanWorkerEvidence, output []byte, err error) {
	evidence.Readers = readers
	defer func() { evidence.Outcome = scanOutcome(evidence.Started, output, err) }()
	readyRead, readyWrite, err := scanWorkerPipe()
	if err != nil {
		return evidence, nil, err
	}
	defer func() { err = errors.Join(err, readyRead.Close()) }()
	gateRead, gateWrite, err := scanWorkerPipe()
	if err != nil {
		return evidence, nil, errors.Join(err, readyWrite.Close())
	}
	closeGate := func() error {
		if gateWrite == nil {
			return nil
		}
		err := gateWrite.Close()
		gateWrite = nil
		return err
	}
	defer func() { err = errors.Join(err, closeGate()) }()

	log := new(replayWorkerLog)
	cmd.Stdin = gateRead
	cmd.ExtraFiles = []*os.File{readyWrite}
	cmd.Stdout, cmd.Stderr = log, log
	cmd.WaitDelay = time.Second
	startErr := cmd.Start()
	err = errors.Join(startErr, readyWrite.Close(), gateRead.Close())
	if startErr != nil {
		return evidence, nil, err
	}
	defer func() {
		if !evidence.Started {
			killErr := cmd.Process.Kill()
			if !errors.Is(killErr, os.ErrProcessDone) {
				err = errors.Join(err, killErr)
			}
		}
		waitErr := cmd.Wait()
		evidence.Reaped = cmd.ProcessState != nil && cmd.ProcessState.Pid() == cmd.Process.Pid
		output = log.buffer.Bytes()
		err = errors.Join(err, ctx.Err(), waitErr, log.limitError())
	}()

	if err != nil {
		return evidence, nil, err
	}
	deadline := time.Now().Add(startup)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err = readyRead.SetReadDeadline(deadline); err != nil {
		return evidence, nil, err
	}
	ready, err := readScanWorkerReady(readyRead, scanWorkerReady{
		PID: cmd.Process.Pid, Readers: readers, Arch: runtime.GOARCH, GoVersion: runtime.Version(), Race: scanRaceEnabled,
	})
	if err != nil {
		return evidence, nil, err
	}
	evidence.Ready = &ready
	if err = ctx.Err(); err != nil {
		return evidence, nil, err
	}
	if err = admit(cmd.Process.Pid); err != nil {
		return evidence, nil, err
	}
	if err = ctx.Err(); err != nil {
		return evidence, nil, err
	}
	if err = gateWrite.SetWriteDeadline(deadline); err != nil {
		return evidence, nil, err
	}
	_, err = gateWrite.Write([]byte{scanStartByte})
	evidence.Started = err == nil
	return evidence, nil, errors.Join(err, closeGate())
}

// Preserve nonblocking mode through exec so both ends support deadlines.
func scanWorkerPipe() (*os.File, *os.File, error) {
	var fds [2]int
	if err := syscall.Pipe2(fds[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(fds[0]), "retained-scan-pipe-read"),
		os.NewFile(uintptr(fds[1]), "retained-scan-pipe-write"), nil
}

func announceScanWorker(count int) error {
	readyPipe := os.NewFile(3, "retained-scan-ready")
	if readyPipe == nil {
		return errors.New("missing worker readiness pipe")
	}
	ready := scanWorkerReady{
		Version: scanStartupVersion, PID: os.Getpid(), Readers: count, Arch: runtime.GOARCH,
		GoVersion: runtime.Version(), Race: scanRaceEnabled, Memory: readScanProcessMemory(),
	}
	deadline := time.Now().Add(scanStartupDeadline)
	err := readyPipe.SetWriteDeadline(deadline)
	if err == nil {
		err = json.NewEncoder(readyPipe).Encode(ready)
	}
	if err := errors.Join(err, readyPipe.Close()); err != nil {
		return err
	}
	if err := os.Stdin.SetReadDeadline(deadline); err != nil {
		return errors.Join(err, os.Stdin.Close())
	}
	var signal [1]byte
	_, err = io.ReadFull(os.Stdin, signal[:])
	if err := errors.Join(err, os.Stdin.Close()); err != nil {
		return fmt.Errorf("wait for reader start: %w", err)
	}
	if signal[0] != scanStartByte {
		return fmt.Errorf("invalid reader start byte %q", signal[0])
	}
	return nil
}

func scanWorkerLimits() error {
	// Race shadow mappings can grow long after the startup snapshot.
	// Reader work, live captures, disk and heap have their own limits.
	// Keep RLIMIT_AS for ordinary scan workers and every corpus worker.
	if scanRaceEnabled {
		return applyHardLimit(syscall.RLIMIT_FSIZE, maxOutputFile)
	}
	memory := readScanProcessMemory()
	if !memory.valid() || memory.Unavailable != "" {
		return fmt.Errorf("read address-space baseline: %+v", memory)
	}
	if memory.VirtualBytes > math.MaxUint64-addressHeadroom {
		return fmt.Errorf("address-space baseline %d leaves no room for limit calculation", memory.VirtualBytes)
	}
	if err := applyHardLimit(syscall.RLIMIT_AS, memory.VirtualBytes+addressHeadroom); err != nil {
		return err
	}
	return applyHardLimit(syscall.RLIMIT_FSIZE, maxOutputFile)
}
