package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

const (
	outputWorkerArg   = "--bog-export-output-worker"
	workerHandshake   = "BOGOUT\x01"
	workerStartWindow = 2 * time.Second
	workerStopWindow  = 250 * time.Millisecond
)

// The CLI starts both inherited-stream workers before contacting the server.
// Each worker owns one destination and stays alive until the parent closes
// its request channel or kills and reaps it. Inherited descriptors share
// status flags with the invoker, so only private protocol pipes use deadlines.
// No path lookup or worker startup is needed after the first query.
func openInheritedWorker(ctx context.Context, destination *os.File) (*outputWorker, error) {
	path, err := outputExecutable(os.Executable, os.Args[0])
	if err != nil {
		return nil, fmt.Errorf("locate output worker executable: %w", err)
	}
	return startOutputWorker(ctx, path, destination)
}

// Linux os.Executable uses procfs. Fall back to the invocation path for
// startup in environments without procfs or /dev/fd.
func outputExecutable(current func() (string, error), invocation string) (string, error) {
	path, err := current()
	if err == nil {
		return path, nil
	}
	path, lookupErr := exec.LookPath(invocation)
	if lookupErr != nil {
		return "", errors.Join(err, lookupErr)
	}
	return filepath.Abs(path)
}

type outputWorker struct {
	cmd      *exec.Cmd
	request  *os.File
	reply    *os.File
	lifeline *os.File
	wait     chan error
	failed   bool
}

// makeOutputPipes returns parent and child ends in request, reply, lifeline
// order. Only the parent holds a lifeline writer, so an unexpected parent
// exit also wakes a worker blocked in a destination write.
func makeOutputPipes() (parent, child []*os.File, err error) {
	for _, parentReads := range []bool{false, true, false} {
		read, write, err := os.Pipe()
		if err != nil {
			return nil, nil, errors.Join(err, closeOutputFiles(parent), closeOutputFiles(child))
		}
		if parentReads {
			parent, child = append(parent, read), append(child, write)
		} else {
			parent, child = append(parent, write), append(child, read)
		}
	}
	return parent, child, nil
}

func closeOutputFiles(files []*os.File) error {
	var err error
	for _, file := range files {
		err = errors.Join(err, file.Close())
	}
	return err
}

func startOutputWorker(ctx context.Context, path string, destination *os.File) (_ *outputWorker, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, workerStartWindow)
	defer cancel()
	if err := outputCancellation(ctx, nil); err != nil {
		return nil, err
	}
	parent, child, err := makeOutputPipes()
	if err != nil {
		return nil, fmt.Errorf("create output worker pipes: %w", err)
	}
	worker := &outputWorker{request: parent[0], reply: parent[1], lifeline: parent[2]}
	defer func() {
		if resultErr != nil {
			worker.failed = true
			resultErr = errors.Join(resultErr, worker.close())
		}
	}()

	// Startup has its own deadline. The process lifetime belongs to
	// close, so stderr can still serve a diagnostic after export cancellation.
	cmd := exec.Command(path, outputWorkerArg)
	cmd.Stdin = child[0]
	cmd.Stdout = destination
	// Give the worker a write-inaccessible stderr without using /dev/null.
	cmd.Stderr = child[0]
	cmd.ExtraFiles = child[1:]
	if err := cmd.Start(); err != nil {
		return nil, outputCancellation(ctx, errors.Join(fmt.Errorf("start output worker: %w", err), closeOutputFiles(child)))
	}
	worker.cmd = cmd
	worker.wait = make(chan error, 1)
	go func() { worker.wait <- cmd.Wait() }()
	if err := closeOutputFiles(child); err != nil {
		return nil, fmt.Errorf("close child ends of output pipes: %w", err)
	}

	err = worker.withDeadline(ctx, func() error {
		var handshake [len(workerHandshake)]byte
		if _, err := io.ReadFull(worker.reply, handshake[:]); err != nil {
			return err
		}
		if !bytes.Equal(handshake[:], []byte(workerHandshake)) {
			return errors.New("invalid output worker handshake")
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("probe output worker: %w", err)
	}
	return worker, nil
}

// Each operation joins its deadline callback before returning. A failed
// operation may have left the worker blocked in a syscall or a protocol
// frame incomplete. Only close is valid after a failure.
func (worker *outputWorker) withDeadline(ctx context.Context, operation func() error) error {
	if worker.failed {
		return errors.New("output worker already failed")
	}
	err := outputCancellation(ctx, nil)
	if err == nil {
		var disarm func() error
		disarm, err = armOutputDeadline(ctx, func(deadline time.Time) error {
			return errors.Join(worker.request.SetWriteDeadline(deadline), worker.reply.SetReadDeadline(deadline))
		})
		if err == nil {
			err = errors.Join(operation(), disarm())
		}
	}
	err = outputCancellation(ctx, err)
	worker.failed = err != nil
	return err
}

func (worker *outputWorker) close() error {
	// EOF drains an idle worker. Kill immediately when a write may be
	// outstanding and cap the grace period even for a stalled idle worker.
	requestErr := worker.request.Close()
	var waitErr error
	if worker.cmd != nil {
		if worker.failed || requestErr != nil {
			waitErr = worker.killAndWait()
		} else {
			timer := time.NewTimer(workerStopWindow)
			defer timer.Stop()
			select {
			case waitErr = <-worker.wait:
			case <-timer.C:
				waitErr = errors.Join(errors.New("output worker did not stop after EOF"), worker.killAndWait())
			}
		}
	}
	if err := errors.Join(requestErr, waitErr, worker.reply.Close(), worker.lifeline.Close()); err != nil {
		return fmt.Errorf("close output worker: %w", err)
	}
	return nil
}

func (worker *outputWorker) killAndWait() error {
	killErr := worker.cmd.Process.Kill()
	waitErr := <-worker.wait
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signal() == syscall.SIGKILL {
			waitErr = nil
		}
	}
	return errors.Join(killErr, waitErr)
}
