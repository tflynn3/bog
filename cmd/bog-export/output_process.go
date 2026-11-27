package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

const outputWorkerArg = "--bog-export-output-worker"

// Inherited pipes share their open file description with the invoking
// process. Changing O_NONBLOCK even on a dup would affect the invoker.
// Isolate the potentially blocking syscall in an output worker whose only
// destination is the original descriptor. This also works when /proc and
// /dev/fd are absent. Only the private protocol pipes use Go deadlines.
func openInheritedOutput(ctx context.Context, destination *os.File) (*recordOutput, error) {
	path, err := outputExecutable(os.Executable, os.Args[0])
	if err != nil {
		return nil, fmt.Errorf("locate output worker executable: %w", err)
	}
	worker, err := startOutputWorker(ctx, path, destination)
	if err != nil {
		return nil, err
	}
	return &recordOutput{ctx: ctx, write: worker.writeRecord, finish: worker.close}, nil
}

// Linux os.Executable uses procfs. A normal direct or PATH invocation also
// gives us the executable name even in a procfs-free environment.
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
	ctx      context.Context
	request  *os.File
	reply    *os.File
	lifeline *os.File
	cancel   context.CancelFunc
	disarm   func() error
	failed   bool
}

func startOutputWorker(ctx context.Context, path string, destination *os.File) (_ *outputWorker, resultErr error) {
	requestRead, requestWrite, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create output request pipe: %w", err)
	}
	replyRead, replyWrite, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create output reply pipe: %w", err), requestRead.Close(), requestWrite.Close())
	}
	lifeRead, lifeWrite, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create output lifeline: %w", err),
			requestRead.Close(), requestWrite.Close(), replyRead.Close(), replyWrite.Close())
	}

	processCtx, cancel := context.WithCancel(ctx)
	worker := &outputWorker{ctx: processCtx, request: requestWrite, reply: replyRead, lifeline: lifeWrite, cancel: cancel}
	defer func() {
		if resultErr != nil {
			worker.failed = true
			resultErr = errors.Join(resultErr, worker.close())
		}
	}()
	defer func() {
		if err := errors.Join(requestRead.Close(), replyWrite.Close(), lifeRead.Close()); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close child ends of output pipes: %w", err))
		}
	}()

	worker.disarm, err = armOutputDeadline(ctx, func(deadline time.Time) error {
		return errors.Join(requestWrite.SetWriteDeadline(deadline), replyRead.SetReadDeadline(deadline))
	})
	if err != nil {
		return nil, fmt.Errorf("set output protocol deadline: %w", err)
	}
	cmd := exec.CommandContext(processCtx, path, outputWorkerArg)
	cmd.Stdin = requestRead
	cmd.Stdout = destination
	// The worker has no reason to write to the exporter's other stream.
	// Use the already-open request pipe as a write-inaccessible stderr so
	// launching a worker does not even require /dev/null.
	cmd.Stderr = requestRead
	cmd.ExtraFiles = []*os.File{replyWrite, lifeRead}
	if err := cmd.Start(); err != nil {
		return nil, outputCancellation(ctx, fmt.Errorf("start output worker: %w", err))
	}
	worker.cmd = cmd
	return worker, nil
}

func (worker *outputWorker) close() error {
	defer worker.cancel()
	var interruptErr error
	if worker.disarm != nil {
		interruptErr = worker.disarm()
	}
	// EOF on the request pipe shuts down an idle worker. On failure the
	// worker may still be in a destination write, so signal it through
	// CommandContext before waiting.
	requestErr := worker.request.Close()
	if worker.failed || requestErr != nil {
		worker.cancel()
	}
	var waitErr error
	if worker.cmd != nil {
		waitErr = worker.cmd.Wait()
		var exitErr *exec.ExitError
		if worker.ctx.Err() != nil && errors.As(waitErr, &exitErr) {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signal() == syscall.SIGKILL {
				waitErr = nil // CommandContext killed the worker during failure cleanup.
			}
		}
	}
	if err := errors.Join(interruptErr, requestErr, waitErr, worker.reply.Close(), worker.lifeline.Close()); err != nil {
		return fmt.Errorf("close output worker: %w", err)
	}
	return nil
}
