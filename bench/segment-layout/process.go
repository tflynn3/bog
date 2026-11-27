package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

const workerFlag = "--segment-layout-worker"

// Capture a bounded diagnostic prefix without backpressuring a child.
type childLog []byte

func (b *childLog) Write(p []byte) (int, error) {
	*b = append(*b, p[:min(len(p), 8192-len(*b))]...)
	return len(p), nil
}

func runWorker(ctx context.Context, args []string, read func(io.Reader) error) (*os.ProcessState, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.WaitDelay = 2 * time.Second
	var stderr childLog
	cmd.Stderr = &stderr
	// Own the pipe so cancellation can unblock reads even with another writer open.
	stdout, output, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = output
	startErr := cmd.Start()
	closeErr := output.Close()
	if startErr != nil {
		return nil, errors.Join(startErr, closeErr, stdout.Close())
	}
	var closeOnce sync.Once
	var readCloseErr error
	closeReader := func() {
		closeOnce.Do(func() { readCloseErr = stdout.Close() })
	}
	interrupt := context.AfterFunc(ctx, closeReader)
	readErr := closeErr
	if readErr == nil {
		readErr = read(stdout)
	}
	interrupt()
	if readErr != nil {
		if killErr := cmd.Process.Kill(); !errors.Is(killErr, os.ErrProcessDone) {
			readErr = errors.Join(readErr, killErr)
		}
	}
	waitErr := cmd.Wait()
	closeReader()
	if err := errors.Join(readErr, waitErr, readCloseErr, ctx.Err()); err != nil {
		return cmd.ProcessState, fmt.Errorf("worker %s: %w %.8192s", args[0], err, string(stderr))
	}
	return cmd.ProcessState, nil
}
