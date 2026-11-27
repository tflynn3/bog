package main

import (
	"errors"
	"os"
	"syscall"
)

// An inherited blocking pipe is usually outside Go's poller. On Linux, open
// the same pipe with independent flags so the exporter can set a deadline
// without changing the blocking mode of descriptors shared with the caller.
func openStdoutPipe(out *os.File) (*os.File, error) {
	info, err := out.Stat()
	if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		return nil, err
	}
	pipe, err := os.OpenFile("/proc/self/fd/1", os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.ENXIO) {
		// procfs may be unavailable, or the pipe's reader may already have
		// closed. In either case the original stdout is still the output.
		return nil, nil
	}
	return pipe, err
}
