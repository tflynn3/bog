package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
)

// FileIdentity identifies a pinned WAL inode. Compare identities while their
// captures are live. The filesystem can reuse an inode after its last pin.
type FileIdentity struct {
	Device uint64
	Inode  uint64
}

type captureBoundary struct {
	end         int64
	nextOrdinal uint64
}

// captureFile owns one read-only descriptor shared by the Log, its captures,
// and their in-flight reads. Its mutex protects reference counts and releases.
type captureFile struct {
	f        *os.File
	identity FileIdentity
	mu       sync.Mutex
	refs     int
	base     int64
	name     string
	drained  chan struct{}
}

func openCaptureFile(dir, writer *os.File, name string) (*captureFile, error) {
	written, err := writer.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat WAL writer: %w", err)
	}
	f, err := openRegular(dir, name, os.O_RDONLY)
	if err != nil {
		return nil, fmt.Errorf("open WAL capture reader: %w", err)
	}
	readable, err := f.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("stat WAL capture reader: %w", err), f.Close())
	}
	if !os.SameFile(written, readable) {
		return nil, errors.Join(errors.New("WAL identity changed during open"), f.Close())
	}
	st := readable.Sys().(*syscall.Stat_t) // checked by openRegular on this file
	return &captureFile{f: f, identity: FileIdentity{uint64(st.Dev), st.Ino}, refs: 1, name: name, drained: make(chan struct{})}, nil
}

func (f *captureFile) release() error {
	f.mu.Lock()
	return f.releaseLocked()
}

// releaseLocked drops a reference and unlocks before closing the descriptor.
func (f *captureFile) releaseLocked() error {
	f.refs--
	last := f.refs == 0
	f.mu.Unlock()
	if last {
		err := f.f.Close()
		close(f.drained)
		return err
	}
	return nil
}

// Capture pins one published WAL prefix. A capture remains readable after the
// Log closes, until the caller releases it. Copies share the same release.
type Capture struct{ lease *captureLease }

type captureLease struct {
	file     *captureFile
	boundary captureBoundary
	released bool // protected by file.mu
}

// Capture selects the latest fully published group without waiting for WAL or
// store I/O. Once acquired, the caller owns the pin and must call Release.
func (l *Log) Capture(ctx context.Context) (*Capture, error) {
	l.mu.Lock()
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	if err := l.admissionError(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	f := l.captures
	if f == nil {
		l.mu.Unlock()
		return nil, os.ErrClosed
	}
	f.mu.Lock()
	f.refs++
	f.mu.Unlock()
	c := &Capture{lease: &captureLease{file: f, boundary: l.committed}}
	l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, c.Release())
	}
	return c, nil
}

// Identity returns the identity of the pinned file.
func (c *Capture) Identity() FileIdentity { return c.lease.file.identity }

// CommittedEnd is the exclusive absolute byte boundary of this capture.
func (c *Capture) CommittedEnd() int64 { return c.lease.boundary.end }

// NextOrdinal is the zero-based append ordinal of the first row after the capture.
func (c *Capture) NextOrdinal() uint64 { return c.lease.boundary.nextOrdinal }

// ReadAt reads at a logical WAL offset. A rebased capture begins at its tail base.
// Reads that cross CommittedEnd return the captured bytes and io.EOF.
func (c *Capture) ReadAt(p []byte, off int64) (n int, err error) {
	f := c.lease.file
	f.mu.Lock()
	if c.lease.released {
		f.mu.Unlock()
		return 0, os.ErrClosed
	}
	if off < f.base {
		f.mu.Unlock()
		return 0, fmt.Errorf("WAL capture offset %d: %w", off, os.ErrInvalid)
	}
	end := c.CommittedEnd()
	if off > end || off == end && len(p) != 0 {
		f.mu.Unlock()
		return 0, io.EOF
	}
	if len(p) == 0 {
		f.mu.Unlock()
		return 0, nil
	}
	f.refs++
	f.mu.Unlock()
	defer func() {
		if closeErr := f.release(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if remaining := end - off; int64(len(p)) > remaining {
		n, err := f.f.ReadAt(p[:remaining], off-f.base)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return f.f.ReadAt(p, off-f.base)
}

// Release surrenders the pin. Reads already admitted can finish.
func (c *Capture) Release() error {
	f := c.lease.file
	f.mu.Lock()
	if c.lease.released {
		f.mu.Unlock()
		return nil
	}
	c.lease.released = true
	return f.releaseLocked()
}
