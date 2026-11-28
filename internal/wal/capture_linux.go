package wal

import (
	"cmp"
	"context"
	"slices"
	"sort"
	"time"

	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"bog/internal/store"
)

// FileIdentity identifies a pinned WAL inode. Compare identities while their
// captures are live. The filesystem can reuse an inode after its last pin.
type FileIdentity struct {
	device uint64
	inode  uint64
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
	base     int64
	drained  chan struct{}
	mu       sync.Mutex
	refs     int
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
	return &captureFile{f: f, identity: fileIdentity(readable), refs: 1, drained: make(chan struct{})}, nil
}

func fileIdentity(info os.FileInfo) FileIdentity {
	st := info.Sys().(*syscall.Stat_t)
	return FileIdentity{uint64(st.Dev), st.Ino}
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
	frames   []Frame
	bounds   frameBounds
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
	if l.captures == nil {
		l.mu.Unlock()
		return nil, os.ErrClosed
	}
	c := l.captureLocked()
	l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, c.Release())
	}
	return c, nil
}

// WithCapture pairs a committed file and its bounds with caller-owned state
// under the same mutex used for append publication and WAL replacement.
// The callback owns the capture even when it returns an error.
func (l *Log) WithCapture(ctx context.Context, take func(*Capture, Authority) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := l.admissionError(); err != nil {
		return err
	}
	return take(l.captureLocked(), l.authority)
}

func (l *Log) captureLocked() *Capture {
	f := l.captures
	f.mu.Lock()
	f.refs++
	f.mu.Unlock()
	return &Capture{lease: &captureLease{file: f, boundary: l.committed, frames: l.frames, bounds: l.bounds}}
}

// Cutoff locates an exact whole-frame boundary in the captured tail.
func (c *Capture) Cutoff(ordinal uint64) (int64, bool) {
	i, found := slices.BinarySearchFunc(c.lease.frames, ordinal, func(f Frame, n uint64) int {
		return cmp.Compare(f.NextOrdinal, n)
	})
	if !found {
		return 0, false
	}
	return c.lease.frames[i].End, true
}

// Scan visits the captured frames selected by allow, in append order.
func (c *Capture) Scan(start, end time.Time, allow func(Frame) (bool, error), visit func(uint64, store.Row) error) error {
	frames := c.lease.frames
	if !c.lease.bounds.unordered {
		lo := sort.Search(len(frames), func(i int) bool { return !frames[i].MaxTime.Before(start) })
		hi := lo + sort.Search(len(frames)-lo, func(i int) bool { return !frames[lo+i].MinTime.Before(end) })
		frames = frames[lo:hi]
	}
	var header [frameHeader]byte
	var payload []byte
	for _, frame := range frames {
		selected, err := allow(frame)
		if err != nil {
			return err
		}
		if !selected {
			continue
		}
		if err := readFrame(io.NewSectionReader(c, frame.Start, frame.End-frame.Start), frame.Start, frame.End, &header, &payload); err != nil {
			return err
		}
		next := frame.FirstOrdinal
		if err := store.VisitDecoded(payload, func(row store.Row) error {
			err := visit(next, row)
			next++
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// Identity returns the identity of the pinned file.
func (c *Capture) Identity() FileIdentity { return c.lease.file.identity }

// CommittedEnd is the exclusive absolute byte boundary of this capture.
func (c *Capture) CommittedEnd() int64 { return c.lease.boundary.end }

// NextOrdinal is the zero-based append ordinal of the first row after the capture.
func (c *Capture) NextOrdinal() uint64 { return c.lease.boundary.nextOrdinal }

// ReadAt reads at an absolute offset, including the WAL header at offset zero.
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

// ScanBounds observes the current index bounds without taking a file pin.
func (l *Log) ScanBounds() (frameBytes, lineBytes int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bounds.frameBytes, l.bounds.lineBytes
}

func (c *Capture) Ordered() bool { return !c.lease.bounds.unordered }
