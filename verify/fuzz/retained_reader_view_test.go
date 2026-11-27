//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"bog/internal/backup"
	"bog/internal/checkpoint"
)

// Each operation borrows a directory pin and gives the consumer independent
// file descriptors. Keep the instrumentation local to the view so concurrent
// opens, including multiple opens of a single artifact, can be checked.
type retainedReaderView struct {
	ctx      context.Context
	root     *os.Root
	point    backup.RecoveryPoint
	manifest string
	skip     int64

	cancelAt    int
	cancel      context.CancelFunc
	closeErr    error
	reservation *retainedReaderReservation
	beforeRead  func(context.Context) error

	mu      sync.Mutex
	readers []*retainedViewFile
	sealed  bool
}

func (v *retainedReaderView) Point() backup.RecoveryPoint { return v.point }

func (v *retainedReaderView) OpenManifest() (io.ReadCloser, error) {
	return v.openReader(v.manifest, 0)
}

func (v *retainedReaderView) OpenSegment(name string) (io.ReadCloser, error) {
	return v.openReader(name, 0)
}

func (v *retainedReaderView) OpenSuffix() (io.ReadCloser, error) {
	return v.openReader(tailFile, v.skip)
}

type retainedCheckpointView struct{ *retainedReaderView }

func (v retainedCheckpointView) Point() checkpoint.RecoveryPoint {
	return checkpoint.RecoveryPoint(v.point)
}

var (
	_ backup.RecoveryView     = (*retainedReaderView)(nil)
	_ checkpoint.RecoveryView = retainedCheckpointView{}
)

func (v *retainedReaderView) openReader(name string, skip int64) (io.ReadCloser, error) {
	r, err := v.open(name, skip)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (v *retainedReaderView) open(name string, skip int64) (*retainedViewFile, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.sealed {
		return nil, os.ErrClosed
	}
	if err := v.ctx.Err(); err != nil {
		return nil, err
	}
	if len(v.readers) >= replayMaxFiles {
		return nil, fmt.Errorf("%w: too many recovery-view opens", errReplayLimit)
	}
	f, closeFile, err := openRetainedHandle(v.ctx, v.reservation, retainedWorkerPrivateFDs, func() (*os.File, error) {
		return v.root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	})
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, closeFile())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("non-regular recovery artifact %q: %w", name, os.ErrInvalid), closeFile())
	}
	if skip != 0 {
		if _, err := f.Seek(skip, io.SeekStart); err != nil {
			return nil, errors.Join(err, closeFile())
		}
	}
	r := &retainedViewFile{ctx: v.ctx, file: f, closeFile: closeFile, name: name, beforeRead: v.beforeRead}
	if name == tailFile {
		r.cancelAt, r.cancel, r.closeErr = v.cancelAt, v.cancel, v.closeErr
	}
	v.readers = append(v.readers, r)
	return r, nil
}

type retainedViewFile struct {
	ctx        context.Context
	file       *os.File
	closeFile  func() error
	name       string
	beforeRead func(context.Context) error
	cancelAt   int
	cancel     context.CancelFunc
	closeErr   error

	mu       sync.Mutex
	bytes    int
	canceled bool
	closed   bool
}

func (r *retainedViewFile) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.checkRead(); err != nil {
		return 0, err
	}
	limit := replayMaxFileBytes + 1 - r.bytes
	if limit <= 0 {
		return 0, fmt.Errorf("%w: recovery view read %s", errReplayLimit, r.name)
	}
	n, err := r.file.Read(p[:min(len(p), limit)])
	r.bytes += n
	if readErr := r.ctx.Err(); readErr != nil {
		return n, readErr
	}
	if r.cancel != nil && !r.canceled && r.bytes >= r.cancelAt {
		r.canceled = true
		r.cancel()
	}
	return n, err
}

// Positional WAL replay passes logical offsets through a section reader.
// The section owns range validation and this file owns read cancellation.
func (r *retainedViewFile) ReadAt(p []byte, offset int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkRead(); err != nil {
		return 0, err
	}
	n, err := r.file.ReadAt(p, offset)
	if readErr := r.ctx.Err(); readErr != nil {
		return n, readErr
	}
	return n, err
}

func (r *retainedViewFile) checkRead() error {
	if r.closed {
		return os.ErrClosed
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if r.beforeRead != nil {
		if err := r.beforeRead(r.ctx); err != nil {
			return err
		}
	}
	return r.ctx.Err()
}

func (r *retainedViewFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("recovery reader %s was closed twice", r.name)
	}
	r.closed = true
	return errors.Join(r.closeFile(), r.closeErr)
}

func (v *retainedReaderView) checkClosed() (err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.sealed = true
	cancellations := 0
	for _, r := range v.readers {
		r.mu.Lock()
		if !r.closed {
			err = errors.Join(err, fmt.Errorf("consumer left %s open", r.name), r.closeFile(), r.closeErr)
			r.closed = true
		}
		if r.canceled {
			cancellations++
			if r.bytes != v.cancelAt {
				err = errors.Join(err, fmt.Errorf("consumer read %d suffix bytes at cancellation, want %d", r.bytes, v.cancelAt))
			}
		}
		r.mu.Unlock()
	}
	if v.cancelAt != 0 && cancellations != 1 {
		err = errors.Join(err, fmt.Errorf("cancellation occurred on %d readers, want one", cancellations))
	}
	return err
}

func runWithRetainedView(ctx context.Context, path, manifest string, p backup.RecoveryPoint, skip int64,
	budget *checkpoint.Budget, tc retainedReaderCase, cancelAt int,
	run func(context.Context, *retainedReaderView) error) (operationErr, cleanupErr error) {
	return runRetainedReaderView(ctx, path, manifest, p, skip, budget,
		retainedViewOptions{fault: tc, cancelAt: cancelAt}, run)
}

type retainedViewOptions struct {
	fault       retainedReaderCase
	cancelAt    int
	reservation *retainedReaderReservation
	beforeRead  func(context.Context) error
}

func runRetainedReaderView(ctx context.Context, path, manifest string, p backup.RecoveryPoint, skip int64,
	budget *checkpoint.Budget, options retainedViewOptions,
	run func(context.Context, *retainedReaderView) error) (operationErr, cleanupErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pin, err := budget.Reserve(ctx, checkpoint.LiveCaptures, 1)
	if err != nil {
		return nil, err
	}
	defer pin.Release()
	root, closeRoot, err := openRetainedHandle(ctx, options.reservation, retainedWorkerPrivateFDs, func() (*os.Root, error) {
		return os.OpenRoot(path)
	})
	if err != nil {
		return nil, err
	}
	defer func() { cleanupErr = errors.Join(cleanupErr, closeRoot()) }()
	view := &retainedReaderView{ctx: ctx, root: root, point: p, manifest: manifest, skip: skip,
		reservation: options.reservation, beforeRead: options.beforeRead}
	if options.fault.cancel {
		view.cancelAt, view.cancel = options.cancelAt, cancel
	}
	if options.fault.closeError {
		view.closeErr = errRetainedReaderClose
	}
	defer func() { cleanupErr = errors.Join(cleanupErr, view.checkClosed()) }()
	return run(ctx, view), nil
}
