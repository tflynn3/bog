//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"bog/internal/backup"
	"bog/internal/checkpoint"
)

// Each operation borrows a directory pin and gives the consumer independent
// file descriptors. Keep the instrumentation local to the view so concurrent
// opens, including multiple opens of a single artifact, can be checked.
type retainedReaderView struct {
	root     *os.Root
	point    backup.RecoveryPoint
	manifest string
	skip     int64

	cancelAt int
	cancel   context.CancelFunc
	closeErr error

	mu      sync.Mutex
	readers []*retainedViewFile
}

func (v *retainedReaderView) Point() backup.RecoveryPoint { return v.point }

func (v *retainedReaderView) OpenManifest() (io.ReadCloser, error) {
	return v.open(v.manifest, 0)
}

func (v *retainedReaderView) OpenSegment(name string) (io.ReadCloser, error) {
	return v.open(name, 0)
}

func (v *retainedReaderView) OpenSuffix() (io.ReadCloser, error) {
	return v.open(tailFile, v.skip)
}

type retainedCheckpointView struct{ *retainedReaderView }

func (v retainedCheckpointView) Point() checkpoint.RecoveryPoint {
	return checkpoint.RecoveryPoint(v.point)
}

var (
	_ backup.RecoveryView     = (*retainedReaderView)(nil)
	_ checkpoint.RecoveryView = retainedCheckpointView{}
)

func (v *retainedReaderView) open(name string, skip int64) (io.ReadCloser, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.readers) >= replayMaxFiles {
		return nil, fmt.Errorf("%w: too many recovery-view opens", errReplayLimit)
	}
	f, err := v.root.Open(name)
	if err != nil {
		return nil, err
	}
	if skip != 0 {
		if _, err := f.Seek(skip, io.SeekStart); err != nil {
			return nil, errors.Join(err, f.Close())
		}
	}
	r := &retainedViewFile{file: f, name: name}
	if name == tailFile {
		r.cancelAt, r.cancel, r.closeErr = v.cancelAt, v.cancel, v.closeErr
	}
	v.readers = append(v.readers, r)
	return r, nil
}

type retainedViewFile struct {
	file     *os.File
	name     string
	cancelAt int
	cancel   context.CancelFunc
	closeErr error

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
	limit := replayMaxFileBytes + 1 - r.bytes
	if limit <= 0 {
		return 0, fmt.Errorf("%w: recovery view read %s", errReplayLimit, r.name)
	}
	n, err := r.file.Read(p[:min(len(p), limit)])
	r.bytes += n
	if r.cancel != nil && !r.canceled && r.bytes >= r.cancelAt {
		r.canceled = true
		r.cancel()
	}
	return n, err
}

func (r *retainedViewFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("recovery reader %s was closed twice", r.name)
	}
	r.closed = true
	return errors.Join(r.file.Close(), r.closeErr)
}

func (v *retainedReaderView) checkClosed() (err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	cancellations := 0
	for _, r := range v.readers {
		r.mu.Lock()
		if !r.closed {
			err = errors.Join(err, fmt.Errorf("consumer left %s open", r.name), r.file.Close())
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer func() { cleanupErr = errors.Join(cleanupErr, root.Close()) }()
	pin, err := budget.Reserve(ctx, checkpoint.LiveCaptures, 1)
	if err != nil {
		return nil, err
	}
	defer pin.Release()
	view := &retainedReaderView{root: root, point: p, manifest: manifest, skip: skip}
	if tc.cancel {
		view.cancelAt, view.cancel = cancelAt, cancel
	}
	if tc.closeError {
		view.closeErr = errRetainedReaderClose
	}
	operationErr = run(ctx, view)
	return operationErr, view.checkClosed()
}
