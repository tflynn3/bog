//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestRetainedReaderBudgetArithmeticAndCancellation(t *testing.T) {
	limit := retainedResources{math.MaxUint64, 2, 2}
	b := newRetainedReaderBudget(limit)
	ctx := t.Context()
	for resource := retainedMemory; resource < retainedResourceCount; resource++ {
		need := retainedResources{1, 1, 1}
		need[resource] = 3
		r, err := b.reserve(ctx, need, true)
		if r != nil {
			t.Fatal("oversized request was admitted")
		}
		requireRetainedExhaustion(t, err, resource, limit[resource], 0, need[resource])
	}
	need := retainedResources{math.MaxUint64 - 1, 1, 1}
	r, err := b.reserve(ctx, need, true)
	requireRetainedOK(t, err)
	defer func() { requireRetainedOK(t, r.release()) }()

	overflow := retainedResources{2, 1, 1}
	other, err := b.reserve(ctx, overflow, false)
	if other != nil {
		t.Fatal("overflowing request was admitted")
	}
	requireRetainedExhaustion(t, err, retainedBytes, limit[retainedBytes], need[retainedBytes], overflow[retainedBytes])

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	other, err = b.reserve(canceled, limit, true)
	if other != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled reserve = %v, %v", other, err)
	}
	requireRetainedOK(t, r.release())
	requireRetainedOK(t, r.release())
	assertRetainedWorkerBaseline(t, t.TempDir(), b, nil)
}

func TestRetainedReaderBudgetCanceledHeadUnblocksNext(t *testing.T) {
	g := newRetainedWorkerGroup(t)
	b := newRetainedReaderBudget(retainedResources{2, 2, 2})
	held, err := b.reserve(g.ctx, retainedResources{1, 1, 1}, false)
	requireRetainedOK(t, err)
	defer func() { requireRetainedOK(t, held.release()) }()

	headCtx, cancelHead := context.WithCancel(g.ctx)
	defer cancelHead()
	head := g.start(func() error {
		r, err := b.reserve(headCtx, retainedResources{2, 2, 2}, true)
		if r != nil {
			return errors.Join(errors.New("head admitted while capacity was held"), r.release())
		}
		return err
	})
	awaitRetainedQueue(t, g.ctx, b, 1)
	tail := g.start(func() error {
		r, err := b.reserve(g.ctx, retainedResources{1, 1, 1}, true)
		if err != nil {
			return err
		}
		return r.release()
	})
	usage := awaitRetainedQueue(t, g.ctx, b, 2)
	if usage.inUse != held.charge {
		t.Fatalf("waiters charged resources before admission: %+v", usage)
	}
	cancelHead()
	requireRetainedOK(t, checkRetainedReaderRejection(awaitRetainedResult(t, g.ctx, head), context.Canceled))
	requireRetainedOK(t, awaitRetainedResult(t, g.ctx, tail))
	requireRetainedOK(t, held.release())
	assertRetainedWorkerBaseline(t, g.parent, b, nil)
}

func TestRetainedReaderBudgetOpens(t *testing.T) {
	g := newRetainedWorkerGroup(t)
	b := newRetainedReaderBudget(retainedResources{1, 1, 1})
	r, err := b.reserve(g.ctx, b.limit, false)
	requireRetainedOK(t, err)
	defer func() { requireRetainedOK(t, r.release()) }()

	path := filepath.Join(g.parent, "file")
	_, closeFile, err := openRetainedHandle(g.ctx, r, 0, func() (*os.File, error) {
		return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	})
	requireRetainedOK(t, err)
	defer func() { requireRetainedOK(t, closeFile()) }()
	if err := r.release(); err == nil {
		t.Fatal("released reservation with an open file")
	}
	called := false
	extra, closeExtra, err := openRetainedHandle(g.ctx, r, 0, func() (*os.File, error) {
		called = true
		return os.Open(path)
	})
	if called || extra != nil || closeExtra != nil {
		t.Fatal("exhausted open reached the file opener")
	}
	requireRetainedExhaustion(t, err, retainedDescriptors, 1, 1, 1)
	requireRetainedOK(t, closeFile())

	ctx, cancel := context.WithCancel(g.ctx)
	defer cancel()
	var opened *os.File
	f, closeCanceled, err := openRetainedHandle(ctx, r, 0, func() (*os.File, error) {
		f, err := os.Open(path)
		opened = f
		cancel()
		return f, err
	})
	if f != nil || closeCanceled != nil || opened == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation during open returned file %v, original %v, error %v", f, opened, err)
	}
	if _, err := opened.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("partially acquired file remained open: %v", err)
	}
	requireRetainedOK(t, os.Remove(path))
	requireRetainedOK(t, r.release())
	assertRetainedWorkerBaseline(t, g.parent, b, nil)
}
