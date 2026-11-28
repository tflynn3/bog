package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	tailLimit         = 64 << 20
	checkpointTrigger = 12 << 20
	captureMetadata   = 24 << 20
	manifestName      = "bog.checkpoint"
)

type retainedRuntime struct {
	views     int
	viewsDone chan struct{}
	viewBytes uint64
	tailPins  map[tailVersion]tailMetadata
	wal       *wal.Log
	root      *os.Root
	dir       *os.File
	publisher *checkpoint.Publisher
	catalog   *history.Catalog
	budget    *checkpoint.Budget

	// Capture and selection share this gate. The WAL takes its own write
	// lock for the final selection, leaving appends free during staging.
	mu             sync.Mutex
	generation     history.Generation
	manifest       string
	captures       map[string]*retainedView
	nextCapture    uint64
	retired        []retiredGeneration
	reclaimed      retiredGeneration
	compacting     uint64
	stopped        atomic.Bool
	maintenanceErr error
	publicationLag uint64
	fatal          chan error
	jobs           chan uint64
	wake           chan struct{}
	cancel         context.CancelFunc
	done           chan struct{}
}

type retiredGeneration struct {
	history.Generation
	manifest    string
	sequence    uint64
	replacement uint64
	bytes       uint64
}

func openRuntime(ctx context.Context, path string) (_ *retainedRuntime, err error) {
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      uint64(checkpoint.MaxSegments+1) * checkpoint.MaxBlockSize,
		checkpoint.ReplayTailBytes:   tailLimit,
		checkpoint.PinnedGenerations: 6,
		checkpoint.LiveCaptures:      80,
	})
	r := &retainedRuntime{
		budget: budget, catalog: history.NewCatalog(budget), captures: make(map[string]*retainedView),
		tailPins: make(map[tailVersion]tailMetadata),
		fatal:    make(chan error, 1), jobs: make(chan uint64, 1), wake: make(chan struct{}, 1), done: make(chan struct{}), viewsDone: make(chan struct{}),
	}
	r.wal, err = wal.OpenOwner(path, func(w *wal.Log, root *os.Root, dir *os.File) error {
		r.wal, r.root, r.dir = w, root, dir
		r.publisher = checkpoint.NewPublisher(root, dir, budget)
		if err := r.recover(ctx); err != nil {
			return errors.Join(err, r.catalog.Close())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	maintenance, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go r.maintain(maintenance)
	return r, nil
}

func (r *retainedRuntime) LastCommitOutcome() wal.CommitOutcome { return r.wal.LastCommitOutcome() }

func (r *retainedRuntime) Append(payload []byte, rows []store.Row) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1400*time.Millisecond)
	defer cancel()
	err := r.wal.AppendWaiting(ctx, payload, rows)
	r.notify()
	return err
}

func (r *retainedRuntime) notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *retainedRuntime) stop() {
	r.stopped.Store(true)
	r.cancel()
}

func (r *retainedRuntime) close(ctx context.Context) error {
	r.stop()
	select {
	case <-r.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-r.fatal:
		return err
	default:
	}
	r.mu.Lock()
	views := r.views
	r.mu.Unlock()
	if views > 0 {
		select {
		case <-r.viewsDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		err := errors.Join(r.reclaim(context.Background()), r.catalog.Close())
		if err == nil && ctx.Err() == nil {
			err = r.wal.Close()
		}
		done <- errors.Join(err, ctx.Err())
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
