package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	advanceAt = 20 << 20
	tailLimit = 40 << 20
)

const canonicalManifest = "bog.checkpoint"

// storageOwner is the command's synchronization boundary for append
// publication, acquisition and selection. WAL I/O stays with wal.Log.
type storageOwner struct {
	mu         sync.Mutex
	log        *wal.Log
	root       *os.Root
	dir        *os.File
	catalog    *history.Catalog
	publisher  *checkpoint.Publisher
	budget     *checkpoint.Budget
	generation history.Generation
	manifest   *checkpoint.Manifest
	name       string
	tail       *liveTail
	fenced     error
	retired    []retiredGeneration

	wake       chan struct{}
	progress   chan struct{} // Closed and replaced under mu after each attempt.
	advancing  bool
	advanceErr error
	stopped    <-chan struct{}
	cancel     context.CancelFunc
	done       chan struct{}
	fatal      chan error
}

type liveTail struct {
	rows    store.Store
	charges []*checkpoint.Reservation
	refs    int // The owner and all matched views, under storageOwner.mu.
	end     int64
	maxLine int
}

func (t *liveTail) release() {
	t.refs--
	if t.refs == 0 {
		for _, charge := range t.charges {
			charge.Release()
		}
	}
}

func (t *liveTail) publish(rows []store.Row) {
	for _, row := range rows {
		t.maxLine = max(t.maxLine, len(row.Line))
	}
	t.rows.Publish(rows)
}

type matchedView struct {
	owner      *storageOwner
	generation *history.Reader
	wal        *wal.Capture
	tail       *store.PositionedSnapshot
	live       *liveTail
	name       string
	manifest   *checkpoint.Manifest
	point      checkpoint.RecoveryPoint
	maxLine    int
}

func (s *storageOwner) acquire(ctx context.Context) (*matchedView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fenced != nil {
		return nil, s.fenced
	}
	capture, err := s.log.Capture(ctx)
	if err != nil {
		return nil, err
	}
	reader, err := s.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return nil, errors.Join(err, capture.Release())
	}
	gen := reader.Generation()
	view := &matchedView{owner: s, generation: reader, wal: capture, tail: s.tail.rows.CapturePositionedSnapshot(),
		live: s.tail, name: s.name, manifest: s.manifest, maxLine: max(gen.MaxLineBytes, s.tail.maxLine), point: checkpoint.RecoveryPoint{
			Generation: gen.ID, CoveredOffset: gen.Boundary.CoveredOffset, NextOrdinal: gen.Boundary.NextOrdinal,
			CommittedEnd: capture.CommittedEnd(), CommittedOrdinal: capture.NextOrdinal(),
		}}
	if a, ok := s.log.Authority(); ok {
		if a.Generation != view.point.Generation || a.Covered != view.point.CoveredOffset ||
			a.First != view.point.NextOrdinal || a.End != view.point.CommittedEnd ||
			a.Next != view.point.CommittedOrdinal || a.Identity != capture.Identity() {
			return nil, errors.Join(errors.New("history and WAL authority disagree"), reader.Close(), capture.Release())
		}
		view.point.SuffixSHA256 = a.Digest
	}
	s.tail.refs++
	return view, nil
}

func (v *matchedView) close() error {
	err := errors.Join(v.generation.Close(), v.wal.Release())
	v.owner.mu.Lock()
	v.live.release()
	v.owner.mu.Unlock()
	return err
}

func (s *storageOwner) Append(payload []byte, rows []store.Row) error {
	size := int64(len(payload) + 16)
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.fenced == nil && size > tailLimit-(s.tail.end-s.generation.Boundary.CoveredOffset) {
		select {
		case <-s.stopped:
			return os.ErrClosed
		default:
		}
		if !s.advancing {
			s.schedule()
		}
		current := s.progress
		s.mu.Unlock()
		select {
		case <-current:
		case <-s.stopped:
		}
		s.mu.Lock()
		if s.advanceErr != nil {
			return s.advanceErr
		}
	}
	if s.fenced != nil {
		return s.fenced
	}
	charge, err := s.budget.Reserve(context.Background(), checkpoint.ReplayTailBytes, uint64(size))
	if err != nil {
		s.schedule()
		return err
	}
	if err = s.log.Append(payload, rows); err != nil {
		charge.Release()
		if errors.Is(err, wal.ErrUncertain) {
			s.fence(err)
		}
		return err
	}
	s.tail.charges = append(s.tail.charges, charge)
	s.tail.publish(rows)
	s.tail.end += size
	if s.tail.end-s.generation.Boundary.CoveredOffset >= advanceAt {
		s.schedule()
	}
	return nil
}

func (s *storageOwner) LastCommitOutcome() wal.CommitOutcome { return s.log.LastCommitOutcome() }
