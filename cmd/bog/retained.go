package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	advanceBytes      = 16 << 20
	maxTailBytes      = 48 << 20
	canonicalManifest = "bog.checkpoint"
)

// retained serializes visibility with durable appends and generation selection.
// Only the worker stages and reclaims. Queries hold immutable views of the
// epoch and batch backing, so disk I/O runs without the owner mutex.
type retained struct {
	mu        sync.Mutex
	owner     *wal.Owner
	budget    *checkpoint.Budget
	publisher *checkpoint.Publisher
	catalog   *history.Catalog

	current      *epoch
	retired      []*epoch
	rows         *store.Store
	tailSnapshot *tailSnapshot
	batches      []checkpoint.ReplayBatch
	ordinal      uint64

	captures        map[string]*queryView
	nextCapture     uint64
	closing, fenced bool
	stop            chan struct{}
	wake            chan struct{}
	done            chan struct{}
	job             *advance
	failed          error
	drainErr        error
}

type epoch struct {
	tail     *wal.Tail
	name     string
	id       history.GenerationID
	manifest *checkpoint.Manifest
	covered  int64
	sequence uint64
	first    uint64
	pins     int
	overlap  *checkpoint.Reservation
	retired  bool
}

type advance struct {
	cutoff  int64
	ordinal uint64
	view    recoveryView
	done    chan struct{}
}

// recoveryView is independently reusable while the owner holds its epoch.
// It never exposes bytes outside the captured committed suffix.
type recoveryView struct {
	owner *wal.Owner
	epoch *epoch
	point checkpoint.RecoveryPoint
}

func (v *recoveryView) Point() checkpoint.RecoveryPoint { return v.point }
func (v *recoveryView) OpenManifest() (io.ReadCloser, error) {
	return v.owner.OpenFile(v.epoch.name)
}
func (v *recoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	for _, s := range v.epoch.manifest.Segments {
		if s.Name == name {
			return v.owner.OpenFile(name)
		}
	}
	return nil, os.ErrNotExist
}
func (v *recoveryView) OpenSuffix() (io.ReadCloser, error) {
	return io.NopCloser(io.NewSectionReader(v.epoch.tail, v.point.CoveredOffset, v.point.CommittedEnd-v.point.CoveredOffset)), nil
}

func (r *retained) viewLocked() recoveryView {
	e := r.current
	return recoveryView{owner: r.owner, epoch: e, point: checkpoint.RecoveryPoint{
		Generation: e.id, CoveredOffset: e.covered, NextOrdinal: e.first,
		CommittedEnd: e.tail.End(), CommittedOrdinal: r.ordinal,
	}}
}

func (r *retained) LastCommitOutcome() wal.CommitOutcome { return r.owner.LastCommitOutcome() }

func (r *retained) Append(payload []byte, rows []store.Row) error {
	chargeBytes := uint64(wal.FrameHeaderSize + len(payload))
	for {
		r.mu.Lock()
		if r.fenced {
			r.mu.Unlock()
			return os.ErrClosed
		}
		e := r.current
		size := uint64(e.tail.End() - e.covered)
		// Let an admitted advance finish if the active suffix has outrun
		// staging. A failed attempt falls through to normal WAL admission.
		if r.job != nil && size+chargeBytes > maxTailBytes {
			done := r.job.done
			r.mu.Unlock()
			<-done
			continue
		}
		defer r.mu.Unlock()
		if uint64(len(rows)) > math.MaxUint64-r.ordinal {
			return errors.New("append ordinal overflow")
		}
		charge, err := r.budget.Reserve(context.Background(), checkpoint.ReplayTailBytes, chargeBytes)
		if err != nil {
			// A prior clean staging failure may have been due to a full
			// filesystem. An append attempt can prompt another advance.
			r.notify()
			return err
		}
		start, next := e.tail.End(), r.ordinal+uint64(len(rows))
		if err := r.owner.Append(e.tail, payload, next); err != nil {
			charge.Release()
			if errors.Is(err, wal.ErrUncertain) {
				r.fenced = true
			}
			return err
		}
		r.rows.Publish(rows)
		r.tailSnapshot = nil
		r.batches = append(r.batches, checkpoint.ReplayBatch{Start: start, End: e.tail.End(), FirstOrdinal: r.ordinal, Rows: rows, Reservation: charge})
		r.ordinal = next
		r.notify()
		return nil
	}
}

func (r *retained) notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *retained) startLocked(cutoff uint64) error {
	if r.closing || r.fenced || r.job != nil {
		return errors.New("compaction unavailable")
	}
	end := r.current.covered
	for _, batch := range r.batches {
		if batch.FirstOrdinal+uint64(len(batch.Rows)) == cutoff {
			end = batch.End
			break
		}
	}
	if cutoff <= r.current.first || end == r.current.covered {
		return errors.New("cutoff must advance to a committed batch boundary")
	}
	r.failed = nil
	r.job = &advance{end, cutoff, r.viewLocked(), make(chan struct{})}
	r.notify()
	return nil
}

func (r *retained) worker() {
	defer close(r.done)
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	for {
		select {
		case <-r.stop:
			r.drainErr = r.reclaim(context.Background())
			return
		case <-r.wake:
		case <-retry.C:
			r.mu.Lock()
			r.failed = nil
			r.mu.Unlock()
		}
		r.mu.Lock()
		if r.job == nil && !r.closing && !r.fenced && r.failed == nil &&
			r.current.tail.End()-r.current.covered >= advanceBytes {
			if err := r.startLocked(r.ordinal); err != nil {
				log.Printf("schedule checkpoint: %v", err)
			}
		}
		job := r.job
		r.mu.Unlock()
		if job != nil {
			err := r.compact(job)
			if err != nil {
				log.Printf("advance checkpoint to row %d: %v", job.ordinal, err)
			}
			r.mu.Lock()
			r.failed, r.job = err, nil
			close(job.done)
			r.mu.Unlock()
		}
		if err := r.reclaim(context.Background()); err != nil {
			log.Printf("reclaim retained history: %v", err)
		}
	}
}

func (r *retained) stopAdmission() {
	r.mu.Lock()
	r.closing = true
	r.notify()
	r.mu.Unlock()
}

func (r *retained) drain(ctx context.Context) error {
	r.stopAdmission()
	r.mu.Lock()
	job := r.job
	r.mu.Unlock()
	if job != nil {
		select {
		case <-job.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(r.stop)
	select {
	case <-r.done:
		return r.drainErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *retained) close() error {
	var err error
	if r.current != nil {
		err = r.current.tail.Close()
	}
	for _, old := range r.retired {
		err = errors.Join(err, old.tail.Close())
		if old.overlap != nil {
			old.overlap.Release()
		}
	}
	for _, batch := range r.batches {
		batch.Reservation.Release()
	}
	return errors.Join(err, r.catalog.Close())
}

func digestRange(input io.ReaderAt, start, end int64) ([32]byte, error) {
	h := sha256.New()
	_, err := io.CopyN(h, io.NewSectionReader(input, start, end-start), end-start)
	return [32]byte(h.Sum(nil)), err
}

func (r *retained) readManifest(name string) (*checkpoint.Manifest, history.GenerationID, []byte, error) {
	f, err := r.owner.OpenFile(name)
	if err != nil {
		return nil, history.GenerationID{}, nil, err
	}
	const maxManifest = 256 << 10
	raw, err := io.ReadAll(io.LimitReader(f, maxManifest+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return nil, history.GenerationID{}, nil, err
	}
	m, id, err := checkpoint.DecodeManifest(raw)
	return m, id, raw, err
}

func (r *retained) installReplay(replay *checkpoint.ReplayedTail) {
	r.batches, r.ordinal = replay.Batches, replay.NextOrdinal
	r.rebuildTail()
}

func (r *retained) rebuildTail() {
	b := store.NewRecoveryBuilder()
	for _, batch := range r.batches {
		b.Add(batch.Rows)
	}
	r.tailSnapshot = nil
	r.rows = new(store.Store)
	b.Install(r.rows)
}

func (r *retained) installCurrent(ctx context.Context, e *epoch) (err error) {
	ticket, err := r.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	lock, err := checkpoint.LockPublication(r.owner.Directory(), true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	prepared, err := ticket.PrepareSelected(ctx, r.owner.Directory(), e.id)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, prepared.Close()) }()
	if _, err := prepared.Activate(); err != nil {
		return fmt.Errorf("activate generation %x: %w", e.id, err)
	}
	return nil
}
