package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	advanceBytes    = 15 << 20
	maxReplayBytes  = 40 << 20
	maxTailBacking  = 56 << 20
	advanceFrames   = 1536
	maxTailFrames   = 2048
	maxCaptures     = 64
	maxOrdinaryPins = 8
	maxGenerations  = 4
)

type tailBatch struct {
	end    int64
	next   uint64
	rows   []checkpoint.BlockRow
	charge *checkpoint.Reservation
	extra  *checkpoint.Reservation
	refs   int
}

func batchBacking(rows []store.Row) uint64 {
	// Include the sorted row slots and allocator rounding for line strings.
	bytes := uint64(len(rows)) * 64
	for _, row := range rows {
		length := uint64(len(row.Line))
		bytes += length + length/8
	}
	return bytes
}

func makeBatch(end int64, first uint64, rows []store.Row, charge *checkpoint.Reservation) *tailBatch {
	batch := &tailBatch{end: end, next: first + uint64(len(rows)),
		rows: make([]checkpoint.BlockRow, len(rows)), charge: charge, refs: 1}
	for i, row := range rows {
		batch.rows[i] = checkpoint.BlockRow{Row: row, Ordinal: first + uint64(i)}
	}
	slices.SortFunc(batch.rows, compareRows)
	return batch
}

func compareRows(a, b checkpoint.BlockRow) int {
	if c := a.Row.TS.Compare(b.Row.TS); c != 0 {
		return c
	}
	return cmp.Compare(a.Ordinal, b.Ordinal)
}

type epoch struct {
	generation history.Generation
	name       string
	manifest   *checkpoint.Manifest
	tail       *wal.Tail
	batches    []*tailBatch
	pins       int
	sequence   uint64
	replacedBy uint64
	tailBytes  uint64
}

func (e *epoch) releaseBatches() {
	for _, b := range e.batches {
		b.refs--
		if b.refs == 0 {
			b.charge.Release()
			if b.extra != nil {
				b.extra.Release()
			}
		}
	}
	e.batches = nil
}

type retained struct {
	owner     *wal.Owner
	root      *os.Root
	dir       *os.File
	budget    *checkpoint.Budget
	catalog   *history.Catalog
	publisher *checkpoint.Publisher

	// Owner synchronization protects authority, epochs, batches and captures.
	active                                *epoch
	retired                               []*epoch
	captures                              map[uint64]*pageCapture
	nextCapture                           uint64
	job                                   *advanceJob
	lastFailure                           error
	failedCutoff                          uint64
	reclaimed, reclaimedFrom, reclaimedTo uint64
	lag                                   time.Duration

	wake, stop, done chan struct{}
	stopping         atomic.Bool
	stopOnce         sync.Once
}

type advanceJob struct {
	view     recoveryView
	epoch    *epoch
	cutoff   int64
	ordinal  uint64
	done     chan struct{}
	at       time.Time
	selected bool
}

func openRetained(ctx context.Context, path string) (_ *retained, err error) {
	owner, err := wal.OpenOwner(path)
	if err != nil {
		return nil, err
	}
	root, dir := owner.Directory()
	budget := checkpoint.NewBudget(checkpoint.Limits{
		// One online job stages consumed frames and bounded block
		// rewrites with room for a second copy of the carried suffix.
		checkpoint.StagingBytes:      128 << 20,
		checkpoint.ReplayTailBytes:   maxTailBacking,
		checkpoint.PinnedGenerations: maxGenerations,
		checkpoint.LiveCaptures:      maxCaptures + maxOrdinaryPins,
	})
	s := &retained{owner: owner, root: root, dir: dir, budget: budget,
		catalog: history.NewCatalog(budget), publisher: checkpoint.NewPublisher(root, dir, budget),
		active: &epoch{}, captures: make(map[uint64]*pageCapture),
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	defer func() {
		if err != nil {
			s.active.releaseBatches()
			err = errors.Join(err, s.catalog.Close(), owner.Close())
		}
	}()
	if err := owner.WithLock(func(l *wal.Locked) error { return s.recover(ctx, l) }); err != nil {
		return nil, err
	}
	if err := owner.WithLock(func(l *wal.Locked) error {
		p, _ := l.State()
		s.active.sequence = p.Sequence
		return nil
	}); err != nil {
		return nil, err
	}
	go s.maintenance()
	return s, nil
}

func (s *retained) LastCommitOutcome() wal.CommitOutcome { return s.owner.LastCommitOutcome() }

func (s *retained) Append(payload []byte, rows []store.Row) error {
	// The only production caller supplies decoded rows from this same body.
	// HTTP holds its ingest slot across read, decode, and commit.
	return s.append(payload, rows, true)
}

func (s *retained) append(payload []byte, rows []store.Row, mayWait bool) error {
	var wait <-chan struct{}
	err := s.owner.WithLock(func(l *wal.Locked) error {
		p, _ := l.State()
		size := int64(len(payload)) + wal.FrameOverhead
		if p.End-p.Covered+size > maxReplayBytes || len(s.active.batches) >= maxTailFrames {
			s.scheduleAutomatic(p)
			if s.job != nil {
				wait = s.job.done
			}
			return wal.ErrCapacity
		}
		charge, err := s.budget.Reserve(context.Background(), checkpoint.ReplayTailBytes, max(uint64(size), batchBacking(rows)))
		if err != nil {
			if s.job != nil {
				wait = s.job.done
			}
			return errors.Join(wal.ErrCapacity, err)
		}
		batch := makeBatch(p.End+size, p.Next, rows, charge)
		if err := l.Append(payload, len(rows)); err != nil {
			charge.Release()
			return err
		}
		s.active.batches = append(s.active.batches, batch)
		p, _ = l.State()
		s.scheduleAutomatic(p)
		return nil
	})
	if mayWait && wait != nil && errors.Is(err, wal.ErrCapacity) {
		timer := time.NewTimer(1250 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-wait:
			return s.append(payload, rows, false)
		case <-timer.C:
		}
	}
	return err
}

func (s *retained) scheduleAutomatic(p wal.Point) {
	if s.stopping.Load() || s.job != nil || len(s.active.batches) < 2 {
		return
	}
	if p.End-p.Covered < advanceBytes && len(s.active.batches) < advanceFrames {
		return
	}
	// Keep the newest frame in the suffix. Work starts well below the
	// admission limit, with headroom for the writer during staging.
	b := s.active.batches[len(s.active.batches)-2]
	s.schedule(p, b.end, b.next)
}

func (s *retained) schedule(p wal.Point, cutoff int64, ordinal uint64) {
	e := s.active
	e.pins++
	s.lastFailure, s.failedCutoff = nil, 0
	s.job = &advanceJob{view: fileView(s.dir, e.name, e.manifest, p, e.tail), epoch: e,
		cutoff: cutoff, ordinal: ordinal, done: make(chan struct{}), at: time.Now()}
	s.notifyMaintenance()
}

func (s *retained) notifyMaintenance() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *retained) maintenance() {
	defer close(s.done)
	for {
		select {
		case <-s.wake:
			s.maintain()
		case <-s.stop:
			s.maintain()
			return
		}
	}
}

func (s *retained) maintain() {
	var job *advanceJob
	s.owner.Synchronize(func(l *wal.Locked) { job = s.job })
	if job != nil {
		err := s.advance(context.Background(), job)
		s.owner.Synchronize(func(l *wal.Locked) {
			job.epoch.pins--
			s.job = nil
			if err != nil {
				s.lastFailure, s.failedCutoff = err, job.ordinal
			}
			close(job.done)
		})
		if err != nil && !errors.Is(err, checkpoint.ErrExhausted) {
			log.Printf("advance retained checkpoint: %v", err)
		}
	}
	if err := s.reclaim(context.Background()); err != nil {
		log.Printf("reclaim retained history: %v", err)
	}
	if job != nil {
		// If staging fell behind ongoing appends, advance again without
		// waiting for another request. A clean refusal is retried on ingest.
		_ = s.owner.WithLock(func(l *wal.Locked) error {
			if s.lastFailure == nil {
				p, _ := l.State()
				s.scheduleAutomatic(p)
			}
			return nil
		})
	}
}

func (s *retained) advance(ctx context.Context, job *advanceJob) error {
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	// Rebase can temporarily hold a second copy of the complete live suffix.
	staging, err := s.budget.Reserve(ctx, checkpoint.StagingBytes, maxReplayBytes)
	if err != nil {
		return err
	}
	defer staging.Release()
	_, err = s.publisher.PublishPinnedRecoveryView(ctx, job.view, job.cutoff, job.ordinal, s.catalog.CheckCurrent,
		func(ctx context.Context, next checkpoint.RecoveryPoint) (err error) {
			name := checkpoint.GenerationManifestName(next.Generation)
			m, _, err := readManifest(s.dir, name)
			if err != nil {
				return errors.Join(checkpoint.ErrSelectionRefused, err)
			}
			prepared, err := ticket.PrepareSelected(ctx, s.dir, name, history.GenerationID(next.Generation))
			if err != nil {
				return errors.Join(checkpoint.ErrSelectionRefused, err)
			}
			defer func() { err = errors.Join(err, prepared.Close()) }()
			var activationErr error
			err = s.owner.WithLock(func(l *wal.Locked) error {
				p, tail := l.State()
				if s.active != job.epoch || p.Generation != job.view.point.Generation {
					return errors.New("checkpoint predecessor changed")
				}
				// Verify the captured remainder independently, then extend
				// its digest with commits that arrived during publication.
				h := sha256.New()
				n, err := io.Copy(h, tail.Section(job.cutoff, next.CommittedEnd))
				if err != nil || n != next.CommittedEnd-job.cutoff {
					return errors.Join(err, io.ErrUnexpectedEOF)
				}
				var sum [32]byte
				h.Sum(sum[:0])
				if sum != next.SuffixSHA256 {
					return l.Fence(errors.New("staged suffix digest changed"))
				}
				n, err = io.Copy(h, tail.Section(next.CommittedEnd, p.End))
				if err != nil || n != p.End-next.CommittedEnd {
					return errors.Join(err, io.ErrUnexpectedEOF)
				}
				h.Sum(sum[:0])
				i, found := slices.BinarySearchFunc(job.epoch.batches, job.cutoff, func(b *tailBatch, end int64) int {
					return cmp.Compare(b.end, end)
				})
				if !found || job.epoch.batches[i].next != job.ordinal {
					return errors.New("live cutoff is not a whole committed frame")
				}
				successor := wal.Point{Generation: next.Generation, Covered: job.cutoff, End: p.End,
					First: job.ordinal, Next: p.Next}
				if _, err := l.Select(successor, tail.Section(job.cutoff, p.End), sum); err != nil {
					return err
				}
				batches := slices.Clone(job.epoch.batches[i+1:])
				for _, b := range batches {
					b.refs++
				}
				generation, installErr := prepared.Install()
				selected, newTail := l.State()
				seq := selected.Sequence
				s.active = &epoch{generation: generation, name: name, manifest: m, tail: newTail,
					batches: batches, sequence: seq}
				job.epoch.replacedBy, job.epoch.tailBytes = seq, uint64(p.End-p.Covered)
				s.retired = append(s.retired, job.epoch)
				s.lag = time.Since(job.at)
				job.selected = true
				activationErr = installErr
				return nil
			})
			if activationErr != nil {
				log.Printf("release previous generation: %v", activationErr)
			}
			if err != nil && !errors.Is(err, wal.ErrUncertain) {
				err = errors.Join(checkpoint.ErrSelectionRefused, err)
			}
			return err
		})
	if errors.Is(err, checkpoint.ErrPublicationUncertain) {
		s.owner.Fence(err)
	}
	return err
}

func (s *retained) reclaim(ctx context.Context) error {
	if err := s.catalog.ReclaimSelected(ctx, s.dir); err != nil {
		return err
	}
	var result error
	s.owner.Synchronize(func(l *wal.Locked) {
		if l.Err() != nil {
			return
		}
		remaining := s.retired[:0]
		for _, e := range s.retired {
			if e.pins != 0 {
				remaining = append(remaining, e)
				continue
			}
			if e.tail != nil {
				if err := l.Reclaim(e.tail); err != nil {
					result = errors.Join(result, err)
					// Keep the reservation and retry the unlink/sync.
					remaining = append(remaining, e)
					continue
				}
				e.tail = nil
			}
			if e.name != canonicalManifest {
				if err := history.RemoveManifest(s.root, s.dir, e.name, e.generation.ID); err != nil {
					result = errors.Join(result, err)
					remaining = append(remaining, e)
					continue
				}
			}
			e.releaseBatches()
			s.reclaimed = e.tailBytes
			s.reclaimedFrom, s.reclaimedTo = e.sequence, e.replacedBy
		}
		clear(s.retired[len(remaining):])
		s.retired = remaining
	})
	return result
}

func (s *retained) stopScheduling() { s.stopping.Store(true) }

func (s *retained) drain(ctx context.Context) error {
	s.stopScheduling()
	s.stopOnce.Do(func() { close(s.stop) })
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	result := make(chan error, 1)
	go func() {
		if err := s.reclaim(ctx); err != nil {
			result <- err
			return
		}
		var err error
		s.owner.Synchronize(func(l *wal.Locked) {
			if len(s.captures) != 0 {
				err = errors.New("storage captures remain after drain")
				return
			}
			for _, e := range s.retired {
				e.releaseBatches()
				err = errors.Join(err, e.tail.Close())
			}
			s.active.releaseBatches()
		})
		result <- errors.Join(err, s.catalog.Close())
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
