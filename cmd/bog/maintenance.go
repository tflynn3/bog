package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

type compactionJob struct {
	cutoff uint64
	delay  time.Duration
}

func (s *retained) Append(payload []byte, rows []store.Row) error {
	return s.AppendContext(context.Background(), payload, rows)
}

func (s *retained) AppendContext(ctx context.Context, payload []byte, rows []store.Row) error {
	deadline := time.Now().Add(time.Second)
	var retry *time.Timer
	defer func() {
		if retry != nil {
			retry.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.progressMu.Lock()
		progress := s.progress
		s.progressMu.Unlock()
		err := s.wal.Append(payload, rows)
		if err == nil {
			s.signalMaintenance()
			return nil
		}
		if !errors.Is(err, wal.ErrCapacity) || !s.auto || s.stopping.Load() {
			return err
		}
		// Capacity refusal precedes all WAL writes. Let an admitted batch
		// wait briefly for checkpoint selection to free tail capacity.
		s.signalMaintenance()
		if retry == nil {
			retry = time.NewTimer(time.Until(deadline))
		}
		select {
		case <-progress:
			if !time.Now().Before(deadline) {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return err
		case <-retry.C:
			return err
		}
	}
}

func (s *retained) LastCommitOutcome() wal.CommitOutcome { return s.wal.LastCommitOutcome() }

func (s *retained) signalMaintenance() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *retained) startMaintenance(manual bool) {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.auto = !manual
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
			case <-ticker.C:
			}
			if ctx.Err() != nil {
				return
			}
			if err := s.reclaim(ctx); err != nil && ctx.Err() == nil {
				log.Printf("reclaim retained history: %v", err)
			}
			job := s.nextCompaction(ctx, manual)
			if job.cutoff == 0 {
				continue
			}
			err := s.compact(ctx, job)
			s.mu.Lock()
			if s.requested == job.cutoff {
				s.running = false
				s.failed = err
			}
			s.mu.Unlock()
			if err == nil {
				s.progressMu.Lock()
				close(s.progress)
				s.progress = make(chan struct{})
				s.progressMu.Unlock()
				s.signalMaintenance()
				continue
			}
			if errors.Is(err, wal.ErrUncertain) ||
				errors.Is(err, checkpoint.ErrPublicationUncertain) && ctx.Err() == nil {
				s.wal.Fence(err)
				s.fatal <- fmt.Errorf("retained authority requires recovery: %w", err)
				return
			}
			if ctx.Err() == nil {
				log.Printf("advance retained history: %v", err)
			}
		}
	}()
}

func (s *retained) nextCompaction(ctx context.Context, manual bool) compactionJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping.Load() {
		return compactionJob{}
	}
	if s.pending.cutoff != 0 {
		job := s.pending
		s.pending = compactionJob{}
		return job
	}
	if manual {
		return compactionJob{}
	}
	c, err := s.wal.Capture(ctx)
	if err != nil {
		return compactionJob{}
	}
	size, cutoff, frames := c.CommittedEnd()-s.current.Boundary.CoveredOffset, c.NextOrdinal(), c.FrameCount()
	if err := c.Release(); err != nil {
		log.Printf("release WAL maintenance observation: %v", err)
	}
	if size < checkpointBytes && frames < checkpointFrames {
		return compactionJob{}
	}
	s.running, s.requested = true, cutoff
	return compactionJob{cutoff: cutoff}
}

func (s *retained) compact(ctx context.Context, job compactionJob) (err error) {
	s.mu.Lock()
	if len(s.retired) >= maxRetired-1 {
		s.mu.Unlock()
		return checkpoint.ErrExhausted
	}
	view, err := s.pinLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, view.close()) }()
	cutoff, ok := view.tail.Cutoff(job.cutoff)
	if !ok || job.cutoff <= view.point.NextOrdinal {
		return errors.New("compaction cutoff is outside committed frame boundaries")
	}
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	if view.name == ordinaryManifest {
		view.point.SuffixSHA256, err = rangeDigest(view.tail, view.point.CoveredOffset, view.point.CommittedEnd)
		if err != nil {
			return err
		}
	}
	// Explicit observation schedules small replacements in a bounded window
	// so a status poll and overlapping ingest can observe admitted work.
	selectAt := time.Now().Add(job.delay)
	var tail wal.RetiredTail
	var selected checkpoint.RecoveryPoint
	var closeErr error
	var replacement *wal.Replacement
	locked := false
	defer func() {
		if replacement != nil {
			replacement.Close()
		}
		if locked {
			s.mu.Unlock()
		}
	}()
	published, err := s.publisher.PublishRecoveryView(ctx, &view.recoveryView, cutoff, job.cutoff,
		func(ctx context.Context, point checkpoint.RecoveryPoint) error {
			if remaining := time.Until(selectAt); remaining > 0 {
				timer := time.NewTimer(remaining)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
			}
			s.mu.Lock()
			locked = true
			a := wal.Authority{
				Sequence: s.ordinal + 1, Generation: hex.EncodeToString(point.Generation[:]), CoveredOffset: point.CoveredOffset,
				NextOrdinal: point.NextOrdinal, CommittedEnd: point.CommittedEnd, CommittedOrdinal: point.CommittedOrdinal,
				SuffixSHA256: hex.EncodeToString(point.SuffixSHA256[:]),
			}
			predecessor := ""
			if view.name != ordinaryManifest {
				predecessor = hex.EncodeToString(view.point.Generation[:])
			}
			var err error
			replacement = s.wal.BeginReplacement()
			tail, err = replacement.Select(ctx, a, predecessor)
			if tail.Drained != nil {
				// A populated retired tail means the selection is durable.
				selected, closeErr = point, err
				return nil
			}
			if err == nil {
				return errors.New("replacement did not return the predecessor")
			}
			return err
		})
	if published == nil {
		if locked && (ctx.Err() == nil || errors.Is(err, wal.ErrUncertain)) {
			s.wal.Fence(err)
		}
		return err
	}
	old := retiredGeneration{s.current, s.manifest, tail, s.manifest == ordinaryManifest}
	// A durable selector must finish activation even on shutdown.
	activateErr := s.install(context.WithoutCancel(ctx), ticket, selected.Generation, false)
	if activateErr != nil {
		s.wal.Fence(activateErr)
		return errors.Join(wal.ErrUncertain, err, activateErr, closeErr)
	}
	s.retired = append(s.retired, old)
	s.ordinal++
	s.running, s.failed = false, nil
	return errors.Join(err, closeErr)
}

func drained(ch <-chan struct{}) bool {
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (s *retained) reclaim(ctx context.Context) error {
	// Only the maintenance worker mutates retired until it drains, after
	// which shutdown performs the final pass. Status reads under mu.
	s.mu.Lock()
	current, name := s.current.ID, s.manifest
	retired := s.retired
	s.mu.Unlock()
	if len(retired) == 0 || name == ordinaryManifest || ctx.Err() != nil {
		return ctx.Err()
	}
	if err := s.catalog.ReclaimSelected(ctx, s.dir, current); err != nil {
		return err
	}
	for len(retired) > 0 {
		old := retired[0]
		if !drained(old.generation.Drained) || !drained(old.tail.Drained) {
			break
		}
		if err := s.wal.RemoveRetired(old.tail); err != nil {
			return err
		}
		if old.ordinary {
			if err := s.removeRuntimeFile("bog.commit"); err != nil {
				return err
			}
		}
		if old.manifest != "" && old.manifest != name {
			if err := s.removeRuntimeFile(old.manifest); err != nil {
				return err
			}
		}
		if err := s.dir.Sync(); err != nil {
			return err
		}
		retired = retired[1:]
		s.mu.Lock()
		s.retired = retired
		s.mu.Unlock()
	}
	return nil
}

func (s *retained) removeRuntimeFile(name string) error {
	err := s.root.Remove(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *retained) stopMaintenance() {
	s.stopping.Store(true)
	if s.cancel != nil {
		s.cancel()
	}
}

// close waits for maintenance before releasing the owner lock. A timed-out
// caller retains the storage and owner handles until process exit.
func (s *retained) close(ctx context.Context) error {
	if s.cancel != nil {
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	active := len(s.views)
	s.mu.Unlock()
	if active != 0 {
		return fmt.Errorf("%d retained captures still active", active)
	}
	err := s.reclaim(ctx)
	if errors.Is(err, checkpoint.ErrPublicationUncertain) || errors.Is(err, syscall.EWOULDBLOCK) {
		// Keep interrupted staging and obsolete files for locked recovery.
		err = nil
	}
	if err := errors.Join(err, ctx.Err(), s.catalog.Close()); err != nil {
		return err
	}
	return s.wal.Close()
}
