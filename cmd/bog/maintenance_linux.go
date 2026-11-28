package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"slices"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/wal"
)

func (s *retained) scheduleLocked() {
	if s.manual || s.point.CommittedEnd-s.point.CoveredOffset < autoTailBytes &&
		len(s.active.batches) < maxTailBatches/2 && s.active.weight < autoTailBytes {
		return
	}
	if s.maintenance == nil && !s.stopping.Load() && s.fenced == nil {
		if err := s.startLocked(s.point.CommittedOrdinal); err != nil {
			s.maintenanceErr = err
		}
	}
}

func (s *retained) startLocked(ordinal uint64) error {
	switch {
	case s.stopping.Load():
		return os.ErrClosed
	case s.fenced != nil:
		return s.fenced
	case ordinal == s.point.NextOrdinal:
		return nil // idempotent retry of the already selected boundary
	case s.maintenance != nil:
		return errors.New("compaction already running")
	case s.selector.Sequence == math.MaxUint64:
		return errors.New("compaction generation overflow")
	}
	i, ok := slices.BinarySearchFunc(s.active.batches, ordinal, func(b *tailBatch, ordinal uint64) int {
		switch {
		case b.next < ordinal:
			return -1
		case b.next > ordinal:
			return 1
		default:
			return 0
		}
	})
	if !ok {
		return errors.New("cutoff is not a committed whole-batch boundary")
	}
	ctx := context.Background()
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	view, err := s.captureLocked(ctx)
	if err != nil {
		ticket.Close()
		return err
	}
	cut := view.batches[i].end
	start := time.Now()
	done := make(chan struct{})
	s.maintenance, s.maintenanceErr = done, nil
	go func() {
		err := s.advance(ctx, ticket, view, cut, ordinal)
		ticket.Close()
		err = errors.Join(err, view.release())
		s.mu.Lock()
		defer s.mu.Unlock()
		s.maintenance = nil
		err = errors.Join(err, s.reclaimLocked())
		s.maintenanceErr = err
		if err != nil {
			log.Printf("retained checkpoint at ordinal %d: %v", ordinal, err)
		} else {
			s.publicationLag = time.Since(start)
		}
		close(done)
	}()
	return nil
}

func (s *retained) advance(ctx context.Context, ticket *history.Replacement, view *recoveryView, cut int64, ordinal uint64) (err error) {
	var successor *epoch
	var selected authority
	var selectedPoint checkpoint.RecoveryPoint
	locked := false
	m, publishErr := s.publisher.PublishRecoveryView(ctx, view, cut, ordinal, func(ctx context.Context, point checkpoint.RecoveryPoint) error {
		// The costly checkpoint build runs concurrently with appends. Keep
		// this lock through selection and catalog activation so no capture
		// can combine either half of the old and new views.
		s.mu.Lock()
		locked = true
		if s.fenced != nil {
			return s.fenced
		}
		if s.active != view.e {
			return errors.New("checkpoint predecessor changed")
		}
		captured := point
		point.CommittedEnd, point.CommittedOrdinal = s.point.CommittedEnd, s.point.CommittedOrdinal
		if captured.CommittedEnd != point.CommittedEnd {
			if err := verifySuffix(ctx, s.active, s.point); err != nil {
				return s.fence(err)
			}
		}
		f, a, digest, err := s.stageTail(ctx, point, s.active.file, s.active.base)
		if err != nil {
			return err
		}
		closeUnused := func(err error) error { return s.discardSwitch(f, a, err) }
		sum, err := digestField(a.SuffixSHA256)
		if err != nil {
			return closeUnused(err)
		}
		if captured.CommittedEnd == point.CommittedEnd && sum != captured.SuffixSHA256 {
			return closeUnused(errors.New("rebased suffix differs from captured bytes"))
		}
		a.Sequence = s.selector.Sequence + 1
		successor, err = newEpoch(f, a, nil)
		if err != nil {
			return closeUnused(err)
		}
		switchRecord, err := s.prepareSwitch(f, a)
		if err != nil {
			successor = nil
			return closeUnused(err)
		}
		if _, err := s.selectAuthority(a); err != nil {
			successor = nil
			return closeUnused(err)
		}
		s.active.retirement = switchRecord
		point.SuffixSHA256 = sum
		selected, selectedPoint = a, point
		s.digest = digest
		return nil
	})
	if !locked {
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	if errors.Is(publishErr, wal.ErrUncertain) {
		return s.fence(publishErr)
	}
	if m == nil {
		if errors.Is(publishErr, checkpoint.ErrPublicationUncertain) {
			// Selector preparation failed before rename or staging cleanup
			// failed. Revalidate the still-selected predecessor before trying
			// to reclaim losing files or allowing another publication.
			if err := s.recoverPublication(ctx, s.active, s.point, s.manifestName()); err != nil {
				return s.fence(errors.Join(publishErr, err))
			}
		}
		return publishErr
	}
	successor.manifest = m
	name := checkpoint.GenerationManifestName(selectedPoint.Generation)
	if errors.Is(publishErr, checkpoint.ErrPublicationUncertain) {
		if err := s.recoverPublication(ctx, successor, selectedPoint, name); err != nil {
			return s.fence(errors.Join(publishErr, err, successor.file.Close()))
		}
	}
	gen, err := ticket.ReplaceSelected(ctx, s.dir.File, history.GenerationID(selectedPoint.Generation))
	if err != nil {
		return s.fence(errors.Join(publishErr, err, successor.file.Close()))
	}
	successor.gen = gen
	old := s.active
	for _, b := range old.batches {
		if b.end > cut {
			b.refs++
			successor.batches = append(successor.batches, b)
			successor.weight += b.weight
			successor.maxLine = max(successor.maxLine, b.maxLine)
		}
	}
	old.retiredAt, old.size, old.replacedBy = time.Now(), s.point.CommittedEnd-old.base, selected.Sequence
	s.retired = append(s.retired, old)
	s.active, s.point, s.selector = successor, selectedPoint, selected
	return publishErr
}

func (s *retained) recoverPublication(ctx context.Context, e *epoch, p checkpoint.RecoveryPoint, name string) error {
	v := &recoveryView{s: s, e: e, point: p, manifestName: name}
	return s.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) { return v, nil })
}

func (s *retained) fence(err error) error {
	s.fenced = errors.Join(wal.ErrUncertain, err)
	s.dir.RecordCommit("checkpoint_selection", s.fenced)
	return s.fenced
}

func (s *retained) reclaimLocked() error {
	if s.fenced != nil || s.selector.Version == 0 || len(s.retired)+len(s.pendingRetirement) == 0 {
		return nil
	}
	// Reclaim shares the publication lock with the publisher. A reader can
	// drain during publication, so defer attempts until it is finished.
	if s.maintenance != nil {
		return nil
	}
	var err error
	kept := s.retired[:0]
	for _, e := range s.retired {
		if e.pins != 0 {
			kept = append(kept, e)
			continue
		}
		if reclaimErr := s.retireEpoch(e); reclaimErr != nil {
			err = errors.Join(err, reclaimErr)
			kept = append(kept, e)
		}
	}
	clear(s.retired[len(kept):])
	s.retired = kept
	pending := s.pendingRetirement[:0]
	for _, r := range s.pendingRetirement {
		_, complete, cleanupErr := s.finishRetirement(r)
		err = errors.Join(err, cleanupErr)
		if !complete || cleanupErr != nil {
			pending = append(pending, r)
		}
	}
	clear(s.pendingRetirement[len(pending):])
	s.pendingRetirement = pending
	return err
}

func (s *retained) retireEpoch(e *epoch) error {
	if e.file != nil {
		err := e.file.Close()
		e.file = nil
		if err != nil {
			return err
		}
	}
	removed, complete, err := s.finishRetirement(e.retirement)
	if err != nil {
		return err
	}
	if !complete {
		s.pendingRetirement = append(s.pendingRetirement, e.retirement)
	}
	releaseBatches(e.batches)
	e.batches = nil
	if complete && e.gen.ID != (history.GenerationID{}) {
		s.reclaimedBytes = uint64(e.info.Size())
		// The tail may have been appended to after the initial stat.
		if e.size > int64(s.reclaimedBytes) {
			s.reclaimedBytes = uint64(e.size)
		}
		s.reclaimedBytes += removed
		s.reclaimedPredecessor = e.number
		s.reclaimedReplacement = e.replacedBy
	}
	return nil
}

func (s *retained) stopMaintenance() {
	s.stopping.Store(true)
}

func (s *retained) drain(ctx context.Context) error {
	s.mu.Lock()
	done := s.maintenance
	s.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reclaimLocked(); err != nil {
		return err
	}
	for _, e := range append(s.retired[:len(s.retired):len(s.retired)], s.active) {
		if e.pins != 0 {
			return errors.New("storage captures still active")
		}
	}
	return s.closeStorage()
}

// Used after drain or during startup before any work was admitted.
func (s *retained) closeStorage() error {
	err := s.catalog.Close()
	for _, e := range append(s.retired[:len(s.retired):len(s.retired)], s.active) {
		if e == nil {
			continue
		}
		releaseBatches(e.batches)
		e.batches = nil
		if e.file != nil {
			err = errors.Join(err, e.file.Close())
			e.file = nil
		}
	}
	return err
}

func verifySuffix(ctx context.Context, e *epoch, p checkpoint.RecoveryPoint) error {
	h := sha256.New()
	in := io.NewSectionReader(e.file, p.CoveredOffset-e.base, p.CommittedEnd-p.CoveredOffset)
	n, err := io.Copy(h, checkedReader{ctx, in})
	if err != nil {
		return err
	}
	if n != p.CommittedEnd-p.CoveredOffset || [32]byte(h.Sum(nil)) != p.SuffixSHA256 {
		return errors.New("committed suffix changed during checkpoint publication")
	}
	return nil
}
