package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/wal"
)

func (s *storageOwner) schedule() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *storageOwner) maintain(ctx context.Context) {
	defer close(s.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		s.mu.Lock()
		if ctx.Err() != nil || s.fenced != nil {
			s.mu.Unlock()
			return
		}
		s.advancing = s.tail.end-s.generation.Boundary.CoveredOffset >= advanceAt
		advance := s.advancing
		s.mu.Unlock()

		var err error
		if advance {
			err = s.advance(ctx)
			s.mu.Lock()
			s.advanceErr, s.advancing = err, false
			close(s.progress)
			s.progress = make(chan struct{})
			s.mu.Unlock()
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("advance retained history: %v", err)
		}
		if s.fenced == nil {
			if err := s.reclaim(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("reclaim retained history: %v", err)
			}
		}
	}
}

func (s *storageOwner) advance(ctx context.Context) (err error) {
	view, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, view.close()) }()
	return s.publish(ctx, view)
}

// publish stages a fixed view, then reconciles commits made after capture.
func (s *storageOwner) publish(ctx context.Context, view *matchedView) (err error) {
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	point := view.point
	if point.SuffixSHA256 == ([32]byte{}) {
		point.SuffixSHA256, err = digestRange(view.wal, point.CoveredOffset, point.CommittedEnd)
		if err != nil {
			return err
		}
	}
	input := recoveryView{s.log, view.wal, view.name, view.manifest, point}
	published, err := s.publisher.PublishRecoveryView(ctx, input, point.CommittedEnd, point.CommittedOrdinal,
		func(ctx context.Context, next checkpoint.RecoveryPoint) error {
			return s.selectSuccessor(ctx, ticket, view, next)
		})
	if !errors.Is(err, checkpoint.ErrPublicationUncertain) || ctx.Err() != nil {
		return err
	}
	// Checkpoint staging also reports uncertainty on a clean selector
	// refusal. Resolve it using the still-valid current authority.
	current, acquireErr := s.acquire(ctx)
	if acquireErr != nil {
		return errors.Join(err, acquireErr)
	}
	defer func() { err = errors.Join(err, current.close()) }()
	if current.point.SuffixSHA256 == ([32]byte{}) {
		current.point.SuffixSHA256, acquireErr = digestRange(current.wal, current.point.CoveredOffset, current.point.CommittedEnd)
		if acquireErr != nil {
			return errors.Join(err, acquireErr)
		}
	}
	recovery := recoveryView{s.log, current.wal, current.name, current.manifest, current.point}
	if recoveryErr := s.recoverPublication(ctx, recovery); recoveryErr != nil {
		s.mu.Lock()
		s.fence(errors.Join(err, recoveryErr))
		s.mu.Unlock()
		return errors.Join(err, recoveryErr)
	}
	if published != nil {
		return nil
	}
	return err
}

func (s *storageOwner) selectSuccessor(ctx context.Context, ticket *history.Replacement, old *matchedView,
	next checkpoint.RecoveryPoint) (err error) {
	name := checkpoint.GenerationManifestName(next.Generation)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.generation.ID != old.point.Generation || s.tail != old.live {
		return errors.New("checkpoint predecessor changed before selection")
	}
	latest, err := s.log.Capture(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, latest.Release()) }()
	successor, err := s.loadRecoveryView(latest, name)
	if err != nil {
		return err
	}
	if successor.point.Generation != next.Generation || successor.point.CoveredOffset != next.CoveredOffset ||
		successor.point.NextOrdinal != next.NextOrdinal {
		return errors.New("staged checkpoint differs from successor boundary")
	}
	tail, err := s.replayTail(ctx, latest, next.CoveredOffset, next.NextOrdinal)
	if err != nil {
		return err
	}
	a := wal.Authority{Generation: next.Generation, Covered: next.CoveredOffset, First: next.NextOrdinal}
	if err := s.log.Select(a, old.wal, next.SuffixSHA256); err != nil {
		tail.release()
		if errors.Is(err, wal.ErrUncertain) {
			s.fence(err)
		}
		return err
	}
	// Selection is durable. Finish in-memory activation even if shutdown
	// canceled the staging context during the marker sync.
	gen, err := ticket.ReplaceSelectedLocked(context.WithoutCancel(ctx), s.dir, next.Generation)
	if gen.ID != next.Generation {
		tail.release()
		s.fence(err)
		return err
	}
	s.retired = append(s.retired, retiredGeneration{s.name, s.generation})
	s.tail.release()
	s.tail, s.generation, s.manifest, s.name = tail, gen, successor.manifest, name
	if err != nil {
		log.Printf("retire predecessor generation: %v", err)
	}
	return nil
}

func (s *storageOwner) fence(cause error) {
	if s.fenced != nil {
		return
	}
	s.fenced = errors.Join(wal.ErrUncertain, fmt.Errorf("retained authority requires recovery: %w", cause))
	s.fatal <- s.fenced
}

type retiredGeneration struct {
	name string
	gen  history.Generation
}

func (s *storageOwner) reclaim(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.log.ReclaimTails(); err != nil {
		return err
	}
	if len(s.retired) == 0 {
		return nil
	}
	if err := s.catalog.ReclaimSelected(ctx, s.dir, s.generation.ID); err != nil {
		return err
	}
	kept := s.retired[:0]
	var err error
	for _, old := range s.retired {
		select {
		case <-old.gen.Drained:
			removed, removeErr := history.RemoveDrainedManifest(s.dir, s.root, old.name, old.gen.ID, s.manifest)
			err = errors.Join(err, removeErr)
			if removed && removeErr == nil {
				continue
			}
		default:
		}
		kept = append(kept, old)
	}
	clear(s.retired[len(kept):])
	s.retired = kept
	return err
}

func (s *storageOwner) stop() { s.cancel() }

func (s *storageOwner) drain(ctx context.Context) error {
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	if s.tail.refs != 1 || s.budget.Usage().InUse[checkpoint.LiveCaptures] != 0 {
		s.mu.Unlock()
		return errors.New("storage views are still pinned")
	}
	s.tail.release()
	fenced := s.fenced != nil
	s.mu.Unlock()
	err := s.catalog.Close()
	if !fenced {
		err = errors.Join(err, s.reclaim(ctx))
	}
	return err
}

const shutdownTimeout = 30 * time.Second
