package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/wal"
)

type recoveryView struct {
	a        wal.Authority
	wal      *wal.Capture
	manifest func() (io.ReadCloser, error)
	segment  func(string) (io.ReadCloser, error)
}

func (v recoveryView) Point() checkpoint.RecoveryPoint {
	return checkpoint.RecoveryPoint{
		Generation: v.a.Generation, CoveredOffset: v.a.CoveredOffset, NextOrdinal: v.a.NextOrdinal,
		CommittedEnd: v.a.CommittedEnd, CommittedOrdinal: v.a.CommittedOrdinal, SuffixSHA256: v.a.SuffixSHA256,
	}
}

func (v recoveryView) OpenManifest() (io.ReadCloser, error)           { return v.manifest() }
func (v recoveryView) OpenSegment(name string) (io.ReadCloser, error) { return v.segment(name) }
func (v recoveryView) OpenSuffix() (io.ReadCloser, error) {
	return io.NopCloser(io.NewSectionReader(v.wal, v.a.CoveredOffset, v.a.CommittedEnd-v.a.CoveredOffset)), nil
}

func (s *retainedStorage) recoverPublication(ctx context.Context, a wal.Authority) (err error) {
	pin, selected, err := s.owner.CaptureSelected(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, pin.Release()) }()
	if selected != a {
		return errors.New("WAL authority changed before publication recovery")
	}
	name := checkpoint.GenerationManifestName(a.Generation)
	return s.pub.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
		return recoveryView{a: a, wal: pin,
			manifest: func() (io.ReadCloser, error) { return s.owner.OpenDataFile(name) },
			segment:  func(name string) (io.ReadCloser, error) { return s.owner.OpenDataFile(name) },
		}, nil
	})
}

// advance stages outside the operation lock, then reconciles appends and
// changes the writable selector under it. Captures wait for the catalog handoff.
func (s *retainedStorage) advance(ctx context.Context, ordinal uint64) (err error) {
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	if err := s.lockView(ctx); err != nil {
		return err
	}
	cutoff := s.cutoffLocked(ordinal)
	if cutoff == 0 {
		s.mu.Unlock()
		return errors.New("checkpoint cutoff must end an uncovered committed batch")
	}
	pin, err := s.acquireLocked(ctx, false)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, pin.release()) }()

	predecessor := pin.a
	view := recoveryView{a: predecessor, wal: pin.wal, manifest: pin.reader.OpenManifest, segment: pin.reader.OpenSegment}
	var transition wal.TailSwitch
	var selectionErr error
	m, publishErr := s.pub.PublishRecoveryView(ctx, view, cutoff, ordinal, func(ctx context.Context, p checkpoint.RecoveryPoint) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.owner.Selected().Generation != predecessor.Generation {
			return errors.New("checkpoint predecessor is no longer active")
		}
		s.switching = make(chan struct{})
		transition, selectionErr = s.owner.Select(ctx, toAuthority(p), nil, false)
		if s.owner.Selected().Generation == p.Generation {
			// A close error on the old writer does not invalidate selection.
			return nil
		}
		return selectionErr
	})
	if m == nil {
		if errors.Is(publishErr, checkpoint.ErrPublicationUncertain) && !errors.Is(selectionErr, wal.ErrUncertain) && ctx.Err() == nil {
			// A failed selector preparation leaves the old record definitive.
			// Hold off new appends while the publisher validates and cleans up.
			s.mu.Lock()
			if s.switching == nil {
				s.switching = make(chan struct{})
			}
			a := s.owner.Selected()
			s.mu.Unlock()
			recoverErr := s.recoverPublication(ctx, a)
			if recoverErr != nil {
				s.fence(recoverErr)
			}
			publishErr = errors.Join(publishErr, recoverErr)
		} else if errors.Is(selectionErr, wal.ErrUncertain) {
			s.fence(selectionErr)
		}
		s.endSwitch()
		return publishErr
	}

	// Once selected, complete the in-process handoff even if shutdown has
	// canceled new maintenance work. The service deadline bounds the drain.
	handoff := context.WithoutCancel(ctx)
	a := s.owner.Selected()
	if publishErr != nil {
		if err := s.recoverPublication(handoff, a); err != nil {
			s.fence(err)
			s.endSwitch()
			return errors.Join(publishErr, selectionErr, err)
		}
	}
	g, installErr := s.install(handoff, a, ticket)
	s.mu.Lock()
	if g.Drained != nil {
		s.retired = append(s.retired, retiredGeneration{transition, s.active.Drained, time.Now()})
		s.active = g
		count := 0
		for count < len(s.tail) && s.tail[count].end <= cutoff {
			count++
		}
		s.dropTail(count)
	} else {
		s.fenced = fmt.Errorf("activate selected checkpoint: %w", installErr)
	}
	s.mu.Unlock()
	s.endSwitch()
	s.notify()
	return errors.Join(publishErr, selectionErr, installErr)
}

func (s *retainedStorage) cutoffLocked(ordinal uint64) int64 {
	for _, batch := range s.tail {
		if batch.next == ordinal {
			return batch.end
		}
	}
	return 0
}

func (s *retainedStorage) fence(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fenced = fmt.Errorf("retained authority requires recovery: %w", err)
}

func (s *retainedStorage) endSwitch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.switching != nil {
		close(s.switching)
		s.switching = nil
	}
}

func (s *retainedStorage) reclaim(ctx context.Context) error {
	s.mu.Lock()
	n, fenced := len(s.retired), s.fenced
	s.mu.Unlock()
	if n == 0 || fenced != nil {
		return nil
	}
	if err := s.catalog.ReclaimSelected(ctx, s.dir, selection(s.owner.Selected())); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < len(s.retired); {
		old := s.retired[i]
		select {
		case <-old.drained:
			if err := s.reclaimSide(old.switches.Before); err != nil {
				return err
			}
			if err := s.owner.FinishTailSwitch(old.switches); err != nil {
				return err
			}
			copy(s.retired[i:], s.retired[i+1:])
			s.retired[len(s.retired)-1] = retiredGeneration{}
			s.retired = s.retired[:len(s.retired)-1]
		default:
			i++
		}
	}
	return nil
}

func (s *retainedStorage) recoverRetirement(ctx context.Context, selected wal.Authority) error {
	switches, err := s.owner.TailSwitches(ctx)
	if err != nil {
		return err
	}
	for _, transition := range switches {
		for _, old := range []wal.Authority{transition.Before, transition.After} {
			if old.Generation == [32]byte{} || old.Generation == selected.Generation {
				continue
			}
			if err := s.catalog.RegisterRetired(ctx, s.dir, history.GenerationID(old.Generation)); err != nil {
				return err
			}
		}
	}
	if err := s.catalog.ReclaimSelected(ctx, s.dir, selection(selected)); err != nil {
		return err
	}
	for _, transition := range switches {
		for _, old := range []wal.Authority{transition.Before, transition.After} {
			if err := s.reclaimSide(old); err != nil {
				return err
			}
		}
		if err := s.owner.FinishTailSwitch(transition); err != nil {
			return err
		}
	}
	return nil
}

func (s *retainedStorage) reclaimSide(old wal.Authority) error {
	if err := s.owner.ReclaimTail(old); err != nil {
		return err
	}
	if old.Generation == s.owner.Selected().Generation {
		return nil
	}
	return s.reclaimManifest(old)
}

func (s *retainedStorage) reclaimManifest(a wal.Authority) (err error) {
	if a.Generation == [32]byte{} {
		return nil
	}
	name := checkpoint.GenerationManifestName(a.Generation)
	f, err := s.owner.OpenDataFile(name)
	if errors.Is(err, os.ErrNotExist) {
		return s.dir.Sync()
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, (256<<10)+1))
	if err != nil {
		return err
	}
	_, id, err := checkpoint.DecodeManifest(b)
	if err != nil || id != a.Generation {
		return errors.Join(err, errors.New("retired checkpoint identity changed"))
	}
	if err := s.root.Remove(name); err != nil {
		return err
	}
	return s.dir.Sync()
}
