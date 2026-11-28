package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

type recoveryView struct {
	log      *wal.Log
	capture  *wal.Capture
	name     string
	manifest *checkpoint.Manifest
	point    checkpoint.RecoveryPoint
}

var _ checkpoint.RecoveryView = recoveryView{}

func (v recoveryView) Point() checkpoint.RecoveryPoint { return v.point }

func (v recoveryView) OpenManifest() (io.ReadCloser, error) { return v.log.OpenStorageFile(v.name) }

func (v recoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	for _, s := range v.manifest.Segments {
		if name == s.Name {
			return v.log.OpenStorageFile(name)
		}
	}
	return nil, os.ErrNotExist
}

func (v recoveryView) OpenSuffix() (io.ReadCloser, error) {
	p := v.point
	return io.NopCloser(io.NewSectionReader(v.capture, p.CoveredOffset, p.CommittedEnd-p.CoveredOffset)), nil
}

func openStorage(ctx context.Context, path string) (_ *storageOwner, err error) {
	log, err := wal.OpenStreaming(path, tailLimit)
	if err != nil {
		return nil, err
	}
	root, dir := log.Directory()
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.ReplayTailBytes:   72 << 20,
		checkpoint.StagingBytes:      uint64(checkpoint.MaxBlockSize)*checkpoint.MaxSegments + 1<<20,
		checkpoint.PinnedGenerations: 8,
		checkpoint.LiveCaptures:      82,
	})
	s := &storageOwner{log: log, root: root, dir: dir, budget: budget, catalog: history.NewCatalog(budget),
		publisher: checkpoint.NewPublisher(root, dir, budget), wake: make(chan struct{}, 1),
		progress: make(chan struct{}), done: make(chan struct{}), fatal: make(chan error, 1)}
	defer func() {
		if err != nil {
			if s.tail != nil {
				s.tail.release()
			}
			err = errors.Join(err, s.catalog.Close(), log.Close())
		}
	}()
	if err := s.recover(ctx); err != nil {
		return nil, err
	}
	worker, cancel := context.WithCancel(context.Background())
	s.cancel, s.stopped = cancel, worker.Done()
	go s.maintain(worker)
	if s.tail.end-s.generation.Boundary.CoveredOffset >= advanceAt {
		s.schedule()
	}
	return s, nil
}

func (s *storageOwner) recover(ctx context.Context) (err error) {
	c, err := s.log.Capture(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.Release()) }()
	a, retained := s.log.Authority()
	name := canonicalManifest
	if retained && !a.Canonical {
		name = checkpoint.GenerationManifestName(a.Generation)
	}
	view, err := s.loadRecoveryView(c, name)
	if retained {
		if err != nil {
			return err
		}
		if a.Generation != view.point.Generation || a.Covered != view.point.CoveredOffset ||
			a.First != view.point.NextOrdinal || a.Identity != c.Identity() {
			return errors.New("selected checkpoint does not match WAL authority")
		}
		view.point.SuffixSHA256 = a.Digest
		if err := s.recoverPublication(ctx, view); err != nil {
			return err
		}
	} else {
		view, err = s.recoverLegacy(ctx, c, view, err)
		if err != nil {
			return err
		}
	}
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	if name == canonicalManifest {
		s.generation, err = ticket.ReplacePinned(ctx, s.dir)
	} else {
		s.generation, err = ticket.ReplaceSelected(ctx, s.dir, view.point.Generation)
	}
	if err != nil {
		return err
	}
	s.name, s.manifest = name, view.manifest
	s.tail, err = s.replayTail(ctx, c, view.point.CoveredOffset, view.point.NextOrdinal)
	if err != nil {
		return err
	}
	if err := history.RecoverRetired(ctx, s.dir, s.name, s.manifest); err != nil {
		return err
	}
	if err := c.Release(); err != nil {
		return err
	}
	if err := s.log.FinishRecovery(); err != nil {
		return err
	}
	return nil
}

func (s *storageOwner) loadRecoveryView(c *wal.Capture, name string) (v recoveryView, err error) {
	f, err := s.log.OpenStorageFile(name)
	if err != nil {
		return v, err
	}
	// The maximum encoded manifest is 256 KiB.
	const maxManifest = 256 << 10
	b, err := io.ReadAll(io.LimitReader(f, maxManifest+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return v, err
	}
	m, id, err := checkpoint.DecodeManifest(b)
	if err != nil {
		return v, err
	}
	if name == canonicalManifest && m.Version != 1 || name != canonicalManifest && name != checkpoint.GenerationManifestName(id) {
		return v, errors.New("invalid checkpoint manifest name or version")
	}
	if m.CoveredOffset > c.CommittedEnd() || m.NextOrdinal > c.NextOrdinal() {
		return v, errors.New("checkpoint exceeds committed WAL bounds")
	}
	return recoveryView{log: s.log, capture: c, name: name, manifest: m, point: checkpoint.RecoveryPoint{
		Generation: id, CoveredOffset: m.CoveredOffset, NextOrdinal: m.NextOrdinal,
		CommittedEnd: c.CommittedEnd(), CommittedOrdinal: c.NextOrdinal(),
	}}, nil
}

func (s *storageOwner) recoverLegacy(ctx context.Context, c *wal.Capture, view recoveryView, loadErr error) (recoveryView, error) {
	if loadErr != nil && !errors.Is(loadErr, os.ErrNotExist) {
		return view, loadErr
	}
	if loadErr == nil {
		var sum [32]byte
		if _, err := hex.Decode(sum[:], []byte(view.manifest.SourceSHA256)); err != nil {
			return view, err
		}
		if err := c.ValidateLegacyBoundary(ctx, view.point.CoveredOffset, view.point.NextOrdinal, sum); err != nil {
			return view, err
		}
	}
	if loadErr == nil && c.CommittedEnd()-view.point.CoveredOffset <= tailLimit {
		sum, err := digestRange(c, view.point.CoveredOffset, c.CommittedEnd())
		if err != nil {
			return view, err
		}
		view.point.SuffixSHA256 = sum
		if err := s.recoverPublication(ctx, view); err != nil {
			return view, err
		}
	} else {
		err := s.publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
			return history.ValidatePinnedAuthority(ctx, s.dir)
		})
		if err != nil && !(errors.Is(err, os.ErrNotExist) && loadErr != nil) {
			return view, err
		}
	}
	if loadErr != nil || c.CommittedEnd()-view.point.CoveredOffset >= advanceAt {
		end, ordinal := c.CommittedEnd(), c.NextOrdinal()
		if loadErr != nil && end-8 < advanceAt {
			end, ordinal = 8, 0
		}
		sum, err := digestRange(c, 0, end)
		if err != nil {
			return view, err
		}
		if _, err := s.publisher.PublishCaptured(ctx, c, end, view.manifest, ordinal, hex.EncodeToString(sum[:])); err != nil {
			return view, err
		}
		view, err = s.loadRecoveryView(c, canonicalManifest)
		if err != nil {
			return view, err
		}
	}
	if view.point.CoveredOffset > 8 {
		a := wal.Authority{Generation: view.point.Generation, Canonical: true, Covered: view.point.CoveredOffset, First: view.point.NextOrdinal}
		sum, err := digestRange(c, a.Covered, c.CommittedEnd())
		if err != nil {
			return view, err
		}
		if err := s.log.Select(a, c, sum); err != nil {
			return view, err
		}
	}
	return view, nil
}

func (s *storageOwner) recoverPublication(ctx context.Context, view recoveryView) error {
	return s.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
		return view, nil
	})
}

func digestRange(c *wal.Capture, start, end int64) ([32]byte, error) {
	h := sha256.New()
	n, err := io.Copy(h, io.NewSectionReader(c, start, end-start))
	if err == nil && n != end-start {
		err = io.ErrUnexpectedEOF
	}
	return [32]byte(h.Sum(nil)), err
}

func (s *storageOwner) replayTail(ctx context.Context, c *wal.Capture, from int64, ordinal uint64) (*liveTail, error) {
	end := c.CommittedEnd()
	if from > end || end-from > tailLimit {
		return nil, errors.New("committed WAL suffix exceeds tail bounds")
	}
	replayed, err := s.budget.ReplayCapturedTail(ctx, c, from, end, ordinal)
	if err != nil {
		return nil, err
	}
	t := &liveTail{refs: 1, end: end}
	for _, b := range replayed.Batches {
		t.charges = append(t.charges, b.Reservation)
	}
	if replayed.NextOrdinal != c.NextOrdinal() {
		t.release()
		return nil, fmt.Errorf("replay ordinal %d differs from committed ordinal %d", replayed.NextOrdinal, c.NextOrdinal())
	}
	builder := store.NewRecoveryBuilder()
	for _, b := range replayed.Batches {
		builder.Add(b.Rows)
		for _, row := range b.Rows {
			t.maxLine = max(t.maxLine, len(row.Line))
		}
	}
	builder.Install(&t.rows)
	return t, nil
}
