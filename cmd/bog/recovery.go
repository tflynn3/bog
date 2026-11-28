package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

func openRetained(ctx context.Context, path string) (_ *retained, err error) {
	owner, err := wal.OpenOwner(path)
	if err != nil {
		return nil, err
	}
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.ReplayTailBytes:   maxTailBytes,
		checkpoint.StagingBytes:      128 << 20,
		checkpoint.LiveCaptures:      72,
		checkpoint.PinnedGenerations: 16,
		checkpoint.CaptureBytes:      24 << 20,
	})
	r := &retained{
		owner: owner, budget: budget,
		publisher: checkpoint.NewPublisher(owner.Root(), owner.Directory(), budget),
		catalog:   history.NewCatalog(budget), captures: make(map[string]*queryView),
		stop: make(chan struct{}), wake: make(chan struct{}, 1), done: make(chan struct{}),
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, r.close(), owner.Close())
		}
	}()
	a, err := owner.ReadAuthority()
	switch {
	case err == nil:
		err = r.recoverSelected(ctx, a)
	case errors.Is(err, os.ErrNotExist):
		_, restoreErr := owner.Root().Lstat("bog.backup")
		switch {
		case restoreErr == nil:
			err = r.recoverRestore(ctx)
		case errors.Is(restoreErr, os.ErrNotExist):
			err = r.recoverLegacy(ctx)
		default:
			err = restoreErr
		}
	}
	if err != nil {
		return nil, err
	}
	if err := errors.Join(owner.CleanStages(), owner.Directory().Sync()); err != nil {
		return nil, err
	}
	go r.worker()
	r.notify()
	return r, nil
}

func (r *retained) recoverSelected(ctx context.Context, a wal.Authority) error {
	e := &epoch{covered: a.CoveredOffset, first: a.NextOrdinal, sequence: a.Sequence}
	generation, err := hex.DecodeString(a.Generation)
	if err != nil {
		return err
	}
	e.name = checkpoint.GenerationManifestName([32]byte(generation))
	m, id, _, err := r.readManifest(e.name)
	if err != nil {
		return err
	}
	if id != history.GenerationID(generation) || m.CoveredOffset != e.covered || m.NextOrdinal != e.first {
		return errors.New("selected manifest does not match WAL authority")
	}
	e.id, e.manifest = id, m
	e.tail, err = r.owner.OpenSelected(a, maxTailBytes)
	if err != nil {
		return err
	}
	r.current = e
	replay, err := r.budget.ReplayCapturedTail(ctx, e.tail, e.covered, a.End, e.first)
	if err != nil {
		return err
	}
	r.installReplay(replay)
	if r.ordinal != a.Ordinal {
		return errors.New("selected WAL suffix ordinal mismatch")
	}
	if err := r.installCurrent(ctx, e); err != nil {
		return err
	}
	if err := r.recoverStaging(ctx, r.viewLocked()); err != nil {
		return err
	}
	if err := errors.Join(e.tail.FinishRecovery(), r.owner.Directory().Sync()); err != nil {
		return err
	}
	return r.owner.CleanUnselectedTails(a.Tail)
}

func (r *retained) recoverRestore(ctx context.Context) (err error) {
	for _, name := range []string{"bog.wal", "bog.commit"} {
		if _, err := r.owner.Root().Lstat(name); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return fmt.Errorf("inspect legacy entry %s: %w", name, err)
			}
			return fmt.Errorf("restore mixed with legacy WAL entry %s", name)
		}
	}
	point, input, err := backup.VerifyPinnedRecovery(ctx, r.owner.Root(), func(start, end int64) error {
		if end-start > maxTailBytes {
			return errors.New("restore suffix exceeds replay budget")
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	m, id, raw, err := r.readManifest(canonicalManifest)
	if err != nil {
		return err
	}
	if id != history.GenerationID(point.Generation) || m.CoveredOffset != point.CoveredOffset || m.NextOrdinal != point.NextOrdinal {
		return errors.New("restore manifest changed during activation")
	}
	name := checkpoint.GenerationManifestName(point.Generation)
	if err := r.owner.InstallImmutable(name, raw); err != nil {
		return err
	}
	a := wal.Authority{Sequence: 1, Generation: hex.EncodeToString(id[:]), CoveredOffset: point.CoveredOffset, NextOrdinal: point.NextOrdinal,
		End: point.CommittedEnd, Ordinal: point.CommittedOrdinal, SuffixSHA256: hex.EncodeToString(point.SuffixSHA256[:])}
	tail, err := r.owner.NewTail(input, a)
	if err != nil {
		return err
	}
	r.current = &epoch{tail: tail, name: name, id: id, manifest: m, covered: a.CoveredOffset, first: a.NextOrdinal, sequence: a.Sequence}
	replay, err := r.budget.ReplayCapturedTail(ctx, tail, a.CoveredOffset, a.End, a.NextOrdinal)
	if err != nil {
		return err
	}
	r.installReplay(replay)
	if r.ordinal != a.Ordinal {
		return errors.New("restored WAL suffix ordinal mismatch")
	}
	if err := r.installCurrent(ctx, r.current); err != nil {
		return err
	}
	if err := r.owner.Select(tail); err != nil {
		return err
	}
	return r.owner.CleanUnselectedTails(tail.Name())
}

func (r *retained) recoverLegacy(ctx context.Context) error {
	_, manifestErr := r.owner.Root().Lstat(canonicalManifest)
	if manifestErr == nil {
		if _, err := r.owner.Root().Lstat("bog.wal"); err != nil {
			return fmt.Errorf("legacy checkpoint requires its full WAL: %w", err)
		}
	} else if !errors.Is(manifestErr, os.ErrNotExist) {
		return manifestErr
	}
	tail, err := r.owner.OpenLegacy()
	if err != nil {
		return err
	}
	e := &epoch{tail: tail, covered: 8}
	r.current = e
	h := sha256.New()
	if manifestErr == nil || checkpoint.CheckPublication(r.owner.Directory()) != nil {
		err = r.publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
			m, err := history.ValidatePinnedAuthority(ctx, r.owner.Directory())
			if err != nil {
				return nil, err
			}
			if m.Version != 1 || m.CoveredOffset > tail.End() {
				return nil, errors.New("invalid legacy checkpoint boundary")
			}
			if _, err := io.CopyN(h, io.NewSectionReader(tail, 0, 8), 8); err != nil {
				return nil, err
			}
			var ordinal uint64
			input := io.TeeReader(io.NewSectionReader(tail, 8, m.CoveredOffset-8), h)
			if err := checkpoint.ScanFrames(ctx, input, 8, m.CoveredOffset, 0, func(_, _ int64, first uint64, rows []store.Row) error {
				ordinal = first + uint64(len(rows))
				return nil
			}); err != nil {
				return nil, err
			}
			if m.NextOrdinal != ordinal || m.SourceSHA256 != hex.EncodeToString(h.Sum(nil)) {
				return nil, errors.New("legacy checkpoint does not match WAL prefix")
			}
			e.manifest, e.covered, e.first = m, m.CoveredOffset, m.NextOrdinal
			return m, nil
		})
	}
	if errors.Is(err, checkpoint.ErrPublicationUncertain) && e.manifest != nil {
		// The first v2 advance may have staged a successor, with the
		// canonical checkpoint and full WAL still selected.
		m, id, _, manifestErr := r.readManifest(canonicalManifest)
		if manifestErr != nil {
			return manifestErr
		}
		e.name, e.id, e.manifest = canonicalManifest, id, m
		replay, replayErr := r.budget.ReplayCapturedTail(ctx, tail, e.covered, tail.End(), e.first)
		if replayErr != nil {
			return replayErr
		}
		r.installReplay(replay)
		if recoverErr := r.recoverStaging(ctx, r.viewLocked()); recoverErr != nil {
			return recoverErr
		}
		if finishErr := tail.FinishRecovery(); finishErr != nil {
			return finishErr
		}
		return r.activateLegacy(ctx)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if e.manifest == nil {
		if _, err := io.CopyN(h, io.NewSectionReader(tail, 0, 8), 8); err != nil {
			return err
		}
	}
	// Bound migration independently of the full WAL size. Publish covered
	// whole-frame windows while keeping the original commit marker intact.
	if tail.End()-e.covered > advanceBytes {
		input := io.TeeReader(io.NewSectionReader(tail, e.covered, tail.End()-e.covered), h)
		if err := checkpoint.ScanFrames(ctx, input, e.covered, tail.End(), e.first, func(_, end int64, first uint64, rows []store.Row) error {
			if end-e.covered < advanceBytes {
				return nil
			}
			next := first + uint64(len(rows))
			m, err := r.publisher.PublishCaptured(ctx, tail, end, e.manifest, next, hex.EncodeToString(h.Sum(nil)))
			if err != nil {
				return err
			}
			e.manifest, e.covered, e.first = m, end, next
			return nil
		}); err != nil {
			return err
		}
	}
	replay, err := r.budget.ReplayCapturedTail(ctx, tail, e.covered, tail.End(), e.first)
	if err != nil {
		return err
	}
	r.installReplay(replay)
	if err := tail.FinishRecovery(); err != nil {
		return err
	}
	if e.manifest != nil {
		return r.activateLegacy(ctx)
	}
	return nil
}

func (r *retained) activateLegacy(ctx context.Context) (err error) {
	old := r.current
	m, id, raw, err := r.readManifest(canonicalManifest)
	if err != nil {
		return err
	}
	if m.CoveredOffset != old.covered || m.NextOrdinal != old.first {
		return errors.New("legacy manifest changed during migration")
	}
	name := checkpoint.GenerationManifestName([32]byte(id))
	if err := r.owner.InstallImmutable(name, raw); err != nil {
		return err
	}
	a := wal.Authority{Sequence: 1, Generation: hex.EncodeToString(id[:]), CoveredOffset: old.covered,
		NextOrdinal: old.first, End: old.tail.End(), Ordinal: r.ordinal}
	tail, err := r.owner.NewTail(old.tail, a)
	if err != nil {
		return err
	}
	e := &epoch{tail: tail, name: name, id: id, manifest: m, covered: old.covered, first: old.first, sequence: a.Sequence}
	defer func() {
		if r.current != e {
			if errors.Is(err, wal.ErrUncertain) {
				err = errors.Join(err, tail.Close())
			} else {
				err = errors.Join(err, r.owner.Retire(tail))
			}
		}
	}()
	if err := r.installCurrent(ctx, e); err != nil {
		return err
	}
	if err := r.owner.Select(tail); err != nil {
		return err
	}
	r.current = e
	r.retired = append(r.retired, old)
	return nil
}

func (r *retained) recoverStaging(ctx context.Context, view recoveryView) error {
	if err := checkpoint.CheckPublication(r.owner.Directory()); err == nil {
		return nil
	} else if !errors.Is(err, checkpoint.ErrPublicationUncertain) {
		return err
	}
	sum, err := digestRange(view.epoch.tail, view.point.CoveredOffset, view.point.CommittedEnd)
	if err != nil {
		return err
	}
	view.point.SuffixSHA256 = sum
	return r.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
		return &view, nil
	})
}
