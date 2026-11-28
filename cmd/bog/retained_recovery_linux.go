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

func (r *retainedRuntime) recover(ctx context.Context) error {
	point, err := r.wal.ReadAuthority()
	switch {
	case err == nil:
		name := checkpoint.GenerationManifestName(point.Generation)
		if err := r.wal.RecoverAuthority(point, tailLimit, func(input io.ReaderAt, a wal.Authority) (*store.Store, error) {
			view := recoveryView{r.dir, name, input, recoveryPoint(a)}
			if err := r.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) { return view, nil }); err != nil {
				return nil, err
			}
			return r.replay(ctx, input, a)
		}); err != nil {
			return err
		}
		if err := r.install(ctx, point.Generation, name); err != nil {
			return err
		}
	case !errors.Is(err, os.ErrNotExist):
		return err
	default:
		restore, err := r.hasRestore()
		if err != nil {
			return err
		}
		if restore {
			if err := r.restore(ctx); err != nil {
				return err
			}
		} else if err := r.recoverLegacy(ctx); err != nil {
			return err
		}
	}
	return r.wal.RecoverTails()
}

func (r *retainedRuntime) hasRestore() (bool, error) {
	restore, legacy := false, false
	for _, name := range []string{"bog.backup", "bog.wal.tail", "bog.wal", "bog.commit"} {
		_, err := r.root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		if name == "bog.backup" || name == "bog.wal.tail" {
			restore = true
		} else {
			legacy = true
		}
	}
	if restore && legacy {
		return false, errors.New("mixed restore and legacy WAL authority")
	}
	if !restore && !legacy {
		if _, err := r.root.Lstat(manifestName); !errors.Is(err, os.ErrNotExist) {
			return false, errors.Join(errors.New("checkpoint exists without writable WAL authority"), err)
		}
	}
	return restore, nil
}

func (r *retainedRuntime) restore(ctx context.Context) (err error) {
	point, input, err := backup.VerifyPinnedRecovery(ctx, r.root, func(start, end int64) error {
		reservation, err := r.budget.ReserveReplayTail(ctx, start, end)
		if err == nil {
			reservation.Release()
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("verify restore: %w", err)
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	a := wal.Authority{
		Generation: point.Generation, Covered: point.CoveredOffset, End: point.CommittedEnd,
		First: point.NextOrdinal, Next: point.CommittedOrdinal, SuffixSHA256: point.SuffixSHA256,
	}
	if err := r.publisher.InstallCanonicalGeneration(ctx, a.Generation); err != nil {
		return err
	}
	if err := r.install(ctx, a.Generation, checkpoint.GenerationManifestName(a.Generation)); err != nil {
		return err
	}
	return r.wal.Select(a, input, tailLimit, func(input io.ReaderAt, a wal.Authority) (*store.Store, error) {
		return r.replay(ctx, input, a)
	})
}

func (r *retainedRuntime) recoverLegacy(ctx context.Context) error {
	input, end, finish, err := r.wal.OpenLegacyRuntime()
	if err != nil {
		return err
	}
	covered, first := int64(8), uint64(0)
	var previous *checkpoint.Manifest
	var generation [32]byte
	err = r.publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
		m, id, err := history.ValidatePinnedGeneration(ctx, r.dir)
		if err != nil {
			return nil, err
		}
		if m.CoveredOffset > end {
			return nil, errors.New("checkpoint covers uncommitted WAL bytes")
		}
		sum, err := digestRange(input, 0, m.CoveredOffset)
		if err != nil {
			return nil, err
		}
		if m.SourceSHA256 != hex.EncodeToString(sum[:]) {
			return nil, errors.New("checkpoint WAL prefix digest mismatch")
		}
		previous, generation = m, id
		covered, first = m.CoveredOffset, m.NextOrdinal
		return m, nil
	})
	pendingAdvance := previous != nil && errors.Is(err, checkpoint.ErrPublicationUncertain)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !pendingAdvance {
		return err
	}
	if covered == 8 && first != 0 {
		return errors.New("empty WAL prefix has a nonzero checkpoint ordinal")
	}

	// Count and validate the full ordinary WAL in bounded frames. Only
	// retain a small uncovered prefix, or stream it into a checkpoint.
	next := uint64(0)
	boundarySeen := covered == 8
	keep := end-covered < checkpointTrigger
	builder := store.NewRecoveryBuilder()
	err = checkpoint.WalkCapturedFrames(ctx, input, 8, end, 0, func(pos int64, ordinal uint64, rows []store.Row) error {
		next = ordinal + uint64(len(rows))
		if pos <= covered {
			if pos == covered {
				boundarySeen = true
				if next != first {
					return errors.New("checkpoint WAL ordinal mismatch")
				}
			}
			return nil
		}
		if !boundarySeen || ordinal < first {
			return errors.New("checkpoint boundary splits a WAL frame")
		}
		if keep && uint64(end-covered)+192*(next-first) >= checkpointTrigger {
			keep, builder = false, nil
		}
		if keep {
			builder.Add(rows)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if pendingAdvance {
		// WAL authority was never switched. Validate the canonical predecessor
		// and committed suffix before removing the interrupted advance.
		sum, err := digestRange(input, covered, end)
		if err != nil {
			return err
		}
		view := recoveryView{r.dir, manifestName, input, checkpoint.RecoveryPoint{
			Generation: generation, CoveredOffset: covered, NextOrdinal: first,
			CommittedEnd: end, CommittedOrdinal: next, SuffixSHA256: sum,
		}}
		if err := r.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) { return view, nil }); err != nil {
			return err
		}
	}
	if !keep || previous == nil {
		cutoff, ordinal := covered, first
		if !keep {
			cutoff, ordinal = end, next
		}
		sum, err := digestRange(input, 0, cutoff)
		if err != nil {
			return err
		}
		previous, err = r.publisher.PublishCaptured(ctx, input, cutoff, previous, ordinal, hex.EncodeToString(sum[:]))
		if err != nil {
			return err
		}
		covered, first = previous.CoveredOffset, previous.NextOrdinal
	}
	tail := &store.Store{}
	if keep {
		builder.Install(tail)
	}
	if uint64(tail.PublishedCount()) != next-first {
		return errors.New("legacy checkpoint and committed WAL ordinal mismatch")
	}
	if err := finish(tail, next, tailLimit, covered, first); err != nil {
		return err
	}
	gen, err := r.catalog.ReplacePinned(ctx, r.dir)
	if err != nil {
		return err
	}
	r.generation, r.manifest = gen, manifestName
	if covered == 8 {
		return nil
	}
	if err := r.publisher.InstallCanonicalGeneration(ctx, gen.ID); err != nil {
		return err
	}
	if err := r.install(ctx, gen.ID, checkpoint.GenerationManifestName(gen.ID)); err != nil {
		return err
	}
	return r.wal.Select(wal.Authority{Generation: gen.ID, Covered: covered, First: first}, nil, tailLimit,
		func(input io.ReaderAt, a wal.Authority) (*store.Store, error) { return r.replay(ctx, input, a) })
}

func (r *retainedRuntime) replay(ctx context.Context, input io.ReaderAt, a wal.Authority) (*store.Store, error) {
	reservation, err := r.budget.ReserveReplayTail(ctx, a.Covered, a.End)
	if err != nil {
		return nil, err
	}
	defer reservation.Release()
	builder := store.NewRecoveryBuilder()
	next := a.First
	if err := checkpoint.WalkCapturedFrames(ctx, input, a.Covered, a.End, a.First, func(_ int64, first uint64, rows []store.Row) error {
		next = first + uint64(len(rows))
		if next > a.Next {
			return errors.New("committed WAL tail has excess rows")
		}
		builder.Add(rows)
		return nil
	}); err != nil {
		return nil, err
	}
	if next != a.Next {
		return nil, errors.New("committed WAL ordinal mismatch")
	}
	s := &store.Store{}
	builder.Install(s)
	return s, nil
}

func (r *retainedRuntime) install(ctx context.Context, id [32]byte, name string) error {
	ticket, err := r.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	gen, err := ticket.ReplaceSelected(ctx, r.dir, id)
	if err == nil {
		r.generation, r.manifest = gen, name
	}
	return err
}

func recoveryPoint(a wal.Authority) checkpoint.RecoveryPoint {
	return checkpoint.RecoveryPoint{
		Generation: a.Generation, CoveredOffset: a.Covered, CommittedEnd: a.End,
		NextOrdinal: a.First, CommittedOrdinal: a.Next, SuffixSHA256: a.SuffixSHA256,
	}
}

type recoveryView struct {
	dir      *os.File
	manifest string
	input    io.ReaderAt
	point    checkpoint.RecoveryPoint
}

func (v recoveryView) Point() checkpoint.RecoveryPoint { return v.point }
func (v recoveryView) OpenManifest() (io.ReadCloser, error) {
	return history.OpenRecoveryFile(v.dir, v.manifest)
}
func (v recoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	return history.OpenRecoveryFile(v.dir, name)
}
func (v recoveryView) OpenSuffix() (io.ReadCloser, error) {
	return io.NopCloser(io.NewSectionReader(v.input, v.point.CoveredOffset, v.point.CommittedEnd-v.point.CoveredOffset)), nil
}

func digestRange(input io.ReaderAt, start, end int64) ([32]byte, error) {
	h := sha256.New()
	_, err := io.CopyN(h, io.NewSectionReader(input, start, end-start), end-start)
	return [32]byte(h.Sum(nil)), err
}
