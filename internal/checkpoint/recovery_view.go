package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"bog/internal/store"
)

const generationPrefix = "bog-checkpoint-"

// GenerationManifestName is the immutable data-directory name for a new v2
// manifest. Backup images retain the canonical bog.checkpoint name.
func GenerationManifestName(id [32]byte) string {
	return generationPrefix + hex.EncodeToString(id[:])
}

func validGenerationName(name string) bool {
	return len(name) == len(generationPrefix)+64 && name[:len(generationPrefix)] == generationPrefix &&
		lowercaseDigest(name[len(generationPrefix):])
}

func (a *publicationAttempt) stageGeneration(id [32]byte, b []byte) error {
	name := GenerationManifestName(id)
	path := stageDir + "/" + name
	if err := a.writeNew(path, b); err != nil {
		return err
	}
	info, err := a.pub.root.Lstat(path)
	if err != nil {
		return err
	}
	a.manifest = &ownedSegment{Name: name, Inode: info.Sys().(*syscall.Stat_t).Ino, Size: int64(len(b)), SHA256: digest(b)}
	return nil
}

func (p *publication) validateInstalledGeneration(ctx context.Context, id [32]byte) error {
	name := GenerationManifestName(id)
	b, err := readPublicationFile(p.dir, name, maxManifestSize)
	if err != nil {
		return err
	}
	installed, actual, err := DecodeManifest(b)
	if err != nil {
		return err
	}
	if actual != id {
		return fmt.Errorf("%w: installed manifest changed", ErrIntegrity)
	}
	return validateManifestBlocks(ctx, *installed, func(name string) (io.ReadCloser, error) {
		return openPublicationFile(p.dir, name)
	}, nil)
}

func readViewManifest(ctx context.Context, view RecoveryView, point RecoveryPoint) (_ *Manifest, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := view.OpenManifest()
	if err != nil {
		return nil, fmt.Errorf("open captured checkpoint manifest: %w", err)
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	b, err := io.ReadAll(io.LimitReader(recoveryReader{ctx, r}, maxManifestSize+1))
	if err != nil {
		return nil, err
	}
	m, id, err := DecodeManifest(b)
	if err != nil {
		return nil, err
	}
	if id != point.Generation || m.CoveredOffset != point.CoveredOffset || m.NextOrdinal != point.NextOrdinal {
		return nil, fmt.Errorf("%w: captured manifest does not match recovery point", ErrIntegrity)
	}
	return m, nil
}

func (p *publication) validateViewGeneration(ctx context.Context, view RecoveryView, point RecoveryPoint) (*Manifest, error) {
	m, err := readViewManifest(ctx, view, point)
	if err != nil {
		return nil, err
	}
	if err := validateManifestBlocks(ctx, *m, view.OpenSegment, nil); err != nil {
		return nil, err
	}
	// Ensure the immutable files referenced by the successor are present in
	// the owned directory, even when the view opens them through another pin.
	var scratch [32 << 10]byte
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := verifyPreviousSegment(p.dir, s, &scratch); err != nil {
			return nil, fmt.Errorf("%w: validate installed segment %s: %w", ErrIntegrity, s.Name, err)
		}
	}
	return m, nil
}

func checkViewPoint(point RecoveryPoint, maxBytes uint64) error {
	if point.CoveredOffset < 8 || point.CommittedEnd < point.CoveredOffset ||
		point.CommittedOrdinal < point.NextOrdinal ||
		(point.CommittedEnd == point.CoveredOffset) != (point.CommittedOrdinal == point.NextOrdinal) {
		return fmt.Errorf("%w: inconsistent captured suffix bounds", ErrIntegrity)
	}
	if size := uint64(point.CommittedEnd - point.CoveredOffset); size > maxBytes {
		return &ExhaustedError{Resource: ReplayTailBytes, Limit: maxBytes, Need: size}
	}
	return nil
}

// scanView validates the entire captured suffix in one pass. The prefix passed
// to visit ends at cutoff. Splitting the decoder there enforces a whole frame
// boundary and lets the owner bind the remainder when selecting the successor.
func scanView(ctx context.Context, view RecoveryView, point RecoveryPoint, cutoff int64, ordinal uint64,
	visit func(uint64, []store.Row) error) (consumed, remaining [32]byte, err error) {
	if err := ctx.Err(); err != nil {
		return consumed, remaining, err
	}
	r, err := view.OpenSuffix()
	if err != nil {
		return consumed, remaining, fmt.Errorf("open captured suffix: %w", err)
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	in := recoveryReader{ctx, r}
	whole, part := sha256.New(), sha256.New()
	source := io.TeeReader(in, io.MultiWriter(whole, part))
	next := point.NextOrdinal
	replay := func(start, end int64, visit func(uint64, []store.Row) error) error {
		return scanFrames(ctx, source, start, end, next, func(_, _ int64, first uint64, rows []store.Row) error {
			next = first + uint64(len(rows))
			if visit != nil {
				return visit(first, rows)
			}
			return nil
		})
	}
	if err := replay(point.CoveredOffset, cutoff, visit); err != nil {
		return consumed, remaining, err
	}
	if next != ordinal {
		return consumed, remaining, fmt.Errorf("%w: checkpoint cutoff ordinal mismatch", ErrIntegrity)
	}
	consumed = [32]byte(part.Sum(nil))
	part.Reset()
	if err := replay(cutoff, point.CommittedEnd, nil); err != nil {
		return consumed, remaining, err
	}
	if err := requireEnd(in); err != nil {
		return consumed, remaining, err
	}
	if next != point.CommittedOrdinal || [32]byte(whole.Sum(nil)) != point.SuffixSHA256 {
		return consumed, remaining, fmt.Errorf("%w: captured suffix ordinal or digest mismatch", ErrIntegrity)
	}
	return consumed, [32]byte(part.Sum(nil)), ctx.Err()
}

// ErrSelectionUnchanged means the selector did not attempt to replace durable
// authority. Other selector errors require RecoverRecoveryView.
var ErrSelectionUnchanged = errors.New("checkpoint selection unchanged")

// PublishRecoveryView advances to a strict whole-frame cutoff and stages a
// v2 generation. The caller holds the WAL directory lock and view pin. The
// selector runs with the publication lock held after the files are durable.
// It checks that the predecessor remains selected, reconciles any later
// commits, and durably selects a manifest and tail together. The point it
// receives describes the remainder of the captured suffix at the new boundary.
// A returned manifest means selection succeeded, even if cleanup failed.
func (p *Publisher) PublishRecoveryView(ctx context.Context, view RecoveryView, cutoff int64, ordinal uint64,
	selectSuccessor func(context.Context, RecoveryPoint) error) (_ *Manifest, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if view == nil || selectSuccessor == nil || p.stage.budget == nil {
		return nil, errors.New("checkpoint publication requires a view, selector and budget")
	}
	if !p.mu.TryLock() {
		return nil, syscall.EWOULDBLOCK
	}
	defer p.mu.Unlock()
	if p.stage.pending {
		return nil, ErrPublicationUncertain
	}
	point := view.Point()
	if err := checkViewPoint(point, p.stage.budget.limit[ReplayTailBytes]); err != nil {
		return nil, err
	}
	if cutoff <= point.CoveredOffset || cutoff > point.CommittedEnd ||
		ordinal <= point.NextOrdinal || ordinal > point.CommittedOrdinal {
		return nil, fmt.Errorf("%w: invalid checkpoint cutoff", ErrIntegrity)
	}
	pub, lock, err := lockOwnedPublication(p.root, p.dir)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := CheckPublication(p.dir); err != nil {
		return nil, err
	}
	old, err := pub.validateViewGeneration(ctx, view, point)
	if err != nil {
		return nil, err
	}
	m := &Manifest{Version: 2, CoveredOffset: cutoff, NextOrdinal: old.NextOrdinal, Segments: old.Segments,
		Advance: &Advance{Generation: hex.EncodeToString(point.Generation[:]), CoveredOffset: point.CoveredOffset, NextOrdinal: point.NextOrdinal}}
	successor := RecoveryPoint{CoveredOffset: cutoff, NextOrdinal: ordinal, CommittedEnd: point.CommittedEnd,
		CommittedOrdinal: point.CommittedOrdinal}
	pub.stage = &p.stage
	return pub.publishGeneration(ctx, m, &ordinal, func(visit func(uint64, []store.Row) error) error {
		consumed, remaining, err := scanView(ctx, view, point, cutoff, ordinal, visit)
		m.Advance.ConsumedSHA256 = hex.EncodeToString(consumed[:])
		successor.SuffixSHA256 = remaining
		return err
	}, func(id [32]byte) error {
		successor.Generation = id
		return selectSuccessor(ctx, successor)
	})
}

// RecoverRecoveryView validates the selected view before settling a pending
// publication. The loader runs under the publication lock and must validate
// and sync the owner's authority record and tail identity. The caller holds
// the WAL directory lock and keeps the returned view pinned through recovery.
func (p *Publisher) RecoverRecoveryView(ctx context.Context, selected func() (RecoveryView, error)) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if selected == nil || p.stage.budget == nil {
		return errors.New("checkpoint recovery requires an authority loader and budget")
	}
	if !p.mu.TryLock() {
		return syscall.EWOULDBLOCK
	}
	defer p.mu.Unlock()
	pub, lock, err := lockOwnedPublication(p.root, p.dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	pub.cleanupContext = ctx
	info, err := p.root.Lstat(stageDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if info != nil {
		if err := checkStaging(info); err != nil {
			return err
		}
	}
	view, err := selected()
	if err != nil {
		return err
	}
	if view == nil {
		return fmt.Errorf("%w: missing selected recovery view", ErrIntegrity)
	}
	point := view.Point()
	if err := checkViewPoint(point, p.stage.budget.limit[ReplayTailBytes]); err != nil {
		return err
	}
	m, err := pub.validateViewGeneration(ctx, view, point)
	if err != nil {
		return err
	}
	// A restored v2 image can supply the canonical manifest through the
	// loader. An installed candidate is reachable only by payload identity.
	if _, _, err := scanView(ctx, view, point, point.CoveredOffset, point.NextOrdinal, nil); err != nil {
		return err
	}
	if err := p.dir.Sync(); err != nil {
		return err
	}
	if info != nil {
		if err := pub.recoverStaging(info, m, GenerationManifestName(point.Generation), false); err != nil {
			return errors.Join(ErrPublicationUncertain, err)
		}
	}
	p.stage.settle()
	return nil
}
