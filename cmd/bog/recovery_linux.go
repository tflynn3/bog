package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const manifestLimit = 256 << 10

func openRetained(ctx context.Context, path string) (_ *retained, err error) {
	dir, err := wal.LockDirectory(path)
	if err != nil {
		return nil, err
	}
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      1 << 30,
		checkpoint.ReplayTailBytes:   maxTailBytes,
		checkpoint.PinnedGenerations: 4,
		checkpoint.LiveCaptures:      maxLiveCaptures + 16, // cursors, transient queries, and maintenance
	})
	s := &retained{dir: dir, budget: budget, catalog: history.NewCatalog(budget)}
	s.publisher = checkpoint.NewPublisher(dir.Root, dir.File, budget)
	defer func() {
		if err != nil {
			err = errors.Join(err, s.closeStorage(), dir.Close())
		}
	}()
	if err := dir.SyncParent(); err != nil {
		return nil, err
	}
	if err := s.recover(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *retained) readFile(name string, limit int64) (_ []byte, err error) {
	f, err := s.dir.OpenRegular(name, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(b)) > limit {
		err = fmt.Errorf("%s exceeds %d bytes", name, limit)
	}
	return b, err
}

func (s *retained) exists(name string) (bool, error) {
	_, err := s.dir.Root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (s *retained) recover(ctx context.Context) error {
	raw, err := s.readFile(authorityName, 4<<10)
	if err == nil {
		a, err := decodeAuthority(raw)
		if err != nil {
			return err
		}
		return s.recoverSelected(ctx, a)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	meta, metaErr := s.exists(backupMetadata)
	tail, tailErr := s.exists(backupTail)
	if err := errors.Join(metaErr, tailErr); err != nil {
		return err
	}
	if meta || tail {
		for _, name := range []string{wal.LegacyName, wal.LegacyMarkerName} {
			exists, err := s.exists(name)
			if err != nil {
				return err
			}
			if exists {
				return fmt.Errorf("mixed ordinary WAL and restore image (%s)", name)
			}
		}
		return s.recoverRestore(ctx)
	}
	return s.recoverLegacy(ctx)
}

func (s *retained) readManifest(name string) ([]byte, *checkpoint.Manifest, [32]byte, error) {
	raw, err := s.readFile(name, manifestLimit)
	if err != nil {
		return nil, nil, [32]byte{}, err
	}
	m, id, err := checkpoint.DecodeManifest(raw)
	return raw, m, id, err
}

func (s *retained) recoverSelected(ctx context.Context, a authority) error {
	p, err := a.point()
	if err != nil {
		return err
	}
	if p.CommittedEnd-p.CoveredOffset > maxTailBytes {
		return checkpoint.ErrExhausted
	}
	name := checkpoint.GenerationManifestName(p.Generation)
	_, m, id, err := s.readManifest(name)
	if err != nil {
		return err
	}
	if id != p.Generation || m.CoveredOffset != p.CoveredOffset || m.NextOrdinal != p.NextOrdinal {
		return errors.New("selected manifest does not match writable authority")
	}
	e, err := s.openEpoch(a.Tail, a.TailBase, m)
	if err != nil {
		return err
	}
	s.active = e // startup owns cleanup until recovery completes
	e.number = a.Sequence
	if !fileMatches(e.info, a.TailDevice, a.TailInode) {
		return errors.New("selected tail file identity changed")
	}
	s.point, s.selector = p, a
	end, sum, err := s.replay(ctx, e, p.CoveredOffset, p.CommittedEnd, p.NextOrdinal)
	if err != nil {
		return err
	}
	if end != p.CommittedOrdinal || sum != p.SuffixSHA256 {
		return errors.New("selected suffix digest or ordinal mismatch")
	}
	v := &recoveryView{s: s, e: e, point: p, manifestName: name}
	if err := s.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
		if err := s.dir.File.Sync(); err != nil {
			return nil, err
		}
		return v, nil
	}); err != nil {
		return err
	}
	if err := wal.FinishRecovery(e.file, p.CommittedEnd-e.base); err != nil {
		return err
	}
	if err := s.dir.CleanStages(); err != nil {
		return err
	}
	if err := s.installGeneration(ctx); err != nil {
		return err
	}
	if err := s.recoverSwitches(ctx); err != nil {
		return err
	}
	for _, name := range []string{wal.LegacyName, wal.LegacyMarkerName} {
		exists, err := s.exists(name)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("untracked ordinary WAL beside retained authority (%s)", name)
		}
	}
	return nil
}

func (s *retained) openEpoch(name string, base int64, m *checkpoint.Manifest) (*epoch, error) {
	f, err := s.dir.OpenRegular(name, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &epoch{file: f, info: info, name: name, base: base, manifest: m}, nil
}

func (s *retained) replay(ctx context.Context, e *epoch, start, end int64, first uint64) (uint64, [32]byte, error) {
	h := sha256.New().(hash.Cloner)
	in := io.TeeReader(io.NewSectionReader(e.file, start-e.base, end-start), h)
	next := first
	err := checkpoint.ScanFrames(ctx, in, start, end, first, func(pos, end int64, first uint64, rows []store.Row) error {
		if len(e.batches) == maxTailBatches {
			return checkpoint.ErrExhausted
		}
		b, err := s.newBatch(ctx, pos, end, first, rows)
		if err != nil {
			return err
		}
		next = b.next
		e.batches = append(e.batches, b)
		e.weight += b.weight
		e.maxLine = max(e.maxLine, b.maxLine)
		return nil
	})
	if err == nil {
		s.digest = h
	}
	return next, [32]byte(h.Sum(nil)), err
}

func (s *retained) recoverRestore(ctx context.Context) (err error) {
	point, tail, err := backup.VerifyPinnedRecovery(ctx, s.dir.Root, func(start, end int64) error {
		if end-start > maxTailBytes {
			return checkpoint.ErrExhausted
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("verify restore: %w", err)
	}
	defer func() { err = errors.Join(err, tail.Close()) }()
	raw, m, id, err := s.readManifest(canonicalManifest)
	if err != nil {
		return err
	}
	if id != point.Generation {
		return errors.New("restore manifest changed after verification")
	}
	if err := s.dir.CleanStages(); err != nil {
		return err
	}
	if err := s.recoverSwitches(ctx); err != nil {
		return err
	}
	return s.activateInitial(ctx, raw, m, checkpoint.RecoveryPoint(point), tail)
}

func (s *retained) activateInitial(ctx context.Context, raw []byte, m *checkpoint.Manifest, p checkpoint.RecoveryPoint, source io.ReaderAt) (err error) {
	lock, err := checkpoint.LockPublication(s.dir.File, true)
	if err != nil {
		return err
	}
	defer func() {
		if lock != nil {
			err = errors.Join(err, lock.Close())
		}
	}()
	name := checkpoint.GenerationManifestName(p.Generation)
	old, err := s.readFile(name, manifestLimit)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if _, err := s.dir.Select(name, raw); err != nil {
			return err
		}
	case err != nil:
		return err
	case !bytes.Equal(old, raw):
		return errors.New("existing named manifest differs from verified manifest")
	}
	f, a, _, err := s.stageTail(ctx, p, source, 0)
	if err != nil {
		return err
	}
	e, err := newEpoch(f, a, m)
	if err != nil {
		return s.discardSwitch(f, a, err)
	}
	defer func() {
		if s.active != e {
			releaseBatches(e.batches)
			err = s.discardSwitch(f, a, err)
		}
	}()
	next, sum, err := s.replay(ctx, e, p.CoveredOffset, p.CommittedEnd, p.NextOrdinal)
	if err != nil {
		return err
	}
	if next != p.CommittedOrdinal || sum != p.SuffixSHA256 {
		return errors.New("writable activation differs from verified recovery point")
	}
	switchRecord, err := s.prepareSwitch(f, a)
	if err != nil {
		return err
	}
	if _, err := s.selectAuthority(a); err != nil {
		return err
	}
	previous := s.active
	s.active, s.point, s.selector = e, p, a
	if previous != nil {
		previous.retirement = switchRecord
		s.retired = append(s.retired, previous)
	} else {
		s.pendingRetirement = append(s.pendingRetirement, switchRecord)
	}
	err = lock.Close()
	lock = nil
	if err != nil {
		return err
	}
	if err := s.installGeneration(ctx); err != nil {
		return err
	}
	return s.reclaimLocked()
}

func newEpoch(f *os.File, a authority, m *checkpoint.Manifest) (*epoch, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &epoch{file: f, info: info, name: a.Tail, base: a.TailBase, manifest: m, number: a.Sequence}, nil
}

func (s *retained) installGeneration(ctx context.Context) error {
	if s.selector.Version == 0 {
		g, err := s.catalog.ReplacePinned(ctx, s.dir.File)
		s.active.gen = g
		return err
	}
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	g, err := ticket.ReplaceSelected(ctx, s.dir.File, history.GenerationID(s.point.Generation))
	s.active.gen = g
	return err
}

func scanPrefix(ctx context.Context, f *os.File, end int64) (uint64, [32]byte, error) {
	h := sha256.New()
	in := io.TeeReader(io.NewSectionReader(f, 0, end), h)
	var magic [len(wal.Magic)]byte
	if _, err := io.ReadFull(in, magic[:]); err != nil {
		return 0, [32]byte{}, err
	}
	if string(magic[:]) != wal.Magic {
		return 0, [32]byte{}, errors.New("invalid WAL magic")
	}
	var next uint64
	err := checkpoint.ScanFrames(ctx, in, int64(len(magic)), end, 0, func(_, _ int64, first uint64, rows []store.Row) error {
		next = first + uint64(len(rows))
		return nil
	})
	return next, [32]byte(h.Sum(nil)), err
}

func (s *retained) recoverLegacy(ctx context.Context) error {
	f, end, marked, err := s.dir.OpenLegacy()
	if err != nil {
		return err
	}
	e := &epoch{file: f, name: wal.LegacyName}
	s.active = e
	e.info, err = f.Stat()
	if err != nil {
		return err
	}
	var m *checkpoint.Manifest
	err = s.publisher.RecoverOrdinary(ctx, func() (*checkpoint.Manifest, error) {
		prev, err := history.ValidatePinnedAuthority(ctx, s.dir.File)
		if err != nil {
			return nil, err
		}
		if prev.CoveredOffset > end {
			return nil, errors.New("checkpoint extends past committed WAL")
		}
		rows, sum, err := scanPrefix(ctx, f, prev.CoveredOffset)
		if err != nil {
			return nil, err
		}
		if rows != prev.NextOrdinal || hexDigest(sum) != prev.SourceSHA256 {
			return nil, errors.New("checkpoint disagrees with committed WAL prefix")
		}
		m = prev
		return m, nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if m == nil {
		if _, _, err := scanPrefix(ctx, f, end); err != nil {
			return err
		}
		sum := sha256.Sum256([]byte(wal.Magic))
		m, err = s.publisher.PublishCaptured(ctx, f, int64(len(wal.Magic)), nil, 0, hexDigest(sum))
		if err != nil {
			return err
		}
	}
	migrate := end-m.CoveredOffset >= autoTailBytes
	var next uint64
	var sum [32]byte
	if !migrate {
		next, sum, err = s.replay(ctx, e, m.CoveredOffset, end, m.NextOrdinal)
		migrate = errors.Is(err, checkpoint.ErrExhausted)
		if err != nil && !migrate {
			return err
		}
	}
	if migrate {
		releaseBatches(e.batches)
		e.batches = nil
		e.weight = 0
		next, sum, err = scanPrefix(ctx, f, end)
		if err != nil {
			return err
		}
		if _, err := s.publisher.PublishCaptured(ctx, f, end, m, next, hexDigest(sum)); err != nil {
			return err
		}
	}
	if err := s.dir.FinishLegacy(f, end, marked); err != nil {
		return err
	}
	if err := s.recoverSwitches(ctx); err != nil {
		return err
	}
	raw, m, id, err := s.readManifest(canonicalManifest)
	if err != nil {
		return err
	}
	e.manifest = m
	p := checkpoint.RecoveryPoint{Generation: id, CoveredOffset: m.CoveredOffset, NextOrdinal: m.NextOrdinal,
		CommittedEnd: end, CommittedOrdinal: next, SuffixSHA256: sum}
	if migrate {
		p.SuffixSHA256 = sha256.Sum256(nil)
		return s.activateInitial(ctx, raw, m, p, nil)
	}
	s.point = p
	return s.installGeneration(ctx)
}
