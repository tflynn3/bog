package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const canonicalManifest = "bog.checkpoint"

// recoveryView lends independent readers through a command-owned epoch pin.
// The checkpoint package cannot open or mutate the writable WAL authority.
type lockedRecovery interface {
	OpenSelected() (bool, error)
	OpenOrdinary() (int64, error)
	OpenRestore(int64, int64) (io.ReadCloser, error)
	State() (wal.Point, *wal.Tail)
	FinishRecovery(wal.Point) error
	Select(wal.Point, io.Reader, [32]byte) (*wal.Tail, error)
	Reclaim(*wal.Tail) error
	CleanStartup() error
}

type recoveryView struct {
	dir      *os.File
	name     string
	manifest *checkpoint.Manifest
	point    wal.Point
	suffix   func() (io.ReadCloser, error)
}

var _ checkpoint.RecoveryView = recoveryView{}

func (v recoveryView) Point() checkpoint.RecoveryPoint {
	p := v.point
	return checkpoint.RecoveryPoint{
		Generation: p.Generation, CoveredOffset: p.Covered, NextOrdinal: p.First,
		CommittedEnd: p.End, CommittedOrdinal: p.Next, SuffixSHA256: p.Suffix,
	}
}
func (v recoveryView) OpenManifest() (io.ReadCloser, error) {
	return history.OpenArtifact(v.dir, v.name)
}
func (v recoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	for _, segment := range v.manifest.Segments {
		if segment.Name == name {
			return history.OpenArtifact(v.dir, name)
		}
	}
	return nil, fmt.Errorf("segment %q is outside the pinned generation", name)
}
func (v recoveryView) OpenSuffix() (io.ReadCloser, error) { return v.suffix() }

func readArtifact(dir *os.File, name string, limit int64) (b []byte, err error) {
	f, err := history.OpenArtifact(dir, name)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err = io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(b)) > limit {
		err = fmt.Errorf("%s exceeds size limit", name)
	}
	return b, err
}

func readManifest(dir *os.File, name string) (*checkpoint.Manifest, [32]byte, error) {
	b, err := readArtifact(dir, name, 256<<10)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return checkpoint.DecodeManifest(b)
}

func fileView(dir *os.File, name string, m *checkpoint.Manifest, p wal.Point, tail *wal.Tail) recoveryView {
	return recoveryView{dir: dir, name: name, manifest: m, point: p,
		suffix: func() (io.ReadCloser, error) { return io.NopCloser(tail.Section(p.Covered, p.End)), nil }}
}

func (s *retained) recover(ctx context.Context, l lockedRecovery) error {
	selected, err := l.OpenSelected()
	if err != nil {
		return err
	}
	if selected {
		p, tail := l.State()
		name := checkpoint.GenerationManifestName(p.Generation)
		m, id, err := readManifest(s.dir, name)
		if err != nil {
			return err
		}
		if id != p.Generation {
			return errors.New("selected checkpoint digest mismatch")
		}
		view := fileView(s.dir, name, m, p, tail)
		return s.recoverView(ctx, l, view, true)
	}
	meta, err := readArtifact(s.dir, "bog.backup", 136)
	if err == nil {
		return s.recoverRestore(ctx, l, meta)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.recoverOrdinary(ctx, l)
}

func (s *retained) recoverView(ctx context.Context, l lockedRecovery, view recoveryView, selected bool) error {
	p := view.point
	if err := s.replay(ctx, &view, false); err != nil {
		return err
	}
	if err := s.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
		if view.manifest.CoveredOffset != p.Covered || view.manifest.NextOrdinal != p.First {
			return nil, errors.New("checkpoint boundary disagrees with WAL authority")
		}
		if _, err := history.ValidateManifest(ctx, s.dir, view.name); err != nil {
			return nil, err
		}
		if err := l.FinishRecovery(p); err != nil {
			return nil, err
		}
		return view, nil
	}); err != nil {
		return err
	}
	_, tail := l.State()
	if err := s.installInitial(ctx, view.name, view.manifest, p.Generation, tail); err != nil {
		return err
	}
	if selected {
		if err := s.catalog.ReclaimOrphans(ctx, s.root, s.dir, view.name); err != nil {
			return err
		}
	}
	return l.CleanStartup()
}

func (s *retained) recoverOrdinary(ctx context.Context, l lockedRecovery) error {
	end, err := l.OpenOrdinary()
	if err != nil {
		return err
	}
	_, tail := l.State()
	m, id, err := readManifest(s.dir, canonicalManifest)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if m == nil {
		// An interrupted v1 bootstrap has not made a manifest authoritative.
		if err := s.publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
			return nil, os.ErrNotExist
		}); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		_, err := s.publisher.PublishCaptured(ctx, tail.Section(0, end), 8, nil, 0, fmt.Sprintf("%x", sha256.Sum256([]byte("BOGWAL01"))))
		if err != nil {
			return err
		}
		m, id, err = readManifest(s.dir, canonicalManifest)
		if err != nil {
			return err
		}
	}
	if m.Version != 1 || m.CoveredOffset > end {
		return errors.New("ordinary WAL has an incompatible checkpoint")
	}
	next, _, prefix, err := scanWAL(ctx, tail, 8, m.CoveredOffset, 0, true)
	if err != nil {
		return err
	}
	if next != m.NextOrdinal || hex.EncodeToString(prefix[:]) != m.SourceSHA256 {
		return errors.New("ordinary WAL prefix disagrees with checkpoint")
	}
	next, frames, sum, err := scanWAL(ctx, tail, m.CoveredOffset, end, next, false)
	if err != nil {
		return err
	}
	if end-m.CoveredOffset > maxReplayBytes || frames > maxTailFrames {
		return s.migrateOrdinary(ctx, l, tail, end, next, m)
	}
	p := wal.Point{Generation: id, Covered: m.CoveredOffset, End: end, First: m.NextOrdinal, Next: next, Suffix: sum}
	return s.recoverView(ctx, l, fileView(s.dir, canonicalManifest, m, p, tail), false)
}

func scanWAL(ctx context.Context, tail *wal.Tail, start, end int64, ordinal uint64, magic bool) (uint64, int, [32]byte, error) {
	h := sha256.New()
	var frames int
	var sum [32]byte
	if magic {
		if _, err := io.Copy(h, tail.Section(0, 8)); err != nil {
			return 0, 0, sum, err
		}
	}
	err := checkpoint.ScanFrames(ctx, io.TeeReader(tail.Section(start, end), h), start, end, ordinal,
		func(_, _ int64, first uint64, rows []store.Row) error {
			frames++
			ordinal = first + uint64(len(rows))
			return nil
		})
	h.Sum(sum[:0])
	return ordinal, frames, sum, err
}

func (s *retained) migrateOrdinary(ctx context.Context, l lockedRecovery, tail *wal.Tail, end int64, next uint64, m *checkpoint.Manifest) error {
	// Ordinary migration has a separate disk staging ceiling before the
	// listener starts. Live replacement reserves only a bounded suffix.
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes: uint64(checkpoint.MaxSegments+1)*checkpoint.MaxBlockSize + (2 << 20),
	})
	migration := checkpoint.NewPublisher(s.root, s.dir, budget)
	if err := migration.RecoverCaptured(func() (*checkpoint.Manifest, error) {
		return history.ValidateManifest(ctx, s.dir, canonicalManifest)
	}); err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(h, tail.Section(0, end))
	if err != nil || n != end {
		return errors.Join(err, io.ErrUnexpectedEOF)
	}
	if _, err := migration.PublishCaptured(ctx, tail.Section(0, end), end, m, next, hex.EncodeToString(h.Sum(nil))); err != nil {
		return err
	}
	m, id, err := readManifest(s.dir, canonicalManifest)
	if err != nil {
		return err
	}
	p := wal.Point{Generation: id, Covered: end, End: end, First: next, Next: next, Suffix: sha256.Sum256(nil)}
	return s.activateInitial(ctx, l, m, p, tail.Section(end, end))
}

func (s *retained) recoverRestore(ctx context.Context, l lockedRecovery, b []byte) (err error) {
	p, version, err := decodeRestore(b)
	if err != nil {
		return err
	}
	m, id, err := readManifest(s.dir, canonicalManifest)
	if err != nil {
		return err
	}
	if id != p.Generation || m.CoveredOffset != p.Covered || m.NextOrdinal != p.First || (version == 1 && m.Version != 1) {
		return errors.New("restore metadata disagrees with checkpoint")
	}
	open := func() (io.ReadCloser, error) { return l.OpenRestore(p.Covered, p.End) }
	view := recoveryView{dir: s.dir, name: canonicalManifest, manifest: m, point: p, suffix: open}
	if err := s.replay(ctx, &view, version == 1); err != nil {
		return err
	}
	if err := s.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) { return view, nil }); err != nil {
		return err
	}
	r, err := open()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	return s.activateInitial(ctx, l, m, view.point, r)
}

func decodeRestore(b []byte) (p wal.Point, version int, err error) {
	suffixAt := 0
	switch {
	case len(b) == 128 && string(b[:8]) == "BOGBAK01":
		version, suffixAt = 1, 64
	case len(b) == 136 && string(b[:8]) == "BOGBAK02":
		version, suffixAt = 2, 72
	default:
		return p, 0, errors.New("unsupported restore metadata")
	}
	if sha256.Sum256(b[:suffixAt+32]) != [32]byte(b[suffixAt+32:]) {
		return p, 0, errors.New("invalid restore metadata checksum")
	}
	p = wal.Point{Generation: [32]byte(b[8:40]), Covered: int64(binary.LittleEndian.Uint64(b[40:48])),
		First: binary.LittleEndian.Uint64(b[48:56]), End: int64(binary.LittleEndian.Uint64(b[56:64])),
		Suffix: [32]byte(b[suffixAt : suffixAt+32])}
	if version == 2 {
		p.Next = binary.LittleEndian.Uint64(b[64:72])
	}
	if p.Covered < 8 || p.End < p.Covered || (version == 2 && (p.Next < p.First ||
		(p.End == p.Covered) != (p.Next == p.First))) {
		return p, 0, errors.New("invalid restore bounds")
	}
	return p, version, nil
}

// activateInitial copies an already validated canonical manifest to its
// immutable digest name. The original input remains recoverable until the
// selector makes the writable copy authoritative.
func (s *retained) activateInitial(ctx context.Context, l lockedRecovery, m *checkpoint.Manifest, p wal.Point, suffix io.Reader) (err error) {
	name := checkpoint.GenerationManifestName(p.Generation)
	b, err := readArtifact(s.dir, canonicalManifest, 256<<10)
	if err != nil {
		return err
	}
	lock, err := checkpoint.LockPublication(s.dir, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := s.publisher.InstallInitialManifest(name, b); err != nil {
		return err
	}
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	prepared, err := ticket.PrepareSelected(ctx, s.dir, name, history.GenerationID(p.Generation))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, prepared.Close()) }()
	old, err := l.Select(p, suffix, p.Suffix)
	if err != nil {
		return err
	}
	_, tail := l.State()
	s.active.name, s.active.manifest, s.active.tail = name, m, tail
	s.active.generation, err = prepared.Install()
	if old != nil {
		err = errors.Join(err, l.Reclaim(old))
	}
	return errors.Join(err, l.CleanStartup())
}

func (s *retained) installInitial(ctx context.Context, name string, m *checkpoint.Manifest, id [32]byte, tail *wal.Tail) (err error) {
	lock, err := checkpoint.LockPublication(s.dir, false)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	prepared, err := ticket.PrepareSelected(ctx, s.dir, name, history.GenerationID(id))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, prepared.Close()) }()
	s.active.name, s.active.manifest, s.active.tail = name, m, tail
	s.active.generation, err = prepared.Install()
	return err
}

func (s *retained) replay(ctx context.Context, v *recoveryView, deriveOrdinal bool) (err error) {
	p := &v.point
	size := p.End - p.Covered
	if size < 0 || size > maxReplayBytes {
		return wal.ErrCapacity
	}
	input, err := v.OpenSuffix()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	h := sha256.New()
	next, err := s.budget.ReplayTail(ctx, io.TeeReader(input, h), p.Covered, p.End, p.First,
		func(_, end int64, first uint64, rows []store.Row, charge *checkpoint.Reservation) error {
			if len(s.active.batches) >= maxTailFrames {
				return wal.ErrCapacity
			}
			s.active.batches = append(s.active.batches, makeBatch(end, first, rows, charge))
			return nil
		})
	if err != nil {
		return err
	}
	var sum [32]byte
	h.Sum(sum[:0])
	if sum != p.Suffix || (!deriveOrdinal && next != p.Next) || next > math.MaxInt64 {
		return errors.New("invalid recovered suffix digest or ordinals")
	}
	p.Next = next
	return nil
}
