package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/wal"
)

const (
	ordinaryManifest = "bog.checkpoint"
	manifestLimit    = 256 << 10
	checkpointBytes  = 16 << 20
	checkpointFrames = 4096
	maxRetired       = 8
)

type retained struct {
	wal       *wal.Log
	root      *os.Root
	dir       *os.File
	publisher *checkpoint.Publisher
	catalog   *history.Catalog
	budget    *checkpoint.Budget

	// mu pairs the installed catalog generation with the WAL selector and
	// protects view ownership and maintenance status.
	mu        sync.Mutex
	current   history.Generation
	manifest  string
	views     map[string]*retainedView
	nextID    uint64
	retired   []retiredGeneration
	pending   compactionJob
	requested uint64
	running   bool
	stopping  atomic.Bool
	failed    error
	ordinal   uint64 // Durable publication sequence.
	wake      chan struct{}
	done      chan struct{}
	fatal     chan error
	cancel    context.CancelFunc
	auto      bool

	progressMu sync.Mutex
	progress   chan struct{}

	// Maintenance owns this flag through drain. The selected view is known,
	// but the publisher still holds staging charges for unfinished cleanup.
	cleanupPending bool
}

type retiredGeneration struct {
	generation history.Generation
	manifest   string
	tail       wal.RetiredTail
	ordinary   bool
}

func openRetained(path string) (_ *retained, err error) {
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      4 << 30,
		checkpoint.ReplayTailBytes:   wal.MaxTailBytes,
		checkpoint.PinnedGenerations: 4,
		checkpoint.LiveCaptures:      72, // 64 HTTP cursors plus transient reads and publication.
	})
	s := &retained{
		catalog: history.NewCatalog(budget), budget: budget, views: make(map[string]*retainedView),
		wake: make(chan struct{}, 1), done: make(chan struct{}), fatal: make(chan error, 1),
		progress: make(chan struct{}),
	}
	_, err = wal.OpenRuntime(path, func(w *wal.Log) error {
		s.wal = w
		s.root, s.dir = w.Directory()
		s.publisher = checkpoint.NewPublisher(s.root, s.dir, budget)
		if err := s.recover(context.Background()); err != nil {
			return errors.Join(err, s.catalog.Close())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *retained) recover(ctx context.Context) error {
	a, err := s.wal.ReadAuthority()
	switch {
	case err == nil:
		if err := s.recoverSelected(ctx, a); err != nil {
			return err
		}
		if err := s.recoverRetiredGenerations(ctx); err != nil {
			return err
		}
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("read retained authority: %w", err)
	default:
		restored, err := s.anyEntries("bog.backup", "bog.wal.tail")
		if err != nil {
			return err
		}
		if restored {
			mixed, err := s.anyEntries("bog.wal", "bog.commit")
			if err != nil {
				return err
			}
			if mixed {
				return errors.New("mixed ordinary WAL and restore image")
			}
			if err := s.restore(ctx); err != nil {
				return fmt.Errorf("verify and activate restore: %w", err)
			}
		} else if err := s.recoverOrdinary(ctx); err != nil {
			return err
		}
	}
	return s.wal.RecoverTails()
}

func (s *retained) recoverRetiredGenerations(ctx context.Context) error {
	name, id := s.manifest, [32]byte(s.current.ID)
	var predecessorNames []string
	for {
		b, actual, err := s.readManifest(name)
		if errors.Is(err, os.ErrNotExist) && len(predecessorNames) != 0 {
			predecessorNames = predecessorNames[:len(predecessorNames)-1]
			break
		}
		if err != nil || actual != id {
			return errors.Join(errors.New("invalid retained generation ancestry"), err)
		}
		if name != s.manifest {
			if err := s.catalog.RegisterRetired(ctx, s.dir, history.GenerationID(id)); err != nil {
				return err
			}
		}
		m, _, err := checkpoint.DecodeManifest(b)
		if err != nil {
			return err
		}
		if m.Advance == nil {
			break
		}
		if len(predecessorNames) == maxRetired {
			return errors.New("too many unreclaimed checkpoint predecessors")
		}
		if _, err := hex.Decode(id[:], []byte(m.Advance.Generation)); err != nil {
			return err
		}
		name = checkpoint.GenerationManifestName(id)
		predecessorNames = append(predecessorNames, name)
	}
	if err := s.catalog.ReclaimSelected(ctx, s.dir, s.current.ID); err != nil {
		return err
	}
	for i := len(predecessorNames) - 1; i >= 0; i-- {
		if err := s.removeRuntimeFile(predecessorNames[i]); err != nil {
			return err
		}
		if err := s.dir.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (s *retained) anyEntries(names ...string) (bool, error) {
	found := false
	for _, name := range names {
		_, err := s.root.Lstat(name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		found = found || err == nil
	}
	return found, nil
}

func (s *retained) recoverSelected(ctx context.Context, a wal.Authority) error {
	return s.wal.OpenSelected(a, func(c *wal.Capture) error {
		point, err := checkpointPoint(a)
		if err != nil {
			return err
		}
		view := &recoveryView{s: s, name: checkpoint.GenerationManifestName(point.Generation), input: c, point: point}
		switch err := checkpoint.CheckPublication(s.dir); {
		case errors.Is(err, checkpoint.ErrPublicationUncertain):
			if err := s.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) { return view, nil }); err != nil {
				return err
			}
		case err != nil:
			return err
		}
		ticket, err := s.catalog.ReserveReplacement(ctx)
		if err != nil {
			return err
		}
		defer ticket.Close()
		if err := s.install(ctx, ticket, point.Generation, false); err != nil {
			return err
		}
		s.ordinal = a.Sequence
		return s.checkBoundary(point.CoveredOffset, point.NextOrdinal)
	})
}

func (s *retained) restore(ctx context.Context) (err error) {
	var charge *checkpoint.Reservation
	point, input, err := backup.VerifyPinnedRecovery(ctx, s.root, func(start, end int64) error {
		var err error
		charge, err = s.budget.ReserveReplayTail(ctx, start, end)
		return err
	})
	if charge != nil {
		defer charge.Release()
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	sel := wal.Authority{
		Generation: hex.EncodeToString(point.Generation[:]), CoveredOffset: point.CoveredOffset,
		NextOrdinal: point.NextOrdinal, CommittedEnd: point.CommittedEnd,
		CommittedOrdinal: point.CommittedOrdinal, SuffixSHA256: hex.EncodeToString(point.SuffixSHA256[:]),
	}
	return s.activate(ctx, sel, point.Generation, input)
}

func (s *retained) recoverOrdinary(ctx context.Context) (err error) {
	var previous *checkpoint.Manifest
	err = s.wal.OpenOrdinary(func(f *os.File, end int64, ordinal uint64) error {
		err := s.publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
			m, err := history.ValidatePinnedAuthority(ctx, s.dir)
			if err != nil {
				return nil, err
			}
			if m.Version != 1 || m.CoveredOffset > end || m.NextOrdinal > ordinal {
				return nil, errors.New("ordinary checkpoint exceeds validated WAL boundary")
			}
			if err := wal.ValidatePrefix(f, m.CoveredOffset, m.NextOrdinal); err != nil {
				return nil, err
			}
			digest, err := rangeDigest(f, 0, m.CoveredOffset)
			if err != nil {
				return nil, err
			}
			if m.SourceSHA256 != hex.EncodeToString(digest[:]) {
				return nil, errors.New("ordinary checkpoint WAL prefix checksum mismatch")
			}
			previous = m
			return m, nil
		})
		if errors.Is(err, checkpoint.ErrPublicationUncertain) && previous != nil {
			_, id, err := s.readManifest(ordinaryManifest)
			if err != nil {
				return err
			}
			sum, err := rangeDigest(f, previous.CoveredOffset, end)
			if err != nil {
				return err
			}
			view := &recoveryView{s: s, name: ordinaryManifest, input: f, point: checkpoint.RecoveryPoint{
				Generation: id, CoveredOffset: previous.CoveredOffset, NextOrdinal: previous.NextOrdinal,
				CommittedEnd: end, CommittedOrdinal: ordinal, SuffixSHA256: sum,
			}}
			return s.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) { return view, nil })
		}
		if errors.Is(err, os.ErrNotExist) && previous == nil {
			// Only an absent canonical manifest permits first publication.
			if _, statErr := s.root.Lstat(ordinaryManifest); statErr == nil || !errors.Is(statErr, os.ErrNotExist) {
				return errors.Join(err, statErr)
			}
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	c, err := s.wal.Capture(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.Release()) }()
	end, ordinal := c.CommittedEnd(), c.NextOrdinal()
	migrate := end-8 >= checkpointBytes || c.FrameCount() >= checkpointFrames || previous != nil && previous.NextOrdinal != 0
	target, next := int64(8), uint64(0)
	if migrate {
		target, next = end, ordinal
	}
	if previous == nil || previous.CoveredOffset != target {
		sum, err := rangeDigest(c, 0, target)
		if err != nil {
			return err
		}
		if _, err := s.publisher.PublishCaptured(ctx, c, target, previous, next, hex.EncodeToString(sum[:])); err != nil {
			return err
		}
	}
	_, id, err := s.readManifest(ordinaryManifest)
	if err != nil {
		return err
	}
	if migrate {
		sel := wal.Authority{
			Generation: hex.EncodeToString(id[:]), CoveredOffset: end,
			NextOrdinal: ordinal, CommittedEnd: end, CommittedOrdinal: ordinal,
		}
		return s.activate(ctx, sel, id, c)
	}
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	return s.install(ctx, ticket, id, true)
}

func (s *retained) activate(ctx context.Context, a wal.Authority, id [32]byte, input io.ReaderAt) (err error) {
	raw, actual, err := s.readManifest(ordinaryManifest)
	if err != nil {
		return err
	}
	if actual != id {
		return errors.New("activation manifest changed after verification")
	}
	if err := s.wal.EnsureArtifact(checkpoint.GenerationManifestName(id), raw); err != nil {
		return err
	}
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	if err := s.install(ctx, ticket, id, false); err != nil {
		return err
	}
	if err := s.checkBoundary(a.CoveredOffset, a.NextOrdinal); err != nil {
		return err
	}
	if a.NextOrdinal != 0 {
		a.Sequence = 1
	}
	tail, err := s.wal.Activate(ctx, a, input)
	s.ordinal = a.Sequence
	if tail.Drained != nil {
		s.retired = append(s.retired, retiredGeneration{tail: tail, ordinary: true})
	}
	return err
}

func (s *retained) checkBoundary(at int64, ordinal uint64) error {
	if s.current.Boundary.CoveredOffset != at || s.current.Boundary.NextOrdinal != ordinal {
		return errors.New("selected checkpoint and WAL boundaries differ")
	}
	return nil
}

func (s *retained) install(ctx context.Context, ticket *history.Replacement, id [32]byte, ordinary bool) error {
	var g history.Generation
	var err error
	name := checkpoint.GenerationManifestName(id)
	if ordinary {
		name = ordinaryManifest
		g, err = ticket.ReplacePinned(ctx, s.dir)
	} else {
		g, err = ticket.ReplaceSelected(ctx, s.dir, id)
	}
	if g.Drained != nil {
		s.current, s.manifest = g, name
	}
	return err
}

func (s *retained) readManifest(name string) (_ []byte, _ [32]byte, err error) {
	f, err := s.wal.ReadArtifact(name)
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, manifestLimit+1))
	if err != nil {
		return nil, [32]byte{}, err
	}
	_, id, err := checkpoint.DecodeManifest(b)
	return b, id, err
}

func rangeDigest(input io.ReaderAt, start, end int64) ([32]byte, error) {
	h := sha256.New()
	n, err := io.Copy(h, io.NewSectionReader(input, start, end-start))
	if err == nil && n != end-start {
		err = io.ErrUnexpectedEOF
	}
	return [32]byte(h.Sum(nil)), err
}

func checkpointPoint(a wal.Authority) (point checkpoint.RecoveryPoint, err error) {
	point.CoveredOffset, point.NextOrdinal = a.CoveredOffset, a.NextOrdinal
	point.CommittedEnd, point.CommittedOrdinal = a.CommittedEnd, a.CommittedOrdinal
	_, genErr := hex.Decode(point.Generation[:], []byte(a.Generation))
	_, sumErr := hex.Decode(point.SuffixSHA256[:], []byte(a.SuffixSHA256))
	return point, errors.Join(genErr, sumErr)
}

type recoveryView struct {
	s     *retained
	input io.ReaderAt
	point checkpoint.RecoveryPoint
	name  string

	segmentsOnce sync.Once
	segments     map[string]bool
	segmentsErr  error
}

func (v *recoveryView) Point() checkpoint.RecoveryPoint { return v.point }
func (v *recoveryView) OpenManifest() (io.ReadCloser, error) {
	f, err := v.s.wal.ReadArtifact(v.name)
	if err != nil {
		return nil, err
	}
	return f, nil
}
func (v *recoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	v.segmentsOnce.Do(func() {
		raw, id, err := v.s.readManifest(v.name)
		if err != nil || id != v.point.Generation {
			v.segmentsErr = errors.Join(errors.New("captured manifest changed"), err)
			return
		}
		m, _, err := checkpoint.DecodeManifest(raw)
		if err != nil {
			v.segmentsErr = err
			return
		}
		v.segments = make(map[string]bool, len(m.Segments))
		for _, segment := range m.Segments {
			v.segments[segment.Name] = true
		}
	})
	if v.segmentsErr != nil {
		return nil, v.segmentsErr
	}
	if !v.segments[name] {
		return nil, fmt.Errorf("segment %q is outside the captured manifest", name)
	}
	f, err := v.s.wal.ReadArtifact(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}
func (v *recoveryView) OpenSuffix() (io.ReadCloser, error) {
	p := v.point
	return io.NopCloser(io.NewSectionReader(v.input, p.CoveredOffset, p.CommittedEnd-p.CoveredOffset)), nil
}
