package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	tailLimit         = 64 << 20
	checkpointTrigger = 12 << 20
	captureMetadata   = 24 << 20
	manifestName      = "bog.checkpoint"
)

type retainedRuntime struct {
	views     int
	viewsDone chan struct{}
	viewBytes uint64
	tailPins  map[tailVersion]tailMetadata
	wal       *wal.Log
	root      *os.Root
	dir       *os.File
	publisher *checkpoint.Publisher
	catalog   *history.Catalog
	budget    *checkpoint.Budget

	// Capture and selection share this gate. The WAL takes its own write
	// lock for the final selection, leaving appends free during staging.
	mu             sync.Mutex
	generation     history.Generation
	manifest       string
	captures       map[string]*retainedView
	nextCapture    uint64
	retired        []retiredGeneration
	reclaimed      retiredGeneration
	compacting     uint64
	stopped        atomic.Bool
	maintenanceErr error
	publicationLag uint64
	fatal          chan error
	jobs           chan uint64
	wake           chan struct{}
	cancel         context.CancelFunc
	done           chan struct{}
}

type retiredGeneration struct {
	history.Generation
	manifest    string
	sequence    uint64
	replacement uint64
	bytes       uint64
}

func openRuntime(ctx context.Context, path string) (_ *retainedRuntime, err error) {
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      uint64(checkpoint.MaxSegments+1) * checkpoint.MaxBlockSize,
		checkpoint.ReplayTailBytes:   tailLimit,
		checkpoint.PinnedGenerations: 6,
		checkpoint.LiveCaptures:      80,
	})
	r := &retainedRuntime{
		budget: budget, catalog: history.NewCatalog(budget), captures: make(map[string]*retainedView),
		tailPins: make(map[tailVersion]tailMetadata),
		fatal:    make(chan error, 1), jobs: make(chan uint64, 1), wake: make(chan struct{}, 1), done: make(chan struct{}), viewsDone: make(chan struct{}),
	}
	r.wal, err = wal.OpenOwner(path, func(w *wal.Log, root *os.Root, dir *os.File) error {
		r.wal, r.root, r.dir = w, root, dir
		r.publisher = checkpoint.NewPublisher(root, dir, budget)
		if err := r.recover(ctx); err != nil {
			return errors.Join(err, r.catalog.Close())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	maintenance, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go r.maintain(maintenance)
	return r, nil
}

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

func (r *retainedRuntime) LastCommitOutcome() wal.CommitOutcome { return r.wal.LastCommitOutcome() }

func (r *retainedRuntime) Append(payload []byte, rows []store.Row) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1400*time.Millisecond)
	defer cancel()
	err := r.wal.AppendWaiting(ctx, payload, rows)
	r.notify()
	return err
}

func (r *retainedRuntime) notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *retainedRuntime) maintain(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		var cutoff uint64
		select {
		case <-ctx.Done():
			return
		case cutoff = <-r.jobs:
		case <-r.wake:
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return
		}
		if err := r.reclaim(ctx); err != nil {
			log.Printf("reclaim retained storage: %v", err)
		}
		if cutoff == 0 {
			r.mu.Lock()
			point, active := r.wal.PublishedAuthority()
			if r.compacting == 0 && !r.stopped.Load() && active >= checkpointTrigger {
				r.compacting, cutoff = point.Next, point.Next
			}
			r.mu.Unlock()
		}
		if cutoff == 0 {
			continue
		}
		err := r.advance(ctx, cutoff)
		r.mu.Lock()
		if r.compacting == cutoff {
			r.compacting, r.maintenanceErr = 0, err
		}
		r.mu.Unlock()
		if errors.Is(err, checkpoint.ErrPublicationUncertain) || errors.Is(err, wal.ErrUncertain) {
			r.fatal <- err
			return
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("advance checkpoint: %v", err)
		}
		if err == nil {
			if err := r.reclaim(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("reclaim retained storage: %v", err)
			}
		}
	}
}

func (r *retainedRuntime) advance(ctx context.Context, ordinal uint64) (err error) {
	started := time.Now()
	view, err := r.acquireView(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, view.close()) }()
	ticket, err := r.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	point := recoveryPoint(view.point)
	point.Generation = view.reader.Generation().ID
	cutoff := point.CommittedEnd
	if ordinal != point.CommittedOrdinal {
		cutoff, err = findCutoff(ctx, view.wal, point, ordinal)
		if err != nil {
			return err
		}
	}
	if view.point.Sequence == 0 {
		point.SuffixSHA256, err = digestRange(view.wal, point.CoveredOffset, point.CommittedEnd)
		if err != nil {
			return err
		}
	}
	_, err = r.publisher.PublishRecoveryView(ctx, recoveryView{r.dir, view.manifest, view.wal, point}, cutoff, ordinal,
		func(ctx context.Context, p checkpoint.RecoveryPoint) error {
			gate := false
			defer func() {
				if gate {
					r.mu.Unlock()
				}
			}()
			gen, err := ticket.InstallPublished(ctx, r.dir, p.Generation, func() error {
				r.mu.Lock()
				gate = true
				// Once selection begins, finish the durable decision even if
				// shutdown cancels staging. Shutdown drains this worker.
				return r.wal.Select(wal.Authority{Generation: p.Generation, Covered: p.CoveredOffset, First: p.NextOrdinal,
					End: p.CommittedEnd, Next: p.CommittedOrdinal, SuffixSHA256: p.SuffixSHA256},
					nil, tailLimit, func(input io.ReaderAt, a wal.Authority) (*store.Store, error) {
						return r.replay(context.Background(), input, a)
					})
			})
			if gen.ID == (history.GenerationID{}) {
				if !errors.Is(err, wal.ErrUncertain) {
					err = errors.Join(checkpoint.ErrSelectionRejected, err)
				}
				return err
			}
			r.retired = append(r.retired, retiredGeneration{r.generation, r.manifest, view.point.Sequence,
				view.point.Sequence + 1, uint64(cutoff - point.CoveredOffset)})
			r.generation, r.manifest = gen, checkpoint.GenerationManifestName(gen.ID)
			r.publicationLag = uint64(time.Since(started))
			return err
		})
	return err
}

var errCutoffFound = errors.New("checkpoint cutoff found")

func findCutoff(ctx context.Context, input io.ReaderAt, p checkpoint.RecoveryPoint, ordinal uint64) (int64, error) {
	var cutoff int64
	err := checkpoint.WalkCapturedFrames(ctx, input, p.CoveredOffset, p.CommittedEnd, p.NextOrdinal, func(end int64, start uint64, rows []store.Row) error {
		next := start + uint64(len(rows))
		if next == ordinal {
			cutoff = end
			return errCutoffFound
		}
		if next > ordinal {
			return errors.New("checkpoint cutoff splits a committed batch")
		}
		return nil
	})
	if errors.Is(err, errCutoffFound) {
		return cutoff, nil
	}
	return 0, errors.Join(errors.New("checkpoint cutoff is not a committed batch boundary"), err)
}

func (r *retainedRuntime) reclaim(ctx context.Context) error {
	if err := r.catalog.ReclaimPinned(ctx, r.dir); err != nil {
		return err
	}
	if err := r.wal.ReclaimTails(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.retired) > 0 {
		old := r.retired[0]
		select {
		case <-old.Drained:
		default:
			return nil
		}
		if old.manifest != manifestName {
			err := r.root.Remove(old.manifest)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := r.dir.Sync(); err != nil {
				return err
			}
		}
		r.reclaimed = old
		r.retired[0] = retiredGeneration{}
		r.retired = r.retired[1:]
	}
	return nil
}

func (r *retainedRuntime) stop() {
	r.stopped.Store(true)
	r.cancel()
}

func (r *retainedRuntime) close(ctx context.Context) error {
	r.stop()
	select {
	case <-r.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	r.mu.Lock()
	views := r.views
	r.mu.Unlock()
	if views > 0 {
		select {
		case <-r.viewsDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		err := errors.Join(r.reclaim(context.Background()), r.catalog.Close())
		if err == nil && ctx.Err() == nil {
			err = r.wal.Close()
		}
		done <- errors.Join(err, ctx.Err())
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
