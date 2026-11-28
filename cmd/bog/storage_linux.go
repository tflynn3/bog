package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
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
	tailTarget      = 16 << 20
	tailLimit       = 32 << 20
	tailBudget      = 48 << 20 // Shared by the live tail and capture-held batches.
	tailFrameLimit  = 16_384
	stagingLimit    = 1 << 30
	generationLimit = 8
	captureLimit    = 64
)

type retainedStorage struct {
	owner   *wal.Log
	dir     *os.File
	root    *os.Root
	pub     *checkpoint.Publisher
	catalog *history.Catalog
	budget  *checkpoint.Budget
	control bool

	mu             sync.Mutex // Serializes publication of tail rows, capture, and selection.
	active         history.Generation
	tail           []*tailBatch
	retired        []retiredGeneration
	switching      chan struct{}
	fenced         error
	job            *maintenanceJob
	failedCutoff   uint64
	publicationLag time.Duration
	stopping       atomic.Bool
	closed         bool
	wake           chan struct{}
	cutoffs        chan *maintenanceJob
	cancel         context.CancelFunc
	stopped        chan struct{}
	ops            sync.WaitGroup
	captures       map[string]*operationPin
	nextCapture    uint64
}

type maintenanceJob struct {
	cutoff  uint64
	started time.Time
	done    chan struct{}
	err     error // Published by closing done.
}

type tailBatch struct {
	end    int64
	next   uint64
	rows   []checkpoint.BlockRow // Sorted by timestamp and global append ordinal.
	refs   int                   // Accessed under the operation lock while serving.
	charge *checkpoint.Reservation
}

type retiredGeneration struct {
	switches wal.TailSwitch
	drained  <-chan struct{}
	at       time.Time
}

func openRetained(ctx context.Context, path string) (_ *retainedStorage, err error) {
	owner, err := wal.LockDirectory(path)
	if err != nil {
		return nil, err
	}
	root, dir := owner.Directory()
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      stagingLimit,
		checkpoint.ReplayTailBytes:   tailBudget,
		checkpoint.PinnedGenerations: generationLimit,
		checkpoint.LiveCaptures:      captureLimit + 9, // Includes ordinary scans and maintenance.
	})
	s := &retainedStorage{
		owner: owner, dir: dir, root: root, budget: budget,
		catalog: history.NewCatalog(budget), pub: checkpoint.NewPublisher(root, dir, budget),
		wake: make(chan struct{}, 1), cutoffs: make(chan *maintenanceJob, 1), stopped: make(chan struct{}),
		captures: make(map[string]*operationPin),
	}
	defer func() {
		if err != nil {
			s.dropTail(len(s.tail))
			err = errors.Join(err, s.catalog.Close(), owner.Close())
		}
	}()
	if err := s.recover(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *retainedStorage) startMaintenance(control bool) {
	s.control = control
	work, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.maintain(work)
}

func selection(a wal.Authority) history.Selection {
	return history.Selection{ID: history.GenerationID(a.Generation)}
}

func (s *retainedStorage) install(ctx context.Context, a wal.Authority, ticket *history.Replacement) (history.Generation, error) {
	return ticket.ReplaceSelected(ctx, s.dir, selection(a))
}

func (s *retainedStorage) recover(ctx context.Context) error {
	a, err := s.owner.ReadAuthority()
	switch {
	case err == nil:
		m, err := history.ValidateSelection(ctx, s.dir, selection(a))
		if err != nil {
			return err
		}
		if m.CoveredOffset != a.CoveredOffset || m.NextOrdinal != a.NextOrdinal {
			return errors.New("checkpoint boundary differs from WAL authority")
		}
		next := a.NextOrdinal
		if err := s.owner.RecoverSelected(a, tailLimit, func(start, end int64, rows []store.Row) error {
			if err := s.addReplay(ctx, start, end, next, rows); err != nil {
				return err
			}
			next += uint64(len(rows))
			return nil
		}); err != nil {
			return err
		}
	case errors.Is(err, os.ErrNotExist):
		if err := s.owner.CheckUnselected(ctx); err != nil {
			return err
		}
		if err := s.bootstrap(ctx); err != nil {
			return err
		}
		a = s.owner.Selected()
	default:
		return err
	}

	// Settle any interrupted staging before the catalog can acquire its
	// publication lock. No HTTP is listening while recovery owns these files.
	if err := s.recoverPublication(ctx, a); err != nil {
		return err
	}
	ticket, err := s.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	s.active, err = s.install(ctx, a, ticket)
	if err != nil {
		return err
	}
	return s.recoverRetirement(ctx, a)
}

func (s *retainedStorage) bootstrap(ctx context.Context) error {
	for _, name := range []string{"bog.backup", "bog.wal.tail"} {
		if _, err := s.root.Lstat(name); err == nil {
			return s.restore(ctx)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := s.pub.RecoverUnselected(ctx, func() (*checkpoint.Manifest, error) {
		return history.ValidatePinnedAuthority(ctx, s.dir)
	}); err != nil {
		return err
	}
	var next uint64
	var stream bool
	if err := s.owner.RecoverOrdinary(func(start, end int64, rows []store.Row) error {
		if !stream && (end-8 >= tailTarget || len(s.tail) >= tailFrameLimit/2) {
			s.dropTail(len(s.tail))
			stream = true
		}
		if !stream {
			if err := s.addReplay(ctx, start, end, next, rows); err != nil {
				return err
			}
		}
		next += uint64(len(rows))
		return ctx.Err()
	}); err != nil {
		return err
	}
	pin, err := s.owner.Capture(ctx)
	if err != nil {
		return err
	}
	if !stream {
		err := s.selectOrdinary(ctx, pin)
		return errors.Join(err, pin.Release())
	}
	var transition wal.TailSwitch
	_, err = s.pub.BootstrapRecovery(ctx, pin, pin.CommittedEnd(), pin.NextOrdinal(), func(point checkpoint.RecoveryPoint) error {
		var err error
		transition, err = s.owner.Select(ctx, toAuthority(point), nil, false)
		return err
	})
	if err := errors.Join(err, pin.Release()); err != nil {
		return err
	}
	if err := s.owner.ReclaimTail(transition.Before); err != nil {
		return err
	}
	return s.owner.FinishTailSwitch(transition)
}

func (s *retainedStorage) selectOrdinary(ctx context.Context, pin *wal.Capture) error {
	const start = int64(8)
	b, id, err := checkpoint.EmptyRecoveryManifest()
	if err != nil {
		return err
	}
	if err := s.owner.InstallDataFile(checkpoint.GenerationManifestName(id), b); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(pin, start, pin.CommittedEnd()-start)); err != nil {
		return err
	}
	_, err = s.owner.Select(ctx, wal.Authority{
		Generation: id, CoveredOffset: start, CommittedEnd: pin.CommittedEnd(),
		CommittedOrdinal: pin.NextOrdinal(), SuffixSHA256: [32]byte(h.Sum(nil)),
	}, nil, true)
	return err
}

func (s *retainedStorage) restore(ctx context.Context) error {
	for _, name := range []string{"bog.wal", "bog.commit", ".bog-checkpoint-stage"} {
		if _, err := s.root.Lstat(name); err == nil {
			return fmt.Errorf("ambiguous restore and writable storage: found %s", name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	point, input, err := backup.VerifyPinnedRecovery(ctx, s.root, func(start, end int64) error {
		if end-start > tailLimit {
			return errors.New("restore WAL suffix exceeds replay budget")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("verify restore: %w", err)
	}
	a := toAuthority(checkpoint.RecoveryPoint(point))
	if err := s.installRestoreManifest(a.Generation); err != nil {
		return errors.Join(err, input.Close())
	}
	var next = a.NextOrdinal
	err = wal.ReplayTail(ctx, input, a.CoveredOffset, a.CommittedEnd, a.NextOrdinal, func(start, end int64, rows []store.Row) error {
		if err := s.addReplay(ctx, start, end, next, rows); err != nil {
			return err
		}
		next += uint64(len(rows))
		return nil
	})
	if err == nil && next != a.CommittedOrdinal {
		err = errors.New("restore suffix ordinal mismatch")
	}
	if err != nil {
		return errors.Join(err, input.Close())
	}
	_, err = s.owner.Select(ctx, a, input, false)
	if err := errors.Join(err, input.Close()); err != nil {
		return err
	}
	return nil
}

func (s *retainedStorage) installRestoreManifest(id [32]byte) (err error) {
	f, err := s.owner.OpenDataFile("bog.checkpoint")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, (256<<10)+1))
	if err != nil {
		return err
	}
	_, actual, err := checkpoint.DecodeManifest(b)
	if err != nil || actual != id {
		return errors.Join(err, errors.New("restore checkpoint identity changed"))
	}
	return s.owner.InstallDataFile(checkpoint.GenerationManifestName(id), b)
}

func toAuthority(p checkpoint.RecoveryPoint) wal.Authority {
	return wal.Authority{Generation: p.Generation, CoveredOffset: p.CoveredOffset,
		NextOrdinal: p.NextOrdinal, CommittedEnd: p.CommittedEnd, CommittedOrdinal: p.CommittedOrdinal,
		SuffixSHA256: p.SuffixSHA256}
}

func (s *retainedStorage) addReplay(ctx context.Context, start, end int64, first uint64, rows []store.Row) error {
	if len(s.tail) >= tailFrameLimit {
		// A verified restore may contain many very small frames. The count
		// budget is part of the storage contract as well as the byte budget.
		return errors.New("retained WAL frame count exceeds replay budget")
	}
	charge, err := s.reserveTail(ctx, end-start, rows)
	if err != nil {
		return err
	}
	s.tail = append(s.tail, makeTailBatch(end, first, rows, charge))
	return nil
}

func (s *retainedStorage) reserveTail(ctx context.Context, frameBytes int64, rows []store.Row) (*checkpoint.Reservation, error) {
	// Count the sorted BlockRow backing (48 bytes per row on both serving
	// targets), line storage and allocator rounding, and the batch metadata.
	decoded := uint64(128) + 48*uint64(len(rows))
	for _, row := range rows {
		if n := uint64(len(row.Line)); n != 0 {
			decoded += n + max(15, n/4)
		}
	}
	return s.budget.Reserve(ctx, checkpoint.ReplayTailBytes, max(uint64(frameBytes), decoded))
}

func makeTailBatch(end int64, first uint64, rows []store.Row, charge *checkpoint.Reservation) *tailBatch {
	sorted := make([]checkpoint.BlockRow, len(rows))
	for i, row := range rows {
		sorted[i] = checkpoint.BlockRow{Row: row, Ordinal: first + uint64(i)}
	}
	slices.SortFunc(sorted, compareRows)
	return &tailBatch{end: end, next: first + uint64(len(rows)), rows: sorted, charge: charge, refs: 1}
}

func (b *tailBatch) release() {
	b.refs--
	if b.refs == 0 {
		b.charge.Release()
	}
}

func (s *retainedStorage) dropTail(count int) {
	for _, batch := range s.tail[:count] {
		batch.release()
	}
	s.tail = slices.Delete(s.tail, 0, count)
}

func (s *retainedStorage) lockView(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.fenced != nil {
			err := s.fenced
			s.mu.Unlock()
			return err
		}
		wait := s.switching
		if wait == nil && ctx.Err() == nil {
			return nil // caller owns s.mu
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

func (s *retainedStorage) Append(payload []byte, rows []store.Row) error {
	ctx := context.Background()
	size := int64(len(payload)) + 16
	for {
		if err := s.lockView(ctx); err != nil {
			return err
		}
		a := s.owner.Selected()
		if a.CommittedEnd-a.CoveredOffset+size > tailLimit || len(s.tail) >= tailFrameLimit {
			if s.stopping.Load() {
				s.mu.Unlock()
				return os.ErrClosed
			}
			if s.job == nil {
				s.scheduleLocked(a.CommittedOrdinal)
			}
			job := s.job
			s.mu.Unlock()
			<-job.done
			if job.err != nil {
				return job.err
			}
			continue
		}
		charge, err := s.reserveTail(ctx, size, rows)
		if err == nil {
			err = s.owner.Append(payload, rows)
			if err == nil {
				s.tail = append(s.tail, makeTailBatch(a.CommittedEnd+size, a.CommittedOrdinal, rows, charge))
				if a.CommittedEnd-a.CoveredOffset+size >= tailTarget || len(s.tail) >= tailFrameLimit/2 {
					s.notify()
				}
			} else {
				charge.Release()
			}
		}
		s.mu.Unlock()
		return err
	}
}

func (s *retainedStorage) LastCommitOutcome() wal.CommitOutcome { return s.owner.LastCommitOutcome() }

func (s *retainedStorage) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *retainedStorage) stopMaintenance() {
	s.stopping.Store(true)
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *retainedStorage) drain(ctx context.Context) error {
	s.stopMaintenance()
	if s.cancel != nil {
		select {
		case <-s.stopped:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	drained := make(chan struct{})
	go func() {
		s.ops.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.dropTail(len(s.tail))
	return errors.Join(s.reclaim(ctx), s.catalog.Close())
}

func (s *retainedStorage) maintain(ctx context.Context) {
	defer close(s.stopped)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.finishJobLocked(ctx.Err())
			s.mu.Unlock()
			return
		case job := <-s.cutoffs:
			err := s.advance(ctx, job.cutoff)
			s.mu.Lock()
			s.finishJobLocked(err)
			s.mu.Unlock()
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("advance retained checkpoint: %v", err)
			}
		case <-tick.C:
			if err := s.reclaim(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("reclaim retained history: %v", err)
			}
			s.scheduleAutomatic()
		case <-s.wake:
			s.scheduleAutomatic()
		}
	}
}

func (s *retainedStorage) scheduleAutomatic() {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.owner.Selected()
	if !s.control && !s.stopping.Load() && s.job == nil && s.fenced == nil &&
		(a.CommittedEnd-a.CoveredOffset >= tailTarget || len(s.tail) >= tailFrameLimit/2) {
		s.scheduleLocked(a.CommittedOrdinal)
	}
}

func (s *retainedStorage) scheduleLocked(cutoff uint64) {
	s.failedCutoff = 0
	s.job = &maintenanceJob{cutoff: cutoff, started: time.Now(), done: make(chan struct{})}
	s.cutoffs <- s.job
}

func (s *retainedStorage) finishJobLocked(err error) {
	if s.job != nil {
		s.job.err = err
		if s.active.Boundary.NextOrdinal == s.job.cutoff {
			s.publicationLag = time.Since(s.job.started)
		} else if err != nil {
			s.failedCutoff = s.job.cutoff
		}
		close(s.job.done)
		s.job = nil
	}
}
