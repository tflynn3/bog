package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

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
			r.wal.Fence(err)
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
	for i := 0; i < len(r.retired); {
		old := r.retired[i]
		select {
		case <-old.Drained:
		default:
			i++
			continue
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
		copy(r.retired[i:], r.retired[i+1:])
		r.retired[len(r.retired)-1] = retiredGeneration{}
		r.retired = r.retired[:len(r.retired)-1]
	}
	return nil
}
