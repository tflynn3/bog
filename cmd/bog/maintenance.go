package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/wal"
)

func (r *retained) scheduleLocked() {
	if r.busy != nil || r.ctx.Err() != nil ||
		(r.point.CommittedEnd-r.point.CoveredOffset < advanceBytes && len(r.batches) < advanceFrames) {
		return
	}
	if err := r.startLocked(r.point.CommittedOrdinal, false); err != nil {
		logMaintenance(err)
	}
}

func (r *retained) startLocked(cutoff uint64, manual bool) error {
	if r.fenced != nil || r.ctx.Err() != nil {
		return os.ErrClosed
	}
	if r.busy != nil {
		if r.cutoff == cutoff {
			return nil
		}
		return errors.New("checkpoint advancement in progress")
	}
	var end int64
	for _, b := range r.batches {
		if b.first+uint64(len(b.rows)) == cutoff {
			end = b.end
			break
		}
	}
	if end == 0 {
		return errors.New("cutoff must advance to a committed frame boundary")
	}
	ticket, err := r.catalog.ReserveReplacement(r.ctx)
	if err != nil {
		return err
	}
	pin, err := r.pinLocked(r.ctx, false)
	if err != nil {
		ticket.Close()
		return err
	}
	r.cutoff, r.busy, r.maintErr = cutoff, make(chan struct{}), nil
	go r.advance(ticket, pin, end, cutoff, manual)
	return nil
}

func (r *retained) advance(ticket *history.Replacement, pin *pinnedView, end int64, ordinal uint64, manual bool) {
	var locked bool
	err := r.publish(ticket, pin, end, ordinal, manual, &locked)
	ticket.Close()
	if !locked {
		r.mu.Lock()
	}
	if errors.Is(err, checkpoint.ErrPublicationUncertain) || errors.Is(err, wal.ErrUncertain) {
		r.fenceLocked(err)
	}
	r.maintErr = err
	r.recordRelease(r.dropViewLocked(pin))
	if r.fenced == nil {
		r.recordRelease(r.reclaimLocked())
	}
	close(r.busy)
	r.busy = nil
	r.mu.Unlock()
	logMaintenance(err)
}

func (r *retained) publish(ticket *history.Replacement, pin *pinnedView, end int64, ordinal uint64, manual bool, locked *bool) error {
	if manual {
		// The qualification control is asynchronous and leaves an observation
		// window for concurrent capture replay and whole-batch appends.
		timer := time.NewTimer(250 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-timer.C:
		}
	}
	view, err := r.recoveryView(pin.point, pin.manifest, pin.tail)
	if err != nil {
		return errors.Join(wal.ErrUncertain, err)
	}
	var point checkpoint.RecoveryPoint
	var next *tailFile
	var overlap *checkpoint.Reservation
	// Keep the append/activation boundary from choosing the carried suffix
	// until the catalog and query-visible tail have both been installed.
	m, err := r.publisher.PublishRecoveryView(r.ctx, view, end, ordinal, func(ctx context.Context, p checkpoint.RecoveryPoint) error {
		r.mu.Lock()
		*locked = true
		if r.fenced != nil {
			return r.fenced
		}
		if ctx.Err() != nil {
			return errors.Join(checkpoint.ErrSelectionRefused, context.Canceled)
		}
		p.CommittedEnd, p.CommittedOrdinal = r.point.CommittedEnd, r.point.CommittedOrdinal
		size := p.CommittedEnd - p.CoveredOffset
		overlap, err = r.budget.Reserve(ctx, checkpoint.StagingBytes, uint64(size))
		if err != nil {
			return errors.Join(checkpoint.ErrSelectionRefused, err)
		}
		f, err := r.owner.NewTail(ctx, r.tail.section(p.CoveredOffset, p.CommittedEnd), size)
		if err != nil {
			if errors.Is(err, wal.ErrUncertain) {
				return err
			}
			overlap.Release()
			return errors.Join(checkpoint.ErrSelectionRefused, err)
		}
		next = &tailFile{File: f, base: p.CoveredOffset, refs: 1}
		digest, err := digestRange(next.section(p.CoveredOffset, p.CommittedEnd), size)
		if err == nil {
			p.SuffixSHA256 = [32]byte(digest.Sum(nil))
			_, err = r.selectAuthority(next, p, r.sequence+1)
		}
		if err != nil {
			err = errors.Join(err, f.Close())
			if !errors.Is(err, wal.ErrUncertain) {
				cleanup := r.owner.Remove(f.Name())
				if cleanup == nil {
					overlap.Release()
					return errors.Join(checkpoint.ErrSelectionRefused, err)
				}
				err = errors.Join(err, cleanup)
			}
			return err
		}
		point = p
		r.digest = digest
		return nil
	})
	if m == nil {
		return err
	}
	if err != nil {
		return errors.Join(wal.ErrUncertain, err, next.Close())
	}
	name := checkpoint.GenerationManifestName(point.Generation)
	gen, err := ticket.ReplaceSelectedPinned(context.Background(), r.dir, history.GenerationID(point.Generation))
	if err != nil {
		return errors.Join(wal.ErrUncertain, fmt.Errorf("activate durable successor: %w", err), next.Close())
	}
	r.segments, r.maxLine = gen.Segments, max(r.maxLine, gen.MaxLineBytes)
	old := r.tail
	old.retired, old.overlap = true, overlap
	r.dead[r.point.Generation] = r.manifest
	r.point, r.tail, r.manifest = point, next, name
	r.sequence++
	var cut int
	for cut < len(r.batches) && r.batches[cut].end <= end {
		b := r.batches[cut]
		old.covered = append(old.covered, b.disk)
		b.release()
		cut++
	}
	clear(r.batches[:cut])
	r.batches = r.batches[cut:]
	return r.dropTailLocked(old)
}

func (b *tailBatch) release() {
	b.refs--
	if b.refs == 0 {
		b.mem.Release()
	}
}

func (r *retained) dropTailLocked(t *tailFile) error {
	t.refs--
	if t.refs != 0 {
		return nil
	}
	if !t.retired {
		return errors.New("attempted to retire the selected WAL")
	}
	var err error
	if t.Name() == "bog.wal" {
		err = r.owner.Remove("bog.commit")
	}
	if err == nil {
		err = r.owner.Remove(t.Name())
	}
	if err == nil && t.overlap != nil {
		t.overlap.Release()
	}
	if err == nil {
		for _, charge := range t.covered {
			charge.Release()
		}
		t.covered = nil
	}
	return errors.Join(err, t.Close())
}

func (r *retained) reclaimLocked() error {
	var err error
	for id, name := range r.dead {
		if r.pins[id] != 0 {
			continue
		}
		if removeErr := r.owner.Remove(name); removeErr != nil {
			err = errors.Join(err, removeErr)
		} else {
			delete(r.dead, id)
		}
	}
	if r.manifest != canonicalManifest {
		err = errors.Join(err, r.catalog.ReclaimSelectedPinned(context.Background(), r.dir, history.GenerationID(r.point.Generation)))
	}
	return err
}
