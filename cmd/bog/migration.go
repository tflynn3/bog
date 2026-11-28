package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
)

func (r *retained) recoverLegacy(ctx context.Context) error {
	f, end, hasMarker, err := r.owner.OpenLegacy()
	if err != nil {
		return err
	}
	r.tail = &tailFile{File: f, base: 0, refs: 1}
	var magic [8]byte
	if _, err := f.ReadAt(magic[:], 0); err != nil {
		return err
	}
	if string(magic[:]) != "BOGWAL01" {
		return errors.New("invalid WAL magic")
	}
	var previous *checkpoint.Manifest
	var next uint64
	var frames int
	// Validate every frame before truncating anything, including the precise
	// predecessor boundary when an offline checkpoint is already present.
	err = r.publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
		m, err := history.ValidatePinnedAuthority(ctx, r.dir)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		previous = m
		split := int64(8)
		if m != nil {
			if m.Version != 1 || m.CoveredOffset > end {
				return nil, errors.New("legacy checkpoint exceeds committed WAL")
			}
			split = m.CoveredOffset
			h, err := digestRange(r.tail.section(0, split), split)
			if err != nil {
				return nil, err
			}
			if hexSum([32]byte(h.Sum(nil))) != m.SourceSHA256 {
				return nil, errors.New("legacy checkpoint prefix digest differs")
			}
		}
		count := func(_, _ int64, first uint64, rows []store.Row) error {
			frames++
			next = first + uint64(len(rows))
			return nil
		}
		if err := checkpoint.ScanCapturedFrames(ctx, f, 8, split, 0, count); err != nil {
			return nil, err
		}
		if m != nil && next != m.NextOrdinal {
			return nil, errors.New("legacy checkpoint prefix ordinal differs")
		}
		frames = 0
		if err := checkpoint.ScanCapturedFrames(ctx, f, split, end, next, count); err != nil {
			return nil, err
		}
		if m == nil {
			return nil, os.ErrNotExist
		}
		return m, nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := r.owner.FinishRecovery(f, end); err != nil {
		return err
	}
	if !hasMarker {
		if _, err := r.owner.CommitLegacy(end); err != nil {
			return err
		}
	}
	start := int64(8)
	if previous != nil {
		start = previous.CoveredOffset
	}
	if end-start >= advanceBytes || frames >= advanceFrames {
		h, err := digestRange(r.tail.section(0, end), end)
		if err != nil {
			return err
		}
		if _, err := r.publisher.PublishCaptured(ctx, f, end, previous, next, hexSum([32]byte(h.Sum(nil)))); err != nil {
			return fmt.Errorf("migrate committed legacy WAL: %w", err)
		}
		start = end
	} else if previous == nil {
		if _, err := r.publisher.PublishCaptured(ctx, f, 8, nil, 0, hexSum(sha256.Sum256(magic[:]))); err != nil {
			return err
		}
	}
	raw, m, id, err := r.readManifest(canonicalManifest)
	if err != nil {
		return err
	}
	h, err := digestRange(r.tail.section(start, end), end-start)
	if err != nil {
		return err
	}
	p := checkpoint.RecoveryPoint{Generation: id, CoveredOffset: start, NextOrdinal: m.NextOrdinal,
		CommittedEnd: end, CommittedOrdinal: next, SuffixSHA256: [32]byte(h.Sum(nil))}
	// Keep the ordinary layout for new, small WALs. A populated offline
	// checkpoint or a migrated prefix becomes a suffix-only writable view.
	if start == 8 {
		if err := r.installRecovered(ctx, p, canonicalManifest); err != nil {
			return err
		}
		return r.cleanRecoveredTails()
	}
	name := checkpoint.GenerationManifestName(id)
	if _, err := r.owner.Select(name, raw); err != nil {
		return err
	}
	tail, err := r.owner.NewTail(ctx, r.tail.section(start, end), end-start)
	if err != nil {
		return err
	}
	r.tail = &tailFile{File: tail, base: start, refs: 1}
	if err := f.Close(); err != nil {
		return err
	}
	if err := r.installRecovered(ctx, p, name); err != nil {
		return err
	}
	if _, err := r.selectAuthority(r.tail, p, r.sequence); err != nil {
		return err
	}
	return errors.Join(r.cleanLegacy(), r.cleanRecoveredTails())
}

func (r *retained) cleanLegacy() error {
	// Selector durability precedes retiring the fallback layout. Remove
	// the marker first so an interruption never leaves a dangling marker.
	for _, name := range []string{"bog.commit", "bog.wal", canonicalManifest} {
		if err := r.owner.Remove(name); err != nil {
			return err
		}
	}
	return nil
}
