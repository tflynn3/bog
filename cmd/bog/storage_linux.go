package main

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	autoTailBytes   = 16 << 20
	maxTailBytes    = 36 << 20
	maxTailBatches  = 8192
	maxLiveCaptures = 64
)

type tailBatch struct {
	start, end int64
	next       uint64
	rows       []checkpoint.BlockRow // immutable, sorted by timestamp then ordinal
	maxLine    int
	charge     *checkpoint.Reservation
	weight     uint64
	refs       int // epochs sharing the batch
}

type epoch struct {
	gen                history.Generation
	manifest           *checkpoint.Manifest
	name               string
	file               *os.File
	info               os.FileInfo
	base               int64
	batches            []*tailBatch
	maxLine            int
	weight             uint64
	size               int64
	number, replacedBy uint64
	pins               int
	retiredAt          time.Time
	retirement         tailSwitch
}

type retained struct {
	mu                sync.Mutex // append publication, capture and activation
	dir               *wal.Directory
	catalog           *history.Catalog
	publisher         *checkpoint.Publisher
	budget            *checkpoint.Budget
	active            *epoch
	retired           []*epoch
	pendingRetirement []tailSwitch
	point             checkpoint.RecoveryPoint
	selector          authority // zero version while the ordinary WAL is selected
	digest            hash.Cloner
	fenced            error

	manual                                     bool
	stopping                                   atomic.Bool
	maintenance                                chan struct{}
	maintenanceErr                             error
	publicationLag                             time.Duration
	reclaimedBytes                             uint64
	reclaimedPredecessor, reclaimedReplacement uint64
}

func batchCharge(start, end int64, count int) uint64 {
	// In addition to encoded bytes, cover row structs, allocation rounding,
	// the immutable batch and its entry in every sharing epoch's index.
	return uint64(end-start) + uint64(count)*112 + 256
}

func (s *retained) newBatch(ctx context.Context, start, end int64, first uint64, rows []store.Row) (*tailBatch, error) {
	weight := batchCharge(start, end, len(rows))
	charge, err := s.budget.Reserve(ctx, checkpoint.ReplayTailBytes, weight)
	if err != nil {
		return nil, err
	}
	b := &tailBatch{start: start, end: end, next: first + uint64(len(rows)),
		rows: make([]checkpoint.BlockRow, len(rows)), charge: charge, weight: weight, refs: 1}
	for i, row := range rows {
		b.rows[i] = checkpoint.BlockRow{Row: row, Ordinal: first + uint64(i)}
		b.maxLine = max(b.maxLine, len(row.Line))
	}
	slices.SortFunc(b.rows, compareRows)
	return b, nil
}

func releaseBatches(batches []*tailBatch) {
	for _, b := range batches {
		b.refs--
		if b.refs == 0 {
			b.charge.Release()
		}
	}
}

func (s *retained) Append(payload []byte, rows []store.Row) error {
	for {
		s.mu.Lock()
		if s.fenced != nil {
			err := errors.Join(wal.ErrUncertain, s.fenced)
			s.mu.Unlock()
			return err
		}
		p := s.point
		frameSize := int64(wal.FrameHeaderSize + len(payload))
		if frameSize > math.MaxInt64-p.CommittedEnd || uint64(len(rows)) > math.MaxUint64-p.CommittedOrdinal {
			s.mu.Unlock()
			return errors.New("WAL coordinate overflow")
		}
		var b *tailBatch
		err := checkpoint.ErrExhausted
		if p.CommittedEnd-p.CoveredOffset+frameSize <= maxTailBytes && len(s.active.batches) < maxTailBatches {
			b, err = s.newBatch(context.Background(), p.CommittedEnd, p.CommittedEnd+frameSize, p.CommittedOrdinal, rows)
		}
		if err == nil {
			err = s.appendLocked(b, payload)
			s.scheduleLocked()
			s.mu.Unlock()
			return err
		}
		s.scheduleLocked()
		done := s.maintenance
		s.mu.Unlock()
		if done == nil || s.manual {
			return err
		}
		<-done
		s.mu.Lock()
		maintenanceErr := s.maintenanceErr
		s.mu.Unlock()
		if maintenanceErr != nil {
			return maintenanceErr
		}
	}
}

func (s *retained) appendLocked(b *tailBatch, payload []byte) (err error) {
	defer func() {
		if err != nil {
			releaseBatches([]*tailBatch{b})
		}
	}()
	h, err := s.digest.Clone()
	if err != nil {
		return err
	}
	f := s.active.file
	if _, err := f.Seek(b.start-s.active.base, io.SeekStart); err != nil {
		return err
	}
	operation, err := wal.WriteFrame(f, h, payload)
	if err == nil {
		operation = "wal_sync"
		err = f.Sync()
	}
	a := s.selector
	sum := [32]byte(h.Sum(nil))
	if err == nil {
		if a.Version == 0 {
			operation, err = s.dir.CommitLegacy(b.end)
		} else {
			a.CommittedEnd, a.CommittedOrdinal, a.SuffixSHA256 = b.end, b.next, hexDigest(sum)
			operation, err = s.selectAuthority(a)
		}
	}
	if err != nil {
		if !errors.Is(err, wal.ErrUncertain) {
			err = wal.RollbackFrame(f, b.start-s.active.base, err)
		}
		if errors.Is(err, wal.ErrUncertain) {
			s.fenced = err
		}
		s.dir.RecordCommit(operation, err)
		return err
	}
	s.active.batches = append(s.active.batches, b)
	s.active.weight += b.weight
	s.active.maxLine = max(s.active.maxLine, b.maxLine)
	s.point.CommittedEnd, s.point.CommittedOrdinal, s.point.SuffixSHA256 = b.end, b.next, sum
	s.digest, s.selector = h, a
	s.dir.RecordCommit("commit", nil)
	return nil
}

func (s *retained) LastCommitOutcome() wal.CommitOutcome { return s.dir.LastCommitOutcome() }

// recoveryView is created under the append/activation lock. The epoch keeps
// the file identity and logical base stable. The point fixes both suffix bounds
// and its digest, and the generation reader protects all referenced segments.
type recoveryView struct {
	s            *retained
	e            *epoch
	reader       *history.Reader
	point        checkpoint.RecoveryPoint
	batches      []*tailBatch
	manifestName string
	shape        scanShape
}

func (v *recoveryView) Point() checkpoint.RecoveryPoint { return v.point }
func (v *recoveryView) OpenManifest() (io.ReadCloser, error) {
	return v.s.dir.OpenRegular(v.manifestName, os.O_RDONLY)
}
func (v *recoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	if !slices.ContainsFunc(v.e.manifest.Segments, func(seg checkpoint.Segment) bool { return seg.Name == name }) {
		return nil, os.ErrNotExist
	}
	return v.s.dir.OpenRegular(name, os.O_RDONLY)
}
func (v *recoveryView) OpenSuffix() (io.ReadCloser, error) {
	p := v.point
	return io.NopCloser(io.NewSectionReader(v.e.file, p.CoveredOffset-v.e.base, p.CommittedEnd-p.CoveredOffset)), nil
}

func (s *retained) captureLocked(ctx context.Context) (*recoveryView, error) {
	if s.fenced != nil {
		return nil, s.fenced
	}
	r, err := s.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return nil, err
	}
	e := s.active
	e.pins++
	return &recoveryView{s: s, e: e, reader: r, point: s.point, batches: e.batches, manifestName: s.manifestName(), shape: e.scanShape()}, nil
}

func (s *retained) manifestName() string {
	if s.selector.Version == 0 {
		return canonicalManifest
	}
	return checkpoint.GenerationManifestName(s.point.Generation)
}

func (v *recoveryView) release() error {
	err := v.reader.Close()
	s := v.s
	s.mu.Lock()
	defer s.mu.Unlock()
	v.e.pins--
	return errors.Join(err, s.reclaimLocked())
}

func (s *retained) remove(name string) error {
	err := s.dir.Root.Remove(name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", name, err)
	}
	return s.dir.File.Sync()
}
