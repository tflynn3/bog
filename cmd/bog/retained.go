package main

import (
	"context"
	"errors"
	"hash"
	"log"
	"math"
	"os"
	"slices"
	"sync"
	"syscall"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	advanceBytes  = 18 << 20
	advanceFrames = 2048
	maxTailFrames = 4096
	tailLimit     = 64 << 20
)

type tailFile struct {
	*os.File
	base    int64
	refs    int // current authority and each acquired view, under retained.mu
	retired bool
	overlap *checkpoint.Reservation
	covered []*checkpoint.Reservation
}

type tailBatch struct {
	end, start int64
	first      uint64
	rows       []store.Row
	order      []uint32
	disk, mem  *checkpoint.Reservation
	maxLine    int
	refs       int
}

func (b *tailBatch) position(i int) history.Position {
	ordinal := b.order[i]
	return history.Position{Time: b.rows[ordinal].TS, Ordinal: b.first + uint64(ordinal)}
}

type retained struct {
	owner     *wal.Owner
	root      *os.Root
	dir       *os.File
	budget    *checkpoint.Budget
	publisher *checkpoint.Publisher
	catalog   *history.Catalog

	// mu serializes durable appends, generation activation, capture acquisition
	// and reference accounting. Checkpoint construction runs outside mu.
	mu         sync.Mutex
	point      checkpoint.RecoveryPoint
	tail       *tailFile
	batches    []*tailBatch
	segments   int
	maxLine    int
	manifest   string
	digest     hash.Cloner
	sequence   uint64
	fenced     error
	releaseErr error
	failed     chan error

	ctx      context.Context
	cancel   context.CancelFunc
	busy     chan struct{}
	cutoff   uint64
	maintErr error
	// Protected by mu. Retired files stay named until the final view drains.
	pins     map[[32]byte]int
	dead     map[[32]byte]string
	views    map[string]*pinnedView
	nextView uint64
}

func openRetained(path string) (_ *retained, err error) {
	owner, err := wal.OpenOwner(path)
	if err != nil {
		return nil, err
	}
	root, dir := owner.Directory()
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      4 << 30,
		checkpoint.ReplayTailBytes:   tailLimit,
		checkpoint.PinnedGenerations: 8,
		checkpoint.LiveCaptures:      80,
		checkpoint.TailMemoryBytes:   44 << 20,
		checkpoint.IngestMemoryBytes: 48 << 20,
	})
	ctx, cancel := context.WithCancel(context.Background())
	r := &retained{
		owner: owner, root: root, dir: dir, budget: budget,
		publisher: checkpoint.NewPublisher(root, dir, budget),
		catalog:   history.NewCatalog(budget), ctx: ctx, cancel: cancel,
		pins: make(map[[32]byte]int), dead: make(map[[32]byte]string),
		views: make(map[string]*pinnedView), failed: make(chan error, 1),
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, r.close())
		}
	}()
	if err := r.recover(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// reserveBatch charges the encoded frame and immutable decoded/index backing
// before building the retained index. The decoded input is still charged to
// the ingest or replay operation until ownership transfers here.
func (r *retained) reserveBatch(ctx context.Context, start, end int64, first uint64, rows []store.Row) (*tailBatch, error) {
	disk, err := r.budget.ReserveReplayTail(ctx, start, end)
	if err != nil {
		return nil, err
	}
	// String allocation classes can round up, and short strings can occupy
	// individual allocation slots. Include the row slice and the ordinal index.
	need := uint64(256) + uint64(cap(rows))*48 + uint64(len(rows))*32
	var maxLine int
	for _, row := range rows {
		need += uint64(len(row.Line)) * 2
		maxLine = max(maxLine, len(row.Line))
	}
	mem, err := r.budget.Reserve(ctx, checkpoint.TailMemoryBytes, need)
	if err != nil {
		disk.Release()
		return nil, err
	}
	return &tailBatch{start: start, end: end, first: first, rows: rows, disk: disk, mem: mem, maxLine: maxLine, refs: 1}, nil
}

func (b *tailBatch) index() {
	b.order = make([]uint32, len(b.rows))
	for i := range b.order {
		b.order[i] = uint32(i)
	}
	slices.SortFunc(b.order, func(i, j uint32) int {
		if cmp := b.rows[i].TS.Compare(b.rows[j].TS); cmp != 0 {
			return cmp
		}
		return int(i) - int(j)
	})
}

func (r *retained) LastCommitOutcome() wal.CommitOutcome { return r.owner.LastCommitOutcome() }

func (r *retained) Append(payload []byte, rows []store.Row) error {
	var attemptedAdvance bool
	for {
		r.mu.Lock()
		if r.fenced != nil {
			err := r.fenced
			r.mu.Unlock()
			return errors.Join(os.ErrClosed, err)
		}
		p, tail := r.point, r.tail
		size := int64(len(payload) + 16)
		if p.CommittedEnd > math.MaxInt64-size || p.CommittedOrdinal > math.MaxUint64-uint64(len(rows)) ||
			len(rows) == 0 || len(payload) == 0 || len(payload) > store.MaxBatch {
			r.mu.Unlock()
			return errors.New("invalid WAL append bounds")
		}
		b, err := r.reserveBatch(context.Background(), p.CommittedEnd, p.CommittedEnd+size, p.CommittedOrdinal, rows)
		if err != nil || len(r.batches) >= maxTailFrames {
			if b != nil {
				b.disk.Release()
				b.mem.Release()
			}
			if r.busy == nil && !attemptedAdvance && len(r.batches) != 0 {
				// Tail memory can fill before the byte or frame threshold,
				// for example with many small JSON records per frame.
				logMaintenance(r.startLocked(p.CommittedOrdinal, false))
			}
			busy := r.busy
			r.mu.Unlock()
			if busy == nil {
				return errors.Join(checkpoint.ErrExhausted, err)
			}
			select {
			case <-busy:
				attemptedAdvance = true
				continue
			case <-r.ctx.Done():
				return os.ErrClosed
			}
		}
		b.index()
		digest, err := r.digest.Clone()
		if err == nil {
			h := wal.FrameHeader(payload)
			_, err = digest.Write(h[:])
			if err == nil {
				_, err = digest.Write(payload)
			}
		}
		p.CommittedEnd, p.CommittedOrdinal = b.end, b.first+uint64(len(rows))
		if err == nil {
			p.SuffixSHA256 = [32]byte(digest.Sum(nil))
			err = r.owner.CommitFrame(tail.File, b.start-tail.base, payload, func() (string, error) {
				if tail.Name() == "bog.wal" {
					return r.owner.CommitLegacy(p.CommittedEnd)
				}
				return r.selectAuthority(tail, p, r.sequence)
			})
		}
		if err == nil {
			r.point, r.digest = p, digest
			r.batches = append(r.batches, b)
			r.maxLine = max(r.maxLine, b.maxLine)
			r.scheduleLocked()
		} else {
			b.disk.Release()
			b.mem.Release()
			if errors.Is(err, wal.ErrUncertain) {
				r.fenceLocked(err)
			}
		}
		r.mu.Unlock()
		return err
	}
}

func (r *retained) fenceLocked(err error) {
	if r.fenced == nil {
		r.fenced = errors.Join(wal.ErrUncertain, err)
		r.failed <- r.fenced
	}
}

func (r *retained) stop() {
	r.cancel()
}

func (r *retained) drain(ctx context.Context) error {
	r.mu.Lock()
	busy := r.busy
	r.mu.Unlock()
	if busy != nil {
		select {
		case <-busy:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pins) != 0 {
		return errors.New("storage views remain pinned")
	}
	if r.fenced != nil {
		return r.fenced
	}
	return r.releaseErr
}

func (r *retained) close() error {
	r.cancel()
	for _, b := range r.batches {
		b.disk.Release()
		b.mem.Release()
	}
	err := r.catalog.Close()
	if r.tail != nil {
		err = errors.Join(err, r.tail.Close())
	}
	if err != nil {
		// Startup has no outstanding users. The caller of close during
		// shutdown retains the directory lease if storage cleanup fails.
		return err
	}
	return r.owner.Close()
}

func (r *retained) recordRelease(err error) {
	if err != nil && r.releaseErr == nil {
		r.releaseErr = err
		log.Printf("retained storage cleanup: %v", err)
	}
}

func (r *retained) hasEntry(name string) (bool, error) {
	_, err := r.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (r *retained) OpenProbeDirectory() (*os.File, error) {
	return r.root.Open(".")
}

func logMaintenance(err error) {
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, syscall.ENOSPC) {
		log.Printf("retained checkpoint: %v", err)
	}
}

var _ interface {
	Append([]byte, []store.Row) error
	LastCommitOutcome() wal.CommitOutcome
} = (*retained)(nil)
