// Package history serves immutable, published v1 checkpoints from disk.
package history

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"bog/internal/checkpoint"
)

var (
	// ErrExpired indicates that a generation is unavailable for acquisition.
	ErrExpired = errors.New("history generation expired")
	// ErrIntegrity indicates a malformed checkpoint or an invalid referenced file.
	ErrIntegrity = errors.New("invalid history checkpoint")
)

// GenerationID is the SHA-256 of the exact manifest payload bytes.
type GenerationID [sha256.Size]byte

// ExpiredError identifies the unavailable generation. A zero ID represents a
// request for the current generation when there is none.
type ExpiredError struct {
	ID GenerationID
}

func (e *ExpiredError) Error() string { return fmt.Sprintf("%s: %x", ErrExpired, e.ID) }
func (e *ExpiredError) Unwrap() error { return ErrExpired }

// Boundary identifies the immutable WAL prefix covered by the generation.
type Boundary struct {
	SourceSHA256  [sha256.Size]byte
	CoveredOffset int64  // Exclusive byte offset, including the WAL magic.
	NextOrdinal   uint64 // First append ordinal after CoveredOffset.
}

// Generation describes a published snapshot. Drained closes after retirement
// and release of every reader and active scan. A reclaimer must also check
// the authoritative manifest and any other generations that share its files.
type Generation struct {
	ID           GenerationID
	MaxLineBytes int
	Boundary     Boundary
	Drained      <-chan struct{}
}

// Catalog owns the current generation. Its zero value is ready for use. It
// must not be copied after first use.
type Catalog struct {
	replace sync.Mutex
	mu      sync.Mutex
	current *generation
	closed  bool
	retired []retiredDirectory
	budget  *checkpoint.Budget
}

type generation struct {
	info     Generation
	drained  chan struct{}
	refs     atomic.Int64
	dir      *os.File
	dirInfo  os.FileInfo
	pin      *os.File
	charge   *checkpoint.Reservation
	segments []segment
}

func (g *generation) release() error {
	if g.refs.Add(-1) != 0 {
		return nil
	}
	err := errors.Join(g.releasePin(), g.dir.Close())
	if g.charge != nil {
		g.charge.Release()
	}
	close(g.drained)
	return err
}

// NewCatalog bounds generation and capture admission using budget. The legacy
// zero-value Catalog retains its original unbudgeted behavior.
func NewCatalog(budget *checkpoint.Budget) *Catalog {
	return &Catalog{budget: budget}
}

// Replace validates and installs the manifest in path for new captures.
func (c *Catalog) Replace(ctx context.Context, path string) (Generation, error) {
	return c.replaceWith(ctx, func() (*os.File, os.FileInfo, error) { return openDirectory(path) })
}

// ReplacePinned is Replace using the caller's pinned directory. The catalog
// opens and owns an independent handle to the same directory.
func (c *Catalog) ReplacePinned(ctx context.Context, dir *os.File) (Generation, error) {
	return c.replaceWith(ctx, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) })
}

func (c *Catalog) replaceWith(ctx context.Context, open directoryOpener) (Generation, error) {
	ticket, err := c.ReserveReplacement(ctx)
	if err != nil {
		return Generation{}, err
	}
	defer ticket.Close()
	return ticket.install(ctx, manifestName, open)
}

// Acquire pins id if it is current. Already acquired readers keep their
// snapshots after replacement, and a different current ID expires id.
func (c *Catalog) Acquire(id GenerationID) (*Reader, error) {
	return c.acquire(context.Background(), &id, time.Time{})
}

// AcquireCurrent pins the current generation.
func (c *Catalog) AcquireCurrent() (*Reader, error) {
	return c.AcquireCurrentUntil(context.Background(), time.Time{})
}

// AcquireCurrentUntil pins the current generation and admits scans until the
// optional deadline or explicit Close. The caller closes expired readers to
// retire idle captures. Cancellation of ctx does not retire a returned reader.
func (c *Catalog) AcquireCurrentUntil(ctx context.Context, deadline time.Time) (*Reader, error) {
	return c.acquire(ctx, nil, deadline)
}

func (c *Catalog) acquire(ctx context.Context, id *GenerationID, deadline time.Time) (*Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g := c.current
	if g == nil || (id != nil && *id != g.info.ID) {
		err := &ExpiredError{}
		if id != nil {
			err.ID = *id
		}
		return nil, err
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return nil, &ExpiredError{ID: g.info.ID}
	}
	var charge *checkpoint.Reservation
	if c.budget != nil {
		var err error
		charge, err = c.budget.Reserve(ctx, checkpoint.LiveCaptures, 1)
		if err != nil {
			return nil, err
		}
	}
	g.refs.Add(1)
	return &Reader{gen: g, info: g.info, deadline: deadline, charge: charge}, nil
}

// Close retires the current generation and prevents further replacement. It
// does not wait for active readers. It is safe to call more than once.
func (c *Catalog) Close() error {
	c.replace.Lock()
	defer c.replace.Unlock()
	c.mu.Lock()
	c.closed = true
	g := c.current
	c.current = nil
	c.mu.Unlock()
	if g != nil {
		c.trackRetired(g)
		return g.release()
	}
	return nil
}

// Reader pins one generation until Close. It supports concurrent scans and
// must not be copied. Closing a Reader does not interrupt scans already started.
type Reader struct {
	mu       sync.Mutex
	gen      *generation
	info     Generation
	deadline time.Time
	charge   *checkpoint.Reservation
}

// Generation returns information about the pinned snapshot.
func (r *Reader) Generation() Generation { return r.info }

// Close releases the reader's reference. It is safe to call more than once.
func (r *Reader) Close() error {
	r.mu.Lock()
	g, charge := r.gen, r.charge
	r.gen, r.charge = nil, nil
	r.mu.Unlock()
	if charge != nil {
		defer charge.Release()
	}
	if g != nil {
		return g.release()
	}
	return nil
}
