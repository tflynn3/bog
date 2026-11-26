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
	ID       GenerationID
	Boundary Boundary
	Drained  <-chan struct{}
}

// Catalog owns the current generation. Its zero value is ready for use. It
// must not be copied after first use.
type Catalog struct {
	replace sync.Mutex
	mu      sync.Mutex
	current *generation
	closed  bool
	retired []retiredDirectory
}

type generation struct {
	info     Generation
	drained  chan struct{}
	refs     atomic.Int64
	dir      *os.File
	dirInfo  os.FileInfo
	segments []segment
}

func (g *generation) release() error {
	if g.refs.Add(-1) != 0 {
		return nil
	}
	err := errors.Join(g.closePins(), g.dir.Close())
	close(g.drained)
	return err
}

// Replace validates the authoritative bog.checkpoint in dir and installs it
// atomically for new readers. A shared publication lock keeps the manifest
// stable until the new generation is pinned. A validation or cancellation
// failure leaves the current generation available. If closing the retired
// directory fails, Replace returns the installed generation and that error.
func (c *Catalog) Replace(ctx context.Context, path string) (_ Generation, err error) {
	c.replace.Lock()
	defer c.replace.Unlock()
	// Close and Replace are serialized, so closed is stable here.
	if c.closed {
		return Generation{}, os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return Generation{}, err
	}
	dir, info, err := openDirectory(path)
	if err != nil {
		return Generation{}, err
	}
	lock, err := checkpoint.LockPublication(dir, false)
	if err != nil {
		return Generation{}, errors.Join(err, dir.Close())
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	next, err := loadAt(ctx, dir)
	if err != nil {
		return Generation{}, errors.Join(err, dir.Close())
	}
	next.dirInfo = info
	if err := next.pinSegments(ctx); err != nil {
		return Generation{}, errors.Join(err, next.closePins(), dir.Close())
	}
	next.refs.Store(1)
	c.mu.Lock()
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return Generation{}, errors.Join(err, next.release())
	}
	prev := c.current
	c.current = next
	c.mu.Unlock()
	if prev != nil {
		c.trackRetired(prev)
		err = prev.release()
	}
	return next.info, err
}

// Acquire pins id if it is current. Already acquired readers keep their
// snapshots after replacement, and a different current ID expires id.
func (c *Catalog) Acquire(id GenerationID) (*Reader, error) {
	return c.acquire(&id)
}

// AcquireCurrent pins the current generation.
func (c *Catalog) AcquireCurrent() (*Reader, error) {
	return c.acquire(nil)
}

func (c *Catalog) acquire(id *GenerationID) (*Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.current
	if g != nil && (id == nil || *id == g.info.ID) {
		g.refs.Add(1)
		return &Reader{gen: g, info: g.info}, nil
	}
	err := &ExpiredError{}
	if id != nil {
		err.ID = *id
	}
	return nil, err
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
	mu   sync.Mutex
	gen  *generation
	info Generation
}

// Generation returns information about the pinned snapshot.
func (r *Reader) Generation() Generation { return r.info }

// Close releases the reader's reference. It is safe to call more than once.
func (r *Reader) Close() error {
	r.mu.Lock()
	g := r.gen
	r.gen = nil
	r.mu.Unlock()
	if g != nil {
		return g.release()
	}
	return nil
}
