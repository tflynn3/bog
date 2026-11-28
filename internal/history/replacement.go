package history

import (
	"context"
	"errors"
	"os"
	"sync"

	"bog/internal/checkpoint"
)

type directoryOpener func() (*os.File, os.FileInfo, error)

// Replacement owns a reserved generation slot until Close or successful
// installation. A failed installation leaves the reservation available for retry.
type Replacement struct {
	mu      sync.Mutex
	catalog *Catalog
	charge  *checkpoint.Reservation
}

// ReserveReplacement admits a successor before its manifest is published.
// The owner serializes publication and catalog activation under its own lock.
func (c *Catalog) ReserveReplacement(ctx context.Context) (*Replacement, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ticket := &Replacement{catalog: c}
	if c.budget != nil {
		var err error
		ticket.charge, err = c.budget.Reserve(ctx, checkpoint.PinnedGenerations, 1)
		if err != nil {
			return nil, err
		}
	}
	return ticket, nil
}

// Close releases an unused slot. It is safe to call more than once.
func (r *Replacement) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.charge != nil {
		r.charge.Release()
	}
	r.catalog, r.charge = nil, nil
}

// ReplacePinned validates the durable successor, pins it and installs it for
// new captures. A populated Generation means installation succeeded even if
// releasing the previous generation or closing the lock reports an error.
func (r *Replacement) ReplacePinned(ctx context.Context, dir *os.File) (Generation, error) {
	return r.install(ctx, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) }, manifestName, nil, nil)
}

// ReplaceSelected validates and installs the generation named by recovered
// writable authority while holding the publication lock shared.
func (r *Replacement) ReplaceSelected(ctx context.Context, dir *os.File, id GenerationID) (Generation, error) {
	return r.install(ctx, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) }, checkpoint.GenerationManifestName(id), &id, nil)
}

// InstallPublished validates and pins a staged successor while the caller
// holds the publisher's exclusive lock. The callback durably selects it
// and excludes captures until the replacement has been activated.
func (r *Replacement) InstallPublished(ctx context.Context, dir *os.File, id GenerationID, selectAuthority func() error) (Generation, error) {
	return r.install(ctx, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) }, checkpoint.GenerationManifestName(id), &id, selectAuthority)
}

func (r *Replacement) install(ctx context.Context, open directoryOpener, name string, id *GenerationID, selectAuthority func() error) (_ Generation, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.catalog
	if c == nil {
		return Generation{}, os.ErrClosed
	}
	c.replace.Lock()
	defer c.replace.Unlock()
	if c.closed {
		return Generation{}, os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return Generation{}, err
	}
	dir, info, err := open()
	if err != nil {
		return Generation{}, err
	}
	if selectAuthority == nil {
		lock, err := checkpoint.LockPublication(dir, false)
		if err != nil {
			return Generation{}, errors.Join(err, dir.Close())
		}
		defer func() { err = errors.Join(err, lock.Close()) }()
		if err := checkpoint.CheckPublication(dir); err != nil {
			return Generation{}, errors.Join(err, dir.Close())
		}
	}
	var previous *generation
	if selectAuthority != nil && c.current != nil && os.SameFile(c.current.dirInfo, info) {
		previous = c.current
	}
	next, _, err := loadAuthority(ctx, dir, name, id, previous)
	if err != nil {
		return Generation{}, errors.Join(err, dir.Close())
	}
	next.dirInfo = info
	if err := next.registerPin(ctx); err != nil {
		return Generation{}, errors.Join(err, dir.Close())
	}
	next.refs.Store(1)
	if err := ctx.Err(); err != nil {
		return Generation{}, errors.Join(err, next.release())
	}
	if selectAuthority != nil {
		if err := selectAuthority(); err != nil {
			return Generation{}, errors.Join(err, next.release())
		}
	}
	c.mu.Lock()
	next.charge = r.charge
	r.catalog, r.charge = nil, nil
	prev := c.current
	c.current = next
	c.mu.Unlock()
	if prev != nil {
		c.trackSuperseded(prev, next)
		err = prev.release()
	}
	return next.info, err
}
