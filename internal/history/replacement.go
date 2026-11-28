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
	return r.install(ctx, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) })
}

func (r *Replacement) install(ctx context.Context, open directoryOpener) (_ Generation, err error) {
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
	if err := next.registerPin(ctx); err != nil {
		return Generation{}, errors.Join(err, dir.Close())
	}
	next.refs.Store(1)
	c.mu.Lock()
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return Generation{}, errors.Join(err, next.release())
	}
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

// PrepareSelected validates and pins a named generation before durable
// selection. The caller holds the checkpoint publication lock and serializes
// replacements. The empty name selects the canonical ordinary manifest.
func (r *Replacement) PrepareSelected(ctx context.Context, dir *os.File, name string, id GenerationID) (*Prepared, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.catalog
	if c == nil {
		return nil, os.ErrClosed
	}
	c.mu.Lock()
	previous := c.current
	c.mu.Unlock()
	d, info, err := openPinnedDirectory(dir)
	if err != nil {
		return nil, err
	}
	if name == "" {
		name = manifestName
	}
	g, _, err := loadNamedAuthority(ctx, d, name, previous)
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	if g.info.ID != id {
		return nil, errors.Join(ErrIntegrity, d.Close())
	}
	g.dirInfo = info
	if err := g.registerPin(ctx); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	g.refs.Store(1)
	g.charge = r.charge
	r.catalog, r.charge = nil, nil
	return &Prepared{catalog: c, next: g}, nil
}

// Prepared holds the reserved slot and a validated disk pin across selection.
// The caller installs it under the WAL owner's synchronization or closes it.
type Prepared struct {
	catalog *Catalog
	next    *generation
}

func (p *Prepared) Install() (Generation, error) {
	c := p.catalog
	c.replace.Lock()
	defer c.replace.Unlock()
	next := p.next
	p.next = nil
	c.mu.Lock()
	prev := c.current
	c.current = next
	c.mu.Unlock()
	if prev != nil {
		c.trackSuperseded(prev, next)
		return next.info, prev.release()
	}
	return next.info, nil
}

func (p *Prepared) Close() error {
	if p.next == nil {
		return nil
	}
	g := p.next
	p.next = nil
	return g.release()
}

// ValidateManifest loads a named immutable checkpoint with the same validation
// used for serving, without installing it or taking a catalog reference.
func ValidateManifest(ctx context.Context, dir *os.File, name string) (*checkpoint.Manifest, error) {
	_, manifest, err := loadNamedAuthority(ctx, dir, name, nil)
	return manifest, err
}
