package history

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"

	"bog/internal/checkpoint"
)

type retiredDirectory struct {
	info     os.FileInfo
	segments map[string]os.FileInfo
}

func (c *Catalog) trackRetired(g *generation) {
	c.trackSuperseded(g, nil)
}

// A shared file will become a retirement candidate when the generation
// that stops referencing it is installed. Keep only disappeared names
// here, so an append-only replacement at the ceiling does not fill the
// candidate table with the entire still-live manifest.
func (c *Catalog) trackSuperseded(g, successor *generation) {
	if len(g.segments) == 0 {
		return
	}
	var retained map[string]struct{}
	if successor != nil && os.SameFile(g.dirInfo, successor.dirInfo) {
		retained = make(map[string]struct{}, len(successor.segments))
		for _, s := range successor.segments {
			retained[s.name] = struct{}{}
		}
	}
	i := c.retiredIndex(g.dirInfo)
	for _, s := range g.segments {
		if _, shared := retained[s.name]; shared {
			continue
		}
		if i < 0 {
			i = len(c.retired)
			c.retired = append(c.retired, retiredDirectory{info: g.dirInfo, segments: make(map[string]os.FileInfo)})
		}
		c.retired[i].segments[s.name] = s.stat
	}
}

func (c *Catalog) retiredIndex(info os.FileInfo) int {
	for i, d := range c.retired {
		if os.SameFile(d.info, info) {
			return i
		}
	}
	return -1
}

// Reclaim removes retired segments previously observed by this catalog after
// validating the authoritative checkpoint under the exclusive publication lock.
// A shared segment or one with live readers remains eligible for a later call.
// It is safe to call after Close, including while retired readers are draining.
func (c *Catalog) Reclaim(ctx context.Context, path string) error {
	return c.reclaim(ctx, false, func() (*os.File, os.FileInfo, error) { return openDirectory(path) })
}

// ReclaimPinned is Reclaim using the caller's pinned directory.
func (c *Catalog) ReclaimPinned(ctx context.Context, dir *os.File) error {
	return c.reclaim(ctx, false, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) })
}

// ReclaimSelected uses the installed catalog generation as authority. The
// command serializes it with durable replacement under the directory lease.
func (c *Catalog) ReclaimSelected(ctx context.Context, dir *os.File) error {
	return c.reclaim(ctx, true, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) })
}

func (c *Catalog) reclaim(ctx context.Context, selected bool, open directoryOpener) (err error) {
	c.replace.Lock()
	defer c.replace.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, info, err := open()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	i := c.retiredIndex(info)
	if i < 0 {
		return nil
	}
	lock, err := checkpoint.LockPublication(dir, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	var current *generation
	if selected {
		current = c.current
		if current == nil || !os.SameFile(current.dirInfo, info) {
			return ErrIntegrity
		}
		if err := checkpoint.CheckPublication(dir); err != nil {
			return err
		}
	} else {
		current, err = loadAt(ctx, dir)
		if err != nil {
			return err
		}
	}
	candidates := c.retired[i].segments
	protected := make(map[string]bool, len(current.segments))
	for _, s := range current.segments {
		if _, retired := candidates[s.name]; retired {
			protected[s.name] = true
		}
	}
	if err := protectPinned(ctx, dir, candidates, protected); err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		return err
	}
	var finished []string
	var reclaimErr error
	for name, expected := range candidates {
		if reclaimErr = ctx.Err(); reclaimErr != nil {
			break
		}
		if protected[name] {
			continue
		}
		complete, err := reclaimSegment(dir, name, expected)
		if complete {
			finished = append(finished, name)
		}
		if err != nil {
			reclaimErr = err
			break
		}
	}
	if len(finished) != 0 {
		if err := dir.Sync(); err != nil {
			return errors.Join(reclaimErr, err)
		}
		for _, name := range finished {
			delete(candidates, name)
		}
	}
	if len(candidates) == 0 {
		last := len(c.retired) - 1
		c.retired[i] = c.retired[last]
		c.retired[last] = retiredDirectory{}
		c.retired = c.retired[:last]
	}
	return reclaimErr
}

func reclaimSegment(dir *os.File, name string, expected os.FileInfo) (complete bool, err error) {
	f, info, err := openRegular(dir, name)
	if errors.Is(err, os.ErrNotExist) {
		// The retired file has already been removed.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if !sameSegment(expected, info) {
		return true, nil
	}
	// Older readers pin segment inodes directly.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return false, nil
		}
		return false, err
	}
	if err := unlinkat(int(dir.Fd()), name); err != nil {
		return false, err
	}
	return true, nil
}

// RemoveManifest verifies the immutable name and payload identity before
// unlinking a retired manifest. The caller holds the directory lease and has
// drained all references to the generation.
func RemoveManifest(root *os.Root, dir *os.File, name string, id GenerationID) error {
	if name != checkpoint.GenerationManifestName(id) {
		return ErrIntegrity
	}
	_, actual, err := readManifest(dir, name)
	if errors.Is(err, os.ErrNotExist) {
		return dir.Sync()
	}
	if err != nil {
		return err
	}
	if actual != id {
		return ErrIntegrity
	}
	return errors.Join(root.Remove(name), dir.Sync())
}

// ReclaimOrphans settles retirement interrupted after durable selection. The
// candidate set comes from verified immutable manifests, processed one at a
// time. Active and independently pinned segment files remain protected.
func (c *Catalog) ReclaimOrphans(ctx context.Context, root *os.Root, dir *os.File, selected string) (err error) {
	const prefix = "bog-checkpoint-"
	c.replace.Lock()
	defer c.replace.Unlock()
	if c.current == nil || selected != checkpoint.GenerationManifestName(c.current.info.ID) {
		return ErrIntegrity
	}
	entries, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, entries.Close()) }()
	lock, err := checkpoint.LockPublication(dir, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	active := make(map[string]bool, len(c.current.segments))
	for _, s := range c.current.segments {
		active[s.name] = true
	}
	for {
		names, readErr := entries.Readdirnames(64)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			raw, ok := strings.CutPrefix(name, prefix)
			id, valid := parseDigest(raw)
			if !ok || !valid || name == selected {
				continue
			}
			if err := reclaimOrphan(ctx, root, dir, active, name, id); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func reclaimOrphan(ctx context.Context, root *os.Root, dir *os.File, active map[string]bool, name string, id GenerationID) error {
	m, actual, err := readManifest(dir, name)
	if err != nil {
		return err
	}
	if actual != id {
		return ErrIntegrity
	}
	candidates := make(map[string]os.FileInfo)
	var scratch []byte
	for _, meta := range m.Segments {
		if active[meta.Name] {
			continue
		}
		f, info, err := openRegular(dir, meta.Name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Size() != meta.Size {
			return errors.Join(ErrIntegrity, f.Close())
		}
		if int64(cap(scratch)) < meta.Size {
			scratch = make([]byte, meta.Size)
		}
		b := scratch[:meta.Size]
		readErr := readExactAt(f, b, 0)
		if err := errors.Join(readErr, f.Close()); err != nil {
			return err
		}
		sum, _ := parseDigest(meta.SHA256)
		if sha256.Sum256(b) != sum {
			return ErrIntegrity
		}
		candidates[meta.Name] = info
	}
	scan, err := root.Open(".")
	if err != nil {
		return err
	}
	protected := make(map[string]bool)
	if err := errors.Join(protectPinned(ctx, scan, candidates, protected), scan.Close()); err != nil {
		return err
	}
	all := true
	for name, info := range candidates {
		if protected[name] {
			all = false
			continue
		}
		complete, err := reclaimSegment(dir, name, info)
		if err != nil {
			return err
		}
		all = all && complete
	}
	if !all {
		return dir.Sync()
	}
	return RemoveManifest(root, dir, name, id)
}
