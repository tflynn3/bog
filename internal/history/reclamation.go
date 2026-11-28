package history

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
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
	return c.reclaim(ctx, func() (*os.File, os.FileInfo, error) { return openDirectory(path) })
}

// ReclaimPinned is Reclaim using the caller's pinned directory.
func (c *Catalog) ReclaimPinned(ctx context.Context, dir *os.File) error {
	return c.reclaim(ctx, func() (*os.File, os.FileInfo, error) { return openPinnedDirectory(dir) })
}

func (c *Catalog) reclaim(ctx context.Context, open directoryOpener) (err error) {
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
	current, err := loadAt(ctx, dir, nil)
	if err != nil {
		return err
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

// ReclaimObsolete reclaims predecessor segments using the manifest preserved by
// the owner's durable retirement record. It returns the byte count actually
// unlinked and whether every obsolete file has drained. The caller serializes
// this operation with changes to selected authority.
func ReclaimObsolete(ctx context.Context, dir *os.File, selected GenerationID, predecessor *checkpoint.Manifest) (removed uint64, complete bool, err error) {
	lock, err := checkpoint.LockPublication(dir, true)
	if err != nil {
		return 0, false, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	current, _, err := readManifest(dir, &selected)
	if err != nil {
		return 0, false, err
	}
	names := make(map[string]bool, len(current.Segments))
	for _, seg := range current.Segments {
		names[seg.Name] = true
	}
	candidates := make(map[string]os.FileInfo)
	var scratch []byte
	for _, seg := range predecessor.Segments {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		if names[seg.Name] {
			continue
		}
		expected, ok := parseDigest(seg.SHA256)
		if !validSegmentName(seg.Name) || !ok || seg.Size < 0 || seg.Size > checkpoint.MaxBlockSize {
			return 0, false, corrupt(seg.Name, "invalid retired segment metadata")
		}
		f, info, err := openRegular(dir, seg.Name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, false, err
		}
		if info.Size() != seg.Size {
			return 0, false, errors.Join(corrupt(seg.Name, "retired segment size changed"), f.Close())
		}
		if int64(cap(scratch)) < seg.Size {
			scratch = make([]byte, seg.Size)
		}
		b := scratch[:seg.Size]
		if err := errors.Join(readExactAt(f, b, 0), f.Close()); err != nil {
			return 0, false, err
		}
		if sha256.Sum256(b) != expected {
			return 0, false, corrupt(seg.Name, "retired segment digest changed")
		}
		candidates[seg.Name] = info
	}
	protected := make(map[string]bool)
	if len(candidates) != 0 {
		if err := protectPinned(ctx, dir, candidates, protected); err != nil {
			return 0, false, err
		}
	}
	complete = true
	for name, info := range candidates {
		if protected[name] {
			complete = false
			continue
		}
		if err := ctx.Err(); err != nil {
			return removed, false, errors.Join(err, dir.Sync())
		}
		done, err := reclaimSegment(dir, name, info)
		if err != nil {
			return removed, false, errors.Join(err, dir.Sync())
		}
		if done {
			removed += uint64(info.Size())
		} else {
			complete = false
		}
	}
	return removed, complete, dir.Sync()
}
