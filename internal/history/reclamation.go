package history

import (
	"context"
	"errors"
	"os"
	"syscall"

	"bog/internal/checkpoint"
)

// One shared lock per segment pins its name for any generation that can still
// scan it, including generations in another catalog or process. The generation
// holds these descriptors until its last scan and reader release their pins.
func (g *generation) pinSegments(ctx context.Context) error {
	for i := range g.segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		s := &g.segments[i]
		f, info, err := openRegular(g.dir, s.name)
		if err != nil {
			return segmentError(s.name, err)
		}
		s.pin = f
		if !sameSegment(s.stat, info) {
			return corrupt(s.name, "segment file changed while pinning")
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
			return segmentError(s.name, err)
		}
	}
	return nil
}

func (g *generation) closePins() error {
	var err error
	for i := range g.segments {
		if f := g.segments[i].pin; f != nil {
			err = errors.Join(err, f.Close())
		}
	}
	return err
}

type retiredDirectory struct {
	info     os.FileInfo
	segments map[string]os.FileInfo
}

func (c *Catalog) trackRetired(g *generation) {
	if len(g.segments) == 0 {
		return
	}
	i := c.retiredIndex(g.dirInfo)
	if i < 0 {
		i = len(c.retired)
		c.retired = append(c.retired, retiredDirectory{info: g.dirInfo, segments: make(map[string]os.FileInfo)})
	}
	for _, s := range g.segments {
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
func (c *Catalog) Reclaim(ctx context.Context, path string) (err error) {
	c.replace.Lock()
	defer c.replace.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, info, err := openDirectory(path)
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
	current, err := loadAt(ctx, dir)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		return err
	}
	reachable := make(map[string]bool, len(current.segments))
	for _, s := range current.segments {
		reachable[s.name] = true
	}
	candidates := c.retired[i].segments
	var finished []string
	var reclaimErr error
	for name, expected := range candidates {
		if reclaimErr = ctx.Err(); reclaimErr != nil {
			break
		}
		if reachable[name] {
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
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return false, nil
		}
		return false, err
	}
	if err := syscall.Unlinkat(int(dir.Fd()), name); err != nil {
		return false, err
	}
	return true, nil
}
