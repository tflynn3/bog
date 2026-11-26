package checkpoint

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

func ownedByService(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uint32(os.Geteuid())
}

func checkStaging(info os.FileInfo) error {
	if !info.IsDir() || !ownedByService(info) ||
		info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%w: unsafe publication staging directory", ErrIntegrity)
	}
	return nil
}

func openStaging(parent *os.File) (*os.File, error) {
	fd, err := syscall.Openat(int(parent.Fd()), stageDir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(fd), stageDir)
	info, err := dir.Stat()
	if err == nil {
		err = checkStaging(info)
	}
	if err != nil {
		return nil, errors.Join(err, dir.Close())
	}
	return dir, nil
}

func (p *publication) syncStaging() error {
	dir, err := openStaging(p.dir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (p *publication) removeEmptyStaging() error {
	// This name was checked with Lstat under the exclusive publication lock
	// in the service-owned parent. Directory removal tests emptiness atomically
	// without needing read or search permission on the directory itself.
	if err := p.root.Remove(stageDir); err != nil {
		return fmt.Errorf("%w: remove empty publication staging directory: %w", ErrIntegrity, err)
	}
	return p.dir.Sync()
}

func (p *publication) stagingNames(dir *os.File) ([]string, error) {
	names, err := dir.Readdirnames(MaxSegments + 4)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(names) > MaxSegments+3 {
		return nil, fmt.Errorf("%w: too many staging files", ErrIntegrity)
	}
	for _, name := range names {
		segment := validSegmentName(name)
		if !segment && name != intentBase && name != "intent" && name != "manifest" {
			return nil, fmt.Errorf("%w: unexpected staging file %q", ErrIntegrity, name)
		}
		info, err := p.root.Lstat(stageDir + "/" + name)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || !ownedByService(info) || info.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("%w: unsafe staging file %q", ErrIntegrity, name)
		}
		// A segment can have two names during installation. The other
		// staging files are never linked into the final namespace.
		links := info.Sys().(*syscall.Stat_t).Nlink
		if links != 1 && !(segment && links == 2) {
			return nil, fmt.Errorf("%w: unsafe staging link count for %q", ErrIntegrity, name)
		}
	}
	return names, nil
}

func (p *publication) clearStaging() error {
	info, err := p.root.Lstat(stageDir)
	if err != nil {
		return err
	}
	if err := checkStaging(info); err != nil {
		return err
	}
	if info.Mode().Perm() != 0700 {
		return p.removeEmptyStaging()
	}
	dir, err := openStaging(p.dir)
	if err != nil {
		return err
	}
	names, err := p.stagingNames(dir)
	if err = errors.Join(err, dir.Close()); err != nil {
		return err
	}
	return p.removeStaging(names)
}

func (p *publication) removeStaging(names []string) error {
	for _, name := range names {
		if name != intentBase {
			if err := p.root.Remove(stageDir + "/" + name); err != nil {
				return err
			}
		}
	}
	// Remove the ownership record last, after all other staging files.
	if err := p.root.Remove(intentName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return p.removeEmptyStaging()
}
