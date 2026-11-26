package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
)

const (
	stagingGuardMode  = 0600
	stagingGuardFlags = os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
)

type stagingGuardHandle struct {
	parent     *os.Root
	parentInfo os.FileInfo
	file       *os.File
}

// O_EXCL elects a single creator even when the inherited umask clears all
// permission bits. An interrupted creator leaves the canonical inode in
// place. Both bootstrap and repair complete under the same publication lock.
func openStagingGuard(ctx context.Context, parent *os.Root, dir *os.File, parentInfo os.FileInfo) (_ stagingGuardHandle, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("open backup staging guard: %w", err)
		}
	}()
	if err := ctx.Err(); err != nil {
		return stagingGuardHandle{}, err
	}
	file, err := parent.OpenFile(stagingGuard, stagingGuardFlags|os.O_CREATE|os.O_EXCL, stagingGuardMode)
	if errors.Is(err, os.ErrExist) {
		file, err = reopenStagingGuard(ctx, parent, dir, parentInfo)
	}
	if err != nil {
		return stagingGuardHandle{}, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, file.Close())
		}
	}()
	guard := stagingGuardHandle{parent: parent, parentInfo: parentInfo, file: file}
	if err := guard.lock(ctx); err != nil {
		return stagingGuardHandle{}, err
	}
	initErr := guard.initialize(dir)
	if err := errors.Join(initErr, unlock(file)); err != nil {
		return stagingGuardHandle{}, err
	}
	return guard, nil
}

func reopenStagingGuard(ctx context.Context, parent *os.Root, dir *os.File, parentInfo os.FileInfo) (_ *os.File, err error) {
	info, err := parent.Lstat(stagingGuard)
	if err != nil {
		return nil, err
	}
	if err := validateStagingGuard(info, parentInfo); err != nil {
		return nil, err
	}
	if info.Mode().Perm() != stagingGuardMode {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// This metadata step can be needed just to get a lockable handle.
		// Concurrent repairs set the same mode on the same inode.
		if err := repairStagingGuardMode(parent, dir, parentInfo, info); err != nil {
			return nil, err
		}
	}
	file, err := parent.OpenFile(stagingGuard, stagingGuardFlags, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, file.Close())
		}
	}()
	if _, err := (stagingGuardHandle{parent, parentInfo, file}).check(info); err != nil {
		return nil, err
	}
	return file, nil
}

func validateStagingGuard(info, parentInfo os.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	dir, dirOK := parentInfo.Sys().(*syscall.Stat_t)
	if !ok || !dirOK || !info.Mode().IsRegular() || st.Uid != uint32(os.Geteuid()) ||
		st.Dev != dir.Dev || st.Nlink != 1 || info.Size() != 0 || info.Mode()&^stagingGuardMode != 0 {
		return errors.New("unsafe backup staging guard")
	}
	return nil
}

func checkStagingGuardEntry(parent *os.Root, parentInfo, expected os.FileInfo) error {
	current, err := parent.Lstat(stagingGuard)
	if err != nil {
		return err
	}
	if !os.SameFile(current, expected) {
		return errors.New("backup staging guard changed")
	}
	return validateStagingGuard(current, parentInfo)
}

func (g stagingGuardHandle) check(expected os.FileInfo) (os.FileInfo, error) {
	info, err := g.file.Stat()
	if err != nil {
		return nil, err
	}
	if expected != nil && !os.SameFile(info, expected) {
		return nil, errors.New("backup staging guard changed during open")
	}
	if err := validateStagingGuard(info, g.parentInfo); err != nil {
		return nil, err
	}
	return info, checkStagingGuardEntry(g.parent, g.parentInfo, info)
}

func (g stagingGuardHandle) lock(ctx context.Context) error {
	if err := lockFile(ctx, g.file); err != nil {
		return err
	}
	_, err := g.check(nil)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return errors.Join(err, unlock(g.file))
	}
	return nil
}

func (g stagingGuardHandle) initialize(dir *os.File) error {
	info, err := g.file.Stat()
	if err != nil {
		return err
	}
	if info.Mode().Perm() != stagingGuardMode {
		if err := g.file.Chmod(stagingGuardMode); err != nil {
			return err
		}
	}
	// A previous publisher may have exited after chmod but before either
	// sync. Seal both before using the guard to mutate the staging namespace.
	if err := g.file.Sync(); err != nil {
		return err
	}
	return dir.Sync()
}
