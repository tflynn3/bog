package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

const publicationLockName = "bog.checkpoint.lock"

// LockPublication serializes manifest publication and history maintenance in
// the pinned directory. Readers acquiring a history generation use a shared
// lock. Publishers and reclaimers use an exclusive lock. Close the returned
// file to unlock. A competing operation returns EWOULDBLOCK.
func LockPublication(dir *os.File, exclusive bool) (_ *os.File, err error) {
	const flags = syscall.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_NONBLOCK | syscall.O_CLOEXEC
	dirFD := int(dir.Fd())
	fd, err := openat(dirFD, publicationLockName, flags, 0)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		fd, err = openat(dirFD, publicationLockName, flags|syscall.O_CREAT|syscall.O_EXCL, 0600)
		created = err == nil
		if errors.Is(err, os.ErrExist) {
			fd, err = openat(dirFD, publicationLockName, flags, 0)
		}
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), publicationLockName)
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	if created {
		if err := f.Chmod(0600); err != nil {
			return nil, err
		}
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 || info.Mode().Perm()&0022 != 0 {
		return nil, fmt.Errorf("%w: unsafe publication lock", ErrIntegrity)
	}
	mode := syscall.LOCK_SH
	if exclusive {
		mode = syscall.LOCK_EX
	}
	if err := syscall.Flock(fd, mode|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	return f, nil
}

// CheckPublication reports whether a publication needs recovery. Hold the
// publication lock while using this result to load or reclaim history.
func CheckPublication(dir *os.File) error {
	stage, err := openStaging(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if errors.Is(err, os.ErrPermission) {
		// An interrupted mkdir under umask 0777 may be inaccessible to
		// the service. Only exclusive recovery can validate and remove it.
		return ErrPublicationUncertain
	}
	if err != nil {
		return fmt.Errorf("%w: open publication staging directory: %w", ErrIntegrity, err)
	}
	if err := stage.Close(); err != nil {
		return err
	}
	return ErrPublicationUncertain
}
