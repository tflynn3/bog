package backup

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

// Darwin requires data permissions even on an event-only open. Use a
// no-follow chmod relative to the pinned parent, checking identity before
// and after. The caller also verifies the reopened guard's identity.
func repairStagingGuardMode(parent *os.Root, dir *os.File, parentInfo, expected os.FileInfo) error {
	if err := checkStagingGuardEntry(parent, parentInfo, expected); err != nil {
		return err
	}
	if err := chmodStagingEntry(dir, stagingGuard, stagingGuardMode); err != nil {
		return err
	}
	return checkStagingGuardEntry(parent, parentInfo, expected)
}

// Darwin applies umask to symlinks, and readlink requires read permission.
// Recovery runs under the guard. Grant owner read access to the private
// metadata link itself, then leave payload and stage validation to recover.
func readStagingLink(c stageClaim, name string, size int) (string, error) {
	target, err := c.parent.Readlink(name)
	if !errors.Is(err, syscall.EACCES) {
		return target, err
	}
	target, repairErr := repairStagingLink(c, name, size)
	if repairErr != nil {
		// Only ENOENT from the initial read means there was no claim.
		return "", fmt.Errorf("inspect unreadable staging link: %v: %w", repairErr, err)
	}
	return target, nil
}

func repairStagingLink(c stageClaim, name string, size int) (string, error) {
	info, err := c.parent.Lstat(name)
	if err != nil {
		return "", err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	dir, dirOK := c.parentInfo.Sys().(*syscall.Stat_t)
	if !ok || !dirOK || info.Mode()&os.ModeType != os.ModeSymlink ||
		st.Uid != uint32(os.Geteuid()) || st.Dev != dir.Dev || st.Nlink != 1 ||
		info.Size() != int64(size) || info.Mode().Perm()&0400 != 0 {
		return "", errors.New("unsafe unreadable backup staging link")
	}
	if err := chmodStagingEntry(c.dir, name, info.Mode().Perm()|0400); err != nil {
		return "", err
	}
	after, err := c.parent.Lstat(name)
	if err != nil {
		return "", err
	}
	if !os.SameFile(info, after) {
		return "", errors.New("backup staging link changed during repair")
	}
	return c.parent.Readlink(name)
}

func chmodStagingEntry(dir *os.File, name string, mode os.FileMode) error {
	// Darwin's syscall package does not export fchmodat.
	const (
		sysFchmodat       = 467
		atSymlinkNoFollow = 0x0020
	)
	path, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}
	fd := dir.Fd()
	for {
		_, _, errno := syscall.Syscall6(sysFchmodat, fd, uintptr(unsafe.Pointer(path)), uintptr(mode), atSymlinkNoFollow, 0, 0)
		runtime.KeepAlive(dir)
		switch errno {
		case 0:
			return nil
		case syscall.EINTR:
			continue
		default:
			return &os.PathError{Op: "fchmodat", Path: name, Err: errno}
		}
	}
}
