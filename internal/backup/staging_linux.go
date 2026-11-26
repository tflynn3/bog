package backup

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"syscall"
)

func renameStage(_ *os.Root, dir *os.File, stage, name string, _ bool) error {
	fd := int(dir.Fd())
	if err := syscall.Renameat(fd, stage, fd, name); err != nil {
		return &os.LinkError{Op: "renameat", Old: stage, New: name, Err: err}
	}
	return nil
}

// Pin the inode without requiring data access. Chmod through the pin so a
// replacement at the guard name cannot redirect the permission repair.
func repairStagingGuardMode(parent *os.Root, _ *os.File, parentInfo, expected os.FileInfo) (err error) {
	const (
		oPath       = 0x200000
		atEmptyPath = 0x1000
	)
	file, err := parent.OpenFile(stagingGuard, oPath|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	guard := stagingGuardHandle{parent, parentInfo, file}
	info, err := guard.check(expected)
	if err != nil || info.Mode().Perm() == stagingGuardMode {
		return err
	}
	fd := file.Fd()
	err = syscall.Fchmodat(int(fd), "", stagingGuardMode, atEmptyPath)
	if errors.Is(err, syscall.EOPNOTSUPP) {
		// Older kernels lack fchmodat2. Procfs still addresses the pinned
		// inode, including when the parent itself has been renamed.
		err = os.Chmod(fmt.Sprintf("/proc/self/fd/%d", fd), stagingGuardMode)
	}
	runtime.KeepAlive(file)
	return err
}
