package backup

import (
	"os"
	"syscall"
)

func renameStage(_ *os.Root, dir *os.File, stage, name string, _ bool) error {
	fd := int(dir.Fd())
	if err := syscall.Renameat(fd, stage, fd, name); err != nil {
		return &os.LinkError{Op: "renameat", Old: stage, New: name, Err: err}
	}
	return nil
}
