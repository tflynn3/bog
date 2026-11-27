package main

import (
	"os"
	"syscall"
)

type localReportDirectory struct {
	*os.File
}

// The descriptor pins the resolved parent for every operation, including fsync.
func openReportDirectory(path string) (reportDirectory, error) {
	dir, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	return localReportDirectory{dir}, nil
}

func (d localReportDirectory) OpenExclusive(name string) (reportStagingFile, error) {
	fd, err := syscall.Openat(int(d.Fd()), name,
		syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: name, Err: err}
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (d localReportDirectory) Rename(from, to string) error {
	fd := int(d.Fd())
	if err := syscall.Renameat(fd, from, fd, to); err != nil {
		return &os.LinkError{Op: "renameat", Old: from, New: to, Err: err}
	}
	return nil
}

func (d localReportDirectory) Remove(name string) error {
	if err := syscall.Unlinkat(int(d.Fd()), name); err != nil {
		return &os.PathError{Op: "unlinkat", Path: name, Err: err}
	}
	return nil
}
