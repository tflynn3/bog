package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"bog/verify/reportpublication"
)

func openProofDestination(path string) (proofDestination, error) {
	const fd = 3
	const parentPath = "/proc/self/fd/3/"
	parent, name, err := reportpublication.SplitPath(path)
	if err != nil || parent != parentPath {
		return proofDestination{}, fmt.Errorf("-report-create-only requires a basename below %s", parentPath)
	}
	// The supervisor passes the opened parent via ExtraFiles. Use the
	// descriptor itself for publication and keep it out of candidate execs.
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0)
	if errno != 0 {
		return proofDestination{}, fmt.Errorf("inspect inherited report descriptor: %w", errno)
	}
	if flags&syscall.FD_CLOEXEC != 0 {
		return proofDestination{}, errors.New("report descriptor 3 was not inherited")
	}
	file := os.NewFile(fd, parentPath)
	info, err := file.Stat()
	if err == nil && !info.IsDir() {
		err = errors.New("inherited report descriptor is not a directory")
	}
	if err != nil {
		return proofDestination{}, fmt.Errorf("inspect inherited report directory: %w", errors.Join(err, file.Close()))
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_SETFD, flags|syscall.FD_CLOEXEC); errno != 0 {
		return proofDestination{}, fmt.Errorf("set close-on-exec for report directory: %w", errors.Join(errno, file.Close()))
	}
	return proofDestination{path: path, name: name, file: file, dir: reportpublication.BorrowDirectory(file)}, nil
}
