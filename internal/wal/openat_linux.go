package wal

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

func openat(fd int, path string, flags int, mode uint32) (int, error) {
	return syscall.Openat(fd, path, flags, mode)
}

// renameNoReplace publishes a bootstrap file without replacing a winner.
func renameNoReplace(dir *os.File, old, new string) error {
	oldp, err := syscall.BytePtrFromString(old)
	if err != nil {
		return err
	}
	newp, err := syscall.BytePtrFromString(new)
	if err != nil {
		return err
	}
	// syscall does not export SYS_RENAMEAT2 on every Linux architecture.
	var nr uintptr
	switch runtime.GOARCH {
	case "amd64":
		nr = 316
	case "386":
		nr = 353
	case "arm":
		nr = 382
	case "arm64", "riscv64", "loong64":
		nr = 276
	case "ppc64", "ppc64le":
		nr = 357
	case "s390x":
		nr = 347
	case "mips", "mipsle":
		nr = 4351
	case "mips64", "mips64le":
		nr = 5311
	default:
		return syscall.ENOSYS
	}
	_, _, errno := syscall.Syscall6(nr, dir.Fd(), uintptr(unsafe.Pointer(oldp)), dir.Fd(), uintptr(unsafe.Pointer(newp)), 1, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
