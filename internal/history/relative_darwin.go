package history

/*
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

static int bog_openat(int dir, const char *name, int flags, mode_t mode) {
	return openat(dir, name, flags, mode);
}
*/
import "C"

import (
	"syscall"
	"unsafe"
)

func openat(dir int, name string, flags int, mode uint32) (int, error) {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))
	fd, err := C.bog_openat(C.int(dir), path, C.int(flags), C.mode_t(mode))
	if fd == -1 {
		return -1, err
	}
	return int(fd), nil
}

func renameat(dir int, old, new string) error {
	from, to := C.CString(old), C.CString(new)
	defer C.free(unsafe.Pointer(from))
	defer C.free(unsafe.Pointer(to))
	if result, err := C.renameat(C.int(dir), from, C.int(dir), to); result != 0 {
		return err
	}
	return nil
}

func unlinkat(dir int, name string) error {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))
	if result, err := C.unlinkat(C.int(dir), path, 0); result != 0 {
		return err
	}
	return nil
}

func changeTime(stat *syscall.Stat_t) syscall.Timespec {
	return stat.Ctimespec
}
