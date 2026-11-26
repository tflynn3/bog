package checkpoint

/*
#include <fcntl.h>
#include <stdlib.h>

static int bog_openat(int dir, const char *name, int flags, mode_t mode) {
	return openat(dir, name, flags, mode);
}
*/
import "C"

import "unsafe"

func openat(dir int, name string, flags int, mode uint32) (int, error) {
	path := C.CString(name)
	defer C.free(unsafe.Pointer(path))
	fd, err := C.bog_openat(C.int(dir), path, C.int(flags), C.mode_t(mode))
	if fd == -1 {
		return -1, err
	}
	return int(fd), nil
}
