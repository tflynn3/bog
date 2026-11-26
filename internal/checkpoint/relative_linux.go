package checkpoint

import "syscall"

func openat(dir int, name string, flags int, mode uint32) (int, error) {
	return syscall.Openat(dir, name, flags, mode)
}
