package history

import "syscall"

func openat(dir int, name string, flags int, mode uint32) (int, error) {
	return syscall.Openat(dir, name, flags, mode)
}

func renameat(dir int, old, new string) error {
	return syscall.Renameat(dir, old, dir, new)
}

func unlinkat(dir int, name string) error {
	return syscall.Unlinkat(dir, name)
}

func changeTime(stat *syscall.Stat_t) syscall.Timespec {
	return stat.Ctim
}
