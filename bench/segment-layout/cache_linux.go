package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const cachePolicy = "Linux POSIX_FADV_DONTNEED on synced layout before each timed scan"

func invalidate(f *os.File) error {
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
		return fmt.Errorf("evict %s from page cache: %w", f.Name(), err)
	}
	return nil
}

func hostMetadata(dir string) (map[string]string, error) {
	var uts unix.Utsname
	if err := unix.Uname(&uts); err != nil {
		return nil, err
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return nil, err
	}
	return map[string]string{
		"kernel": unix.ByteSliceToString(uts.Release[:]), "machine": unix.ByteSliceToString(uts.Machine[:]),
		"fs_magic": fmt.Sprintf("0x%x", fs.Type), "scratch_dir": dir,
	}, nil
}
