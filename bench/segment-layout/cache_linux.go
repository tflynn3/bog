//go:build linux && (amd64 || arm64)

package main

import (
	"fmt"
	"os"
	"syscall"
)

func prepareCache(f *os.File) string {
	const dontNeed = 4 // POSIX_FADV_DONTNEED on Linux amd64 and arm64
	// Advice is scoped to this synced file. The kernel may keep its pages.
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, dontNeed, 0, 0)
	if errno != 0 {
		return fmt.Sprintf("unverified (benchmark file still potentially warm, fadvise failed: %v)", errno)
	}
	return "unverified (file-local POSIX_FADV_DONTNEED accepted)"
}
