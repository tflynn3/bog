//go:build amd64 || arm64

package history

import (
	"os"
	"syscall"
)

// Sweeping immutable segments for selection can touch more than the process's
// memory cap. Drop the clean validation pages after copying them into scratch
// so they don't contend with the live tail and concurrent WAL writes.
func discardValidationCache(f *os.File) {
	const posixFadvDontNeed = 4
	_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, posixFadvDontNeed, 0, 0)
}
