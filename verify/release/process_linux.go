package main

import (
	"fmt"
	"syscall"
	"unsafe"
)

func candidatePlatform() error { return nil }

// Observe the child without consuming its exit status. waitid with WNOHANG
// leaves si_signo zero when there is no status to report.
func candidateExited(pid int, nonblocking bool) (bool, error) {
	const pidType = 1 // Linux P_PID
	options := syscall.WEXITED | syscall.WNOWAIT
	if nonblocking {
		options |= syscall.WNOHANG
	}
	for {
		var info [16]uint64 // 128-byte Linux siginfo_t, naturally aligned
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, pidType, uintptr(pid),
			uintptr(unsafe.Pointer(&info)), uintptr(options), 0, 0)
		switch errno {
		case syscall.EINTR:
			continue
		case 0:
			return *(*int32)(unsafe.Pointer(&info[0])) != 0, nil
		default:
			return false, fmt.Errorf("observe candidate exit: %w", errno)
		}
	}
}
