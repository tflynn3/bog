package main

import (
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"
)

func waitCandidate(cmd *exec.Cmd, cleanGroup func() error) (waitErr, observeErr error) {
	// Keep the leader PID reserved until cleanup has signaled its group.
	const pid = 1       // Linux waitid P_PID
	var info [16]uint64 // aligned Linux siginfo_t
	for {
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, pid, uintptr(cmd.Process.Pid),
			uintptr(unsafe.Pointer(&info)), syscall.WEXITED|syscall.WNOWAIT, 0, 0)
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			observeErr = fmt.Errorf("observe candidate exit: %w", errno)
		}
		cleanGroup() // The teardown caller receives the cached cleanup result.
		return cmd.Wait(), observeErr
	}
}
