package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func candidateOwnsListener(pid, port int) (bool, error) {
	var sockets []string
	for _, table := range []struct {
		path string
		ipv6 bool
	}{
		{"/proc/net/tcp", false},
		{"/proc/net/tcp6", true},
	} {
		data, err := os.ReadFile(table.path)
		if table.ipv6 && errors.Is(err, os.ErrNotExist) {
			continue // IPv6 can be disabled in the runner kernel.
		}
		if err != nil {
			return false, fmt.Errorf("read listener table: %w", err)
		}
		matching, err := listenerSockets(string(data), port, table.ipv6)
		if err != nil {
			return false, fmt.Errorf("read listener table %s: %w", table.path, err)
		}
		sockets = append(sockets, matching...)
	}
	if len(sockets) == 0 {
		return false, nil
	}

	fdDir := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return false, fmt.Errorf("inspect candidate listener descriptors: %w", err)
	}
	owned := make(map[string]bool, len(entries))
	for _, fd := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // The candidate can close unrelated descriptors.
		}
		if err != nil {
			return false, fmt.Errorf("inspect candidate listener descriptor: %w", err)
		}
		owned[target] = true
	}
	for _, inode := range sockets {
		if !owned["socket:["+inode+"]"] {
			return false, fmt.Errorf("foreign listener on release port %d (socket inode %s belongs outside candidate PID %d)", port, inode, pid)
		}
	}
	return true, nil
}

// The release targets are little-endian Linux amd64 and arm64. Include
// wildcard and mapped IPv6 listeners which may accept IPv4 loopback.
func listenerSockets(table string, port int, ipv6 bool) ([]string, error) {
	var sockets []string
	wantPort := fmt.Sprintf("%04X", port)
	for line := range strings.SplitSeq(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[3] != "0A" {
			continue // Only LISTEN entries can handle a new probe.
		}
		addr, gotPort, ok := strings.Cut(fields[1], ":")
		if !ok || gotPort != wantPort {
			continue
		}
		switch addr {
		case "0100007F", "00000000":
			if ipv6 {
				continue
			}
		case "0000000000000000FFFF00000100007F", "0000000000000000FFFF000000000000", "00000000000000000000000000000000":
			if !ipv6 {
				continue
			}
		default:
			continue
		}
		if len(fields) < 10 {
			return nil, errors.New("incomplete listening socket entry")
		}
		inode, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil || inode == 0 {
			return nil, fmt.Errorf("invalid listening socket inode %q", fields[9])
		}
		sockets = append(sockets, fields[9])
	}
	return sockets, nil
}
