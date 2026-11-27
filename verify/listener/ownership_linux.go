package listener

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

func owns(pid, port int, expected Bind) (bool, error) {
	var sockets []listenerSocket
	wildcards := make(map[uint64]bool)
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
		for _, socket := range matching {
			if socket.ipv6Wildcard {
				wildcards[socket.inode] = true
			}
		}
	}
	if len(wildcards) != 0 {
		dualStack, err := ipv6WildcardModes(port, wildcards)
		if err != nil {
			return false, fmt.Errorf("inspect IPv6 wildcard listeners on port %d: %w", port, err)
		}
		eligible := sockets[:0]
		for _, socket := range sockets {
			if !socket.ipv6Wildcard || dualStack[socket.inode] {
				eligible = append(eligible, socket)
			}
		}
		sockets = eligible
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
	for _, socket := range sockets {
		if !owned["socket:["+strconv.FormatUint(socket.inode, 10)+"]"] {
			return false, fmt.Errorf("foreign listener on IPv4 loopback port %d (socket inode %d belongs outside candidate PID %d)", port, socket.inode, pid)
		}
	}
	if expected != "" {
		for _, socket := range sockets {
			if socket.bind != expected {
				return false, fmt.Errorf("candidate PID %d listener on port %d (socket inode %d) has bind %s, want %s",
					pid, port, socket.inode, socket.bind, expected)
			}
		}
	}
	return true, nil
}
