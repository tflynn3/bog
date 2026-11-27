// Package listener checks candidate listener ownership for release evidence.
package listener

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Owns reports whether pid holds every listening socket that can accept
// connections to 127.0.0.1:port. It returns false with no error while there is
// no matching listener and an error if a foreign listener is present or
// ownership cannot be determined. The caller owns the child lifecycle.
func Owns(pid, port int) (bool, error) {
	if pid <= 0 || port <= 0 || port > 65535 {
		return false, fmt.Errorf("invalid candidate listener PID %d or port %d", pid, port)
	}
	return owns(pid, port)
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
