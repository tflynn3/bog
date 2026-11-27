// Package listener checks candidate listener ownership for release evidence.
package listener

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Bind describes the expected local address of a listener.
type Bind string

const (
	BindLoopback     Bind = "127.0.0.1"
	BindWildcard     Bind = "0.0.0.0"
	BindIPv6Loopback Bind = "::1"
)

// Owns reports whether pid holds every listening socket that can accept
// connections to 127.0.0.1:port. It returns false with no error while there is
// no matching listener and an error if a foreign listener is present or
// ownership cannot be determined. The caller owns the child lifecycle.
func Owns(pid, port int) (bool, error) {
	return checkOwnership(pid, port, "")
}

// OwnsBind requires every socket that can accept the probe to be owned by
// pid and have the expected bind. BindIPv6Loopback probes ::1. All other
// supported binds probe 127.0.0.1 using the same rules as Owns.
func OwnsBind(pid, port int, expected Bind) (bool, error) {
	if expected != BindLoopback && expected != BindWildcard && expected != BindIPv6Loopback {
		return false, fmt.Errorf("invalid expected candidate listener bind %q", expected)
	}
	return checkOwnership(pid, port, expected)
}

func checkOwnership(pid, port int, expected Bind) (bool, error) {
	if pid <= 0 || port <= 0 || port > 65535 {
		return false, fmt.Errorf("invalid candidate listener PID %d or port %d", pid, port)
	}
	return owns(pid, port, expected)
}

type listenerSocket struct {
	inode        uint64
	bind         Bind
	ipv6Wildcard bool
}

// The release targets are little-endian Linux amd64 and arm64. The TCP
// tables show potential matches, including IPv6 wildcards whose IPV6_V6ONLY
// option must be checked separately.
func listenerSockets(table string, port int, ipv6, probeIPv6 bool) ([]listenerSocket, error) {
	const wildcard6 = "00000000000000000000000000000000"
	var sockets []listenerSocket
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
		var bind Bind
		switch {
		case probeIPv6 && ipv6 && addr == "00000000000000000000000001000000":
			bind = BindIPv6Loopback
		case ipv6 && addr == wildcard6:
			bind = "::"
			if !probeIPv6 {
				bind = BindWildcard
			}
		case !probeIPv6 && ((!ipv6 && addr == "0100007F") || (ipv6 && addr == "0000000000000000FFFF00000100007F")):
			bind = BindLoopback
		case !probeIPv6 && ((!ipv6 && addr == "00000000") || (ipv6 && addr == "0000000000000000FFFF000000000000")):
			bind = BindWildcard
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
		sockets = append(sockets, listenerSocket{inode: inode, bind: bind, ipv6Wildcard: addr == wildcard6})
	}
	return sockets, nil
}
