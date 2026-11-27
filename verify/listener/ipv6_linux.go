package listener

import (
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"time"
)

// Linux UAPI from sock_diag.h and inet_diag.h.
const (
	netlinkSockDiag  = 4
	netlinkDumpIntr  = 0x10
	sockDiagByFamily = 20
	inetDiagRequest  = 56 // inet_diag_req_v2
	inetDiagResponse = 72 // inet_diag_msg
	inetDiagSKV6Only = 11
	tcpListen        = 10
	diagSequence     = 1
)

// ipv6WildcardModes returns the sockets that can accept IPv4. Procfs does
// not expose IPV6_V6ONLY, but Linux includes INET_DIAG_SKV6ONLY in diagnostics
// for IPv6 listeners. Correlate by inode and port in the same namespace.
func ipv6WildcardModes(port int, inodes map[uint64]bool) (dualStack map[uint64]bool, err error) {
	fd, err := syscall.Socket(syscall.AF_NETLINK, syscall.SOCK_RAW|syscall.SOCK_CLOEXEC, netlinkSockDiag)
	if err != nil {
		return nil, fmt.Errorf("open socket diagnostics: %w", err)
	}
	defer func() {
		if closeErr := syscall.Close(fd); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close socket diagnostics: %w", closeErr))
		}
	}()

	kernel := &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}
	if err := syscall.Bind(fd, kernel); err != nil {
		return nil, fmt.Errorf("bind socket diagnostics: %w", err)
	}
	timeout := syscall.NsecToTimeval(int64(250 * time.Millisecond))
	if err := syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &timeout); err != nil {
		return nil, fmt.Errorf("set socket diagnostics timeout: %w", err)
	}
	if err := syscall.Sendto(fd, ipv6DiagRequest(), syscall.MSG_DONTWAIT, kernel); err != nil {
		return nil, fmt.Errorf("request socket diagnostics: %w", err)
	}

	modes := make(map[uint64]bool, len(inodes))
	buf := make([]byte, 64<<10)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		n, _, flags, from, err := syscall.Recvmsg(fd, buf, nil, 0)
		if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("receive socket diagnostics: %w", err)
		}
		if flags&syscall.MSG_TRUNC != 0 {
			return nil, errors.New("truncated socket diagnostics datagram")
		}
		source, ok := from.(*syscall.SockaddrNetlink)
		if !ok || source.Pid != 0 {
			return nil, errors.New("socket diagnostics response was not from the kernel")
		}
		done, err := readIPv6Diag(buf[:n], port, inodes, modes)
		if err != nil {
			return nil, err
		}
		if done {
			if len(modes) != len(inodes) {
				return nil, errors.New("IPv6 wildcard sockets changed during ownership inspection")
			}
			return modes, nil
		}
	}
	return nil, errors.New("socket diagnostics timed out")
}

func ipv6DiagRequest() []byte {
	req := make([]byte, syscall.NLMSG_HDRLEN+inetDiagRequest)
	binary.NativeEndian.PutUint32(req[:4], uint32(len(req)))
	binary.NativeEndian.PutUint16(req[4:6], sockDiagByFamily)
	binary.NativeEndian.PutUint16(req[6:8], syscall.NLM_F_REQUEST|syscall.NLM_F_DUMP)
	binary.NativeEndian.PutUint32(req[8:12], diagSequence)
	body := req[syscall.NLMSG_HDRLEN:]
	body[0], body[1] = syscall.AF_INET6, syscall.IPPROTO_TCP
	binary.NativeEndian.PutUint32(body[4:8], 1<<tcpListen)
	// Request any socket cookie (INET_DIAG_NOCOOKIE).
	binary.NativeEndian.PutUint64(body[48:56], ^uint64(0))
	return req
}

func readIPv6Diag(data []byte, port int, inodes, modes map[uint64]bool) (bool, error) {
	messages, err := syscall.ParseNetlinkMessage(data)
	if err != nil {
		return false, fmt.Errorf("parse socket diagnostics: %w", err)
	}
	for _, message := range messages {
		if message.Header.Seq != diagSequence || message.Header.Flags&netlinkDumpIntr != 0 {
			return false, errors.New("unexpected or interrupted socket diagnostics response")
		}
		switch message.Header.Type {
		case sockDiagByFamily:
			if err := readIPv6DiagSocket(message.Data, port, inodes, modes); err != nil {
				return false, err
			}
		case syscall.NLMSG_DONE, syscall.NLMSG_ERROR:
			// NLMSG_DONE can contain a negative errno, just like NLMSG_ERROR.
			if len(message.Data) >= 4 {
				status := int32(binary.NativeEndian.Uint32(message.Data[:4]))
				if status < 0 {
					return false, fmt.Errorf("socket diagnostics: %w", syscall.Errno(-int64(status)))
				}
				if status != 0 {
					return false, fmt.Errorf("invalid socket diagnostics status %d", status)
				}
			} else if len(message.Data) != 0 || message.Header.Type == syscall.NLMSG_ERROR {
				return false, errors.New("incomplete socket diagnostics status")
			}
			return true, nil
		default:
			return false, fmt.Errorf("unexpected socket diagnostics message type %d", message.Header.Type)
		}
	}
	return false, nil
}

func readIPv6DiagSocket(data []byte, port int, inodes, modes map[uint64]bool) error {
	if len(data) < inetDiagResponse {
		return errors.New("incomplete IPv6 socket diagnostics")
	}
	inode := uint64(binary.NativeEndian.Uint32(data[68:72]))
	if !inodes[inode] || int(binary.BigEndian.Uint16(data[4:6])) != port ||
		data[0] != syscall.AF_INET6 || data[1] != tcpListen || [16]byte(data[8:24]) != [16]byte{} {
		return nil
	}

	for attrs := data[inetDiagResponse:]; len(attrs) != 0; {
		if len(attrs) < 4 {
			return errors.New("incomplete socket diagnostics attribute")
		}
		size := int(binary.NativeEndian.Uint16(attrs[:2]))
		aligned := (size + 3) &^ 3
		if size < 4 || aligned > len(attrs) {
			return errors.New("invalid socket diagnostics attribute length")
		}
		if binary.NativeEndian.Uint16(attrs[2:4]) == inetDiagSKV6Only {
			if size != 5 || attrs[4] > 1 {
				return fmt.Errorf("invalid IPV6_V6ONLY diagnostic for inode %d", inode)
			}
			modes[inode] = attrs[4] == 0
			return nil
		}
		attrs = attrs[aligned:]
	}
	return fmt.Errorf("missing IPV6_V6ONLY diagnostic for inode %d", inode)
}
