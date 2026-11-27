package listener

import (
	"encoding/binary"
	"errors"
	"syscall"
	"testing"
)

func TestIPv6SocketDiagnostic(t *testing.T) {
	t.Parallel()
	const port, inode = 8080, 123
	for _, tc := range []struct {
		name      string
		attrs     []byte
		present   bool
		dualStack bool
		wantErr   bool
	}{
		{"dual stack", []byte{5, 0, 11, 0, 0, 0, 0, 0}, true, true, false},
		{"IPv6 only", []byte{5, 0, 11, 0, 1, 0, 0, 0}, true, false, false},
		{"unknown attribute", []byte{4, 0, 22, 0, 5, 0, 11, 0, 0, 0, 0, 0}, true, true, false},
		{"missing mode", nil, false, false, true},
		{"truncated attribute header", []byte{5, 0}, false, false, true},
		{"truncated attribute", []byte{5, 0, 11, 0}, false, false, true},
		{"undersized attribute", []byte{3, 0, 11, 0}, false, false, true},
		{"missing option byte", []byte{4, 0, 11, 0}, false, false, true},
		{"invalid option byte", []byte{5, 0, 11, 0, 2, 0, 0, 0}, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, inetDiagResponse)
			data[0], data[1] = syscall.AF_INET6, tcpListen
			binary.BigEndian.PutUint16(data[4:6], port)
			binary.NativeEndian.PutUint32(data[68:72], inode)
			data = append(data, tc.attrs...)
			modes := make(map[uint64]bool)
			err := readIPv6DiagSocket(data, port, map[uint64]bool{inode: true}, modes)
			got, found := modes[inode]
			if (err != nil) != tc.wantErr || found != tc.present || got != tc.dualStack {
				t.Fatalf("mode = %t, found = %t, err = %v", got, found, err)
			}
		})
	}
}

func TestIPv6DiagnosticCompletion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		flags  uint16
		seq    uint32
		status int32
		done   bool
		err    error
	}{
		{name: "completed", seq: diagSequence, done: true},
		{name: "kernel error", seq: diagSequence, status: -int32(syscall.EINVAL), err: syscall.EINVAL},
		{name: "interrupted", seq: diagSequence, flags: netlinkDumpIntr},
		{name: "foreign sequence", seq: diagSequence + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := make([]byte, syscall.NLMSG_HDRLEN+4)
			binary.NativeEndian.PutUint32(msg[:4], uint32(len(msg)))
			binary.NativeEndian.PutUint16(msg[4:6], syscall.NLMSG_DONE)
			binary.NativeEndian.PutUint16(msg[6:8], tc.flags)
			binary.NativeEndian.PutUint32(msg[8:12], tc.seq)
			binary.NativeEndian.PutUint32(msg[16:20], uint32(tc.status))
			done, err := readIPv6Diag(msg, 8080, nil, nil)
			if done != tc.done || (err == nil) != tc.done || (tc.err != nil && !errors.Is(err, tc.err)) {
				t.Fatalf("done = %t, err = %v", done, err)
			}
		})
	}
}
