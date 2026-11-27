package listener

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestListenerSocketTable(t *testing.T) {
	t.Parallel()
	const port = 8080
	for _, tc := range []struct {
		name, local, state string
		ipv6, malformed    bool
		want               []listenerSocket
	}{
		{"IPv4 loopback", "0100007F:1F90", "0A", false, false, []listenerSocket{{inode: 123, bind: BindLoopback}}},
		{"IPv4 wildcard", "00000000:1F90", "0A", false, false, []listenerSocket{{inode: 123, bind: BindWildcard}}},
		{"IPv6 wildcard", "00000000000000000000000000000000:1F90", "0A", true, false, []listenerSocket{{inode: 123, bind: BindWildcard, ipv6Wildcard: true}}},
		{"mapped loopback", "0000000000000000FFFF00000100007F:1F90", "0A", true, false, []listenerSocket{{inode: 123, bind: BindLoopback}}},
		{"mapped wildcard", "0000000000000000FFFF000000000000:1F90", "0A", true, false, []listenerSocket{{inode: 123, bind: BindWildcard}}},
		{"other IPv4 address", "0200007F:1F90", "0A", false, false, nil},
		{"IPv6 loopback cannot accept IPv4 probes", "00000000000000000000000001000000:1F90", "0A", true, false, nil},
		{"other port", "0100007F:1F91", "0A", false, false, nil},
		{"established only", "0100007F:1F90", "01", false, false, nil},
		{"truncated listener", "0100007F:1F90", "0A", false, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := fmt.Sprintf("0: %s remote %s", tc.local, tc.state)
			if !tc.malformed {
				line += " tx timer retr uid timeout 123"
			}
			inodes, err := listenerSockets(line, port, tc.ipv6)
			if (err != nil) != tc.malformed || !slices.Equal(inodes, tc.want) {
				t.Fatalf("inodes %v, error %v", inodes, err)
			}
		})
	}
}

func TestListenerSocketTableRecordsEveryMatchingInode(t *testing.T) {
	t.Parallel()
	const lines = `sl local_address rem_address st tx_queue tr retrnsmt uid timeout inode
  0: 0100007F:1F90 00000000:0000 0A 0 0 0 1000 0 129
  1: 0100007F:1F90 0100007F:E1B1 01 0 0 0 1000 0 130
  2: 00000000:1F90 00000000:0000 0A 0 0 0 1000 0 131
  3: 0100007F:1F91 00000000:0000 0A 0 0 0 1000 0 132`
	got, err := listenerSockets(lines, 8080, false)
	if err != nil || !slices.Equal(got, []listenerSocket{{inode: 129, bind: BindLoopback}, {inode: 131, bind: BindWildcard}}) {
		t.Fatalf("inodes %v, error %v", got, err)
	}
}

func TestListenerSocketTableRejectsInvalidInodes(t *testing.T) {
	t.Parallel()
	for _, inode := range []string{"0", "-1", "not-an-inode", "18446744073709551616"} {
		line := "0: 0100007F:1F90 remote 0A tx timer retr uid timeout " + inode
		if got, err := listenerSockets(line, 8080, false); got != nil || err == nil {
			t.Errorf("inode %q produced %v, %v", inode, got, err)
		}
	}
}

func TestOwnsRejectsInvalidEndpoint(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ pid, port int }{{0, 8080}, {-1, 8080}, {1, 0}, {1, -1}, {1, 65536}} {
		if owned, err := Owns(tc.pid, tc.port); owned || err == nil || !strings.Contains(err.Error(), "invalid") {
			t.Errorf("Owns(%d, %d) = %t, %v", tc.pid, tc.port, owned, err)
		}
		if owned, err := OwnsBind(tc.pid, tc.port, BindLoopback); owned || err == nil || !strings.Contains(err.Error(), "invalid") {
			t.Errorf("OwnsBind(%d, %d, BindLoopback) = %t, %v", tc.pid, tc.port, owned, err)
		}
	}
}

func TestOwnsBindRejectsInvalidBind(t *testing.T) {
	t.Parallel()
	for _, bind := range []Bind{"", "127.0.0.2", "::", "::1"} {
		if owned, err := OwnsBind(1, 8080, bind); owned || err == nil || !strings.Contains(err.Error(), "invalid expected") {
			t.Errorf("OwnsBind(1, 8080, %q) = %t, %v", bind, owned, err)
		}
	}
}
