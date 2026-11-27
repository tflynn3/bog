package listener

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestIPv6WildcardOwnershipFromIPv4(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		v6only int
	}{
		{"IPv6-only", 1},
		{"dual stack", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := net.ListenConfig{Control: func(_, _ string, raw syscall.RawConn) error {
				var optionErr error
				controlErr := raw.Control(func(fd uintptr) {
					optionErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, tc.v6only)
				})
				return errors.Join(controlErr, optionErr)
			}}
			sock, err := config.Listen(t.Context(), "tcp6", "[::]:0")
			if err != nil {
				t.Fatal(err)
			}
			ipv6 := sock.(*net.TCPListener)
			port := ipv6.Addr().(*net.TCPAddr).Port
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := ipv6.Close(); err != nil {
						t.Error(err)
					}
				}
			})

			endpoint := net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
			dialer := net.Dialer{Timeout: time.Second}
			conn, err := dialer.DialContext(t.Context(), "tcp4", endpoint.String())
			dualStack := tc.v6only == 0
			if dualStack {
				if err != nil {
					t.Fatal(err)
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if conn != nil {
					t.Fatal(errors.Join(errors.New("IPv4 connected to IPv6-only wildcard"), conn.Close()))
				}
				if !errors.Is(err, syscall.ECONNREFUSED) {
					t.Fatalf("expected IPv4 connection refusal, got %v", err)
				}
			}
			if got, err := Owns(os.Getpid(), port); got != dualStack || err != nil {
				t.Fatalf("candidate wildcard ownership = %t, %v, want %t, nil", got, err, dualStack)
			}

			childPID := startListenerHolder(t, ipv6)
			closed = true
			if err := ipv6.Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := Owns(childPID, port); got != dualStack || err != nil {
				t.Fatalf("child wildcard ownership = %t, %v, want %t, nil", got, err, dualStack)
			}
			got, err := Owns(os.Getpid(), port)
			if dualStack {
				if got || err == nil || !strings.Contains(err.Error(), "foreign listener") {
					t.Fatalf("foreign dual-stack ownership = %t, %v", got, err)
				}
				return
			}
			if got || err != nil {
				t.Fatalf("foreign IPv6-only wildcard affected IPv4 ownership = %t, %v", got, err)
			}

			ipv4, err := net.ListenTCP("tcp4", &endpoint)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := ipv4.Close(); err != nil {
					t.Error(err)
				}
			})
			if got, err := Owns(os.Getpid(), port); !got || err != nil {
				t.Fatalf("IPv4 candidate alongside foreign IPv6-only wildcard = %t, %v", got, err)
			}
		})
	}
}

// The IPv6 descriptor survives exec in the child until the test closes stdin.
func startListenerHolder(t *testing.T, listener *net.TCPListener) int {
	t.Helper()
	file, err := listener.File()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, "/bin/cat")
	cmd.ExtraFiles = []*os.File{file}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(errors.Join(err, file.Close()))
	}
	startErr := cmd.Start()
	closeErr := file.Close()
	if startErr != nil {
		t.Fatal(errors.Join(startErr, closeErr, stdin.Close()))
	}
	t.Cleanup(func() {
		if err := errors.Join(stdin.Close(), cmd.Wait()); err != nil {
			t.Errorf("stop listener holder: %v", err)
		}
	})
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return cmd.Process.Pid
}
