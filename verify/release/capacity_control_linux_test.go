package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const capacityOptIn = "BOG_CAPACITY_CONTROL"

func TestReleaseCapacityControlScope(t *testing.T) {
	binary := os.Getenv("BOG_RELEASE_BINARY")
	if binary == "" {
		t.Skip("pinned release candidate required")
	}
	id, err := readIdentity(binary, os.Getenv("BOG_RELEASE_REVISION"), os.Getenv("BOG_RELEASE_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate revision=%s binary_sha256=%s fixture=%s", id.Revision, id.BinarySHA256, id.Fixture)

	for _, tc := range []struct {
		name  string
		ip    net.IP
		optIn string
	}{
		{name: "loopback_without_opt_in", ip: net.IPv4(127, 0, 0, 1)},
		{name: "wildcard_with_opt_in", ip: net.IPv4zero, optIn: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Setenv registers restoration and prevents parallel execution. The
			// default case must remove even an opt-in inherited from the runner.
			t.Setenv(capacityOptIn, tc.optIn)
			if tc.optIn == "" {
				if err := os.Unsetenv(capacityOptIn); err != nil {
					t.Fatal(err)
				}
			}

			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			transport := &http.Transport{}
			client := &http.Client{
				Transport: transport,
				Timeout:   2 * time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
			defer client.CloseIdleConnections()
			bind := candidateBind{
				ip: tc.ip,
				verify: func(pid, port int) error {
					if err := requireCapacityBind(pid, port, tc.ip.IsUnspecified()); err != nil {
						return err
					}
					t.Logf("confirmed candidate pid=%d bind=%s:%d", pid, tc.ip, port)
					return nil
				},
			}
			err := withFixtureRoot(func(dir string) error {
				return serveDirectoryOn(ctx, binary, dir, client, nil, bind, (*candidateProcess).stop,
					func(ctx context.Context, base string) error {
						// Commit a row so cutoff=1 is valid if the route is exposed.
						if _, err := ingest(ctx, client, base, []row{{TS: fixtureStart, Line: "capacity scope seed"}}); err != nil {
							return fmt.Errorf("seed compaction cutoff: %w", err)
						}
						return checkCapacityControlAbsent(ctx, t, client, base)
					})
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkCapacityControlAbsent(ctx context.Context, t *testing.T, client *http.Client, base string) error {
	t.Helper()
	var probeErr error
	for _, probe := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/_capacity/compaction?action=status&cutoff=1", http.StatusNotFound},
		{http.MethodPost, "/_capacity/compaction?action=start&cutoff=1", http.StatusNotFound},
		{http.MethodGet, "/healthz", http.StatusOK},
	} {
		res, err := request(ctx, client, probe.method, base+probe.path, nil)
		if err != nil {
			probeErr = errors.Join(probeErr, fmt.Errorf("%s %s: %w", probe.method, probe.path, err))
			continue
		}
		t.Logf("%s %s returned HTTP %d", probe.method, probe.path, res.status)
		if res.status != probe.want {
			probeErr = errors.Join(probeErr, fmt.Errorf("%s %s returned HTTP %d, want %d: %.200s",
				probe.method, probe.path, res.status, probe.want, res.body))
		}
	}
	return probeErr
}

// listener.Owns establishes ownership for IPv4 HTTP. Match the child's
// LISTEN inodes against procfs as well so a loopback fallback cannot pass the
// wildcard case, even if the process also owns an IPv6-only wildcard.
func requireCapacityBind(pid, port int, wildcard bool) error {
	fdDir := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return fmt.Errorf("inspect candidate descriptors for bind: %w", err)
	}
	owned := make(map[string]bool, len(entries))
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // Unrelated descriptors may close during inspection.
		}
		if err != nil {
			return fmt.Errorf("read candidate descriptor for bind: %w", err)
		}
		owned[target] = true
	}

	// Procfs prints IPv4 words in host byte order on both native runners.
	// A Go TCP wildcard can be IPv4 or dual-stack IPv6.
	wantPort := fmt.Sprintf("%04X", port)
	found := false
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(table)
		if table == "/proc/net/tcp6" && errors.Is(err, os.ErrNotExist) {
			continue // The kernel may have IPv6 disabled.
		}
		if err != nil {
			return fmt.Errorf("read candidate bind table %s: %w", table, err)
		}
		for line := range strings.SplitSeq(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 4 || fields[3] != "0A" {
				continue
			}
			addr, port, ok := strings.Cut(fields[1], ":")
			if !ok || port != wantPort {
				continue
			}
			if len(fields) < 10 {
				return fmt.Errorf("incomplete LISTEN entry in %s for port %s", table, port)
			}
			if !owned["socket:["+fields[9]+"]"] {
				continue
			}
			if !capacityBindMatches(addr, wildcard) {
				return fmt.Errorf("candidate PID %d listener on port %s has unexpected local address %s in %s",
					pid, port, addr, table)
			}
			found = true
		}
	}
	if !found {
		return fmt.Errorf("no owned LISTEN socket for candidate PID %d on port %d", pid, port)
	}
	return nil
}

func capacityBindMatches(addr string, wildcard bool) bool {
	if wildcard {
		return addr == "00000000" || addr == "00000000000000000000000000000000" ||
			addr == "0000000000000000FFFF000000000000"
	}
	return addr == "0100007F" || addr == "0000000000000000FFFF00000100007F"
}

func TestRequireCapacityBind(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		network  string
		bind     string
		wildcard bool
		wantErr  bool
	}{
		{"IPv4 loopback", "tcp4", "127.0.0.1:0", false, false},
		{"IPv4 wildcard", "tcp4", "0.0.0.0:0", true, false},
		{"Go TCP wildcard", "tcp", "0.0.0.0:0", true, false},
		{"reject loopback fallback", "tcp4", "127.0.0.1:0", true, true},
		{"reject wildcard for loopback", "tcp4", "0.0.0.0:0", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen(tc.network, tc.bind)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := ln.Close(); err != nil {
					t.Error(err)
				}
			})
			err = requireCapacityBind(os.Getpid(), ln.Addr().(*net.TCPAddr).Port, tc.wildcard)
			if (err != nil) != tc.wantErr {
				t.Fatalf("requireCapacityBind returned %v, want error=%t", err, tc.wantErr)
			}
		})
	}
}

func TestCheckCapacityControlAbsent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		status, start, health int
		wantErr               string
	}{
		{"hidden and healthy", 404, 404, 200, ""},
		{"status exposed", 200, 404, 200, "action=status"},
		{"start exposed", 404, 200, 200, "action=start"},
		{"start rejects method", 404, 405, 200, "action=start"},
		{"start redirects", 404, 302, 200, "action=start"},
		{"health failed", 404, 404, 503, "/healthz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := http.NewServeMux()
			handler.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.health)
			})
			for _, probe := range []struct {
				method, action string
				status         int
			}{
				{http.MethodGet, "status", tc.status},
				{http.MethodPost, "start", tc.start},
			} {
				handler.HandleFunc(probe.method+" /_capacity/compaction", func(w http.ResponseWriter, r *http.Request) {
					if r.URL.RawQuery != "action="+probe.action+"&cutoff=1" {
						t.Errorf("unexpected compaction query %q", r.URL.RawQuery)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if probe.status == http.StatusFound {
						w.Header().Set("Location", "/missing")
					}
					w.WriteHeader(probe.status)
				})
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			client := server.Client()
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			err := checkCapacityControlAbsent(ctx, t, client, server.URL)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}
