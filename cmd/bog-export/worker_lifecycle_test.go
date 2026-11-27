//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRenameDuringOpeningQuery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	data, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("error response=%t", fail), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "bog-export")
			if err := os.WriteFile(path, data, 0700); err != nil {
				t.Fatal(err)
			}
			requested := make(chan struct{}, 1)
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case requested <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				if fail {
					http.Error(w, "fixture failure after rename", http.StatusBadRequest)
					return
				}
				servePage(t, w, "", rowA+rowB)
			}))
			defer server.Close()
			defer closeIfOpen(release)

			log := filepath.Join(dir, "workers.txt")
			cmd := exportProcessAt(ctx, path, argsFor(server.URL)...)
			cmd.Env = append(cmd.Env, testWorkerLog+"="+log)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-requested:
				err = os.Rename(path, path+".renamed")
			case <-ctx.Done():
				err = ctx.Err()
			}
			close(release)
			waitErr := cmd.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if fail {
				assertResult(t, exitStatus(t, waitErr), stdout.String(), stderr.String(), exitIncomplete, "", "fixture failure after rename")
			} else {
				assertResult(t, exitStatus(t, waitErr), stdout.String(), stderr.String(), exitOK, rowA+rowB, "")
			}
			assertWorkersReaped(t, log, 2)
		})
	}
}

func closeIfOpen(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func TestUsageWithBlockedStderr(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		arg    string
		status int
	}{{"-bogus", exitUsage}, {"-h", exitIncomplete}} {
		t.Run(tc.arg, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			blocked := newInheritedPipe(t)
			filler := blocked.fill(t)
			log := filepath.Join(t.TempDir(), "workers.txt")
			cmd := exportProcess(ctx, tc.arg)
			cmd.Env = append(cmd.Env, testWorkerLog+"="+log)
			var stdout bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, blocked.writer
			if status := exitStatus(t, cmd.Run()); status != tc.status || stdout.Len() != 0 {
				t.Fatalf("exit = %d, stdout = %q, want exit %d and no stdout", status, stdout.String(), tc.status)
			}
			if data := blocked.drain(t); !bytes.Equal(data, bytes.Repeat([]byte{'!'}, filler)) {
				t.Fatalf("blocked stderr changed: got %d bytes, want %d filler bytes", len(data), filler)
			}
			assertWorkersReaped(t, log, 1)
		})
	}
}

func TestWorkerCleanupOnPartialStartup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		mode, timeout, diagnostic string
	}{
		{"failed probe", "5s", "invalid output worker handshake"},
		{"hold probe", "1s", "probe output worker"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			file := openAppendSink(t, filepath.Join(dir, "rows"))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Error("query sent after worker startup failed")
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer server.Close()

			log := filepath.Join(dir, "workers.txt")
			cmd := exportProcess(ctx, argsFor(server.URL, "-timeout", tc.timeout)...)
			cmd.Env = append(cmd.Env, testWorkerLog+"="+log, testWorkerMode+"="+tc.mode)
			var stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = file, &stderr
			if status := exitStatus(t, cmd.Run()); status != exitIncomplete || !strings.Contains(stderr.String(), tc.diagnostic) {
				t.Fatalf("exit = %d, stderr = %q", status, stderr.String())
			}
			assertWorkersReaped(t, log, 2)
		})
	}
}

func assertWorkersReaped(t *testing.T, log string, count int) {
	t.Helper()
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	pids := strings.Fields(string(data))
	if len(pids) != count {
		t.Fatalf("logged workers = %v, want %d", pids, count)
	}
	for _, pid := range pids {
		n, err := strconv.Atoi(pid)
		if err != nil || n < 2 {
			t.Fatalf("invalid worker PID %q", pid)
		}
		if err := syscall.Kill(n, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("worker %d still present after exporter exit (probe: %v)", n, err)
		}
	}
}
