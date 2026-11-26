package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestProcessCleanup(t *testing.T) {
	t.Parallel()
	p, err := spawn("/bin/sh", "", "-c", "exec sleep 60")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.reap(); err != nil {
			t.Error(err)
		}
	})
	result := make(chan error, 1)
	go func() { result <- p.reap() }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(processTimeout + time.Second):
		t.Fatal("cleanup exceeded the reap deadline")
	}
	select {
	case <-p.done:
		var exit *exec.ExitError
		if !errors.As(p.result, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("expected SIGKILL exit, got %v", p.result)
		}
	default:
		t.Fatal("cleanup returned without reaping process")
	}
}

func TestStartupFailureAndCancellation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, tc := range []struct{ name, command, want string }{
		{"failure", "echo 'test startup failure' >&2; exit 7", "test startup failure"},
		{"timeout", "exec sleep 60", "waiting for /healthz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			file := filepath.Join(dir, tc.name)
			if err := os.WriteFile(file, []byte("#!/bin/sh\n"+tc.command+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			s, err := newServer(file, dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.close(); err != nil {
					t.Error(err)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			if err := s.start(ctx); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("start returned %v, expected %q", err, tc.want)
			}
		})
	}
}

func TestHTTPWalkRejectsBrokenPages(t *testing.T) {
	t.Parallel()
	rows := fixtureRows("test", 2, 1, "")
	for _, tc := range []struct {
		name, want string
		response   func(*testing.T, http.ResponseWriter, *http.Request)
	}{
		{"early terminal", "expected 2", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			writeTestPage(t, w, "", rows[:1])
		}},
		{"nonadvancing cursor", "repeated a cursor", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("cursor") == "" {
				writeTestPage(t, w, "token", rows[:1])
			} else {
				writeTestPage(t, w, "token", rows[1:])
			}
		}},
		{"truncated response", "unexpected EOF", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "200")
			writeTestPage(t, w, "", rows[:1])
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { tc.response(t, w, r) }))
			defer h.Close()
			s := &server{addr: strings.TrimPrefix(h.URL, "http://"), client: h.Client()}
			if _, err := s.reconcile(t.Context(), ledger{{rows, acknowledged}}, "", nil); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("walk returned %v, expected %q", err, tc.want)
			}
		})
	}
}

func writeTestPage(t *testing.T, w http.ResponseWriter, cursor string, rows []row) {
	t.Helper()
	if cursor != "" {
		w.Header().Set("Bog-Next-Cursor", cursor)
	}
	body, err := encodeRows(rows)
	if err != nil {
		t.Error(err)
		return
	}
	if n, err := w.Write(body); err != nil || n != len(body) {
		t.Error(fmt.Errorf("write test page: %d/%d bytes: %v", n, len(body), err))
	}
}

func TestPercentile(t *testing.T) {
	t.Parallel()
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(100-i) * time.Millisecond
	}
	if got := percentileMS(samples, 99); got != 99 {
		t.Errorf("p99 = %v, want 99", got)
	}
	if got := percentileMS(samples, 95); got != 95 {
		t.Errorf("p95 = %v, want 95", got)
	}
}

func TestRunFailsWithoutRequiredEvidence(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	path := filepath.Join(t.TempDir(), "durability.json")
	err := run(ctx, []string{"-binary", "/bin/false", "-revision", testRevision, "-fixture", fixtureID, "-report", path})
	if err == nil {
		t.Fatal("canceled run succeeded")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sigkill", "recovery", "enospc"} {
		if got.Cases[name].Status != "fail" || got.Cases[name].Detail == "" {
			t.Errorf("missing failure evidence for %s: %+v", name, got.Cases[name])
		}
	}
}
