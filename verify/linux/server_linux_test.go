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
	"strconv"
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

func TestCandidateDescendantCleanup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, command, want string
		start               bool
		contextErr          error
	}{
		{name: "normal exit", command: "exit 0"},
		{name: "failed startup", command: "echo 'test startup failure' >&2; exit 7", want: "test startup failure", start: true},
		{name: "cancellation", command: "wait", want: "waiting for /healthz", start: true, contextErr: context.Canceled},
		{name: "startup deadline", command: "wait", want: "waiting for /healthz", start: true, contextErr: context.DeadlineExceeded},
		{name: "graceful stop", command: "wait"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			file := filepath.Join(dir, "candidate")
			// The child inherits the candidate output pipes and ignores SIGTERM.
			script := "#!/bin/sh\ntrap 'exit 0' TERM\nsh -c 'trap \"\" TERM; echo $$ > descendant; exec sleep 60' &\n" +
				"while [ ! -s descendant ]; do sleep 0.01; done\n" + tc.command + "\n"
			if err := os.WriteFile(file, []byte(script), 0700); err != nil {
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
			timeout := processTimeout
			if tc.contextErr == context.DeadlineExceeded {
				timeout = time.Second
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			started := make(chan error, 1)
			if tc.start {
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					started <- s.start(ctx)
				}()
				t.Cleanup(func() {
					cancel()
					<-finished
				})
			} else if err := s.launch(); err != nil {
				t.Fatal(err)
			}
			var pid int
			testPoll(t, func() (bool, error) {
				raw, err := os.ReadFile(filepath.Join(dir, "descendant"))
				if errors.Is(err, os.ErrNotExist) || err == nil && len(raw) == 0 {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				pid, err = strconv.Atoi(strings.TrimSpace(string(raw)))
				return err == nil, err
			})
			if tc.contextErr == context.Canceled {
				cancel()
			}
			if tc.start {
				select {
				case err := <-started:
					if err == nil || !strings.Contains(err.Error(), tc.want) ||
						tc.contextErr != nil && !errors.Is(err, tc.contextErr) {
						t.Fatalf("start returned %v, expected %q", err, tc.want)
					}
				case <-time.After(processTimeout + time.Second):
					t.Fatal("startup did not return")
				}
			} else if tc.name == "graceful stop" {
				if err := s.process.stop(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			} else {
				select {
				case <-s.process.done:
				case <-time.After(processTimeout):
					t.Fatal("waiting for candidate exit")
				}
			}
			testPoll(t, func() (bool, error) {
				raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
					return true, nil
				}
				if err != nil {
					return false, err
				}
				// An orphaned descendant may remain a zombie until PID 1 reaps it.
				_, status, ok := strings.Cut(string(raw), ") ")
				return ok && strings.HasPrefix(status, "Z "), nil
			})
		})
	}
}

func testPoll(t *testing.T, ready func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(processTimeout)
	for {
		ok, err := ready()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true before deadline")
		}
		time.Sleep(5 * time.Millisecond)
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
