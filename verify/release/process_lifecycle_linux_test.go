package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCandidateFixtureProcess(t *testing.T) {
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		return
	}
	flags := flag.NewFlagSet("fixture-process", flag.ContinueOnError)
	dir := flags.String("data-dir", "", "")
	addr := flags.String("addr", "", "")
	mode := flags.String("fixture-mode", "", "")
	if err := flags.Parse(args[1:]); err != nil {
		t.Fatal(err)
	}
	switch *mode {
	case "foreign":
		child := exec.Command(os.Args[0], "-test.run=^TestCandidateFixtureProcess$", "--",
			"-addr", *addr, "-data-dir", *dir, "-fixture-mode=foreign-child")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Run(); err != nil {
			t.Fatal(err)
		}
		return
	case "kill-before-listen":
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		return
	case "ignore-stop":
		signal.Ignore(candidateStopSignal)
	}

	if err := os.WriteFile(filepath.Join(*dir, "pid"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", *addr)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if *mode == "foreign-child" {
				if err := os.WriteFile(filepath.Join(*dir, "foreign-probed"), []byte(r.URL.Path), 0600); err != nil {
					t.Error(err)
				}
			}
			w.Header().Set("X-Candidate-PID", strconv.Itoa(os.Getpid()))
			w.WriteHeader(http.StatusOK)
		}),
	}
	if err := server.Serve(listener); err != nil {
		t.Fatal(err)
	}
}

func fixtureCandidate(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "candidate")
	quoted := "'" + strings.ReplaceAll(executable, "'", "'\\''") + "'"
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec "+quoted+" -test.run='^TestCandidateFixtureProcess$' -- \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func fixturePID(root string) (int, error) {
	data, err := os.ReadFile(filepath.Join(root, "pid"))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(data))
}

func TestServeCandidateLifecycle(t *testing.T) {
	t.Parallel()
	probeErr := errors.New("primary probe failure")
	for _, tc := range []struct {
		name, mode, want string
		kill, cancel     bool
		probeErr         error
	}{
		{name: "intentional stop"},
		{name: "kill before first HTTP", mode: "kill-before-listen", want: "signal: killed"},
		{name: "outside kill after final HTTP", kill: true, want: "signal: killed"},
		{name: "outside kill keeps probe error first", kill: true, probeErr: probeErr, want: "signal: killed"},
		{name: "probe cancellation with intentional stop", cancel: true, want: "context canceled"},
		{name: "foreign listener in candidate group", mode: "foreign", want: "foreign listener"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			binary, root := fixtureCandidate(t), t.TempDir()
			deadline, stopDeadline := context.WithTimeout(t.Context(), 10*time.Second)
			defer stopDeadline()
			ctx, cancel := context.WithCancel(deadline)
			defer cancel()
			raw := &http.Transport{DisableKeepAlives: true}
			defer raw.CloseIdleConnections()
			client := &http.Client{Transport: raw}
			called := false
			err := serveDirectory(ctx, binary, root, client, []string{"-fixture-mode=" + tc.mode}, func(probes context.Context, base string) error {
				called = true
				res, err := request(probes, client, http.MethodGet, base+"/last-probe", nil)
				if err != nil {
					return err
				}
				if res.status != http.StatusOK {
					return fmt.Errorf("last probe returned HTTP %d", res.status)
				}
				if tc.kill {
					pid, err := fixturePID(root)
					if err != nil {
						return err
					}
					if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
						return err
					}
					// Independent exit cancels probes even before teardown is requested.
					<-probes.Done()
					if cause := context.Cause(probes); cause == nil || !strings.Contains(cause.Error(), "signal: killed") {
						return fmt.Errorf("candidate exit cause: %w", cause)
					}
				}
				if tc.cancel {
					cancel()
				}
				return tc.probeErr
			})
			if wantCalled := tc.mode == ""; called != wantCalled {
				t.Errorf("probe invoked = %t, want %t", called, wantCalled)
			}
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("serve returned %v, want %q", err, tc.want)
			}
			if tc.mode == "foreign" {
				if _, err := os.Stat(filepath.Join(root, "foreign-probed")); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("foreign process received HTTP: %v", err)
				}
			}
			if tc.cancel && (!errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "unexpectedly")) {
				t.Errorf("cancellation was misclassified: %v", err)
			}
			if tc.probeErr != nil && (!errors.Is(err, tc.probeErr) || !strings.HasPrefix(err.Error(), tc.probeErr.Error())) {
				t.Errorf("probe failure lost precedence: %v", err)
			}
			if tc.mode == "" {
				pid, err := fixturePID(root)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := candidateExited(pid, true); !errors.Is(err, syscall.ECHILD) {
					t.Errorf("leader was not reaped: %v", err)
				}
			}
		})
	}
}

func TestStopCandidateEscalatesAndReportsTimeout(t *testing.T) {
	t.Parallel()
	binary, root := fixtureCandidate(t), t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	probes, cancelProbes := context.WithCancelCause(ctx)
	defer cancelProbes(nil)
	p, err := startCandidate(probes, binary, []string{"-data-dir", root, "-addr=127.0.0.1:0", "-fixture-mode=ignore-stop"}, cancelProbes)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			if err := p.stop(candidateStopTimeout); err != nil {
				t.Error(err)
			}
		}
	})
	for {
		if _, err := fixturePID(root); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	err = p.stop(200 * time.Millisecond)
	stopped = true
	if err == nil || !strings.Contains(err.Error(), "did not exit within teardown deadline") {
		t.Fatalf("teardown hang was accepted: %v", err)
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestStopDoesNotClaimAnUnreapedExit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, script, diagnostic string }{
		{"successful exit", "exit 0", "status 0"},
		{"external SIGKILL", "kill -KILL $$", "signal: killed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command("/bin/sh", "-c", tc.script)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			p := &candidateProcess{process: cmd.Process, done: make(chan struct{})}
			// Arrange for the kernel to have the exit status while the
			// lifecycle observer has yet to publish it.
			_, observeErr := candidateExited(cmd.Process.Pid, false)
			stopErr := p.requestStop()
			waitErr := cmd.Wait()
			if err := errors.Join(observeErr, stopErr); err != nil {
				t.Fatal(err)
			}
			if p.stopSent {
				t.Error("stop request claimed a process that had already exited")
			}
			got := candidateExitError(p.stopSent, waitErr)
			if got == nil || !strings.Contains(got.Error(), tc.diagnostic) {
				t.Errorf("exit diagnosis = %v, want %q", got, tc.diagnostic)
			}
		})
	}
}

func TestListenerSocketTable(t *testing.T) {
	t.Parallel()
	const port = 8080
	for _, tc := range []struct {
		name, local, state, want string
		ipv6, malformed          bool
	}{
		{"IPv4 loopback", "0100007F:1F90", "0A", "123", false, false},
		{"IPv4 wildcard", "00000000:1F90", "0A", "123", false, false},
		{"IPv6 wildcard", "00000000000000000000000000000000:1F90", "0A", "123", true, false},
		{"mapped loopback", "0000000000000000FFFF00000100007F:1F90", "0A", "123", true, false},
		{"IPv6 loopback cannot accept IPv4 probes", "00000000000000000000000001000000:1F90", "0A", "", true, false},
		{"other port", "0100007F:1F91", "0A", "", false, false},
		{"established only", "0100007F:1F90", "01", "", false, false},
		{"truncated listener", "0100007F:1F90", "0A", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := fmt.Sprintf("0: %s remote %s", tc.local, tc.state)
			if !tc.malformed {
				line += " tx timer retr uid timeout 123"
			}
			inodes, err := listenerSockets(line, port, tc.ipv6)
			if (err != nil) != tc.malformed || strings.Join(inodes, ",") != tc.want {
				t.Fatalf("inodes %v, error %v", inodes, err)
			}
		})
	}
}
