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
	case "ignore-term":
		signal.Ignore(syscall.SIGTERM)
	case "graceful-exit", "graceful-fail":
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
		defer signal.Stop(signals)
		go func() {
			<-signals
			if *mode == "graceful-fail" {
				os.Exit(19)
			}
			os.Exit(0)
		}()
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
		crash            bool
		probeErr         error
	}{
		{name: "intentional stop"},
		{name: "intentional crash", mode: "ignore-stop", crash: true},
		{name: "kill before first HTTP", mode: "kill-before-listen", want: "signal: killed"},
		{name: "kill before crash fixture listens", mode: "kill-before-listen", crash: true, want: "signal: killed"},
		{name: "outside kill after final HTTP", kill: true, want: "signal: killed"},
		{name: "outside kill before requested crash", kill: true, crash: true, want: "signal: killed"},
		{name: "outside kill keeps probe error first", kill: true, probeErr: probeErr, want: "signal: killed"},
		{name: "failed probe uses routine teardown", crash: true, probeErr: probeErr, want: probeErr.Error()},
		{name: "probe cancellation with intentional stop", cancel: true, want: "context canceled"},
		{name: "canceled crash fixture uses routine teardown", crash: true, cancel: true, want: "context canceled"},
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
			called, crashRequested := false, false
			stopAfterProbe := (*candidateProcess).stop
			if tc.crash {
				stopAfterProbe = func(p *candidateProcess, timeout time.Duration) error {
					crashRequested = true
					return p.crashStop(timeout)
				}
			}
			err := serveDirectoryWithStop(ctx, binary, root, client, []string{"-fixture-mode=" + tc.mode}, stopAfterProbe, func(probes context.Context, base string) error {
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
					select {
					case <-probes.Done():
					case <-time.After(time.Second):
						return errors.New("candidate exit did not cancel probes")
					}
					if cause := context.Cause(probes); cause == nil || !strings.Contains(cause.Error(), "signal: killed") {
						return fmt.Errorf("candidate exit cause: %w", cause)
					}
				}
				if tc.cancel {
					cancel()
				}
				return tc.probeErr
			})
			wantCalled := tc.mode == "" || tc.mode == "ignore-stop"
			if called != wantCalled {
				t.Errorf("probe invoked = %t, want %t", called, wantCalled)
			}
			if wantCrash := tc.crash && tc.want == ""; crashRequested != wantCrash {
				t.Errorf("crash requested = %t, want %t", crashRequested, wantCrash)
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
			if wantCalled {
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
		if _, err := os.Stat(filepath.Join(root, "pid")); err == nil {
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
	if !strings.Contains(err.Error(), "candidate exited unexpectedly: signal: killed") {
		t.Errorf("escalation lost the unexpected SIGKILL status: %v", err)
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := candidateExited(p.process.Pid, true); !errors.Is(err, syscall.ECHILD) {
		t.Errorf("escalated leader was not reaped: %v", err)
	}
}

func TestStopDoesNotClaimAnUnreapedExit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, script, diagnostic string
		request                  syscall.Signal
	}{
		{"successful exit before stop", "exit 0", "status 0", candidateStopSignal},
		{"successful exit before crash", "exit 0", "status 0", syscall.SIGKILL},
		{"successful exit before graceful stop", "exit 0", "status 0", syscall.SIGTERM},
		{"external SIGHUP before stop", "kill -HUP $$", "signal: hangup", candidateStopSignal},
		{"external SIGKILL before stop", "kill -KILL $$", "signal: killed", candidateStopSignal},
		{"external SIGKILL before crash", "kill -KILL $$", "signal: killed", syscall.SIGKILL},
		{"external SIGTERM before graceful stop", "kill -TERM $$", "signal: terminated", syscall.SIGTERM},
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
			stopErr := p.requestStop(tc.request)
			waitErr := cmd.Wait()
			if err := errors.Join(observeErr, stopErr); err != nil {
				t.Fatal(err)
			}
			if p.stopSignal != 0 {
				t.Errorf("stop request claimed a process that had already exited with %s", p.stopSignal)
			}
			got := candidateSignalExitError(p.stopSignal, waitErr)
			if got == nil || !strings.Contains(got.Error(), tc.diagnostic) {
				t.Errorf("exit diagnosis = %v, want %q", got, tc.diagnostic)
			}
		})
	}
}

func TestGracefulStopRequiresZeroExit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		mode, want string
		timeout    time.Duration
	}{
		{"graceful-exit", "", candidateStopTimeout},
		{"graceful-fail", "candidate graceful shutdown did not exit with status 0: exit status 19", candidateStopTimeout},
		{"default-term", "candidate graceful shutdown did not exit with status 0: signal: terminated", candidateStopTimeout},
		{"ignore-term", "candidate graceful shutdown did not exit with status 0: signal: killed\ncandidate did not exit within teardown deadline", 200 * time.Millisecond},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			raw := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
			defer raw.CloseIdleConnections()
			var pid int
			stop := func(p *candidateProcess, _ time.Duration) error {
				return p.gracefulStop(tc.timeout)
			}
			err := superviseDirectory(ctx, fixtureCandidate(t), t.TempDir(), raw, []string{"-fixture-mode=" + tc.mode},
				"", stop, func(_ context.Context, _ string, p *candidateProcess, port int, owns listenerOwner) error {
					pid = p.process.Pid
					if err := p.requireListenerOn(port, owns); err != nil {
						return err
					}
					// The drain probe may signal first and wait after it releases its peer.
					return p.requestStop(syscall.SIGTERM)
				})
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != tc.want {
				t.Fatalf("graceful stop = %v, want %q", err, tc.want)
			}
			if _, err := candidateExited(pid, true); !errors.Is(err, syscall.ECHILD) {
				t.Errorf("leader was not reaped: %v", err)
			}
		})
	}
}

func TestCrashStopRequiresSIGKILLWaitStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, script, diagnostic string }{
		{"matching crash", "kill -KILL $$", ""},
		{"hangup", "kill -HUP $$", "signal: hangup"},
		{"successful exit", "exit 0", "status 0"},
		{"failed exit", "exit 19", "exit status 19"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			waitErr := exec.CommandContext(t.Context(), "/bin/sh", "-c", tc.script).Run()
			err := candidateSignalExitError(syscall.SIGKILL, waitErr)
			if tc.diagnostic == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Errorf("exit diagnosis = %v, want %q", err, tc.diagnostic)
			}
		})
	}
}
