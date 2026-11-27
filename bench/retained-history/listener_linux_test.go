package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestCapacityListenerFixture(t *testing.T) {
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		return
	}
	flags := flag.NewFlagSet("capacity listener fixture", flag.ContinueOnError)
	addr := flags.String("addr", "", "")
	flags.String("data-dir", "", "")
	flags.Uint64("query-work-budget", 0, "")
	drop := flags.String("drop-on", "", "")
	if err := flags.Parse(args[1:]); err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer stop()
	ln, err := net.Listen("tcp4", *addr)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if *drop == req.URL.Path {
				if err := ln.Close(); err != nil {
					t.Error(err)
				}
			}
			switch req.URL.Path {
			case "/ready":
				w.Header().Set("X-Candidate-PID", strconv.Itoa(os.Getpid()))
				w.WriteHeader(http.StatusOK)
			case "/query":
				w.Header().Set("Content-Type", "application/x-ndjson")
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(ln) }()
	<-ctx.Done()
	serverErr := server.Close()
	if err := <-served; !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		t.Error(err)
	}
	if serverErr != nil {
		t.Error(serverErr)
	}
}

func capacityListenerFixture(t *testing.T, dropPath string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return capacityListenerScript(t, "exec '"+strings.ReplaceAll(exe, "'", "'\\''")+
		"' -test.run='^TestCapacityListenerFixture$' -- -drop-on='"+dropPath+"' \"$@\"")
}

func capacityListenerScript(t *testing.T, command string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "candidate")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"+command+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return binary
}

func listenerTestGroup(t *testing.T) string {
	t.Helper()
	// A regular placement file exercises the startup gate without delegation.
	group := t.TempDir()
	if err := os.WriteFile(filepath.Join(group, "cgroup.procs"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	return group
}

func TestCapacityListenerAcrossLifetimes(t *testing.T) {
	t.Parallel()
	dir, group := t.TempDir(), listenerTestGroup(t)
	binary := capacityListenerFixture(t, "")
	idle := capacityListenerScript(t, "exec sleep 30")
	for _, lifetime := range []string{"startup", "restart"} {
		t.Run(lifetime, func(t *testing.T) {
			for _, tc := range []struct {
				name, binary string
				foreign      bool
			}{
				{"candidate", binary, false},
				{"unrelated listener", idle, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					p, base, err := startChild(tc.binary, dir, group, "legacy")
					if p != nil {
						t.Cleanup(func() { testReap(t, p) })
					}
					if err != nil {
						t.Fatal(err)
					}
					var requests atomic.Int32
					if tc.foreign {
						ln, err := net.Listen("tcp4", "127.0.0.1:"+strconv.Itoa(p.port))
						if err != nil {
							t.Fatal(err)
						}
						server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
							requests.Add(1)
							w.WriteHeader(http.StatusOK)
						}))
						if err := server.Listener.Close(); err != nil {
							t.Error(err)
						}
						server.Listener = ln
						server.Start()
						defer server.Close()
					}
					probe := newService(base)
					defer probe.client.CloseIdleConnections()
					err = probe.ready(ctx, p.done, p.ownsListener)
					if tc.foreign {
						if err == nil || !strings.Contains(err.Error(), "foreign listener") || requests.Load() != 0 {
							t.Fatalf("foreign readiness = %v, requests = %d", err, requests.Load())
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					resp, _, err := probe.request(ctx, http.MethodGet, "/ready", nil)
					if err != nil {
						t.Fatal(err)
					}
					if got, want := resp.Header.Get("X-Candidate-PID"), strconv.Itoa(p.cmd.Process.Pid); got != want {
						t.Fatalf("ready PID = %q, want %q", got, want)
					}
					if err := p.requireListener(); err != nil {
						t.Fatal(err)
					}
					if err := p.terminate(ctx, 2*time.Second); err != nil {
						t.Fatal(err)
					}
					if p.waitErr != nil {
						t.Fatal(p.waitErr)
					}
				})
			}
		})
	}
}

func TestCapacityWorkloadCompletionChecksListener(t *testing.T) {
	t.Parallel()
	const control = "/_capacity/compaction"
	for _, tc := range []struct {
		name, lifetime, dropPath, wantWorkloadError string
	}{
		{"startup requires live compaction control", "startup", "", "live compaction status HTTP 404"},
		{"startup loses listener on workload error", "startup", control, "live compaction status HTTP 404"},
		{"restart retains listener", "restart", "", ""},
		{"restart loses listener after final query", "restart", "/query", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			r := result{Diagnostics: &capacityDiagnostics{
				Queries: make(map[string]*queryPhase), Unavailable: make(map[string]string),
			}}
			candidate := &candidateEvidence{Lifetime: tc.lifetime}
			opts := options{mode: "live", control: control}
			p, err := r.exercise(ctx, opts, capacityListenerFixture(t, tc.dropPath), t.TempDir(), listenerTestGroup(t), fixture{}, candidate, nil)
			if p != nil {
				t.Cleanup(func() { testReap(t, p) })
			}
			if candidate.Ready == nil || candidate.StartupError != "" {
				t.Fatalf("candidate did not reach readiness: %+v (%v)", candidate, err)
			}
			var message string
			if err != nil {
				message = err.Error()
			}
			if tc.wantWorkloadError == "" && tc.dropPath == "" && err != nil ||
				tc.wantWorkloadError != "" && !strings.HasPrefix(message, tc.wantWorkloadError) {
				t.Errorf("workload error = %v, want %q", err, tc.wantWorkloadError)
			}
			if got := strings.Contains(message, "workload completion listener: candidate no longer owns"); got != (tc.dropPath != "") {
				t.Errorf("listener diagnosis = %v", err)
			}
		})
	}
}

func TestLiveCandidateDirectoryIsCheckedAfterReadiness(t *testing.T) {
	for _, substitute := range []bool{false, true} {
		t.Run(strconv.FormatBool(substitute), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			trial := testRestartTrial(t)
			r := result{Diagnostics: &capacityDiagnostics{
				Queries: make(map[string]*queryPhase), Unavailable: make(map[string]string),
			}}
			binary := capacityListenerFixture(t, "")
			lifetime, want := "startup", "live compaction status HTTP 404"
			if substitute {
				quote := func(path string) string { return "'" + strings.ReplaceAll(path, "'", "'\\''") + "'" }
				binary = capacityListenerScript(t, "cd "+quote(t.TempDir())+" && exec "+quote(binary)+` "$@"`)
				lifetime, want = "restart", "restart candidate directory: fixture directory changed"
			}
			candidate := &candidateEvidence{Lifetime: lifetime}
			opts := options{mode: "live", control: "/_capacity/compaction"}
			p, err := r.exercise(ctx, opts, binary, trial.dir, listenerTestGroup(t), fixture{}, candidate, trial)
			if p != nil {
				t.Cleanup(func() { testReap(t, p) })
			}
			if err == nil || !strings.Contains(err.Error(), want) || candidate.Ready == nil {
				t.Fatalf("candidate readiness = %v, workload = %v, want %q", candidate.Ready, err, want)
			}
			if substitute && (trial.evidence.Restarted == nil || *trial.evidence.Restarted == *trial.evidence.Original) {
				t.Fatalf("fixture substitution was not recorded: %+v", trial.evidence)
			}
		})
	}
}
