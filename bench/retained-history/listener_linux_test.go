package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const listenerCompactionPath = "/_capacity/compaction"

func listenerRestartFixture() (fixture, compaction) {
	f := fixture{SeedRows: 1200, TailRows: 16, LineBytes: 64, BatchRows: 8, scatteredTail: true}
	cutoff := f.SeedRows + f.BatchRows
	return f, compaction{Generation: 15, CoveredRows: cutoff, CommittedRows: cutoff, State: "complete"}
}

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
	restart := flags.Bool("restart", false, "")
	missingStatus := flags.Bool("missing-status", false, "")
	if err := flags.Parse(args[1:]); err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer stop()
	ln, err := net.Listen("tcp4", *addr)
	if err != nil {
		t.Fatal(err)
	}
	f, recovered := listenerRestartFixture()
	var rows []row
	if *restart {
		recovered.CommittedRows = f.SeedRows + f.TailRows
		for i := 0; i < recovered.CommittedRows; i++ {
			rows = append(rows, f.row(i))
		}
		// Stable sorting preserves append order for tied timestamps.
		slices.SortStableFunc(rows, func(a, b row) int { return a.TS.Compare(b.TS) })
	}
	closeListener := func() {
		if err := ln.Close(); err != nil {
			t.Error(err)
		}
	}
	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/ready":
				w.Header().Set("X-Candidate-PID", strconv.Itoa(os.Getpid()))
				w.WriteHeader(http.StatusOK)
			case listenerCompactionPath:
				if *drop == listenerCompactionPath {
					closeListener()
				}
				if !*restart || *missingStatus {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				q := req.URL.Query()
				if req.Method != http.MethodGet || q.Get("action") != "status" || q.Get("cutoff") != "0" {
					http.Error(w, "expected recovered checkpoint status request", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(recovered); err != nil {
					t.Error(err)
				}
			case "/query":
				if !*restart {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				var dropAfterPage func()
				if *drop == "/query" {
					dropAfterPage = closeListener
				}
				serveListenerRestartPage(t, w, req, f, rows, dropAfterPage)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(ln) }()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Error(err)
		if err := server.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		t.Error(err)
	}
}

func serveListenerRestartPage(t *testing.T, w http.ResponseWriter, req *http.Request, f fixture, rows []row, dropAfterPage func()) {
	const pageSize, cursor = 1000, "restart-page-2"
	q := req.URL.Query()
	endTime := epoch.Add(time.Duration(f.SeedRows) * time.Millisecond)
	if req.Method != http.MethodGet || q.Get("start") != epoch.Format(time.RFC3339Nano) ||
		q.Get("end") != endTime.Format(time.RFC3339Nano) || q.Get("contains") != "" ||
		q.Get("limit") != strconv.Itoa(pageSize) {
		http.Error(w, "expected full restart query", http.StatusBadRequest)
		return
	}
	var page []row
	switch {
	case q.Get("page") == "1" && q.Get("cursor") == "":
		page = rows[:pageSize]
		w.Header().Set("Bog-Next-Cursor", cursor)
	case q.Get("page") == "" && q.Get("cursor") == cursor:
		page = rows[pageSize:]
		if dropAfterPage != nil {
			// Stop accepting connections before HTTP finishes the terminal response.
			defer dropAfterPage()
		}
	default:
		http.Error(w, "expected a first page or restart continuation", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, row := range page {
		if err := enc.Encode(row); err != nil {
			t.Error(err)
			return
		}
	}
}

func capacityListenerFixture(t *testing.T, dropPath string, restart, missingStatus bool) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return capacityListenerScript(t, "exec '"+strings.ReplaceAll(exe, "'", "'\\''")+
		"' -test.run='^TestCapacityListenerFixture$' -- -drop-on='"+dropPath+
		"' -restart="+strconv.FormatBool(restart)+" -missing-status="+strconv.FormatBool(missingStatus)+` "$@"`)
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
	binary := capacityListenerFixture(t, "", false, false)
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
	for _, tc := range []struct {
		name, lifetime, dropPath, wantError string
		missingStatus                       bool
	}{
		{name: "startup requires live compaction control", lifetime: "startup", wantError: "live compaction status HTTP 404:"},
		{name: "startup loses listener on workload error", lifetime: "startup", dropPath: listenerCompactionPath,
			wantError: "live compaction status HTTP 404: \nworkload completion listener: candidate no longer owns the listener"},
		{name: "restart retains listener", lifetime: "restart"},
		{name: "restart requires recovered checkpoint status", lifetime: "restart", missingStatus: true,
			wantError: "observe recovered checkpoint: live compaction status HTTP 404:"},
		{name: "restart loses listener after final query", lifetime: "restart", dropPath: "/query",
			wantError: "workload completion listener: candidate no longer owns the listener"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			r := result{Diagnostics: &capacityDiagnostics{
				Unavailable: make(map[string]string),
			}, resources: resources{Unavailable: make(map[string]string)}}
			candidate := &candidateEvidence{Lifetime: tc.lifetime}
			opts := options{mode: "live", control: listenerCompactionPath}
			restart := tc.lifetime == "restart"
			trial := testRestartTrial(t)
			var f fixture
			if restart {
				f = prepareListenerRestart(t, trial)
			}
			binary := capacityListenerFixture(t, tc.dropPath, restart, tc.missingStatus)
			p, err := r.exercise(ctx, opts, binary, trial.dir, listenerTestGroup(t), f, candidate, trial)
			cleanupListenerCandidate(t, p)
			if candidate.Ready == nil || candidate.StartupError != "" {
				t.Fatalf("candidate did not reach readiness: %+v (%v)", candidate, err)
			}
			var message string
			if err != nil {
				message = strings.TrimSpace(err.Error())
			}
			if message != tc.wantError {
				t.Errorf("workload error = %v, want %q", err, tc.wantError)
			}
			if restart {
				checkListenerRestart(t, r, f, trial.evidence, tc.missingStatus)
			}
		})
	}
}

func prepareListenerRestart(t *testing.T, trial *restartTrial) fixture {
	t.Helper()
	f, publication := listenerRestartFixture()
	for lo := 0; lo < f.SeedRows+f.TailRows; lo += f.BatchRows {
		if err := trial.ledger.acknowledge(lo, lo+f.BatchRows); err != nil {
			t.Fatal(err)
		}
	}
	if err := trial.prepare(f, &publication); err != nil {
		t.Fatal(err)
	}
	if err := trial.beforeStart(cleanupProof{CandidateReaped: true, SubtreeEmpty: true}, nil); err != nil {
		t.Fatal(err)
	}
	return f
}

func checkListenerRestart(t *testing.T, r result, f fixture, e *restartEvidence, missingStatus bool) {
	t.Helper()
	if r.RestartReady == nil || e.Restarted == nil || *e.Restarted != *e.Original ||
		e.AfterWalk == nil || *e.AfterWalk != *e.Original {
		t.Errorf("restart readiness or directory observations missing or changed: %+v", e)
	}
	total := f.SeedRows + f.TailRows
	suffix := total - (f.SeedRows + f.BatchRows)
	want := restartLedgerProof{Status: "pass", Acknowledged: total, Batches: total / f.BatchRows,
		SuffixRows: suffix, Matched: total, MatchedSuffix: suffix, Pages: 2, Exhausted: true}
	if r.RecoveredRows != total || e.Ledger != want {
		t.Errorf("recovered %d rows with ledger %+v, want %d rows with ledger %+v", r.RecoveredRows, e.Ledger, total, want)
	}
	if missingStatus {
		if e.Recovered != nil || e.Phase != "recovered_checkpoint" {
			t.Errorf("missing status produced checkpoint %v in phase %q", e.Recovered, e.Phase)
		}
		return
	}
	recovered := *e.Publication
	recovered.CommittedRows = total
	if e.Recovered == nil || *e.Recovered != recovered || e.Phase != "restart_completion" {
		t.Errorf("recovered checkpoint = %v in phase %q, want %+v in restart_completion", e.Recovered, e.Phase, recovered)
	}
}

func cleanupListenerCandidate(t *testing.T, p *child) {
	t.Helper()
	if p == nil {
		return
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.terminate(ctx, 3*time.Second); err != nil {
			t.Error(err)
			testReap(t, p)
		}
		if p.exited() && p.waitErr != nil {
			t.Errorf("listener fixture: %v\n%s", p.waitErr, p.logs.tail)
		}
	})
}

func TestLiveCandidateDirectoryIsCheckedAfterReadiness(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	trial := testRestartTrial(t)
	f := prepareListenerRestart(t, trial)
	r := result{Diagnostics: &capacityDiagnostics{
		Unavailable: make(map[string]string),
	}, resources: resources{Unavailable: make(map[string]string)}}
	binary := capacityListenerFixture(t, "", true, false)
	quote := func(path string) string { return "'" + strings.ReplaceAll(path, "'", "'\\''") + "'" }
	binary = capacityListenerScript(t, "cd "+quote(t.TempDir())+" && exec "+quote(binary)+` "$@"`)
	candidate := &candidateEvidence{Lifetime: "restart"}
	opts := options{mode: "live", control: listenerCompactionPath}
	p, err := r.exercise(ctx, opts, binary, trial.dir, listenerTestGroup(t), f, candidate, trial)
	cleanupListenerCandidate(t, p)
	if candidate.Ready == nil || candidate.StartupError != "" {
		t.Fatalf("candidate did not reach readiness: %+v (%v)", candidate, err)
	}
	const want = "restart candidate directory: fixture directory changed"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("workload = %v, want %q", err, want)
	}
	e := trial.evidence
	if e.Restarted == nil || *e.Restarted == *e.Original || e.Phase != "restart_directory" ||
		e.Recovered != nil || r.RecoveredRows != 0 {
		t.Fatalf("fixture substitution did not stop restart verification: %+v", e)
	}
}
