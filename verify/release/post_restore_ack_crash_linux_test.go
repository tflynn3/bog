package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/backup"
	querymodel "bog/verify/query-model"
)

const (
	postRestoreAckCrashCase = "post_restore_ack_crash"
	postRestoreCrashRows    = parityFixtureRows + 2
)

type postRestoreAckCrashProof struct {
	Point                  backup.Point   `json:"verified_point"`
	AppendStatus           int            `json:"append_status"`
	ObservedSignal         syscall.Signal `json:"observed_signal"`
	ReplayMatched          bool           `json:"replay_matched"`
	ReplayRows             int            `json:"replay_rows"`
	CrashCandidateReaped   bool           `json:"crash_candidate_reaped"`
	RestartCandidateReaped bool           `json:"restart_candidate_reaped"`
	FixtureRemoved         bool           `json:"fixture_removed"`
}

// TestReleasePostRestoreAckCrash can run on either native release runner
// before this case is wired into the combined parity producer.
func TestReleasePostRestoreAckCrash(t *testing.T) {
	path := os.Getenv("BOG_RELEASE_POST_RESTORE_ACK_CRASH_REPORT")
	if path == "" {
		t.Skip("release post-restore acknowledgment crash report path required")
	}
	binary := os.Getenv("BOG_RELEASE_BINARY")
	id, err := readIdentity(binary, os.Getenv("BOG_RELEASE_REVISION"), os.Getenv("BOG_RELEASE_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	results := report{id, make(map[string]outcome)}
	proof, probeErr := postRestoreAckCrash(ctx, binary)
	probeErr = record(results.Cases, postRestoreAckCrashCase, proof, probeErr)
	if err := errors.Join(probeErr, writeReport(path, results)); err != nil {
		t.Fatal(err)
	}
}

func postRestoreAckCrash(ctx context.Context, binary string) (proof postRestoreAckCrashProof, err error) {
	root, err := os.MkdirTemp("", "bog-release-")
	if err != nil {
		return proof, fmt.Errorf("create release fixture root: %w", err)
	}
	defer func() {
		err = addFixtureCleanup(err, root, func(dir string) error {
			removeErr := os.RemoveAll(dir)
			proof.FixtureRemoved = removeErr == nil
			return removeErr
		})
	}()
	err = probePostRestoreAckCrash(ctx, binary, root, &proof)
	return proof, err
}

func probePostRestoreAckCrash(ctx context.Context, binary, root string, proof *postRestoreAckCrashProof) error {
	rows := parityRows()
	dir, point, err := makeRestoredFixture(ctx, root, rows)
	if err != nil {
		return fmt.Errorf("prepare verified restore: %w", err)
	}
	proof.Point = point

	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport:     transport,
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	flags := []string{fmt.Sprintf("-query-max-response-bytes=%d", restoredPageCap)}

	// Both new rows tie with the restored checkpoint and tail rows. An exact
	// duplicate follows a distinct row so their relative order is observable.
	added := []querymodel.Row{
		{Time: rows[1].Time, Line: "immediate post-restore append [match]"},
		rows[1],
	}
	if err := serveAckCrashCandidate(ctx, binary, dir, client, flags, &proof.CrashCandidateReaped,
		func(ctx context.Context, base string, requireListener, crash func() error) error {
			return appendAndCrash(ctx, client, base, added, proof, requireListener, crash)
		}); err != nil {
		return fmt.Errorf("append to verified restore and crash immediately after acknowledgment: %w", err)
	}

	var log querymodel.Log
	log.Append(rows)
	log.Append(added)
	if err := serveAckCrashCandidate(ctx, binary, dir, client, flags, &proof.RestartCandidateReaped,
		func(ctx context.Context, base string, requireListener, _ func() error) error {
			pager, err := newReleasePager(base, client, restoredPageCap)
			if err != nil {
				return err
			}
			walk := restoredWalk(pager, "", []int{2, 1, 3})
			stats, err := querymodel.VerifyWireWalk(ctx, &log, walk, parityFetch(pager), nil)
			if err != nil {
				return err
			}
			if stats.Rows != postRestoreCrashRows {
				return fmt.Errorf("replay matched %d rows, want %d fixture rows", stats.Rows, postRestoreCrashRows)
			}
			if err := requireListener(); err != nil {
				return err
			}
			proof.ReplayMatched, proof.ReplayRows = true, stats.Rows
			return nil
		}); err != nil {
		return fmt.Errorf("restart restored directory after acknowledged append and SIGKILL: %w", err)
	}
	return nil
}

func appendAndCrash(ctx context.Context, client *http.Client, base string, rows []querymodel.Row,
	proof *postRestoreAckCrashProof, requireListener, crash func() error) (err error) {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, item := range rows {
		if err := enc.Encode(parityWireRow(item)); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/ingest", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if err := requireListener(); err != nil {
		return fmt.Errorf("listener ownership before append: %w", err)
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post-restore ingest: %w", err)
	}
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close ingest response: %w", closeErr))
		}
	}()
	proof.AppendStatus = res.StatusCode
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("post-restore ingest returned HTTP %d, want 204", res.StatusCode)
	}
	// Send SIGKILL from the 204 response path, before touching the response
	// body or making any further HTTP or listener probe.
	if err := crash(); err != nil {
		return fmt.Errorf("SIGKILL and reap candidate: %w", err)
	}
	// crashStop succeeds only after observing a matching wait status and
	// cleaning up the process group and reaping the leader.
	proof.ObservedSignal = syscall.SIGKILL
	return nil
}

// serveAckCrashCandidate uses the shared lifecycle primitives. The crash
// callback owns teardown when invoked, including on an error. All other
// paths stop and reap the candidate here.
func serveAckCrashCandidate(ctx context.Context, binary, dir string, client *http.Client, flags []string,
	reaped *bool, probe func(context.Context, string, func() error, func() error) error) (err error) {
	socket, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return fmt.Errorf("reserve release listener: %w", err)
	}
	addr := socket.Addr().(*net.TCPAddr)
	if err := socket.Close(); err != nil {
		return fmt.Errorf("release reserved listener: %w", err)
	}

	probes, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	p, err := startCandidate(probes, binary, append([]string{"-data-dir", dir, "-addr", addr.String()}, flags...), cancel)
	if err != nil {
		return err
	}
	crashAttempted := false
	defer func() {
		if !crashAttempted {
			err = errors.Join(err, p.stop(candidateStopTimeout))
		}
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
			err = errors.Join(err, ctxErr)
		}
		select {
		case <-p.done:
			*reaped = true
		default:
		}
	}()

	base := "http://" + addr.String()
	if err := p.awaitHealth(probes, client, base, addr.Port); err != nil {
		return err
	}
	if err := probes.Err(); err != nil {
		return context.Cause(probes)
	}
	return probe(probes, base, func() error { return p.requireListener(addr.Port) }, func() error {
		crashAttempted = true
		return p.crashStop(candidateStopTimeout)
	})
}

type ackCrashTransport func(*http.Request) (*http.Response, error)

func (f ackCrashTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type ackCrashBody struct {
	events *[]string
	err    error
}

func (b ackCrashBody) Read([]byte) (int, error) {
	*b.events = append(*b.events, "read response")
	return 0, errors.New("unexpected response body read")
}

func (b ackCrashBody) Close() error {
	*b.events = append(*b.events, "close response")
	return b.err
}

func TestAppendAndCrashOrdering(t *testing.T) {
	t.Parallel()
	failure := errors.New("injected failure")
	for _, tc := range []struct {
		name         string
		listenerErr  error
		transportErr error
		stopErr      error
		closeErr     error
		status       int
		wantError    string
		wantSignal   syscall.Signal
		wantEvents   []string
	}{
		{
			name: "204", status: http.StatusNoContent, wantSignal: syscall.SIGKILL,
			wantEvents: []string{"listener", "POST", "crash and reap", "close response"},
		},
		{
			name: "listener lost", listenerErr: failure, wantError: "listener ownership before append",
			wantEvents: []string{"listener"},
		},
		{
			name: "transport error", transportErr: failure, wantError: "post-restore ingest",
			wantEvents: []string{"listener", "POST"},
		},
		{
			name: "non-204", status: http.StatusOK, wantError: "returned HTTP 200",
			wantEvents: []string{"listener", "POST", "close response"},
		},
		{
			name: "wait failed", status: http.StatusNoContent, stopErr: failure, wantError: "SIGKILL and reap",
			wantEvents: []string{"listener", "POST", "crash and reap", "close response"},
		},
		{
			name: "close failed", status: http.StatusNoContent, closeErr: failure, wantError: "close ingest response",
			wantSignal: syscall.SIGKILL, wantEvents: []string{"listener", "POST", "crash and reap", "close response"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var events []string
			rows := parityRows()[1:3]
			client := &http.Client{Transport: ackCrashTransport(func(req *http.Request) (*http.Response, error) {
				events = append(events, "POST")
				if req.Method != http.MethodPost || req.URL.Path != "/ingest" ||
					req.Header.Get("Content-Type") != "application/x-ndjson" {
					t.Errorf("unexpected ingest request: %s %s, headers %v", req.Method, req.URL, req.Header)
				}
				var got []row
				dec := json.NewDecoder(req.Body)
				for {
					var item row
					err := dec.Decode(&item)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Error(err)
						break
					}
					got = append(got, item)
				}
				if err := req.Body.Close(); err != nil {
					t.Error(err)
				}
				if want := []row{parityWireRow(rows[0]), parityWireRow(rows[1])}; !slices.Equal(got, want) {
					t.Errorf("ingest batch = %v, want %v", got, want)
				}
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				return &http.Response{StatusCode: tc.status, Body: ackCrashBody{&events, tc.closeErr}}, nil
			})}
			var proof postRestoreAckCrashProof
			err := appendAndCrash(t.Context(), client, "http://127.0.0.1", rows, &proof, func() error {
				events = append(events, "listener")
				return tc.listenerErr
			}, func() error {
				events = append(events, "crash and reap")
				return tc.stopErr
			})
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Errorf("append and crash error = %v, want %q", err, tc.wantError)
			}
			if !slices.Equal(events, tc.wantEvents) {
				t.Errorf("events = %q, want %q", events, tc.wantEvents)
			}
			if proof.AppendStatus != tc.status || proof.ObservedSignal != tc.wantSignal {
				t.Errorf("status and signal = %d and %d, want %d and %d", proof.AppendStatus, proof.ObservedSignal, tc.status, tc.wantSignal)
			}
		})
	}
}

func TestAckCrashCandidateReapsOnSuccessAndFailure(t *testing.T) {
	t.Parallel()
	probeFailure := errors.New("injected workload failure")
	for _, tc := range []struct {
		name      string
		crash     bool
		badHealth bool
		want      error
	}{
		{name: "intentional crash", crash: true},
		{name: "probe failure", want: probeFailure},
		{name: "normal restart cleanup"},
		{name: "startup probe failure", badHealth: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			client := &http.Client{Timeout: time.Second}
			defer client.CloseIdleConnections()
			if tc.badHealth {
				client.Transport = ackCrashTransport(func(*http.Request) (*http.Response, error) {
					return nil, probeFailure
				})
			}
			var reaped bool
			probeCalled := false
			err := serveAckCrashCandidate(t.Context(), fixtureCandidate(t), root, client, nil, &reaped,
				func(_ context.Context, _ string, requireListener, crash func() error) error {
					probeCalled = true
					if err := requireListener(); err != nil {
						return err
					}
					if tc.crash {
						return crash()
					}
					return tc.want
				})
			want := tc.want
			if tc.badHealth {
				want = probeFailure
			}
			if !errors.Is(err, want) {
				t.Errorf("candidate result = %v, want %v", err, want)
			}
			if probeCalled == tc.badHealth {
				t.Errorf("workload called = %t with startup probe failure = %t", probeCalled, tc.badHealth)
			}
			if !reaped {
				t.Error("candidate reaping was not recorded")
			}
			pid, err := fixturePID(root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := candidateExited(pid, true); !errors.Is(err, syscall.ECHILD) {
				t.Errorf("waitid after teardown = %v, want ECHILD", err)
			}
		})
	}
}

func TestPostRestoreAckCrashRemovesFixtureOnStartFailure(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	proof, err := postRestoreAckCrash(t.Context(), root+"/missing-candidate")
	if err == nil || !strings.Contains(err.Error(), "start candidate") {
		t.Fatalf("probe result = %v, want candidate start error", err)
	}
	if !proof.FixtureRemoved || proof.Point.NextOrdinal != paritySeedRows ||
		proof.AppendStatus != 0 || proof.ObservedSignal != 0 || proof.ReplayMatched ||
		proof.CrashCandidateReaped || proof.RestartCandidateReaped {
		t.Errorf("unexpected startup failure evidence: %+v", proof)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("fixture parent still contains %v", entries)
	}
}
