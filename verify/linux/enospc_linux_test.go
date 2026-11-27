package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestRetargetDirectory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	healthy, full := t.TempDir(), t.TempDir()
	link := filepath.Join(root, "data-link")
	if err := os.Symlink(healthy, link); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{full, healthy} {
		if err := retargetDirectory(link, target); err != nil {
			t.Fatal(err)
		}
		if got, err := os.Readlink(link); err != nil || got != target {
			t.Fatalf("link target = %q, %v, want %q", got, err, target)
		}
	}
	dir := filepath.Join(root, "directory")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := retargetDirectory(dir, full); err == nil {
		t.Fatal("retarget replaced a directory")
	}
	if _, err := os.Lstat(dir + ".next"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged link remained after rename failure: %v", err)
	}
}

func TestRetargetDiagnosticsBeforeReconciliation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		direction diskDirection
		ready     diskReadiness
		status    int
		body      string
		post      diskReadiness
		outcome   string
		failed    bool
	}{
		{"healthy directory still writable", healthyToFull, diskReady, 204, "", diskReadiness{}, ingestAcknowledged, false},
		{"full directory rolls back", fullToHealthy, diskLow, 503, "WAL unavailable\n", diskRollback, ingestRolledBack, false},
		{"full directory readiness fails after refusal", fullToHealthy, diskLow, 503, "WAL unavailable\n", diskLow, ingestWALRefused, true},
		{"ingest drops and readiness reports uncertainty", fullToHealthy, diskLow, 0, "", diskReadiness{reason: "recovery_required", category: "eio"}, ingestTransportAmbiguous, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var postIngest bool
			d := newCaseDiagnostics()
			d.Candidates = []*processEvidence{{Reaped: true}, {}}
			s := &server{
				addr: "localhost", direction: tc.direction, diagnostics: d,
				client: &http.Client{Transport: readinessTransport(func(req *http.Request) (*http.Response, error) {
					if req.URL.Path == "/ingest" {
						if err := req.Body.Close(); err != nil {
							return nil, err
						}
						postIngest = true
						if tc.status == 0 {
							return nil, io.EOF
						}
						return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
					}
					if req.URL.Path != "/ready" {
						t.Fatalf("unexpected request %s", req.URL.Path)
					}
					state := tc.ready
					if postIngest {
						state = tc.post
					}
					status := http.StatusServiceUnavailable
					if state.ready {
						status = http.StatusOK
					}
					body := fmt.Sprintf(`{"ready":%t,"reason":%q,"category":%q}`, state.ready, state.reason, state.category)
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
				})},
			}
			if _, err := s.observeDiskReadiness(t.Context(), "after_retarget", tc.ready, diskReadiness{}, 0); err != nil {
				t.Fatal(err)
			}
			rows := fixtureRows("retarget", 2, 1, "")
			var err error
			proof := &retargetEvidence{}
			if tc.direction == fullToHealthy {
				err = s.checkFullRetargetIngest(t.Context(), rows, proof)
			} else {
				err = s.submit(t.Context(), "retarget_ingest", rows, "", http.StatusNoContent)
			}
			if (err != nil) != tc.failed {
				t.Fatalf("probe error = %v, want failure = %t", err, tc.failed)
			}
			prefix := string(tc.direction) + "/"
			ready := d.Readiness[prefix+"after_retarget"]
			ingest := d.Ingest[prefix+"retarget_ingest"]
			if ready == nil || ready.Last.state() != tc.ready || ready.Last.Lifetime != 2 ||
				ingest == nil || ingest.Last.Outcome != tc.outcome || ingest.Last.Lifetime != 2 || ingest.Last.Rows != 2 {
				t.Fatalf("partial retarget diagnostics = %+v, %+v", ready, ingest)
			}
			if (ingest.Last.Rollback != nil) != (tc.outcome == ingestRolledBack) ||
				(d.UncertainWAL != nil) != (tc.status == 0) {
				t.Fatalf("rollback = %+v, uncertain WAL = %+v", ingest.Last.Rollback, d.UncertainWAL)
			}
			if tc.direction == fullToHealthy {
				post := d.Readiness[prefix+"after_ingest"]
				if post == nil || post.Last.state() != tc.post || (post.FirstFailure != nil) != tc.failed {
					t.Fatalf("post-ingest probe = %+v", post)
				}
				if proof.IngestStatus != tc.status || (proof.PostIngestReason == diskRollback.reason) != !tc.failed {
					t.Fatalf("legacy retarget evidence = %+v", proof)
				}
			}
		})
	}
}

func TestRetargetReadinessFailureEvidence(t *testing.T) {
	t.Parallel()
	closeFailure := errors.New("readiness close failed")
	for _, tc := range []struct {
		name       string
		direction  diskDirection
		readyErr   error
		malformed  bool
		earlyExit  bool
		ingestErr  error
		badReplay  bool
		wantReplay bool
		wantError  string
	}{
		{name: "healthy WAL stays writable despite low readiness", direction: healthyToFull, wantReplay: true},
		{name: "full WAL rolls back despite healthy readiness", direction: fullToHealthy, wantReplay: true},
		{name: "readiness transport lost after valid sample", direction: healthyToFull, readyErr: io.EOF},
		{name: "malformed readiness", direction: fullToHealthy, malformed: true, wantError: "decode readiness"},
		{name: "readiness body close fails after decoding mismatch", direction: fullToHealthy, readyErr: closeFailure},
		{name: "candidate exited before ingest", direction: healthyToFull, earlyExit: true, wantError: "candidate exited before retarget ingest"},
		{name: "unconfirmed ingest", direction: fullToHealthy, ingestErr: io.EOF},
		{name: "replay order differs", direction: fullToHealthy, badReplay: true, wantError: "replay on original directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			original, target := t.TempDir(), t.TempDir()
			link := filepath.Join(root, "data")
			if err := os.Symlink(original, link); err != nil {
				t.Fatal(err)
			}
			binary := filepath.Join(root, "candidate")
			const script = "#!/bin/sh\ntrap 'exit 0' TERM\nprintf '%s\\n' \"$$\" > .candidate-pid || exit 1\nwhile :; do sleep 1; done\n"
			if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}

			d := newCaseDiagnostics()
			s := &server{binary: binary, dir: link, addr: "localhost", direction: tc.direction, diagnostics: d}
			t.Cleanup(func() {
				if err := s.close(); err != nil {
					t.Error(err)
				}
				for _, process := range d.Candidates {
					if !process.Reaped {
						t.Errorf("candidate was not reaped: %+v", process)
					}
				}
			})
			active := func() (bool, error) {
				pid, err := os.ReadFile(filepath.Join(original, ".candidate-pid"))
				if errors.Is(err, os.ErrNotExist) {
					return false, nil
				}
				return strings.TrimSpace(string(pid)) == strconv.Itoa(s.process.cmd.Process.Pid), err
			}
			if err := s.launch(); err != nil {
				t.Fatal(err)
			}
			testPoll(t, active)
			if err := retargetDirectory(link, target); err != nil {
				t.Fatal(err)
			}
			if tc.earlyExit {
				if err := s.process.stop(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}

			want, observed := diskReady, diskLow
			observedStatus, ingestStatus, outcome := http.StatusServiceUnavailable, http.StatusNoContent, ingestAcknowledged
			ingestRows := fixtureRows("healthy-retarget", 2, 1, "")
			visible := ingestRows
			var l ledger
			var cursor string
			proof := diskEvidence{HealthyToFull: &retargetEvidence{}, FullToHealthy: &retargetEvidence{}}
			evidence := proof.HealthyToFull
			if tc.direction == fullToHealthy {
				want, observed = diskLow, diskReady
				observedStatus, ingestStatus, outcome = http.StatusOK, http.StatusServiceUnavailable, ingestRolledBack
				ingestRows = fixtureRows("disk-rejected", 2, 1, strings.Repeat("x", 32<<10))
				visible = fixtureRows("seed", 2, 1, "")
				l = ledger{{visible, acknowledged}}
				cursor = "issuer-cursor"
				evidence = proof.FullToHealthy
			}
			payload, err := encodeRows(ingestRows)
			if err != nil {
				t.Fatal(err)
			}
			response := func(status int, body string) *http.Response {
				return &http.Response{StatusCode: status, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body))}
			}
			readyResponse := func(state diskReadiness) *http.Response {
				status := http.StatusServiceUnavailable
				if state.ready {
					status = http.StatusOK
				}
				return response(status, fmt.Sprintf(`{"ready":%t,"reason":%q,"category":%q}`, state.ready, state.reason, state.category))
			}
			var readinessCalls, ingests, staleChecks int
			var queryLifetimes []int
			s.client = &http.Client{Timeout: processTimeout, Transport: readinessTransport(func(req *http.Request) (*http.Response, error) {
				lifetime := len(d.Candidates)
				switch req.URL.Path {
				case "/ready":
					readinessCalls++
					if ingests > 0 {
						return readyResponse(diskRollback), nil
					}
					switch {
					case tc.readyErr == io.EOF && readinessCalls == 1:
						return readyResponse(want), nil
					case tc.readyErr == io.EOF:
						return nil, tc.readyErr
					case tc.malformed:
						return response(observedStatus, "{"), nil
					case tc.readyErr != nil:
						resp := readyResponse(observed)
						resp.Body = readinessBody{resp.Body, func() error { return tc.readyErr }}
						return resp, nil
					default:
						return readyResponse(observed), nil
					}
				case "/ingest":
					ingests++
					body, readErr := io.ReadAll(req.Body)
					if err := errors.Join(readErr, req.Body.Close()); err != nil {
						return nil, err
					}
					if req.Method != http.MethodPost || string(body) != string(payload) || lifetime != 1 {
						t.Fatal("unexpected retarget ingest")
					}
					if tc.ingestErr != nil {
						return nil, tc.ingestErr
					}
					if ingestStatus == http.StatusServiceUnavailable {
						return response(ingestStatus, "WAL unavailable\n"), nil
					}
					return response(ingestStatus, ""), nil
				case "/healthz":
					if linkTarget, err := os.Readlink(link); err != nil || linkTarget != original {
						t.Fatalf("restart path = %q, %v, want %q", linkTarget, err, original)
					}
					ok, err := active()
					if err != nil {
						return nil, err
					}
					if !ok {
						return nil, io.EOF
					}
					return response(http.StatusOK, ""), nil
				case "/query":
					token := req.URL.Query().Get("cursor")
					if token != "" && token == cursor {
						staleChecks++
						if lifetime != 2 {
							t.Fatal("stale cursor checked before restart")
						}
						return response(http.StatusBadRequest, "invalid cursor\n"), nil
					}
					queryLifetimes = append(queryLifetimes, lifetime)
					index := 0
					if token == "next" {
						index = 1
					}
					rowIndex := index
					if tc.badReplay && lifetime == 2 {
						rowIndex = 1 - index
					}
					body, err := encodeRows(visible[rowIndex : rowIndex+1])
					if err != nil {
						return nil, err
					}
					resp := response(http.StatusOK, string(body))
					resp.Header = make(http.Header)
					resp.Header.Set("Content-Type", "application/x-ndjson")
					if index == 0 {
						resp.Header.Set("Bog-Next-Cursor", "next")
					}
					return resp, nil
				default:
					t.Fatalf("unexpected request %s", req.URL.Path)
					return nil, nil
				}
			})}

			_, err = s.checkRetarget(t.Context(), original, l, cursor, &proof)
			if err == nil || !strings.Contains(err.Error(), "readiness after retarget:") {
				t.Fatalf("retarget error = %v, want retained readiness failure", err)
			}
			if tc.wantError != "" && !strings.Contains(err.Error(), tc.wantError) {
				t.Errorf("retarget error = %v, want %q", err, tc.wantError)
			}
			for _, cause := range []error{tc.readyErr, tc.ingestErr} {
				if cause != nil && !errors.Is(err, cause) {
					t.Errorf("retarget error = %v, missing %v", err, cause)
				}
			}
			prefix := string(tc.direction) + "/"
			ready := d.Readiness[prefix+"after_retarget"]
			if ready == nil || ready.FirstFailure == nil || ready.FirstFailure.Lifetime != 1 || ready.ExpectedReason != want.reason {
				t.Fatalf("readiness evidence = %+v", ready)
			}
			if tc.readyErr != nil || tc.malformed {
				if evidence.ReadinessStatus != 0 || d.Ingest[prefix+"retarget_ingest"] != nil || ingests != 0 || len(d.Candidates) != 1 || len(queryLifetimes) != 0 || evidence.ReplayedAfterRestart {
					t.Fatalf("invalid readiness allowed follow-up: evidence=%+v ingests=%d queries=%v", evidence, ingests, queryLifetimes)
				}
				return
			}
			if ready.FirstFailure.state() != observed || evidence.ReadinessStatus != observedStatus || evidence.ReadinessReason != observed.reason {
				t.Fatalf("actual readiness missing: evidence=%+v sample=%+v", evidence, ready.FirstFailure)
			}
			if tc.earlyExit {
				if ingests != 0 || len(d.Candidates) != 1 || len(queryLifetimes) != 0 || evidence.ReplayedAfterRestart {
					t.Fatalf("exited candidate allowed follow-up: evidence=%+v ingests=%d queries=%v", evidence, ingests, queryLifetimes)
				}
				return
			}
			ingest := d.Ingest[prefix+"retarget_ingest"]
			if ingests != 1 || ingest == nil || ingest.Last.Lifetime != 1 || ingest.Last.Rows != len(ingestRows) {
				t.Fatalf("missing ingest evidence: %+v", ingest)
			}
			if tc.ingestErr != nil {
				if ingest.Last.Outcome != ingestTransportAmbiguous || len(queryLifetimes) != 0 || len(d.Candidates) != 1 || evidence.ReplayedAfterRestart {
					t.Fatalf("ambiguous ingest allowed replay: evidence=%+v ingest=%+v queries=%v", evidence, ingest.Last, queryLifetimes)
				}
				return
			}
			if ingest.Last.Outcome != outcome || evidence.IngestStatus != ingestStatus || evidence.ReplayedAfterRestart != tc.wantReplay ||
				!slices.Equal(queryLifetimes, []int{1, 1, 2, 2}) || len(d.Candidates) != 2 || !d.Candidates[0].Reaped {
				t.Fatalf("unexpected ingest or replay evidence: evidence=%+v ingest=%+v query lifetimes=%v", evidence, ingest.Last, queryLifetimes)
			}
			if tc.direction == fullToHealthy && (evidence.PostIngestReason != diskRollback.reason ||
				evidence.PostIngestCategory != diskRollback.category || staleChecks != 1 || proof.StaleCursorsRejected != 1) {
				t.Fatalf("missing rollback or cursor evidence: %+v, stale checks=%d", evidence, staleChecks)
			}
		})
	}
}
