package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
