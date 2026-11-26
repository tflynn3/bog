package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type readinessTransport func(*http.Request) (*http.Response, error)

func (f readinessTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type readinessBody struct {
	io.Reader
	close func() error
}

func (b readinessBody) Close() error { return b.close() }

func TestDiskReadinessResponse(t *testing.T) {
	t.Parallel()
	const readyBody = `{"ready":true,"reason":"ready"}`
	closeFailure := errors.New("body close failed")
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		closeErr error
		want     diskReadiness
		problem  string
	}{
		{"ready", 200, readyBody, nil, diskReady, ""},
		{"low", 503, `{"ready":false,"reason":"low_disk_headroom"}`, nil, diskLow, ""},
		{"rollback", 503, `{"ready":false,"reason":"wal_rollback","category":"enospc"}`, nil, diskRollback, ""},
		{"wrong status", 503, readyBody, nil, diskReadiness{}, "inconsistent readiness"},
		{"missing ready", 503, `{"reason":"low_disk_headroom"}`, nil, diskReadiness{}, "inconsistent readiness"},
		{"missing reason", 200, `{"ready":true}`, nil, diskReadiness{}, "inconsistent readiness"},
		{"partial JSON", 200, readyBody[:len(readyBody)-1], nil, diskReadiness{}, "decode readiness"},
		{"trailing JSON", 200, readyBody + "{}", nil, diskReadiness{}, "decode readiness"},
		{"oversized", 200, readyBody + strings.Repeat(" ", 1024), nil, diskReadiness{}, "oversized readiness"},
		{"close failure", 200, readyBody, closeFailure, diskReady, closeFailure.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			closed := 0
			s := &server{addr: "127.0.0.1:1234", client: &http.Client{Transport: readinessTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.Path != "/ready" {
					t.Errorf("unexpected readiness request %s %s", req.Method, req.URL)
				}
				return &http.Response{StatusCode: tc.status, Body: readinessBody{strings.NewReader(tc.body), func() error {
					closed++
					return tc.closeErr
				}}}, nil
			})}}
			got, err := s.diskReadiness(t.Context())
			if got != tc.want || closed != 1 {
				t.Errorf("readiness = %+v, closes = %d, want %+v and one close", got, closed, tc.want)
			}
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Errorf("wanted error %q, got %v", tc.problem, err)
			}
			if tc.closeErr != nil && !errors.Is(err, tc.closeErr) {
				t.Errorf("close failure lost: %v", err)
			}
		})
	}
}

func TestObserveDiskReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		samples       []diskReadiness
		want, interim diskReadiness
		stableFor     time.Duration
		cancelAfter   time.Duration
		wantElapsed   time.Duration
		problem       string
	}{
		{"startup sample", []diskReadiness{diskPending, diskReady}, diskReady, diskPending, 0, 0, readinessPoll, ""},
		{"healthy after retarget", []diskReadiness{diskReady}, diskReady, diskReadiness{}, retargetObservation, 0, retargetObservation, ""},
		{"full after retarget", []diskReadiness{diskLow}, diskLow, diskReadiness{}, retargetObservation, 0, retargetObservation, ""},
		{"stale ready gives way to wrong filesystem", []diskReadiness{diskReady, diskLow}, diskReady, diskReadiness{}, retargetObservation, 0, readinessPoll, "low_disk_headroom"},
		{"stale low gives way to wrong filesystem", []diskReadiness{diskLow, diskReady}, diskLow, diskReadiness{}, retargetObservation, 0, readinessPoll, "ready"},
		{"never samples disk full", []diskReadiness{diskReady}, diskLow, diskReady, 0, 0, readinessTimeout, "deadline exceeded"},
		{"canceled while sampling", []diskReadiness{diskReady}, diskLow, diskReady, 0, 2 * readinessPoll, 2 * readinessPoll, "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				if tc.cancelAfter != 0 {
					deadline, cancel := context.WithCancel(ctx)
					defer cancel()
					timer := time.AfterFunc(tc.cancelAfter, cancel)
					defer timer.Stop()
					ctx = deadline
				}
				calls := 0
				s := &server{addr: "127.0.0.1:1234", client: &http.Client{Transport: readinessTransport(func(*http.Request) (*http.Response, error) {
					sample := tc.samples[min(calls, len(tc.samples)-1)]
					calls++
					status := http.StatusServiceUnavailable
					if sample.ready {
						status = http.StatusOK
					}
					body := fmt.Sprintf(`{"ready":%t,"reason":%q,"category":%q}`, sample.ready, sample.reason, sample.category)
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
				})}}
				start := time.Now()
				err := s.observeDiskReadiness(ctx, tc.want, tc.interim, tc.stableFor)
				if elapsed := time.Since(start); elapsed != tc.wantElapsed {
					t.Errorf("elapsed = %v, want %v", elapsed, tc.wantElapsed)
				}
				if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
					t.Errorf("wanted error %q, got %v", tc.problem, err)
				}
			})
		})
	}
}

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
