package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFixtureReproducibility(t *testing.T) {
	f := fixture{32, 8, 64, 4, 1024, false}
	batch, err := f.batch(0, 40)
	if err != nil {
		t.Fatal(err)
	}
	const digest = "ebdd241e750a0dc64901789b5ff8226f2c27b23ed34bf27d301ecf69f3c6fb49"
	if got := fmt.Sprintf("%x", sha256.Sum256(batch)); got != digest {
		t.Errorf("fixture digest = %s", got)
	}
	want := []int{28, 29, 30, 36, 37, 38, 4, 5, 6, 24, 25, 26, 8, 9, 10, 12, 13, 14}
	if got := f.newWalk(40, true).want; !reflect.DeepEqual(got, want) {
		t.Errorf("broad ordinals = %v, want %v", got, want)
	}
	if got := f.row(0); got != f.row(1) || got.TS != f.row(2).TS || got.Line == f.row(2).Line {
		t.Fatal("expected duplicate and distinguishable equal-timestamp rows")
	}
	for _, f := range fixtures("live") {
		if uint64(f.SeedRows*f.LineBytes) <= f.MemoryBytes || f.TailRows != 2048 {
			t.Errorf("live fixture does not exceed the cap with a fixed tail: %+v", f)
		}
	}
}

func TestWalkProtocol(t *testing.T) {
	f := fixture{32, 8, 64, 4, 1024, false}
	for _, tc := range []struct {
		name     string
		next     []string
		body     []int
		fragment bool
		wantErr  bool
	}{
		{"short page", []string{"continue"}, []int{1}, false, false},
		{"terminal", nil, []int{1, 2}, false, false},
		{"missing cursor", nil, []int{1}, false, true},
		{"empty cursor", []string{""}, []int{1}, false, true},
		{"repeat", []string{"previous"}, []int{1}, false, true},
		{"multiple", []string{"a", "b"}, []int{1}, false, true},
		{"empty page with cursor", []string{"continue"}, nil, false, true},
		{"wrong tie order", nil, []int{2, 1}, false, true},
		{"missing newline", nil, []int{1, 2}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("cursor") != "previous" || r.URL.Query().Get("page") != "" {
					t.Error("continuation request missing opaque cursor")
				}
				for _, next := range tc.next {
					w.Header().Add("Bog-Next-Cursor", next)
				}
				w.Header().Set("Content-Type", "application/x-ndjson")
				var buf bytes.Buffer
				for _, ordinal := range tc.body {
					if err := json.NewEncoder(&buf).Encode(f.row(ordinal)); err != nil {
						t.Error(err)
					}
				}
				data := buf.Bytes()
				if tc.fragment {
					data = data[:len(data)-1]
				}
				if _, err := w.Write(data); err != nil {
					t.Error(err)
				}
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			w := &walk{filter: filter{epoch, epoch.Add(time.Second), ""}, want: []int{1, 2},
				pages: 1, cursor: "previous", seen: map[string]bool{"previous": true}}
			_, err := w.page(t.Context(), s, f)
			if (err != nil) != tc.wantErr {
				t.Errorf("page returned %v", err)
			}
		})
	}
}

func TestReadinessDeadlineAndChildExit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	s := newService(srv.URL)
	defer s.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	exited := make(chan struct{})
	ownsListener := func() (bool, error) { return true, nil }
	if err := s.ready(ctx, exited, ownsListener); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("readiness returned %v", err)
	}
	close(exited)
	if err := s.ready(t.Context(), exited, ownsListener); err == nil || !strings.Contains(err.Error(), "exited") {
		t.Fatalf("readiness returned %v", err)
	}
}

func TestChildExitCancelsInFlightReadinessRequest(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		close(started)
		<-req.Context().Done()
		close(finished)
	}))
	defer srv.Close()
	s := newService(srv.URL)
	defer s.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	exited := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- s.ready(ctx, exited, func() (bool, error) { return true, nil }) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("readiness request never started")
	}
	close(exited)
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "child exited before readiness") || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("readiness returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("child exit did not cancel readiness")
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("canceled readiness request left the handler running")
	}
}

func TestReadinessRequiresListenerOwnership(t *testing.T) {
	t.Parallel()
	foreign := errors.New("foreign listener")
	type observation struct {
		owned bool
		err   error
	}
	for _, tc := range []struct {
		name         string
		observations []observation
		wantRequests int32
		wantError    string
	}{
		{"waits for candidate", []observation{{}, {owned: true}, {owned: true}}, 1, ""},
		{"foreign before request", []observation{{err: foreign}}, 0, "before readiness request"},
		{"foreign after HTTP 200", []observation{{owned: true}, {err: foreign}}, 1, "after readiness response"},
		{"missing after HTTP 200", []observation{{owned: true}, {}}, 1, "no longer owns"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/ready" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				requests.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			checks := 0
			err := s.ready(ctx, make(chan struct{}), func() (bool, error) {
				if checks == len(tc.observations) {
					t.Fatal("unexpected extra listener check")
				}
				if tc.wantRequests != 0 && checks < len(tc.observations)-1 && requests.Load() != 0 {
					t.Fatal("readiness contacted the endpoint before ownership was confirmed")
				}
				next := tc.observations[checks]
				checks++
				return next.owned, next.err
			})
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Errorf("readiness error = %v, want %q", err, tc.wantError)
			}
			if checks != len(tc.observations) || requests.Load() != tc.wantRequests {
				t.Errorf("got %d checks and %d requests, want %d and %d", checks, requests.Load(), len(tc.observations), tc.wantRequests)
			}
		})
	}
}

func TestReportEncodingAndReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")
	r := report{1, strings.Repeat("a", 40), strings.Repeat("b", 64), "linux-release-v1", "legacy", map[string]outcome{
		capacityCase: {Status: "skip"}, "legacy_baseline": {Status: "fail", Evidence: []result{{
			Status: "fail", Phase: "restart", TimedOut: true, resources: resources{
				Unavailable: map[string]string{"restart_to_ready": "no sample collected"},
			},
		}}},
	}}
	for range 2 {
		if err := writeReport(path, r); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded report
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(decoded, r) {
		t.Fatalf("report round trip: %v\n%s", err, data)
	}
	if files, err := os.ReadDir(dir); err != nil || len(files) != 1 {
		t.Fatalf("report temp cleanup: %v %v", files, err)
	}
}

func TestInvalidBinaryProducesFailedReport(t *testing.T) {
	dir := t.TempDir()
	binary, output := filepath.Join(dir, "bog"), filepath.Join(dir, "out.json")
	if err := os.WriteFile(binary, []byte("invalid executable"), 0700); err != nil {
		t.Fatal(err)
	}
	args := []string{"-binary", binary, "-revision", strings.Repeat("a", 40),
		"-fixture", "linux-release-v1", "-report", output}
	if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), "pin binary") {
		t.Fatalf("unexpected result: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var decoded report
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Cases[capacityCase].Status != "fail" || decoded.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte("invalid executable"))) {
		t.Fatalf("unexpected report: %s", data)
	}
}

func TestNearestRankP99(t *testing.T) {
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(100 - i)
	}
	if got := p99(samples); got == nil || *got != 99 || p99(nil) != nil || *p99(samples[:2]) != 2 {
		t.Fatalf("unexpected p99: %v", got)
	}
}
