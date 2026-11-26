package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQueryDiagnosticsPhases(t *testing.T) {
	for _, tc := range []struct {
		name, admission, phase, body string
		status, bytes                int
		cancel, partial              bool
	}{
		{name: "before headers", admission: "unknown", phase: "headers", cancel: true},
		{name: "interrupted body", admission: "ndjson_response", phase: "body", status: 200, body: "partial", bytes: 7, partial: true},
		{name: "capacity", admission: "capacity_rejection", phase: "validation", status: 503, body: "query capacity exhausted\n", bytes: 25},
		{name: "work budget", admission: "work_budget_rejection", phase: "validation", status: 503, body: "query work budget exhausted\n", bytes: 28},
		{name: "completed", admission: "ndjson_response", phase: "complete", status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.cancel {
					t.Error("canceled request reached the handler")
					return
				}
				if tc.status == 200 {
					w.Header().Set("Content-Type", "application/x-ndjson")
				}
				if tc.partial {
					w.Header().Set("Content-Length", "20")
				}
				w.WriteHeader(tc.status)
				if _, err := io.WriteString(w, tc.body); err != nil {
					t.Error(err)
				}
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			s.queryPhases = make(map[string]*queryPhase)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			w := walk{phase: "fresh_backfill"}
			_, err := w.page(ctx, s, fixture{})
			if (err == nil) != (tc.phase == "complete") {
				t.Fatalf("page error = %v", err)
			}
			p := s.queryPhases[w.phase]
			q := p.Last
			if q.Admission != tc.admission || q.Phase != tc.phase || q.BodyBytes != tc.bytes || q.Budget <= 0 ||
				p.Attempts != 1 || p.Admissions[tc.admission] != 1 {
				t.Fatalf("unexpected diagnostics: %+v", p)
			}
			if (q.Headers != nil) != (tc.status != 0) || (q.Status != nil) != (tc.status != 0) ||
				(q.Status != nil && *q.Status != tc.status) ||
				(q.Body != nil) != (tc.status != 0 && !tc.partial) ||
				(p.Failure != nil) != (err != nil) {
				t.Fatalf("invalid observations: %+v", q)
			}
			if q.Body != nil && (*q.Body < *q.Headers || q.Elapsed < *q.Body) {
				t.Fatalf("timing phases out of order: %+v", q)
			}
		})
	}
}

func TestFreshBackfillBodyDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
			return
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	s := newService(srv.URL)
	defer s.client.CloseIdleConnections()
	s.queryPhases = make(map[string]*queryPhase)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	w := walk{phase: "fresh_backfill"}
	if _, err := w.page(ctx, s, fixture{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a body deadline, got %v", err)
	}
	q := s.queryPhases[w.phase].Failure
	if q == nil || q.Headers == nil || q.Body != nil || q.Phase != "body" ||
		q.Admission != "ndjson_response" || q.Budget <= 0 || q.Budget > 500*time.Millisecond {
		t.Fatalf("body deadline evidence = %+v", q)
	}
}

func TestCapacityStorageSamples(t *testing.T) {
	dir := t.TempDir()
	diag := capacityDiagnostics{Unavailable: make(map[string]string)}
	if err := diag.sampleStorage(dir); err != nil {
		t.Fatal(err)
	}
	if *diag.PinCount != 0 || *diag.StagingBytes != 0 || diag.OldestPin != nil {
		t.Fatalf("expected an observed empty directory: %+v", diag)
	}
	stage := filepath.Join(dir, ".bog-checkpoint-stage")
	if err := os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	pin := filepath.Join(dir, ".bog-history-pin-"+strings.Repeat("a", 32))
	for path, data := range map[string]string{
		filepath.Join(stage, "segment"):                                       "abc",
		filepath.Join(stage, "manifest"):                                      "abcd",
		filepath.Join(dir, ".bog-history-pin-stage-"+strings.Repeat("b", 32)): "",
		pin: "",
	} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	then := time.Now().Add(-2 * time.Minute)
	if err := os.Chtimes(pin, then, then); err != nil {
		t.Fatal(err)
	}
	if err := diag.sampleStorage(dir); err != nil {
		t.Fatal(err)
	}
	if *diag.PinCount != 1 || *diag.StagingBytes != 7 || *diag.OldestPin < 2*time.Minute {
		t.Fatalf("unexpected peak samples: %+v", diag)
	}
	if err := os.Remove(pin); err != nil {
		t.Fatal(err)
	}
	if err := diag.sampleStorage(dir); err != nil {
		t.Fatal(err)
	}
	diag.markMissing("live")
	if *diag.PinCount != 1 || diag.Unavailable["replay_duration"] == "" || diag.Unavailable["oldest_sampled_pin_record_age"] != "" {
		t.Fatalf("unexpected measurement availability: %+v", diag)
	}
}

func TestBoundedLogAndMemoryEvents(t *testing.T) {
	var log candidateLog
	for _, segment := range []string{strings.Repeat("x", logLimit+5), "last line"} {
		n, err := log.Write([]byte(segment))
		if n != len(segment) || err != nil {
			t.Fatalf("write = %d, %v", n, err)
		}
	}
	if len(log.tail) != logLimit || !log.truncated || !strings.HasSuffix(string(log.tail), "last line") {
		t.Fatal("candidate log did not retain the bounded tail")
	}
	for _, invalid := range []string{"", "oom 0", "oom_kill -1", "oom_kill 0\noom_kill 1", "oom_kill"} {
		if _, err := memoryEvents(invalid); err == nil {
			t.Errorf("accepted invalid memory.events %q", invalid)
		}
	}
	if events, err := memoryEvents("oom 2\noom_kill 1\n"); err != nil || events["oom_kill"] != 1 {
		t.Fatalf("memory events = %v, %v", events, err)
	}
}
