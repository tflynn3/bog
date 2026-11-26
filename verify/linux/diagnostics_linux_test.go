package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFailedStartupRetainsCandidateEvidence(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "candidate")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf 'startup failed\\n' >&2\nexit 17\n"), 0700); err != nil {
		t.Fatal(err)
	}
	diag := newCaseDiagnostics()
	_, err := runCase(t.Context(), binary, filepath.Join(dir, "store"), func(_ context.Context, _ *server) (any, error) {
		t.Fatal("probe called after failed startup")
		return nil, nil
	}, diag)
	if err == nil || len(diag.Candidates) != 1 {
		t.Fatalf("candidate evidence = %+v, %v", diag, err)
	}
	e := diag.Candidates[0]
	if !e.Reaped || e.Health != nil || e.ExitCode == nil || *e.ExitCode != 17 ||
		e.StartupError == "" || e.Log != "startup failed\n" || e.LogTruncated {
		t.Fatalf("candidate evidence = %+v", e)
	}
}

func TestDurabilityQueryPhaseEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, phase, admission, body string
		actual, expected             int
		truncated                    bool
	}{
		{"capacity refused", "validation", "capacity_rejection", "query capacity exhausted\n", 503, 200, false},
		{"stale cursor", "complete", "http_rejection", "invalid cursor\n", 400, 400, false},
		{"body interrupted", "body", "ndjson_response", "fragment", 200, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.actual == 200 {
					w.Header().Set("Content-Type", "application/x-ndjson")
				}
				if tc.truncated {
					w.Header().Set("Content-Length", fmt.Sprint(len(tc.body)+1))
				}
				w.WriteHeader(tc.actual)
				if _, err := io.WriteString(w, tc.body); err != nil {
					t.Error(err)
				}
			}))
			defer h.Close()
			s := &server{addr: strings.TrimPrefix(h.URL, "http://"), client: h.Client(), diagnostics: newCaseDiagnostics()}
			_, _, err := s.request(t.Context(), http.MethodGet, queryPath("opaque", 1), nil, tc.expected)
			kind := "continuation"
			if tc.expected == 400 {
				kind = "stale_cursor"
			}
			e := s.diagnostics.Queries[kind]
			if e == nil || e.Attempts != 1 {
				t.Fatalf("expected one attempt: %+v", e)
			}
			q := e.Last
			if q.Headers == nil || q.Status == nil || *q.Status != tc.actual ||
				q.Phase != tc.phase || q.Admission != tc.admission || q.Bytes != len(tc.body) ||
				(q.Body == nil) != tc.truncated || (e.Failure != nil) != (err != nil) {
				t.Fatalf("query diagnostics = %+v, %v", q, err)
			}
		})
	}
}
