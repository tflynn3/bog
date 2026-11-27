package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	t.Parallel()
	const valid = `{"ts":"2025-01-01T00:00:00Z","line":"test"}` + "\n"
	for _, tc := range []struct {
		name, phase, admission, body, want string
		status                             int
		stale, truncated, cursor, trailer  bool
	}{
		{name: "valid page", phase: "complete", admission: "ndjson_response", body: valid, status: 200},
		{name: "capacity refused", phase: "validation", admission: "capacity_rejection", body: "query capacity exhausted\n", want: "HTTP 503, expected 200", status: 503},
		{name: "work budget refused", phase: "validation", admission: "work_budget_rejection", body: "query work budget exhausted\n", want: "HTTP 503, expected 200", status: 503},
		{name: "stale cursor", phase: "complete", admission: "http_rejection", body: "invalid cursor\n", status: 400, stale: true},
		{name: "unrelated bad request", phase: "validation", admission: "http_rejection", body: "invalid limit\n", want: "exact invalid cursor", status: 400, stale: true},
		{name: "inexact bad request", phase: "validation", admission: "http_rejection", body: "invalid cursor", want: "exact invalid cursor", status: 400, stale: true},
		{name: "cursor-bearing bad request", phase: "validation", admission: "http_rejection", body: "invalid cursor\n", want: "without a continuation", status: 400, stale: true, cursor: true},
		{name: "trailer-bearing bad request", phase: "validation", admission: "http_rejection", body: "invalid cursor\n", want: "query cursor trailer", status: 400, stale: true, trailer: true},
		{name: "malformed page", phase: "validation", admission: "ndjson_response", body: "{broken}\n", want: "decode query NDJSON", status: 200, cursor: true},
		{name: "unterminated page", phase: "validation", admission: "ndjson_response", body: strings.TrimSuffix(valid, "\n"), want: "unterminated NDJSON", status: 200},
		{name: "body interrupted at row boundary", phase: "body", admission: "ndjson_response", body: valid, want: "unexpected EOF", status: 200, truncated: true, cursor: true},
		{name: "rejection body interrupted", phase: "body", admission: "http_rejection", body: "invalid cursor\n", want: "unexpected EOF", status: 400, stale: true, truncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status == http.StatusOK {
					w.Header().Set("Content-Type", "application/x-ndjson")
				}
				if tc.truncated {
					w.Header().Set("Content-Length", fmt.Sprint(len(tc.body)+1))
				}
				if tc.cursor {
					w.Header().Set("Bog-Next-Cursor", "extra")
				}
				if tc.trailer {
					w.Header().Set("Trailer", "Bog-Next-Cursor")
				}
				w.WriteHeader(tc.status)
				if _, err := io.WriteString(w, tc.body); err != nil {
					t.Error(err)
				}
				if tc.trailer {
					w.Header().Set("Bog-Next-Cursor", "late")
				}
			}))
			defer h.Close()
			h.Client().Timeout = processTimeout
			s := &server{addr: strings.TrimPrefix(h.URL, "http://"), client: h.Client(), diagnostics: newCaseDiagnostics()}
			s.diagnostics.Candidates = []*processEvidence{{}, {}}
			kind := "continuation"
			var err error
			if tc.stale {
				kind = "stale_cursor"
				err = s.rejectCursor(t.Context(), "opaque")
			} else {
				_, _, err = s.page(t.Context(), "opaque", 1)
			}
			if (tc.want == "" && err != nil) || (tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want))) {
				t.Fatalf("query error = %v, want %q", err, tc.want)
			}
			e := s.diagnostics.Queries[kind]
			if len(s.diagnostics.Queries) != 1 || e == nil || e.Attempts != 1 || e.Admissions[tc.admission] != 1 {
				t.Fatalf("expected one %s attempt: %+v", kind, e)
			}
			q := e.Last
			if q.Headers == nil || q.Status == nil || *q.Status != tc.status || q.Lifetime != 2 ||
				q.Phase != tc.phase || q.Admission != tc.admission || q.Bytes != len(tc.body) || q.Cursor != tc.cursor ||
				(q.Body == nil) != tc.truncated || q.Elapsed < *q.Headers ||
				(q.Body != nil && (*q.Body < *q.Headers || q.Elapsed < *q.Body)) || e.MaxHeaders != q.Headers || e.MaxBody != q.Body {
				t.Fatalf("query diagnostics = %+v", q)
			}
			if (err == nil && (e.Failure != nil || q.Error != "")) ||
				(err != nil && (e.Failure == nil || *e.Failure != q || !strings.Contains(q.Error, tc.want))) {
				t.Fatalf("first_failure = %+v, last = %+v", e.Failure, q)
			}
		})
	}
}

func TestQueryTransportFailureEvidence(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("close failed")
	const invalid = "invalid cursor\n"
	for _, tc := range []struct {
		name, body, phase, want string
		closeErr                error
		closeDelimited          bool
	}{
		{name: "close failure", body: invalid, phase: "close", want: "close failed", closeErr: closeErr},
		{name: "oversized response still closes", body: strings.Repeat("x", maxResponseBody+2), phase: "body", want: "response exceeds 4194304 bytes", closeErr: closeErr},
		{name: "close-delimited response", body: invalid, phase: "validation", want: "unverifiable HTTP framing", closeDelimited: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := &queryTestBody{Reader: strings.NewReader(tc.body), closeErr: tc.closeErr}
			s := &server{
				addr: "localhost", diagnostics: newCaseDiagnostics(),
				client: &http.Client{Timeout: processTimeout, Transport: queryTestTransport(func(*http.Request) (*http.Response, error) {
					length := int64(len(tc.body))
					if tc.closeDelimited {
						length = -1
					}
					return &http.Response{StatusCode: 400, Body: body, ContentLength: length, ProtoMajor: 1}, nil
				})},
			}
			err := s.rejectCursor(t.Context(), "opaque")
			if err == nil || !strings.Contains(err.Error(), tc.want) || (tc.closeErr != nil && !errors.Is(err, tc.closeErr)) {
				t.Fatalf("query error = %v, want %q", err, tc.want)
			}
			if body.closes != 1 {
				t.Fatalf("body closed %d times", body.closes)
			}
			e := s.diagnostics.Queries["stale_cursor"]
			if e == nil || e.Attempts != 1 || e.Failure == nil || e.Failure.Phase != tc.phase ||
				e.Failure.Bytes != min(len(tc.body), maxResponseBody+1) || (e.Failure.Body != nil) != (tc.phase != "body") {
				t.Fatalf("query diagnostics = %+v", e)
			}
		})
	}
}

func TestQueryDiagnosticsRetainFirstFailure(t *testing.T) {
	t.Parallel()
	diag := newCaseDiagnostics()
	first := errors.New("malformed page")
	for _, attempt := range []struct {
		err     error
		headers time.Duration
		body    time.Duration
	}{
		{first, 2, 4},
		{errors.New("later failure"), 3, 5},
		{nil, 1, 2},
	} {
		diag.observeQuery("fresh_page", queryResponse{
			Admission: "ndjson_response", Phase: "validation", Headers: &attempt.headers, Body: &attempt.body,
		}, attempt.err)
	}
	e := diag.Queries["fresh_page"]
	if e.Attempts != 3 || e.Admissions["ndjson_response"] != 3 ||
		*e.MaxHeaders != 3 || *e.MaxBody != 5 || e.Last.Phase != "complete" ||
		e.Last.Error != "" || e.Failure == nil || e.Failure.Error != first.Error() || e.Failure.Phase != "validation" {
		t.Fatalf("query diagnostics = %+v", e)
	}
}

type queryTestTransport func(*http.Request) (*http.Response, error)

func (f queryTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type queryTestBody struct {
	io.Reader
	closeErr error
	closes   int
}

func (b *queryTestBody) Close() error {
	b.closes++
	return b.closeErr
}
