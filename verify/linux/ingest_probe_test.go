package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func TestIngestObservations(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("close failed")
	for _, tc := range []struct {
		name, body, outcome, problem string
		status, want                 int
		transport, close             error
		cancel, truncated            bool
	}{
		{name: "acknowledged", status: 204, want: 204, outcome: ingestAcknowledged},
		{name: "acknowledged despite close failure", status: 204, want: 204, outcome: ingestAcknowledged, close: closeErr, problem: "close failed"},
		{name: "invalid batch", status: 400, want: 400, outcome: ingestHTTPRejected},
		{name: "WAL refusal needs readiness evidence", status: 503, want: 503, body: "disk full\n", outcome: ingestWALRefused},
		{name: "different 503", status: 503, want: 503, body: "busy\n", outcome: ingestHTTPRejected, problem: "expected WAL allocation refusal"},
		{name: "partial refusal body", status: 503, want: 503, body: "disk full", outcome: ingestHTTPRejected, truncated: true, problem: "unexpected EOF"},
		{name: "oversized response", status: 503, want: 503, body: strings.Repeat("x", 4097), outcome: ingestHTTPRejected, problem: "response exceeds"},
		{name: "wrong status but actual acknowledgment", status: 204, want: 503, outcome: ingestAcknowledged, problem: "HTTP 204, expected 503"},
		{name: "unknown server failure", status: 500, want: 204, outcome: ingestUnexpectedResponse, problem: "HTTP 500, expected 204"},
		{name: "connection drops before headers", want: 204, outcome: ingestTransportAmbiguous, transport: io.EOF, problem: "EOF"},
		{name: "canceled before request", want: 204, outcome: ingestNotSubmitted, cancel: true, problem: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls, closes := 0, 0
			client := &http.Client{Transport: readinessTransport(func(req *http.Request) (*http.Response, error) {
				calls++
				body, readErr := io.ReadAll(req.Body)
				if err := errors.Join(readErr, req.Body.Close()); err != nil {
					t.Fatal(err)
				}
				if req.Method != http.MethodPost || req.URL.Path != "/ingest" || string(body) != "fixture" {
					t.Fatalf("unexpected request %s %s %q", req.Method, req.URL, body)
				}
				if tc.transport != nil {
					return nil, tc.transport
				}
				var payload io.Reader = strings.NewReader(tc.body)
				if tc.truncated {
					payload = io.MultiReader(payload, iotest.ErrReader(io.ErrUnexpectedEOF))
				}
				return &http.Response{StatusCode: tc.status, Body: readinessBody{payload, func() error {
					closes++
					return tc.close
				}}}, nil
			})}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			obs, err := probeIngest(ctx, client, "localhost", []byte("fixture"), 2, tc.want, time.Now())
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("ingest error = %v, want %q", err, tc.problem)
			}
			if obs.Outcome != tc.outcome || obs.Rows != 2 || obs.ExpectedStatus == nil || *obs.ExpectedStatus != tc.want || obs.Rollback != nil {
				t.Fatalf("ingest observation = %+v", obs)
			}
			if tc.status == 0 {
				if obs.Status != nil || obs.Headers != nil || obs.Body != nil || closes != 0 {
					t.Fatalf("unexpected response evidence: %+v, closes=%d", obs, closes)
				}
			} else if obs.Status == nil || *obs.Status != tc.status || obs.Headers == nil || closes != 1 ||
				(obs.Body == nil) != (tc.truncated || len(tc.body) > 4096) {
				t.Fatalf("incomplete response evidence: %+v, closes=%d", obs, closes)
			}
			if (calls == 0) != tc.cancel || tc.close != nil && !errors.Is(err, tc.close) {
				t.Fatalf("calls=%d, error=%v", calls, err)
			}
		})
	}
}
