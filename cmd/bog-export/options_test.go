package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInvalidOptionsDoNotRequest(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("made an HTTP request with invalid options")
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	for _, tc := range []struct {
		name, diagnostic string
		flags            []string
	}{
		{"missing URL", "-url", []string{"-url", ""}},
		{"relative URL", "-url", []string{"-url", "/query"}},
		{"wrong scheme", "-url", []string{"-url", "ftp://example.test"}},
		{"missing host", "-url", []string{"-url", "http://:1234"}},
		{"invalid bracketed host", "-url", []string{"-url", "http://[not-an-ip]:8080"}},
		{"unbracketed IPv6", "-url", []string{"-url", "http://::1"}},
		{"out of range port", "-url port", []string{"-url", "http://localhost:65536"}},
		{"bad escape", "-url", []string{"-url", server.URL + "/%xx"}},
		{"URL query", "-url", []string{"-url", server.URL + "?query=value"}},
		{"empty URL query", "-url", []string{"-url", server.URL + "?"}},
		{"empty URL fragment", "-url", []string{"-url", server.URL + "#"}},
		{"missing start", "invalid -start (expected RFC3339): invalid timestamp", []string{"-start", ""}},
		{"invalid start", "invalid -start (expected RFC3339): invalid zone", []string{"-start", "2025-01-01T00:00:00+24:00"}},
		{"missing end", "invalid -end (expected RFC3339): invalid timestamp", []string{"-end", ""}},
		{"invalid end", "invalid -end (expected RFC3339): invalid timestamp", []string{"-end", "2025-01-01T00:00:10,1Z"}},
		{"end outside UTC range", "invalid -end (expected RFC3339): timestamp outside supported range", []string{"-end", "9999-12-31T23:59:59-00:01"}},
		{"backward range", "-end precedes -start", []string{"-start", "2025-01-01T00:00:11Z"}},
		{"zero limit", "-limit", []string{"-limit", "0"}},
		{"high limit", "-limit", []string{"-limit", "1001"}},
		{"bad page count", "-max-pages", []string{"-max-pages", "-1"}},
		{"zero page size", "-max-page-bytes", []string{"-max-page-bytes", "0"}},
		{"page byte overflow", "-max-page-bytes", []string{"-max-page-bytes", "9223372036854775807"}},
		{"zero timeout", "-timeout", []string{"-timeout", "0s"}},
		{"bad timeout", "-timeout", []string{"-timeout", "2"}},
		{"bad retries", "-max-retries", []string{"-max-retries", "-1"}},
		{"bad retry delay", "-retry-delay", []string{"-retry-delay", "-1ns"}},
		{"unexpected argument", "unexpected argument", []string{"export"}},
		{"unknown flag", "-bogus", []string{"-bogus"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			status := run(t.Context(), argsFor(server.URL, tc.flags...), &stdout, &stderr)
			assertResult(t, status, stdout.String(), stderr.String(), exitUsage, "", tc.diagnostic)
		})
	}
}

func TestHelpUsesStderr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	status := run(t.Context(), []string{"-h"}, &stdout, &stderr)
	assertResult(t, status, stdout.String(), stderr.String(), exitOK, "", "bog-export -url URL")
}

func TestOptionsAllowEmptyRange(t *testing.T) {
	opts, err := parseOptions(argsFor("https://[::1]:8080", "-end", "2024-12-31T16:00:00-08:00", "-limit", "1000"))
	if err != nil {
		t.Fatal(err)
	}
	if !opts.query.Start.Equal(opts.query.End) || opts.query.Limit != 1000 {
		t.Errorf("query = %+v", opts.query)
	}
}

func TestOptionsNormalizeQueryBounds(t *testing.T) {
	t.Parallel()
	opts, err := parseOptions(argsFor("https://example.test",
		"-start", "2025-01-01T01:30:00.123456789+01:30",
		"-end", "2024-12-31T20:00:01.5-04:00",
	))
	if err != nil {
		t.Fatal(err)
	}
	wantStart := time.Date(2025, 1, 1, 0, 0, 0, 123456789, time.UTC)
	wantEnd := time.Date(2025, 1, 1, 0, 0, 1, 500000000, time.UTC)
	if opts.query.Start != wantStart || opts.query.End != wantEnd {
		t.Errorf("query bounds = %v to %v, want %v to %v", opts.query.Start, opts.query.End, wantStart, wantEnd)
	}
}
