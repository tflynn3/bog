package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequireRejection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, cursor string
		status             int
		pass               bool
	}{
		{name: "plain error", body: "query capacity exhausted\n", status: 503, pass: true},
		{name: "empty error", status: 503, pass: true},
		{name: "wrong status", status: 422},
		{name: "partial NDJSON", body: "oops\n{\"ts\":\"ts\",\"line\":\"row\"}\n", status: 503},
		{name: "cursor", cursor: "token", status: 503},
		{name: "empty cursor header", status: 503},
		{name: "cursor trailer", cursor: "token", status: 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				switch tc.name {
				case "cursor trailer":
					w.Header().Set("Trailer", nextCursor)
				case "cursor", "empty cursor header":
					w.Header().Set(nextCursor, tc.cursor)
				}
				w.WriteHeader(tc.status)
				if _, err := fmt.Fprint(w, tc.body); err != nil {
					t.Error(err)
				}
				if tc.name == "cursor trailer" {
					w.Header().Set(nextCursor, tc.cursor)
				}
			}))
			defer srv.Close()
			got, err := requireRejection(t.Context(), srv.Client(), srv.URL)
			if (err == nil) != tc.pass {
				t.Fatalf("wanted pass=%t, got %+v, %v", tc.pass, got, err)
			}
		})
	}
}

func TestRequireRejectionHonorsDeadline(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer srv.Close()
	start := time.Now()
	if _, err := requireRejection(t.Context(), srv.Client(), srv.URL); err == nil {
		t.Fatal("accepted response after deadline")
	}
	if elapsed := time.Since(start); elapsed >= 3*time.Second {
		t.Fatalf("deadline not enforced: %s", elapsed)
	}
}

func TestHasZeroWindow(t *testing.T) {
	t.Parallel()
	const tuple = " 2: 0100007F:1F90 0100007F:DA04 "
	for _, tc := range []struct {
		name, line string
		pass, bad  bool
	}{
		{"persist", tuple + "01 00004000:00000000 04:00000020", true, false},
		{"empty-send-queue", tuple + "01 00000000:00000000 04:00000020", false, false},
		{"retransmitting", tuple + "01 00004000:00000000 01:00000020", false, false},
		{"closed", tuple + "06 00004000:00000000 04:00000020", false, false},
		{"unrelated", strings.Replace(tuple, ":DA04", ":DA05", 1) + "01 00004000:00000000 04:00000020", false, false},
		{"truncated", tuple + "01", false, true},
		{"malformed queue", tuple + "01 broken:00000000 04:00000020", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hasZeroWindow(tc.line, 8080, 55812)
			if got != tc.pass || (err != nil) != tc.bad {
				t.Fatalf("wanted %t, error=%t, got %t, %v", tc.pass, tc.bad, got, err)
			}
		})
	}
}

func TestRequireSnapshot(t *testing.T) {
	t.Parallel()
	const firstToken = "opaque+/=&"
	const continuation = "next+token"
	const firstRow = `{"ts":"2020-01-01T00:00:00Z","line":"first"}` + "\n"
	const lastRow = `{"ts":"2020-01-01T00:00:00Z","line":"last"}` + "\n"
	const appendRow = `{"ts":"2020-01-01T00:00:00Z","line":"concurrent"}` + "\n"
	for _, tc := range []struct {
		name, first, cursor, last, lastCursor, problem string
	}{
		{"exact pages", firstRow, continuation, lastRow, "", ""},
		{"short snapshot", firstRow, "", "", "", "rows remaining"},
		{"no progress", "", continuation, lastRow, "", "without additional"},
		{"repeated token", firstRow, firstToken, "", "", "cursor repeated"},
		{"append leaked", firstRow, continuation, lastRow + appendRow, "", "unexpected row"},
		{"row order", lastRow, continuation, firstRow, "", "unexpected row"},
		{"duplicate row", firstRow, continuation, firstRow, "", "unexpected row"},
		{"malformed row", firstRow, continuation, "{", "", "row 0"},
		{"extra continuation", firstRow, continuation, lastRow, "extra", "without additional"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				supplied := req.URL.Query().Get("cursor")
				var body, cursor string
				switch supplied {
				case firstToken:
					body, cursor = tc.first, tc.cursor
				case continuation:
					body, cursor = tc.last, tc.lastCursor
				default:
					t.Errorf("unexpected opaque cursor %q", supplied)
					http.Error(w, "invalid cursor", http.StatusBadRequest)
					return
				}
				if cursor != "" {
					w.Header().Set(nextCursor, cursor)
				}
				if _, err := fmt.Fprint(w, body); err != nil {
					t.Error(err)
				}
			}))
			defer srv.Close()
			want := []row{{fixtureStart, "first"}, {fixtureStart, "last"}}
			err := requireSnapshot(t.Context(), srv.Client(), srv.URL, firstToken, want)
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("wanted %q, got %v", tc.problem, err)
			}
		})
	}
}
