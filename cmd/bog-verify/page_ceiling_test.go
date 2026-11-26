package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"bog/internal/client"
	"bog/internal/store"
)

func TestCLIComparePageAboveDefaultCeiling(t *testing.T) {
	scratch := t.TempDir()
	ts, err := store.ParseTime(fixtureStart)
	check(t, err)
	rows := []client.Row{{TS: ts, Line: "keep-first"}, {TS: ts, Line: "keep-second"}}
	line := "keep-" + strings.Repeat("x", 7<<19)
	for range 10 {
		rows = append(rows, client.Row{TS: ts, Line: line})
	}
	dir, _ := makeBundleFromRows(t, rows, 1)
	t.Setenv("TMPDIR", scratch)
	const head = `{"ts":"` + fixtureStart + `","line":"`
	const foot = "\"}\n"
	totalBytes := 0
	for _, row := range rows {
		totalBytes += len(head) + len(row.Line) + len(foot)
	}
	if totalBytes <= client.DefaultMaxPage {
		t.Fatal("large page fixture fits the default ceiling")
	}

	for _, tc := range []struct {
		name  string
		limit int
		exit  int
		want  string
	}{
		{"default ceiling rejects page", 0, exitIncomplete, "increase -max-page-bytes or reduce -limit"},
		{"configured ceiling rejects page", totalBytes - 1, exitIncomplete, fmt.Sprintf("exceeds -max-page-bytes=%d", totalBytes-1)},
		{"exact elevated ceiling matches", totalBytes, exitOK, "rows=12 reference_pages=5 server_pages=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursor := fixtureCursor('a')
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := requests.Add(1)
				q := r.URL.Query()
				if r.Method != http.MethodGet || r.URL.Path != "/query" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				if n == 1 && q.Get("page") == "1" && q.Get("cursor") == "" {
					w.Header().Set("Content-Length", "0")
					w.Header().Set("Bog-Next-Cursor", cursor)
					w.WriteHeader(http.StatusOK)
					return
				}
				if n != 2 || q.Get("page") != "" || q.Get("cursor") != cursor {
					t.Errorf("unexpected request %d: %s %s", n, r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Length", strconv.Itoa(totalBytes))
				w.WriteHeader(http.StatusOK)
				if tc.exit != exitOK {
					if err := http.NewResponseController(w).Flush(); err != nil {
						t.Errorf("flush oversized response headers: %v", err)
					}
					return
				}
				for _, row := range rows {
					for _, part := range []string{head, row.Line, foot} {
						if n, err := io.WriteString(w, part); err != nil || n != len(part) {
							t.Errorf("write response: %d of %d bytes, %v", n, len(part), err)
							return
						}
					}
				}
			}))
			defer server.Close()
			args := append(compareArgs(dir, server.URL), "-limit", "100", "-timeout", "30s")
			if tc.limit != 0 {
				args = append(args, "-max-page-bytes", strconv.Itoa(tc.limit))
			}
			var stdout, stderr bytes.Buffer
			status := run(t.Context(), args, &stdout, &stderr)
			if status != tc.exit || !strings.Contains(stdout.String()+stderr.String(), tc.want) ||
				(status == exitOK && stderr.Len() != 0) || (status != exitOK && stdout.Len() != 0) {
				t.Fatalf("compare exit %d, stdout %q, stderr %q", status, &stdout, &stderr)
			}
			if got := requests.Load(); got != 2 {
				t.Fatalf("made %d requests, want 2", got)
			}
			if status == exitIncomplete && !strings.Contains(stderr.String(), "server walk at row 1: page 2:") {
				t.Fatalf("missing failed page position in %q", &stderr)
			}
			assertEmptyScratch(t, scratch)
		})
	}
}

func TestCLICompareCleansScratchOnMismatch(t *testing.T) {
	scratch := t.TempDir()
	dir, _, _ := makeBundle(t)
	t.Setenv("TMPDIR", scratch)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const row = `{"ts":"` + fixtureStart + `","line":"keep-wrong"}` + "\n"
		if n, err := io.WriteString(w, row); err != nil || n != len(row) {
			t.Errorf("write mismatch response: %d of %d bytes, %v", n, len(row), err)
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	if status := run(t.Context(), compareArgs(dir, server.URL), &stdout, &stderr); status != exitMismatch ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "line mismatch at row 1") {
		t.Fatalf("compare exit %d, stdout %q, stderr %q", status, &stdout, &stderr)
	}
	assertEmptyScratch(t, scratch)
}

func assertEmptyScratch(t *testing.T, scratch string) {
	t.Helper()
	entries, err := os.ReadDir(scratch)
	check(t, err)
	if len(entries) != 0 {
		t.Fatalf("reference scratch still contains %v", entries)
	}
}
