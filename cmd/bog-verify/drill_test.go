package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/client"
)

type restartFunc func(context.Context) error

func (f restartFunc) WaitForRestart(ctx context.Context) error { return f(ctx) }

type drillPage struct {
	cursor, next string
	rows         []client.Row
	status       int
	body         string
}

func TestRestoreDrillPhases(t *testing.T) {
	ts, err := time.Parse(time.RFC3339, fixtureStart)
	check(t, err)
	rows := []client.Row{
		{TS: ts, Line: "keep-é\t\x00\n "},
		{TS: ts, Line: "keep-a"},
		{TS: ts, Line: "keep-a"},
		{TS: ts, Line: "keep-e\u0301"},
	}
	bundle, point := makeBundleFromRows(t, rows, 2)
	replaceable, _ := makeBundleFromRows(t, rows, 2)
	replacement, _ := makeBundleFromRows(t, rows[:3], 1)
	scratch := t.TempDir()
	t.Setenv("TMPDIR", scratch)

	a, b, c := fixtureCursor('a'), fixtureCursor('b'), fixtureCursor('c')
	before := []drillPage{{next: a, rows: rows[:2]}, {cursor: a, rows: rows[2:]}}
	after := []drillPage{
		{next: b, rows: rows[:1]},
		{cursor: b, next: c, rows: rows[1:3]},
		{cursor: c, rows: rows[3:]},
	}
	retry := drillPage{cursor: a, status: http.StatusServiceUnavailable, body: "query capacity exhausted"}
	expired := drillPage{cursor: a, status: http.StatusGone, body: "query capture expired"}
	invalid := drillPage{cursor: a, status: http.StatusBadRequest, body: "invalid cursor"}
	afterExpiration := expired
	afterExpiration.cursor = b
	afterRetry := retry
	afterRetry.cursor = b
	swapped := []client.Row{rows[1], rows[3]}
	changed := slices.Clone(rows[:1])
	changed[0].Line = "keep-é\t\x00\n"
	for _, tc := range []struct {
		name           string
		before, after  []drillPage
		extra          []string
		restartFailure error
		changeImage    bool
		status         int
		wantError      error
		wantDetail     string
	}{
		{
			name: "fresh walks and retry budgets per page",
			before: []drillPage{
				{status: http.StatusServiceUnavailable, body: "query work budget exhausted"},
				before[0], retry, before[1],
			},
			after: []drillPage{after[0], afterRetry, after[1], after[2]},
		},
		{
			name: "first byte difference before restart", before: []drillPage{{rows: changed}},
			status: exitMismatch, wantDetail: "before restart: line mismatch at row 1 (timestamp " + fixtureStart + ", byte 11)",
		},
		{
			name: "tie order differs after restart", before: before,
			after:  []drillPage{after[0], {cursor: b, rows: swapped, next: c}},
			status: exitMismatch, wantDetail: "after restart: line mismatch at row 3",
		},
		{
			name: "duplicate missing after restart", before: before,
			after:  []drillPage{after[0], {cursor: b, rows: rows[1:2]}},
			status: exitMismatch, wantDetail: "after restart: length mismatch at row 3",
		},
		{
			name: "retry exhaustion before restart", before: []drillPage{before[0], retry, retry},
			status: exitIncomplete, wantError: client.ErrPageRetryExhausted, wantDetail: "before restart: server walk at row 3",
		},
		{
			name: "retry exhaustion after restart", before: before,
			after:  []drillPage{after[0], afterRetry, afterRetry},
			status: exitIncomplete, wantError: client.ErrPageRetryExhausted, wantDetail: "after restart: server walk at row 2",
		},
		{
			name: "expiry before restart", before: []drillPage{before[0], expired},
			status: exitIncomplete, wantError: client.ErrCaptureExpired, wantDetail: "before restart: server walk at row 3",
		},
		{
			name: "expiry after retry after restart", before: before,
			after:  []drillPage{after[0], afterRetry, afterExpiration},
			status: exitIncomplete, wantError: client.ErrCaptureExpired, wantDetail: "after restart: server walk at row 2",
		},
		{
			name: "invalid cursor is distinct", before: []drillPage{before[0], invalid},
			status: exitIncomplete, wantError: client.ErrInvalidCursor, wantDetail: "before restart: server walk at row 3",
		},
		{
			name: "page budget resets and still requires a terminal page", before: before, after: after[:2],
			extra: []string{"-max-pages", "2"}, status: exitIncomplete,
			wantError: client.ErrPageBudget, wantDetail: "after restart: server walk at row 4",
		},
		{
			name: "byte limit after restart", before: before,
			after:  []drillPage{{rows: []client.Row{{TS: ts, Line: strings.Repeat("x", 256)}}}},
			status: exitIncomplete, wantError: client.ErrPageBytes, wantDetail: "after restart: server walk at row 1",
		},
		{
			name: "boundary fails", before: before, restartFailure: errors.New("operator interrupted restart"),
			status: exitIncomplete, wantDetail: "restart boundary: operator interrupted restart",
		},
		{
			name: "reference recovery point changes", before: before, changeImage: true,
			status: exitVerification, wantDetail: "after restart: backup verification failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			phase, calls := 0, 0
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				pages := tc.before
				if phase == 1 {
					pages = tc.after
				}
				if calls >= len(pages) {
					t.Errorf("phase %d unexpected request %s", phase, r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				p := pages[calls]
				calls++
				first := ""
				if p.cursor == "" {
					first = "1"
				}
				q := r.URL.Query()
				if r.Method != http.MethodGet || r.URL.Path != "/query" ||
					q.Get("page") != first || q.Get("cursor") != p.cursor ||
					q.Get("start") != fixtureStart || q.Get("end") != fixtureEnd ||
					q.Get("contains") != "keep-" || q.Get("limit") != "2" {
					t.Errorf("phase %d request %d: %s %s, want cursor %q", phase, calls, r.Method, r.URL, p.cursor)
				}
				if p.status != 0 {
					http.Error(w, p.body, p.status)
					return
				}
				writeDrillPage(t, w, p)
			}))
			defer server.Close()

			dir := bundle
			if tc.changeImage {
				dir = replaceable
			}
			args := append(compareArgs(dir, server.URL)[1:],
				"-max-retries", "1", "-retry-delay", "0", "-max-page-bytes", "256")
			opts, err := parseOptions("drill", append(args, tc.extra...))
			check(t, err)
			c, err := client.New(opts.baseURL, nil)
			check(t, err)
			defer c.CloseIdleConnections()
			c.MaxPageBytes = opts.maxPageBytes
			ctx, cancel := context.WithTimeout(t.Context(), opts.timeout)
			defer cancel()
			var out bytes.Buffer
			boundary := restartFunc(func(context.Context) error {
				assertEmptyScratch(t, scratch)
				if !strings.Contains(out.String(), "before restart matched: rows=4 reference_pages=2 server_pages=2 "+formatPoint(point)) {
					t.Errorf("before restart report %q", &out)
				}
				mu.Lock()
				defer mu.Unlock()
				if phase != 0 || calls != len(tc.before) {
					t.Errorf("boundary called in phase %d after %d requests, want %d", phase, calls, len(tc.before))
				}
				phase, calls = 1, 0
				if tc.changeImage {
					if err := os.Rename(dir, filepath.Join(filepath.Dir(dir), "original")); err != nil {
						return err
					}
					if err := os.Rename(replacement, dir); err != nil {
						return err
					}
				}
				return tc.restartFailure
			})
			status, drillErr := drillBackup(ctx, opts, c, boundary, &out)
			if status != tc.status || (tc.wantError != nil && !errors.Is(drillErr, tc.wantError)) ||
				(tc.wantDetail != "" && !strings.Contains(fmt.Sprint(drillErr), tc.wantDetail)) {
				t.Fatalf("status %d, error %v, stdout %q; want %d, %v, %q", status, drillErr, &out, tc.status, tc.wantError, tc.wantDetail)
			}
			if tc.status == exitOK {
				if drillErr != nil || !strings.Contains(out.String(), "restore drill matched after restart: rows=4 reference_pages=2 server_pages=3 "+formatPoint(point)) {
					t.Fatalf("missing complete drill result: %q, %v", &out, drillErr)
				}
			} else if strings.Contains(out.String(), "restore drill matched after restart:") {
				t.Fatalf("failed drill emitted a success report: %q", &out)
			}
			mu.Lock()
			defer mu.Unlock()
			if strings.HasPrefix(tc.wantDetail, "before restart") {
				if phase != 0 || calls != len(tc.before) {
					t.Errorf("phase %d ended after %d requests, want phase 0 and %d", phase, calls, len(tc.before))
				}
			} else if phase != 1 || calls != len(tc.after) {
				t.Errorf("phase %d ended after %d requests, want phase 1 and %d", phase, calls, len(tc.after))
			}
			assertEmptyScratch(t, scratch)
		})
	}
}

func writeDrillPage(t *testing.T, w http.ResponseWriter, p drillPage) {
	t.Helper()
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, row := range p.rows {
		if err := encoder.Encode(row); err != nil {
			t.Errorf("encode fixture row: %v", err)
			http.Error(w, "fixture encoding failed", http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(body.Len()))
	if p.next != "" {
		w.Header().Set("Bog-Next-Cursor", p.next)
	}
	if n, err := w.Write(body.Bytes()); err != nil || n != body.Len() {
		t.Errorf("write fixture response: %d of %d bytes, %v", n, body.Len(), err)
	}
}

type promptWriter func([]byte) (int, error)

func (w promptWriter) Write(p []byte) (int, error) { return w(p) }

func TestDrillCLIWaitsForOperatorSignal(t *testing.T) {
	dir, _, _ := makeBundle(t)
	for _, action := range []string{"signal", "cancel at boundary", "cancel after restart", "output error"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var mu sync.Mutex
			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				requests++
				if r.Method != http.MethodGet || r.URL.Path != "/query" || r.URL.Query().Get("page") != "1" ||
					r.URL.Query().Has("cursor") {
					t.Errorf("expected a fresh read-only query, got %s %s", r.Method, r.URL)
				}
				if action == "cancel after restart" && requests == 2 {
					cancel()
					return
				}
				writeDrillPage(t, w, drillPage{})
			}))
			defer server.Close()
			var output bytes.Buffer
			out := promptWriter(func(p []byte) (int, error) {
				if !bytes.Contains(p, []byte("kill -USR1")) {
					return output.Write(p)
				}
				mu.Lock()
				defer mu.Unlock()
				if requests != 1 {
					t.Errorf("restart prompt after %d queries, want 1", requests)
				}
				switch action {
				case "signal", "cancel after restart":
					process, err := os.FindProcess(os.Getpid())
					if err != nil {
						return 0, err
					}
					if err := process.Signal(syscall.SIGUSR1); err != nil {
						return 0, err
					}
				case "cancel at boundary":
					cancel()
				case "output error":
					return 0, errors.New("output unavailable")
				}
				return output.Write(p)
			})
			args := append(compareArgs(dir, server.URL), "-contains", "no matches")
			args[0] = "drill"
			status, err := execute(ctx, args, out)
			mu.Lock()
			defer mu.Unlock()
			switch action {
			case "signal":
				if status != exitOK || err != nil || requests != 2 {
					t.Fatalf("status %d, error %v, requests %d, output %q", status, err, requests, &output)
				}
			case "cancel at boundary":
				if status != exitIncomplete || !errors.Is(err, context.Canceled) || requests != 1 {
					t.Fatalf("status %d, error %v, requests %d", status, err, requests)
				}
			case "cancel after restart":
				if status != exitIncomplete || !errors.Is(err, context.Canceled) || requests != 2 ||
					!strings.Contains(err.Error(), "after restart:") {
					t.Fatalf("status %d, error %v, requests %d", status, err, requests)
				}
			case "output error":
				if status != exitIncomplete || !strings.Contains(fmt.Sprint(err), "write restart prompt") || requests != 1 {
					t.Fatalf("status %d, error %v, requests %d", status, err, requests)
				}
			}
		})
	}
}
