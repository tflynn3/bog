package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"bog/internal/client"
)

func TestDrillJSONCLI(t *testing.T) {
	dir, rows, point := makeBundle(t)
	a, b, c := fixtureCursor('a'), fixtureCursor('b'), fixtureCursor('c')
	first := drillPage{rows: []client.Row{rows[0], rows[2]}, next: a}
	last := drillPage{rows: rows[3:5], cursor: a}
	empty := drillPage{cursor: a, next: b}
	shuffled := drillPage{cursor: a, rows: []client.Row{rows[4], rows[3]}}
	expired := drillPage{cursor: a, status: http.StatusGone, body: "query capture expired"}
	busy := drillPage{cursor: a, status: http.StatusServiceUnavailable, body: "query capacity exhausted"}
	invalid := drillPage{cursor: a, status: http.StatusBadRequest, body: "invalid cursor"}
	complete := drillCounts{4, 2, 2}
	for _, tc := range []struct {
		name          string
		before, after []drillPage
		action        string
		extra         []string
		wantStatus    int
		wantPhase     drillPhase
		wantCode      string
		wantMessage   string
		beforeCounts  *drillCounts
		afterCounts   *drillCounts
	}{
		{
			name: "success with different pagination", before: []drillPage{first, last},
			after:        []drillPage{first, empty, {cursor: b, rows: rows[3:5]}},
			beforeCounts: &complete, afterCounts: &drillCounts{4, 2, 3},
		},
		{
			name: "missing backup", action: "missing", wantStatus: exitVerification,
			wantPhase: "before_restart", wantCode: "backup_verification_failed", wantMessage: "backup verification failed",
		},
		{
			name: "pre-canceled", action: "cancel before", wantStatus: exitIncomplete,
			wantPhase: "before_restart", wantCode: "canceled", wantMessage: "context canceled",
		},
		{
			name: "mismatch before restart", before: []drillPage{first, shuffled},
			wantStatus: exitMismatch, wantPhase: "before_restart", wantCode: "mismatch",
			wantMessage: "timestamp mismatch at row 3", beforeCounts: &drillCounts{2, 2, 2},
		},
		{
			name: "mismatch after restart", before: []drillPage{first, last}, after: []drillPage{first, shuffled},
			wantStatus: exitMismatch, wantPhase: "after_restart", wantCode: "mismatch",
			wantMessage: "timestamp mismatch at row 3", beforeCounts: &complete, afterCounts: &drillCounts{2, 2, 2},
		},
		{
			name: "expiry after restart", before: []drillPage{first, last}, after: []drillPage{first, expired},
			wantStatus: exitIncomplete, wantPhase: "after_restart", wantCode: "cursor_expired",
			wantMessage: "server walk at row 3: page 2", beforeCounts: &complete, afterCounts: &drillCounts{2, 2, 1},
		},
		{
			name: "retry exhaustion after restart", before: []drillPage{first, last}, after: []drillPage{first, busy, busy},
			wantStatus: exitIncomplete, wantPhase: "after_restart", wantCode: "retries_exhausted",
			wantMessage: "query page retries exhausted", beforeCounts: &complete, afterCounts: &drillCounts{2, 2, 1},
		},
		{
			name: "invalid cursor before restart", before: []drillPage{first, invalid},
			wantStatus: exitIncomplete, wantPhase: "before_restart", wantCode: "invalid_cursor",
			wantMessage: "invalid cursor", beforeCounts: &drillCounts{2, 2, 1},
		},
		{
			name: "all rows matched without terminal page", before: []drillPage{first, last},
			after: []drillPage{first, {cursor: a, next: c, rows: rows[3:5]}}, extra: []string{"-max-pages", "2"},
			wantStatus: exitIncomplete, wantPhase: "after_restart", wantCode: "page_budget_exhausted",
			wantMessage: "server walk at row 5: page 3", beforeCounts: &complete, afterCounts: &complete,
		},
		{
			name: "verification failure after restart", before: []drillPage{first, last}, action: "remove backup",
			wantStatus: exitVerification, wantPhase: "after_restart", wantCode: "backup_verification_failed",
			wantMessage: "backup verification failed", beforeCounts: &complete,
		},
		{
			name: "cancellation at restart boundary", before: []drillPage{first, last}, action: "cancel at prompt",
			wantStatus: exitIncomplete, wantPhase: "restart_boundary", wantCode: "canceled",
			wantMessage: "context canceled", beforeCounts: &complete,
		},
		{
			name: "deadline at restart boundary", before: []drillPage{first, last}, action: "wait at prompt",
			extra: []string{"-timeout", "2s"}, wantStatus: exitIncomplete,
			wantPhase: "restart_boundary", wantCode: "deadline_exceeded",
			wantMessage: "context deadline exceeded", beforeCounts: &complete,
		},
		{
			name: "cancellation fetching the second terminal page", before: []drillPage{first, last},
			after: []drillPage{first, last}, action: "cancel after restart",
			wantStatus: exitIncomplete, wantPhase: "after_restart", wantCode: "canceled",
			wantMessage: "context canceled", beforeCounts: &complete, afterCounts: &drillCounts{2, 2, 1},
		},
		{
			name: "deadline fetching the second terminal page", before: []drillPage{first, last},
			after: []drillPage{first, last}, action: "stall after restart", extra: []string{"-timeout", "2s"},
			wantStatus: exitIncomplete, wantPhase: "after_restart", wantCode: "deadline_exceeded",
			wantMessage: "context deadline exceeded", beforeCounts: &complete, afterCounts: &drillCounts{2, 2, 1},
		},
		{
			name: "restart prompt write fails", before: []drillPage{first, last}, action: "short prompt",
			wantStatus: exitIncomplete, wantPhase: "restart_boundary", wantCode: "incomplete",
			wantMessage: "write restart prompt: short write", beforeCounts: &complete,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := dir
			switch tc.action {
			case "missing":
				bundle = filepath.Join(t.TempDir(), "missing")
			case "remove backup":
				bundle, _, _ = makeBundle(t)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.action == "cancel before" {
				cancel()
			}
			pages := append(append([]drillPage(nil), tc.before...), tc.after...)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(requests.Add(1)) - 1
				if i >= len(pages) {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				p := pages[i]
				query := r.URL.Query()
				if r.Method != http.MethodGet || r.URL.Path != "/query" ||
					query.Get("cursor") != p.cursor || (query.Get("page") == "1") != (p.cursor == "") {
					t.Errorf("request %d = %s %s, want read-only query with cursor %q", i+1, r.Method, r.URL, p.cursor)
				}
				if tc.action == "cancel after restart" && i == len(pages)-1 {
					cancel()
					return
				}
				if tc.action == "stall after restart" && i == len(pages)-1 {
					<-r.Context().Done()
					return
				}
				if p.status != 0 {
					http.Error(w, p.body, p.status)
					return
				}
				writeDrillPage(t, w, p)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			prompts := 0
			diagnostics := promptWriter(func(p []byte) (int, error) {
				if bytes.HasPrefix(p, []byte("restart the restored server now.")) {
					prompts++
					if stdout.Len() != 0 || int(requests.Load()) != len(tc.before) {
						t.Errorf("restart prompt after %d requests with stdout %q", requests.Load(), &stdout)
					}
					switch tc.action {
					case "cancel at prompt":
						cancel()
					case "wait at prompt":
					case "short prompt":
						return 0, nil
					default:
						if tc.action == "remove backup" {
							if err := os.Rename(bundle, bundle+"-moved"); err != nil {
								t.Errorf("move backup: %v", err)
								return 0, err
							}
						}
						if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
							t.Errorf("acknowledge restart: %v", err)
							return 0, err
						}
					}
				}
				return stderr.Write(p)
			})
			args := append(compareArgs(bundle, server.URL), "-json", "-max-retries", "1", "-retry-delay", "0")
			args[0] = "drill"
			status := run(ctx, append(args, tc.extra...), &stdout, diagnostics)
			if status != tc.wantStatus || int(requests.Load()) != len(pages) {
				t.Fatalf("exit %d, requests %d/%d, stdout %q, stderr %q", status, requests.Load(), len(pages), &stdout, &stderr)
			}
			got := decodeDrillResult(t, stdout.Bytes())
			want := drillResult{
				SchemaVersion: 1, ExitStatus: status, Parity: status == exitOK,
				BeforeRestart: drillPhaseResult{Status: "failed", Counts: tc.beforeCounts},
				AfterRestart:  drillPhaseResult{Status: "not_run", Counts: tc.afterCounts},
			}
			if tc.beforeCounts != nil {
				want.RecoveryPoint = &recoveryPoint{
					Generation: fmt.Sprintf("%x", point.Generation), CoveredOffset: point.CoveredOffset,
					NextOrdinal: point.NextOrdinal, CommittedEnd: point.CommittedEnd,
				}
			}
			wantPrompts := 0
			if tc.wantPhase != "before_restart" {
				want.BeforeRestart.Status = "matched"
				wantPrompts = 1
			}
			switch tc.wantPhase {
			case "":
				want.AfterRestart.Status = "matched"
			case "after_restart":
				want.AfterRestart.Status = "failed"
			}
			if status != exitOK {
				if got.Diagnostic == nil || !strings.Contains(got.Diagnostic.Message, tc.wantMessage) ||
					!strings.Contains(stderr.String(), "bog-verify: "+got.Diagnostic.Message+"\n") {
					t.Fatalf("diagnostic %#v, stderr %q, want detail %q", got.Diagnostic, &stderr, tc.wantMessage)
				}
				want.Diagnostic = &drillDiagnostic{tc.wantPhase, tc.wantCode, got.Diagnostic.Message}
			}
			if !reflect.DeepEqual(got, want) {
				wantJSON, err := json.Marshal(want)
				check(t, err)
				t.Errorf("result %s, want %s", &stdout, wantJSON)
			}
			if prompts != wantPrompts || strings.Contains(stderr.String(), "matched:") {
				t.Errorf("prompts %d, want %d, stderr %q", prompts, wantPrompts, &stderr)
			}
		})
	}
}

func TestDrillJSONSchema(t *testing.T) {
	result := drillResult{
		SchemaVersion: drillSchemaVersion,
		Parity:        true,
		RecoveryPoint: &recoveryPoint{
			Generation: strings.Repeat("ab", 32), CoveredOffset: 120,
			NextOrdinal: 1<<53 + 1, CommittedEnd: 420,
		},
		BeforeRestart: drillPhaseResult{Status: phaseMatched, Counts: &drillCounts{0, 1, 1}},
		AfterRestart:  drillPhaseResult{Status: phaseMatched, Counts: &drillCounts{0, 1, 2}},
	}
	const schema = `{
		"schema_version": 1,
		"parity": true,
		"exit_status": 0,
		"recovery_point": {
			"generation": "abababababababababababababababababababababababababababababababab",
			"covered_offset": 120,
			"next_ordinal": 9007199254740993,
			"committed_end": 420
		},
		"before_restart": {"status": "matched", "counts": {"rows": 0, "reference_pages": 1, "server_pages": 1}},
		"after_restart": {"status": "matched", "counts": {"rows": 0, "reference_pages": 1, "server_pages": 2}},
		"diagnostic": null
	}`
	var out bytes.Buffer
	check(t, writeDrillResult(&out, result))
	decode := func(r io.Reader) map[string]any {
		var fields map[string]any
		decoder := json.NewDecoder(r)
		decoder.UseNumber()
		check(t, decoder.Decode(&fields))
		return fields
	}
	if got, want := decode(&out), decode(strings.NewReader(schema)); !reflect.DeepEqual(got, want) {
		t.Fatalf("schema result %#v, want %#v", got, want)
	}
}

func decodeDrillResult(t *testing.T, data []byte) drillResult {
	t.Helper()
	if bytes.Count(data, []byte("\n")) != 1 || !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatalf("want a single newline-terminated result, got %q", data)
	}
	var result drillResult
	check(t, json.Unmarshal(data, &result))
	return result
}

func TestDrillJSONWriteFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"short write", nil, io.ErrShortWrite.Error()},
		{"write error", errors.New("stdout unavailable"), "stdout unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			calls := 0
			out := promptWriter(func([]byte) (int, error) {
				calls++
				return 0, tc.err
			})
			args := append(compareArgs(filepath.Join(t.TempDir(), "missing"), "http://127.0.0.1:1"), "-json")
			args[0] = "drill"
			status := run(t.Context(), args, out, &stderr)
			if status != exitIncomplete || calls != 1 || !strings.Contains(stderr.String(), "write result: "+tc.want) ||
				!strings.Contains(stderr.String(), "backup verification failed") {
				t.Fatalf("exit %d, %d writes, stderr %q", status, calls, &stderr)
			}
		})
	}
}
