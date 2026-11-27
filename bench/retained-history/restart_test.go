package main

import (
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
	"sync/atomic"
	"testing"
)

func testRestartTrial(t *testing.T) *restartTrial {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	trial, err := pinRestartTrial(dir, newRestartEvidence())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := trial.handle.Close(); err != nil {
			t.Error(err)
		}
	})
	return trial
}

func TestRestartRequiresOriginalDirectoryAfterReap(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*testing.T, string, string)
		stopped    cleanupProof
		stopErr    error
	}{
		{name: "same directory", stopped: cleanupProof{CandidateReaped: true, SubtreeEmpty: true}},
		{name: "leader still alive", want: "reaped candidate", stopped: cleanupProof{SubtreeEmpty: true}},
		{name: "descendants still alive", want: "empty fixture cgroup", stopped: cleanupProof{CandidateReaped: true}},
		{name: "unclean shutdown", want: "unclean exit", stopped: cleanupProof{CandidateReaped: true, SubtreeEmpty: true},
			stopErr: errors.New("unclean exit")},
		{name: "renamed and replaced", want: "fixture directory changed", stopped: cleanupProof{CandidateReaped: true, SubtreeEmpty: true},
			change: func(t *testing.T, dir, _ string) {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "symlink to original", want: "is not a directory", stopped: cleanupProof{CandidateReaped: true, SubtreeEmpty: true},
			change: func(t *testing.T, dir, original string) {
				if err := os.Symlink(original, dir); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "missing path", want: "no such file", stopped: cleanupProof{CandidateReaped: true, SubtreeEmpty: true},
			change: func(*testing.T, string, string) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			trial := testRestartTrial(t)
			if tc.change != nil {
				original := trial.dir + "-renamed"
				if err := os.Rename(trial.dir, original); err != nil {
					t.Fatal(err)
				}
				tc.change(t, trial.dir, original)
			}
			err := trial.beforeStart(tc.stopped, tc.stopErr)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("directory check = %v, want %q", err, tc.want)
				}
				if tc.name == "renamed and replaced" {
					e := trial.evidence
					if e.BeforeRestart == nil || *e.BeforeRestart == *e.Original {
						t.Fatalf("substitution should record both identities: %+v", e)
					}
				}
				return
			}
			e := trial.evidence
			if err != nil || e.BeforeRestart == nil || *e.BeforeRestart != *e.Original ||
				!e.FirstReaped || !e.SubtreeDrained {
				t.Fatalf("directory check = %v, evidence %+v", err, e)
			}
		})
	}
}

func TestRestartLedgerRecordsOnlyAcknowledgedBatches(t *testing.T) {
	t.Parallel()
	f := fixture{64, 16, 64, 8, 1024, true}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/ingest" {
			t.Error("expected ingest POST")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if requests.Add(1) == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	s := newService(server.URL)
	defer s.client.CloseIdleConnections()
	s.acknowledged = &acknowledgedRows{}
	if err := f.appendRange(t.Context(), s, 0, 16, nil); err == nil {
		t.Fatal("expected ingest failure")
	}
	if got := s.acknowledged.batchEnds; !slices.Equal(got, []int{8}) {
		t.Fatalf("ledger after refusal = %v", got)
	}
	if err := f.appendRange(t.Context(), s, 8, 13, nil); err != nil {
		t.Fatal(err)
	}
	if got := s.acknowledged.batchEnds; !slices.Equal(got, []int{8, 13}) {
		t.Fatalf("ledger after shorter retry = %v", got)
	}
	for _, r := range [][2]int{{8, 13}, {14, 21}, {13, 13}} {
		if err := s.acknowledged.acknowledge(r[0], r[1]); err == nil || s.acknowledged.rows() != 13 {
			t.Fatalf("invalid acknowledgement %v changed ledger: %v", r, s.acknowledged.batchEnds)
		}
	}
}

func TestRestartRejectsUnprovenCutoff(t *testing.T) {
	f := fixture{64, 16, 64, 8, 1024, true}
	for _, tc := range []struct {
		name, want string
		ledgerEnds []int
		published  *compaction
	}{
		{"missing publication", "observed replacement", []int{72, 80}, nil},
		{"incomplete ledger", "79 acknowledged rows", []int{72, 79}, &compaction{7, 72, 79, "complete"}},
		{"cut within batch", "end an acknowledged batch", []int{64, 80}, &compaction{7, 72, 80, "complete"}},
		{"publication ahead of ledger", "81 committed", []int{72, 80}, &compaction{7, 72, 81, "complete"}},
		{"wrong coverage", "unexpected compaction", []int{72, 80}, &compaction{7, 73, 80, "complete"}},
		{"still running", "unexpected compaction", []int{72, 80}, &compaction{7, 72, 80, "running"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			trial := restartTrial{evidence: newRestartEvidence(), ledger: acknowledgedRows{batchEnds: tc.ledgerEnds}}
			err := trial.prepare(f, tc.published)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("prepare = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRestartWalkAgainstAcknowledgedLedger(t *testing.T) {
	f := fixture{1200, 16, 64, 8, 1024, true}
	cutoff, total := f.SeedRows+f.BatchRows, f.SeedRows+f.TailRows
	for _, tc := range []struct {
		name, want string
	}{
		{name: "exact"},
		{"missing covered row", "row mismatch"},
		{"extra duplicate", "unexpected row"},
		{"same count changed multiplicity", "row mismatch"},
		{"reversed tie", "row mismatch"},
		{"missing suffix", "row mismatch"},
		{"early termination", "did not advance or terminate"},
		{"repeated cursor", "invalid or repeated cursor"},
		{"unterminated row", "unterminated NDJSON row"},
		{"checkpoint not recovered", "recovered checkpoint: unexpected compaction"},
		{"incorrect recovered committed count", "ledger has 1216"},
		{"path removed during walk", "after restarted workload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			trial := testRestartTrial(t)
			if err := trial.beforeStart(cleanupProof{CandidateReaped: true, SubtreeEmpty: true}, nil); err != nil {
				t.Fatal(err)
			}
			for lo := 0; lo < total; lo += f.BatchRows {
				if err := trial.ledger.acknowledge(lo, lo+f.BatchRows); err != nil {
					t.Fatal(err)
				}
			}
			if err := trial.prepare(f, &compaction{15, cutoff, cutoff, "complete"}); err != nil {
				t.Fatal(err)
			}
			var rows []row
			for i := 0; i < total; i++ {
				if tc.name == "missing covered row" && i == 2 || tc.name == "missing suffix" && i >= cutoff {
					continue
				}
				rows = append(rows, f.row(i))
			}
			// Sort decoded fixture values independently of the ordinal walk.
			slices.SortStableFunc(rows, func(a, b row) int { return a.TS.Compare(b.TS) })
			switch tc.name {
			case "extra duplicate":
				rows = append(rows, rows[len(rows)-1])
			case "same count changed multiplicity":
				rows[2] = rows[1]
			case "reversed tie":
				rows[2], rows[3] = rows[3], rows[2]
			case "early termination":
				rows = rows[:1000]
			}
			var openings, continuations atomic.Int32
			recovered := compaction{15, cutoff, total, "complete"}
			if tc.name == "checkpoint not recovered" {
				recovered.Generation, recovered.CoveredRows = 0, 0
			}
			if tc.name == "incorrect recovered committed count" {
				recovered.CommittedRows = cutoff
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				q := req.URL.Query()
				switch req.URL.Path {
				case "/compact":
					if req.Method != http.MethodGet || q.Get("action") != "status" {
						t.Error("restart must only observe compaction status")
					}
					if err := json.NewEncoder(w).Encode(recovered); err != nil {
						t.Error(err)
					}
				case "/query":
					offset := 0
					if q.Get("page") == "1" && q.Get("cursor") == "" {
						openings.Add(1)
					} else if q.Get("page") == "" && q.Get("cursor") == "new-process:1000" {
						continuations.Add(1)
						offset = 1000
					} else {
						t.Errorf("unexpected query parameters: %v", q)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					limit, err := strconv.Atoi(q.Get("limit"))
					if err != nil || limit != 1000 || q.Get("contains") != "" {
						t.Errorf("unexpected full-walk query: %v", q)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					end := min(offset+limit, len(rows))
					if end < len(rows) || tc.name == "repeated cursor" {
						w.Header().Set("Bog-Next-Cursor", "new-process:1000")
					}
					w.Header().Set("Content-Type", "application/x-ndjson")
					for i := offset; i < end; i++ {
						body, err := json.Marshal(rows[i])
						if err == nil && !(tc.name == "unterminated row" && i == end-1) {
							body = append(body, '\n')
						}
						if err != nil {
							t.Error(err)
							return
						}
						if _, err := w.Write(body); err != nil {
							t.Error(err)
							return
						}
					}
					if tc.name == "path removed during walk" && offset > 0 {
						if err := os.Rename(trial.dir, trial.dir+"-moved"); err != nil {
							t.Error(err)
						}
					}
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			err := trial.verify(t.Context(), s, "/compact", f)
			e := trial.evidence
			e.finish(err)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || e.Status != "fail" ||
					!strings.Contains(e.Failure, tc.want) {
					t.Fatalf("verify = %v, want %q, evidence %+v", err, tc.want, e)
				}
				if e.Phase == "restart_walk" && (e.Ledger.Status != "fail" || e.Ledger.Exhausted || e.Ledger.Failure == "") {
					t.Fatalf("incomplete or inexact walk accepted: %+v", e.Ledger)
				}
				if tc.name == "missing suffix" && e.Ledger.MatchedSuffix != 0 {
					t.Fatalf("missing suffix was counted in evidence: %+v", e.Ledger)
				}
				return
			}
			want := restartLedgerProof{Status: "pass", Acknowledged: total, Batches: total / f.BatchRows,
				SuffixRows: total - cutoff, Matched: total, MatchedSuffix: total - cutoff, Pages: 2, Exhausted: true}
			if err != nil || e.Status != "pass" || e.Ledger != want || e.Recovered == nil || *e.Recovered != recovered ||
				e.AfterWalk == nil || *e.AfterWalk != *e.Original || openings.Load() != 1 || continuations.Load() != 1 {
				t.Fatalf("verify = %v, evidence %+v, query starts %d, resumes %d", err, e, openings.Load(), continuations.Load())
			}
			body, err := json.Marshal(result{Diagnostics: &capacityDiagnostics{Restart: e}})
			if err != nil {
				t.Fatal(err)
			}
			for _, witness := range []string{`"restart":{"status":"pass"`, `"original_directory":{"device":`,
				fmt.Sprintf(`"publication":{"generation":15,"covered_rows":%d`, cutoff),
				fmt.Sprintf(`"matched_wal_suffix_rows":%d`, total-cutoff)} {
				if !strings.Contains(string(body), witness) {
					t.Errorf("proof lacks %q in %s", witness, body)
				}
			}
		})
	}
}

func TestRestartCanceledWalkKeepsFailure(t *testing.T) {
	f := fixture{64, 16, 64, 8, 1024, true}
	trial := testRestartTrial(t)
	trial.ledger.batchEnds = []int{72, 80}
	if err := trial.prepare(f, &compaction{7, 72, 80, "complete"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s := newService("http://127.0.0.1:1")
	defer s.client.CloseIdleConnections()
	err := trial.verify(ctx, s, "/compact", f)
	if !errors.Is(err, context.Canceled) || trial.evidence.Ledger.Status != "fail" || trial.evidence.Ledger.Pages != 0 {
		t.Fatalf("canceled restart = %v, ledger %+v", err, trial.evidence.Ledger)
	}
}
