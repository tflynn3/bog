package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type scriptedReplacement struct {
	cutoff                             int
	query, appendDone, overlapped      chan struct{}
	running, resumed, retried, retired bool
	postRetirementReplies              int
	terminalCursor, terminalQuery      string
}

func TestMixedLoadKeepsOriginalCutoffs(t *testing.T) {
	for _, failure := range []string{"", "bad_publication", "timeout", "missing_held", "zero_held", "missing_drain", "bad_retry", "different_retry_bytes", "retire_failed", "bad_fresh"} {
		name := failure
		if name == "" {
			name = "success"
		}
		t.Run(name, func(t *testing.T) { testMixedLoad(t, failure, nil) })
	}
}

func TestMixedLoadCorrelatesBothWorkloads(t *testing.T) {
	testMixedLoad(t, "", func(context.Context) (uint64, error) { return 768, nil })
}

func testMixedLoad(t *testing.T, failure string, readPeak func(context.Context) (uint64, error), slowCase ...string) {
	t.Helper()
	correlated := readPeak != nil
	f := fixture{2048, 32, 64, 8, 1024, true}
	cluster := &scriptedReplacement{cutoff: f.SeedRows - f.BatchRows}
	scatter := &scriptedReplacement{cutoff: f.SeedRows + f.BatchRows}
	for _, op := range []*scriptedReplacement{cluster, scatter} {
		op.query, op.appendDone, op.overlapped = make(chan struct{}), make(chan struct{}), make(chan struct{})
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	var mu sync.Mutex
	var appended []row
	snapshots := make(map[int][]row)
	retired := make(map[int]bool)
	trial := testRestartTrial(t)
	scenario := ""
	slowEnabled := len(slowCase) != 0
	if slowEnabled {
		scenario = slowCase[0]
	}
	var livePaused, openedPaused int
	var restarting bool
	oldManifest := filepath.Join(trial.dir, manifestPrefix+strings.Repeat("a", 64))
	nextManifest := filepath.Join(trial.dir, manifestPrefix+strings.Repeat("b", 64))
	if slowEnabled {
		if err := os.WriteFile(oldManifest, []byte("predecessor"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var generation uint64
	var covered int
	var capture int
	var op *scriptedReplacement
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		switch req.URL.Path {
		case "/compact":
			if q.Get("action") == "retire" {
				mu.Lock()
				defer mu.Unlock()
				cursor := q.Get("cursor")
				if req.Method != http.MethodPost || cursor == "" || op == nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if cursor == op.terminalCursor {
					if !op.retried {
						t.Error("retired held capture before retrying its terminal page")
					}
					if failure == "retire_failed" && op == scatter {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					op.retired = true
					if slowEnabled && scenario == "early_reclamation" && op == scatter {
						if err := os.Remove(oldManifest); err != nil {
							t.Error(err)
						}
					}
				}
				var id, offset int
				if _, err := fmt.Sscanf(cursor, "%d:%d", &id, &offset); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				retired[id] = true
				w.WriteHeader(http.StatusNoContent)
				return
			}
			cutoff, err := strconv.Atoi(q.Get("cutoff"))
			action := q.Get("action")
			if err != nil || action == "start" && req.Method != http.MethodPost ||
				action == "status" && req.Method != http.MethodGet || action != "start" && action != "status" {
				t.Errorf("invalid control request %s %s: %v", req.Method, req.URL, err)
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			state := "complete"
			switch {
			case action == "start" && (cutoff == cluster.cutoff || cutoff == scatter.cutoff):
				op = cluster
				if cutoff == scatter.cutoff {
					op = scatter
				}
				op.running, state = true, "running"
			case op != nil && op.cutoff == cutoff && op.running:
				select {
				case <-op.overlapped:
					if failure == "timeout" && op == scatter {
						cancel(context.DeadlineExceeded)
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					if op == scatter && len(appended) < f.SeedRows+f.TailRows {
						state = "running"
						break
					}
					generation++
					covered, op.running = cutoff, false
					if slowEnabled && op == scatter {
						if err := os.WriteFile(nextManifest, []byte("successor"), 0600); err != nil {
							t.Error(err)
						}
					}
					if failure == "bad_publication" && op == scatter {
						covered--
					}
				default:
					select {
					case <-op.appendDone:
						close(op.overlapped)
					default:
					}
					state = "running"
				}
			case cutoff > covered:
				generation++
				covered = cutoff
			}
			reply := struct {
				compaction
				Metrics map[string]uint64 `json:"metrics"`
			}{compaction{generation, covered, len(appended), state}, map[string]uint64{"staging_used_bytes": 0}}
			if op != nil && cutoff == op.cutoff && state == "complete" {
				reply.Metrics["retired_generation_pins"] = 1
				if op.retired && (!slowEnabled || op == cluster || livePaused == 0) {
					reply.Metrics["retired_generation_pins"] = 0
					op.postRetirementReplies++
				}
				if correlated {
					reply.Metrics["replay_tail_bytes"] = uint64((len(appended) - covered) * f.LineBytes)
					if !op.retired {
						reply.Metrics["predecessor_pin_generation"] = generation - 1
						reply.Metrics["oldest_predecessor_pin_age_ns"] = 0
					} else if op == cluster || op.postRetirementReplies > 1 {
						reply.Metrics["reclaimed_predecessor_generation"] = generation - 1
						reply.Metrics["reclaimed_replacement_generation"] = generation
						reply.Metrics["reclaimed_bytes"] = 0
						if op == scatter {
							reply.Metrics["reclaimed_bytes"] = 2048
						}
					}
				}
				if op == scatter {
					switch {
					case failure == "missing_held", failure == "missing_drain" && op.retired:
						delete(reply.Metrics, "retired_generation_pins")
					case failure == "zero_held":
						reply.Metrics["retired_generation_pins"] = 0
					}
				}
			}
			if err := json.NewEncoder(w).Encode(reply); err != nil && req.Context().Err() == nil {
				t.Error(err)
			}
		case "/ingest":
			var batch []row
			d := json.NewDecoder(req.Body)
			for {
				var r row
				err := d.Decode(&r)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				batch = append(batch, r)
			}
			mu.Lock()
			ordinal, active := len(appended), op
			mu.Unlock()
			if active != nil && ordinal == active.cutoff {
				select {
				case <-active.query:
				case <-req.Context().Done():
					return
				}
			}
			if ordinal == f.SeedRows+f.TailRows-f.BatchRows {
				select {
				case <-scatter.overlapped:
				case <-req.Context().Done():
					return
				}
			}
			mu.Lock()
			appended = append(appended, batch...)
			mu.Unlock()
			if active != nil && ordinal == active.cutoff {
				close(active.appendDone)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/query":
			start, e1 := time.Parse(time.RFC3339Nano, q.Get("start"))
			end, e2 := time.Parse(time.RFC3339Nano, q.Get("end"))
			limit, e3 := strconv.Atoi(q.Get("limit"))
			offset := 0
			cursor := q.Get("cursor")
			var cursorCapture int
			if cursor != "" {
				if _, err := fmt.Sscanf(cursor, "%d:%d", &cursorCapture, &offset); err != nil {
					t.Error("invalid cursor:", err)
					w.WriteHeader(400)
					return
				}
			}
			if e1 != nil || e2 != nil || e3 != nil {
				t.Error("invalid query")
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			active := op
			if slowEnabled && scenario != "false_saturation" &&
				(livePaused == 2 || scenario == "cancel_slot_leak" && openedPaused == 2 && livePaused == 1) {
				mu.Unlock()
				http.Error(w, "query capacity exhausted", http.StatusServiceUnavailable)
				return
			}
			if slowEnabled && retired[cursorCapture] {
				mu.Unlock()
				http.Error(w, "query capture expired", http.StatusGone)
				return
			}
			overlap := offset == 1 && active != nil && active.running
			mu.Unlock()
			if overlap {
				close(active.query)
				select {
				case <-active.appendDone:
				case <-req.Context().Done():
					return
				}
			}
			mu.Lock()
			if offset == 0 {
				capture++
				cursorCapture = capture
				var snapshot []row
				for _, r := range appended {
					if !r.TS.Before(start) && r.TS.Before(end) && strings.Contains(r.Line, q.Get("contains")) {
						snapshot = append(snapshot, r)
					}
				}
				slices.SortStableFunc(snapshot, func(a, b row) int { return a.TS.Compare(b.TS) })
				snapshots[cursorCapture] = snapshot
			}
			snapshot := snapshots[cursorCapture]
			if offset < 0 || offset > len(snapshot) {
				mu.Unlock()
				t.Error("query offset outside snapshot")
				w.WriteHeader(400)
				return
			}
			stop := min(offset+limit, len(snapshot))
			page := slices.Clone(snapshot[offset:stop])
			var differentBytes bool
			if offset > 0 && active != nil && !active.running && covered == active.cutoff && !active.retired &&
				(!slowEnabled || !end.Equal(epoch.Add(17*time.Millisecond))) {
				active.resumed = true
				if stop == len(snapshot) {
					if active.terminalCursor == "" {
						active.terminalCursor, active.terminalQuery = cursor, req.URL.RawQuery
					} else if active.terminalCursor == cursor {
						if active.terminalQuery != req.URL.RawQuery {
							t.Errorf("terminal retry query = %q, want %q", req.URL.RawQuery, active.terminalQuery)
						}
						active.retried = true
						differentBytes = active == scatter && failure == "different_retry_bytes"
						if active == scatter && failure == "bad_retry" {
							page[0].Line = "wrong row"
						}
					}
				}
			}
			if failure == "bad_fresh" && active == scatter && active.retired && q.Get("contains") == "" && offset == 0 {
				page[0].Line = "wrong fresh row"
			}
			if scenario == "different_resume_bytes" && openedPaused == 2 && livePaused == 1 {
				differentBytes = true
			}
			if (scenario == "lost_duplicate" && openedPaused == 2 && livePaused == 0 && end.Equal(epoch.Add(17*time.Millisecond)) ||
				scenario == "restart_duplicate" && restarting) && len(page) > 2 {
				page[1] = page[2]
			}
			if stop < len(snapshot) {
				w.Header().Set("Bog-Next-Cursor", fmt.Sprintf("%d:%d", cursorCapture, stop))
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
			if differentBytes {
				// Whitespace changes the wire body while preserving the exact rows.
				if _, err := io.WriteString(w, " "); err != nil {
					t.Error(err)
					return
				}
			}
			enc := json.NewEncoder(w)
			for _, r := range page {
				if err := enc.Encode(r); err != nil && req.Context().Err() == nil {
					t.Error(err)
					return
				}
			}
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	s := newService(srv.URL)
	defer s.client.CloseIdleConnections()
	s.acknowledged = &trial.ledger
	s.client.Timeout = 2 * time.Second
	s.readMemoryPeak = readPeak
	s.readMemoryCurrent = func(context.Context) (uint64, error) { return f.MemoryBytes / 2, nil }
	r := result{Diagnostics: &capacityDiagnostics{Queries: make(map[string]*queryPhase)}}
	s.queryPhases = r.Diagnostics.Queries
	if slowEnabled {
		s.openPaused = func(_ context.Context, path, phase string, framing pageFraming) (pausedResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			if !strings.Contains(path, "cursor=") || framing.bytes < 64<<10 || framing.next == "" {
				return nil, errors.New("invalid paused request")
			}
			livePaused++
			openedPaused++
			return &testPausedResponse{
				read: func() (socketPressure, error) {
					if scenario == "no_socket_pressure" {
						return socketPressure{}, errors.New("simulated absent backpressure")
					}
					return socketPressure{4096}, nil
				},
				close: sync.OnceValue(func() error {
					mu.Lock()
					defer mu.Unlock()
					livePaused--
					if livePaused == 0 && openedPaused == 3 && scatter.retired && scenario != "early_reclamation" {
						return os.Remove(oldManifest)
					}
					return nil
				}),
			}, nil
		}
	}
	err := f.load(ctx, s, options{mode: "live", control: "/compact", requireCorrelation: correlated, requireSlowOverlap: slowEnabled}, &r, trial.dir)
	clustered, scattered := r.Diagnostics.Backfills[0], r.Diagnostics.Backfills[1]
	if clustered.Status != "pass" || clustered.PinDrain.Status != "observed" || !clustered.FreshVisible || !clustered.HeldExhausted || !clustered.TerminalBytesEqual {
		t.Fatalf("incomplete clustered evidence: %+v (load error %v)", clustered, err)
	}
	if slowEnabled {
		mu.Lock()
		live := livePaused
		mu.Unlock()
		if live != 0 {
			t.Fatalf("%d paused requests leaked", live)
		}
		e := r.Diagnostics.SlowResponses
		switch scenario {
		case "", "restart_duplicate":
			if err != nil {
				t.Fatalf("live workload failed in %s: %v", e.Phase, err)
			}
			if err := trial.prepare(f, r.Compaction); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			restarting = true
			mu.Unlock()
			err = e.verifyRestart(ctx, s, "/compact", f, trial)
			if scenario == "restart_duplicate" {
				assertSlowFailure(t, e, err, "restart_parity", "row mismatch")
				return
			}
			if err != nil || e.Status != "pass" || len(e.Memory) != 6 {
				t.Fatalf("incomplete slow-response evidence %+v: %v", e, err)
			}
			for name, m := range e.Memory {
				if m.Samples < 2 || m.Peak != f.MemoryBytes/2 || m.Ended <= m.Started {
					t.Errorf("incomplete memory phase %s: %+v", name, m)
				}
			}
			want := []string{
				"broad_cursor_live", "first_write_backpressured", "selective_cursor_live",
				"ingest_acknowledged_during_maintenance", "replacement_published", "parallel_write_backpressured",
				"first_write_spans_publication", "both_query_slots_occupied", "first_request_cancelled",
				"same_cursor_retry_exact", "parallel_write_survived_first_cancellation", "held_broad_exact",
				"held_selective_exact", "retirement_write_backpressured", "retired_cursor_barred_with_live_pin",
				"retired_request_cancelled", "predecessor_pins_drained", "predecessor_manifest_reclaimed",
				"fresh_exact", "fresh_selective_exact", "restart_exact", "restart_selective_exact",
			}
			if len(e.Events) != len(want) {
				t.Fatalf("got %d evidence events, want %d", len(e.Events), len(want))
			}
			for i, name := range want {
				if e.Events[i].Name != name || (i > 0 && e.Events[i].Elapsed < e.Events[i-1].Elapsed) {
					t.Errorf("event %d = %+v, want %s in order", i, e.Events[i], name)
				}
			}
			if s.queryPhases["slow_saturation"].Admissions["capacity_rejection"] != 1 ||
				s.queryPhases["slow_retired_cursor"].Admissions["retired_or_expired_capture"] != 1 {
				t.Fatal("missing measured query rejection reasons")
			}
		case "no_socket_pressure":
			assertSlowFailure(t, e, err, "replacement_overlap", "absent backpressure")
		case "false_saturation":
			assertSlowFailure(t, e, err, "replacement_overlap", "expected capacity_rejection")
		case "cancel_slot_leak":
			assertSlowFailure(t, e, err, "cancellation_and_retry", "recover query capacity")
		case "different_resume_bytes":
			assertSlowFailure(t, e, err, "cancellation_and_retry", "changed the complete body")
		case "lost_duplicate":
			assertSlowFailure(t, e, err, "held_parity", "row mismatch")
		case "early_reclamation":
			assertSlowFailure(t, e, err, "retirement_and_reclamation", "observe predecessor before cancellation")
		}
		return
	}
	if failure != "" {
		earlyFailure := failure == "bad_publication" || failure == "timeout"
		if err == nil || scattered.Status != "fail" || (scattered.Publication == nil) != earlyFailure ||
			scattered.Samples[len(scattered.Samples)-1].Event != "phase_failure" ||
			!slices.ContainsFunc(scattered.Samples, func(s phaseSample) bool { return s.Event == "overlap" }) {
			t.Fatalf("expected partial failed evidence: %+v, err %v", scattered, err)
		}
		if failure == "different_retry_bytes" {
			if !strings.Contains(err.Error(), "different NDJSON bytes") || scattered.TerminalRetried ||
				scattered.TerminalBytesEqual || scattered.CaptureRetired ||
				slices.ContainsFunc(scattered.Samples, func(s phaseSample) bool { return s.Event == "terminal_retried" }) {
				t.Fatalf("wire mismatch produced successful retry or retirement evidence: %+v, error %v", scattered, err)
			}
		}
		if failure == "timeout" && !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatal("script did not cancel the operation")
		}
		wantPhase := map[string]string{
			"missing_held": "capture_retirement", "zero_held": "capture_retirement", "missing_drain": "pin_drain",
			"bad_retry": "terminal_retry", "different_retry_bytes": "terminal_retry", "retire_failed": "capture_retirement", "bad_fresh": "fresh_walk",
		}[failure]
		if wantPhase != "" && r.Phase != wantPhase {
			t.Fatalf("failure phase = %q, want %q, error %v", r.Phase, wantPhase, err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if scattered.Status != "pass" || scattered.PinDrain.Status != "observed" || !scattered.FreshVisible ||
		!scattered.HeldExhausted || !scattered.TerminalRetried || !scattered.TerminalBytesEqual || !scattered.CaptureRetired ||
		!scattered.FreshExact || scattered.FreshRows != f.SeedRows+f.TailRows {
		t.Fatalf("incomplete scattered evidence: %+v", scattered)
	}
	for _, timeline := range r.Diagnostics.Backfills {
		if timeline.Memory == nil || timeline.Memory.Samples < 2 || timeline.Memory.Min == 0 ||
			timeline.Memory.Max > f.MemoryBytes || *timeline.Memory != timeline.summarizeMemory() {
			t.Fatalf("%s timeline lacks consistent live memory observations: %+v", timeline.Pattern, timeline.Memory)
		}
	}
	if correlated {
		for i, timeline := range r.Diagnostics.Backfills {
			proof := timeline.Correlation
			var wantCredit uint64
			if i == 1 {
				wantCredit = 2048
			}
			if proof == nil || proof.CgroupPeak == nil || proof.CgroupPeak.Bytes == nil ||
				*proof.CgroupPeak.Bytes != 768 || proof.PredecessorPinAge == nil || *proof.PredecessorPinAge != 0 ||
				proof.ReplayTailBytes == nil || *proof.ReplayTailBytes != uint64((timeline.FinalCutoff-timeline.RequestedCutoff)*f.LineBytes) ||
				proof.ReclaimedBytes == nil || *proof.ReclaimedBytes != wantCredit ||
				proof.FinalPinReleaseNS == nil || proof.ReclaimedObservedNS == nil ||
				(*proof.ReclaimedObservedNS > *proof.FinalPinReleaseNS) != (i == 1) {
				t.Fatalf("%s capacity correlation = %+v", timeline.Pattern, proof)
			}
		}
	}
	mu.Lock()
	resumed := cluster.resumed && scatter.resumed && cluster.retried && scatter.retried && cluster.retired && scatter.retired
	mu.Unlock()
	if !resumed {
		t.Fatal("both captured cursors must survive publication")
	}
	if err := trial.prepare(f, r.Compaction); err != nil {
		t.Fatal(err)
	}
	if err := trial.verify(t.Context(), s, "/compact", f); err != nil {
		t.Fatal(err)
	}
	if proof := trial.evidence.Ledger; proof.Batches != (f.SeedRows+f.TailRows)/f.BatchRows ||
		proof.MatchedSuffix != f.TailRows-f.BatchRows || proof.Status != "pass" {
		t.Fatalf("mixed load did not preserve the append ledger: %+v", proof)
	}
	if d := r.Diagnostics; d.RunningSamples < 1 || d.RunningSamples >= r.IngestSamples ||
		d.RunningIngestP99 == nil || d.OpeningCompaction.State != "running" || d.OverlapCompaction.State != "running" ||
		d.Queries["fresh_backfill"].Last.Admission != "ndjson_response" || d.Queries["restart_walk"].Attempts < 1 {
		t.Fatalf("incomplete phase diagnostics: %+v", d)
	}
	if r.IngestSamples != f.TailRows/f.BatchRows || r.QuerySamples < 2 || r.Compaction == nil ||
		r.Compaction.CoveredRows != f.SeedRows+f.BatchRows || r.Compaction.CommittedRows != f.SeedRows+f.TailRows {
		t.Fatalf("incomplete mixed evidence: %+v", r)
	}
}

type testPausedResponse struct {
	read  func() (socketPressure, error)
	close func() error
}

func (p *testPausedResponse) pressure(context.Context) (socketPressure, error) { return p.read() }
func (p *testPausedResponse) abort() error                                     { return p.close() }

func TestSlowResponseScenario(t *testing.T) {
	for _, tc := range []string{"", "no_socket_pressure", "false_saturation", "cancel_slot_leak", "different_resume_bytes", "lost_duplicate", "early_reclamation", "restart_duplicate"} {
		name := tc
		if name == "" {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testMixedLoad(t, "", nil, tc)
		})
	}
}

func assertSlowFailure(t *testing.T, e *slowResponseEvidence, err error, phase, detail string) {
	t.Helper()
	if err == nil || e == nil || e.Status != "fail" || e.Phase != phase || !strings.Contains(e.Failure, detail) {
		t.Fatalf("want failure in %s containing %q, got %+v: %v", phase, detail, e, err)
	}
}
