package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	terminalCursor                     string
}

func TestMixedLoadKeepsOriginalCutoffs(t *testing.T) {
	for _, failure := range []string{"", "bad_publication", "timeout", "missing_held", "zero_held", "missing_drain", "bad_retry", "retire_failed", "bad_fresh"} {
		name := failure
		if name == "" {
			name = "success"
		}
		t.Run(name, func(t *testing.T) { testMixedLoad(t, failure) })
	}
}

func testMixedLoad(t *testing.T, failure string) {
	f := fixture{2048, 32, 64, 8, 1024, true}
	cluster := &scriptedReplacement{cutoff: f.SeedRows - f.BatchRows}
	scatter := &scriptedReplacement{cutoff: f.SeedRows + f.BatchRows}
	for _, op := range []*scriptedReplacement{cluster, scatter} {
		op.query, op.appendDone, op.overlapped = make(chan struct{}), make(chan struct{}), make(chan struct{})
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	var mu sync.Mutex
	var appended, snapshot []row
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
				}
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
				if op.retired {
					reply.Metrics["retired_generation_pins"] = 0
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
				snapshot = nil
				for _, r := range appended {
					if !r.TS.Before(start) && r.TS.Before(end) && strings.Contains(r.Line, q.Get("contains")) {
						snapshot = append(snapshot, r)
					}
				}
				slices.SortStableFunc(snapshot, func(a, b row) int { return a.TS.Compare(b.TS) })
			} else if cursorCapture != capture {
				t.Error("continuation reused the wrong capture")
			}
			if offset < 0 || offset > len(snapshot) {
				mu.Unlock()
				t.Error("query offset outside snapshot")
				w.WriteHeader(400)
				return
			}
			stop := min(offset+limit, len(snapshot))
			page := slices.Clone(snapshot[offset:stop])
			if offset > 0 && active != nil && !active.running && covered == active.cutoff && !active.retired {
				active.resumed = true
				if stop == len(snapshot) {
					if active.terminalCursor == "" {
						active.terminalCursor = cursor
					} else if active.terminalCursor == cursor {
						active.retried = true
						if active == scatter && failure == "bad_retry" {
							page[0].Line = "wrong row"
						}
					}
				}
			}
			if failure == "bad_fresh" && active == scatter && active.retired && q.Get("contains") == "" && offset == 0 {
				page[0].Line = "wrong fresh row"
			}
			if stop < len(snapshot) {
				w.Header().Set("Bog-Next-Cursor", fmt.Sprintf("%d:%d", capture, stop))
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
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
	s.client.Timeout = 2 * time.Second
	s.readMemoryCurrent = func(context.Context) (uint64, error) { return f.MemoryBytes / 2, nil }
	r := result{Diagnostics: &capacityDiagnostics{Queries: make(map[string]*queryPhase)}}
	s.queryPhases = r.Diagnostics.Queries
	err := f.load(ctx, s, options{mode: "live", control: "/compact"}, &r, t.TempDir())
	clustered, scattered := r.Diagnostics.Backfills[0], r.Diagnostics.Backfills[1]
	if clustered.Status != "pass" || clustered.PinDrain.Status != "observed" || !clustered.FreshVisible || !clustered.HeldExhausted {
		t.Fatalf("incomplete clustered evidence: %+v (load error %v)", clustered, err)
	}
	if failure != "" {
		earlyFailure := failure == "bad_publication" || failure == "timeout"
		if err == nil || scattered.Status != "fail" || (scattered.Publication == nil) != earlyFailure ||
			scattered.Samples[len(scattered.Samples)-1].Event != "phase_failure" ||
			!slices.ContainsFunc(scattered.Samples, func(s phaseSample) bool { return s.Event == "overlap" }) {
			t.Fatalf("expected partial failed evidence: %+v, err %v", scattered, err)
		}
		if failure == "timeout" && !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatal("script did not cancel the operation")
		}
		wantPhase := map[string]string{
			"missing_held": "capture_retirement", "zero_held": "capture_retirement", "missing_drain": "pin_drain",
			"bad_retry": "terminal_retry", "retire_failed": "capture_retirement", "bad_fresh": "fresh_walk",
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
		!scattered.HeldExhausted || !scattered.TerminalRetried || !scattered.CaptureRetired ||
		!scattered.FreshExact || scattered.FreshRows != f.SeedRows+f.TailRows {
		t.Fatalf("incomplete scattered evidence: %+v", scattered)
	}
	for i, want := range newBackfillTimelines(f) {
		timeline := r.Diagnostics.Backfills[i]
		if err := timeline.validateCutover(want, f); err != nil {
			t.Fatalf("%s timeline fails cutover validation: %v", want.Pattern, err)
		}
		if err := timeline.validateLiveMemory(f.MemoryBytes); err != nil {
			t.Fatalf("%s timeline fails memory validation: %v", want.Pattern, err)
		}
	}
	mu.Lock()
	resumed := cluster.resumed && scatter.resumed && cluster.retried && scatter.retried && cluster.retired && scatter.retired
	mu.Unlock()
	if !resumed {
		t.Fatal("both captured cursors must survive publication")
	}
	if n, err := f.recover(t.Context(), s); err != nil || n != f.SeedRows+f.TailRows {
		t.Fatalf("recover: %d %v", n, err)
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
