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
	cutoff                        int
	query, appendDone, overlapped chan struct{}
	running, resumed, exhausted   bool
}

func TestMixedLoadKeepsOriginalCutoffs(t *testing.T) {
	for _, failure := range []string{"", "bad_publication", "timeout"} {
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
	var op *scriptedReplacement
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		switch req.URL.Path {
		case "/compact":
			cutoff, err := strconv.Atoi(q.Get("cutoff"))
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			state := "complete"
			switch {
			case q.Get("action") == "start" && (cutoff == cluster.cutoff || cutoff == scatter.cutoff):
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
					close(op.overlapped)
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
				if op.exhausted {
					reply.Metrics["retired_generation_pins"] = 0
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
			if cursor := q.Get("cursor"); cursor != "" {
				var err error
				offset, err = strconv.Atoi(cursor)
				if err != nil {
					t.Error(err)
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
				snapshot = nil
				for _, r := range appended {
					if !r.TS.Before(start) && r.TS.Before(end) && strings.Contains(r.Line, q.Get("contains")) {
						snapshot = append(snapshot, r)
					}
				}
				slices.SortStableFunc(snapshot, func(a, b row) int { return a.TS.Compare(b.TS) })
			}
			if offset < 0 || offset > len(snapshot) {
				mu.Unlock()
				t.Error("query offset outside snapshot")
				w.WriteHeader(400)
				return
			}
			stop := min(offset+limit, len(snapshot))
			page := slices.Clone(snapshot[offset:stop])
			if offset > 0 && active != nil && !active.running && covered == active.cutoff {
				active.resumed, active.exhausted = true, stop == len(snapshot)
			}
			if stop < len(snapshot) {
				w.Header().Set("Bog-Next-Cursor", fmt.Sprint(stop))
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
	r := result{Diagnostics: &capacityDiagnostics{Queries: make(map[string]*queryPhase)}}
	s.queryPhases = r.Diagnostics.Queries
	err := f.load(ctx, s, options{mode: "live", control: "/compact"}, &r, t.TempDir())
	clustered, scattered := r.Diagnostics.Backfills[0], r.Diagnostics.Backfills[1]
	if clustered.Status != "pass" || clustered.PinDrain.Status != "observed" || !clustered.FreshVisible || !clustered.HeldExhausted {
		t.Fatalf("incomplete clustered evidence: %+v (load error %v)", clustered, err)
	}
	if failure != "" {
		if err == nil || scattered.Status != "fail" || scattered.Publication != nil ||
			scattered.Samples[len(scattered.Samples)-1].Event != "phase_failure" ||
			!slices.ContainsFunc(scattered.Samples, func(s phaseSample) bool { return s.Event == "overlap" }) {
			t.Fatalf("expected partial failed evidence: %+v, err %v", scattered, err)
		}
		if failure == "timeout" && !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatal("script did not cancel the operation")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if scattered.Status != "pass" || scattered.PinDrain.Status != "observed" || !scattered.FreshVisible || !scattered.HeldExhausted {
		t.Fatalf("incomplete scattered evidence: %+v", scattered)
	}
	mu.Lock()
	resumed := cluster.resumed && scatter.resumed
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
