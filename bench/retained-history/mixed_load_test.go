package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type mixedFixtureCapture struct {
	rows       []row
	generation uint64
	retired    bool
	active     int
}

type mixedFixtureReader struct {
	capture *mixedFixtureCapture
	release chan struct{}
}

type mixedHTTPFixture struct {
	t        *testing.T
	f        fixture
	fault    string
	probe    service
	cancel   context.CancelFunc
	slowDone chan struct{}

	mu         sync.Mutex
	rows       []row
	generation uint64
	covered    int
	target     int
	running    bool
	mixed      bool
	afterAck   bool
	restarted  bool
	issued     int
	slowStarts int
	active     map[string]mixedFixtureReader
	captures   map[int]*mixedFixtureCapture
}

func newMixedHTTPFixture(t *testing.T, fault string) *mixedHTTPFixture {
	x := &mixedHTTPFixture{
		t: t, f: fixture{SeedRows: 64, TailRows: 12, BatchRows: 4, LineBytes: 64, MemoryBytes: 128 << 20},
		fault: fault, active: make(map[string]mixedFixtureReader), captures: make(map[int]*mixedFixtureCapture),
		slowDone: make(chan struct{}, 2),
	}
	server := httptest.NewServer(http.HandlerFunc(x.serve))
	t.Cleanup(server.Close)
	x.probe = newService(server.URL)
	t.Cleanup(x.probe.client.CloseIdleConnections)
	return x
}

func (x *mixedHTTPFixture) Service() service { return x.probe }

func (x *mixedHTTPFixture) Restart(ctx context.Context) (service, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.active) != 0 || len(x.captures) != 0 {
		return service{}, errors.New("restart began with outstanding captures or readers")
	}
	if x.fault == "restart_failure" {
		return service{}, errors.New("restart fixture startup failed")
	}
	x.restarted = true
	server := httptest.NewServer(http.HandlerFunc(x.serve))
	x.t.Cleanup(server.Close)
	s := newService(server.URL)
	x.t.Cleanup(s.client.CloseIdleConnections)
	return s, ctx.Err()
}

func (x *mixedHTTPFixture) Sample(ctx context.Context) (mixedLoadSample, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.fault == "sample_error" && x.running && x.mixed {
		return mixedLoadSample{}, errors.New("sample fixture error")
	}
	base := uint64(9)
	rss := uint64(30 << 20)
	if x.fault == "memory_limit" && x.running && x.mixed {
		rss = 129 << 20
	}
	if x.fault == "descriptor_leak" && x.mixed {
		base++
	}
	return mixedLoadSample{RSSBytes: rss, CgroupPeak: new(uint64(58 << 20)), Descriptors: base + uint64(len(x.active))}, ctx.Err()
}

func (x *mixedHTTPFixture) Stalled(ctx context.Context, conn *net.TCPConn) (bool, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	reader, ok := x.active[conn.LocalAddr().String()]
	if x.fault == "lost_stall" && x.afterAck {
		return false, nil
	}
	if ok && x.mixed && !x.running && reader.release != nil {
		close(reader.release)
		reader.release = nil
		x.active[conn.LocalAddr().String()] = reader
	}
	return ok, ctx.Err()
}

func (x *mixedHTTPFixture) serve(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/ingest":
		x.ingest(w, req)
	case "/compact":
		x.control(w, req)
	case "/query":
		x.query(w, req)
	default:
		http.NotFound(w, req)
	}
}

func (x *mixedHTTPFixture) ingest(w http.ResponseWriter, req *http.Request) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.fault == "ingest_refused" && x.mixed {
		http.Error(w, "ingest refused", http.StatusServiceUnavailable)
		return
	}
	var batch []row
	d := json.NewDecoder(req.Body)
	for {
		var next row
		err := d.Decode(&next)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			x.t.Errorf("decode ingest: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		batch = append(batch, next)
	}
	x.rows = append(x.rows, batch...)
	w.WriteHeader(http.StatusNoContent)
}

func (x *mixedHTTPFixture) control(w http.ResponseWriter, req *http.Request) {
	x.mu.Lock()
	defer x.mu.Unlock()
	params := req.URL.Query()
	cutoff, _ := strconv.Atoi(params.Get("cutoff"))
	switch params.Get("action") {
	case "retire":
		id := mixedFixtureTokenID(params.Get("cursor"))
		if x.fault == "cleanup_failure" {
			http.Error(w, "cannot retire", http.StatusInternalServerError)
			return
		}
		if c := x.captures[id]; c != nil {
			c.retired = true
			if c.active == 0 {
				delete(x.captures, id)
			}
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case "start":
		x.target = cutoff
		x.running = cutoff > x.f.SeedRows
		if x.running {
			x.mixed = true
		} else {
			x.generation++
			x.covered = cutoff
		}
	case "status":
		if x.running && len(x.rows) > x.target {
			switch {
			case x.fault == "cancel_overlap":
				x.cancel()
			case len(x.rows) == x.f.SeedRows+x.f.TailRows || x.fault == "early_publication":
				x.running = false
				x.covered = x.target
				x.generation++
			default:
				x.afterAck = true
			}
		}
	default:
		x.t.Errorf("unexpected control request: %s", req.URL)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	status := compaction{x.generation, x.covered, len(x.rows), "complete"}
	metrics := runtimeSample{
		Reserved: new(uint64(0)), Used: new(uint64(0)), RetiredPins: new(uint64(0)), PendingCompactions: new(uint64(0)),
	}
	if x.running {
		status.State = "running"
		metrics.PendingCompactions = new(uint64(1))
		metrics.Reserved = new(uint64(4096))
	}
	if x.restarted && x.fault == "restart_checkpoint" {
		status.Generation++
	}
	if x.generation == 2 {
		for _, c := range x.captures {
			if c.generation == 1 {
				(*metrics.RetiredPins)++
			}
		}
		metrics.ReplayTailBytes = new(uint64(2048))
		switch {
		case *metrics.RetiredPins > 0:
			metrics.PredecessorPinAge, metrics.PredecessorPinGeneration = new(uint64(100)), new(uint64(1))
			if x.fault == "early_reclamation" {
				metrics.ReclaimedBytes, metrics.ReclaimedPredecessor, metrics.ReclaimedReplacement = new(uint64(4096)), new(uint64(1)), new(uint64(2))
			}
		default:
			metrics.ReclaimedBytes, metrics.ReclaimedPredecessor, metrics.ReclaimedReplacement = new(uint64(4096)), new(uint64(1)), new(uint64(2))
			if x.fault == "reservation_leak" {
				metrics.Reserved = new(uint64(1))
			}
		}
		if x.fault == "missing_pins" {
			metrics.RetiredPins = nil
		}
	}
	if err := json.NewEncoder(w).Encode(struct {
		compaction
		Metrics runtimeSample `json:"metrics"`
	}{status, metrics}); err != nil {
		x.writeErr(req, err)
	}
}

func mixedFixtureTokenID(token string) int {
	id, _, _ := strings.Cut(token, ":")
	result, _ := strconv.Atoi(id)
	return result
}

func (x *mixedHTTPFixture) query(w http.ResponseWriter, req *http.Request) {
	params := req.URL.Query()
	filter := params.Get("contains")
	limit, _ := strconv.Atoi(params.Get("limit"))
	id, offset := 0, 0
	token := params.Get("cursor")
	x.mu.Lock()
	c := x.captures[mixedFixtureTokenID(token)]
	if token == "" {
		if x.mixed && x.running && filter != "" {
			if x.fault == "malformed_rejection" {
				w.Header().Set("Trailer", "Bog-Next-Cursor")
			}
			http.Error(w, "query capacity exhausted", http.StatusServiceUnavailable)
			if x.fault == "malformed_rejection" {
				w.Header().Set("Bog-Next-Cursor", "invalid-trailer")
			}
			x.mu.Unlock()
			return
		}
		start, startErr := time.Parse(time.RFC3339Nano, params.Get("start"))
		end, endErr := time.Parse(time.RFC3339Nano, params.Get("end"))
		if err := errors.Join(startErr, endErr); err != nil {
			x.t.Errorf("query time bounds: %v", err)
		}
		c = &mixedFixtureCapture{generation: x.generation}
		for _, r := range x.rows {
			if !r.TS.Before(start) && r.TS.Before(end) && strings.Contains(r.Line, filter) {
				c.rows = append(c.rows, r)
			}
		}
		slices.SortStableFunc(c.rows, func(a, b row) int { return a.TS.Compare(b.TS) })
		x.issued++
		id = x.issued
	} else {
		id = mixedFixtureTokenID(token)
		_, raw, _ := strings.Cut(token, ":")
		offset, _ = strconv.Atoi(raw)
		if c == nil || c.retired {
			x.mu.Unlock()
			http.Error(w, "query capture expired", http.StatusGone)
			return
		}
	}
	slow := token != "" && filter == "" && !x.mixed && x.slowStarts < 2
	var release chan struct{}
	if slow {
		x.slowStarts++
		if x.slowStarts == 1 {
			release = make(chan struct{})
		}
		x.active[req.RemoteAddr] = mixedFixtureReader{c, release}
		c.active++
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	end := min(offset+limit, len(c.rows))
	if end < len(c.rows) {
		w.Header().Set("Bog-Next-Cursor", fmt.Sprintf("%d:%d", id, end))
		x.captures[id] = c
	}
	rows := slices.Clone(c.rows[offset:end])
	bad := (slow && x.fault == "held_mismatch") ||
		(x.restarted && x.fault == "restart_mismatch") ||
		(x.mixed && !x.running && !x.restarted && x.fault == "fresh_mismatch" && token == "" && filter == "" && c.generation == 2) ||
		(!x.mixed && token == "" && x.fault == "opening_mismatch")
	x.mu.Unlock()
	if slow {
		defer func() {
			x.mu.Lock()
			delete(x.active, req.RemoteAddr)
			c.active--
			if c.active == 0 && c.retired {
				delete(x.captures, id)
			}
			x.mu.Unlock()
			x.slowDone <- struct{}{}
		}()
		if err := http.NewResponseController(w).Flush(); err != nil {
			x.writeErr(req, err)
			return
		}
		select {
		case <-req.Context().Done():
			return
		case <-release:
		}
	}
	if bad && len(rows) != 0 {
		rows[0].Line += "-mismatch"
	}
	var body bytes.Buffer
	e := json.NewEncoder(&body)
	for _, r := range rows {
		if err := e.Encode(r); err != nil {
			x.t.Errorf("encode query: %v", err)
			return
		}
	}
	_, err := w.Write(body.Bytes())
	x.writeErr(req, err)
}

func (x *mixedHTTPFixture) writeErr(req *http.Request, err error) {
	if err != nil && req.Context().Err() == nil {
		x.t.Errorf("write fixture response: %v", err)
	}
}

func TestRunMixedLoad(t *testing.T) {
	for _, tc := range []struct {
		fault, phase, detail string
	}{
		{},
		{"opening_mismatch", "cursor_open", "row mismatch"},
		{"early_publication", "stalled_overlap", "no running maintenance"},
		{"lost_stall", "stalled_overlap", "lost backpressure"},
		{"ingest_refused", "stalled_overlap", "ingest HTTP 503"},
		{"cancel_overlap", "stalled_overlap", "context canceled"},
		{"sample_error", "stalled_overlap", "sample fixture error"},
		{"memory_limit", "stalled_overlap", "memory exceeded"},
		{"malformed_rejection", "publication", "HTTP 503"},
		{"held_mismatch", "held_response_parity", "row mismatch"},
		{"missing_pins", "cancel_and_reclaim", "missing retired pin count"},
		{"early_reclamation", "cancel_and_reclaim", "before cancellation"},
		{"reservation_leak", "cancel_and_reclaim", "reservation/descriptor cleanup"},
		{"descriptor_leak", "cancel_and_reclaim", "reservation/descriptor cleanup"},
		{"fresh_mismatch", "fresh_response_parity", "row mismatch"},
		{"restart_failure", "restart_startup", "restart fixture startup failed"},
		{"restart_checkpoint", "restart_parity", "restart checkpoint differs"},
		{"restart_mismatch", "restart_parity", "row mismatch"},
		{"cleanup_failure", "publication", "retire capture"},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			t.Parallel()
			x := newMixedHTTPFixture(t, tc.fault)
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			x.cancel = cancel
			report, err := runMixedLoad(ctx, x, x, x.f, "/compact")
			if tc.phase == "" {
				if err != nil {
					t.Fatalf("unexpected %s failure: %v", report.FailurePhase, err)
				}
				final := x.f.SeedRows + x.f.TailRows
				if report.AcknowledgedRows != final || report.FreshRows != final || report.RestartRows != final ||
					report.HeldRows != x.f.SeedRows+x.f.BatchRows {
					t.Fatalf("incorrect ledger parity: %+v", report)
				}
				o := report.Overlap
				if o == nil || o.StallsBefore != 2 || o.StallsAfter != 2 || o.Memory == nil ||
					o.AckThrough != x.f.SeedRows+2*x.f.BatchRows || o.RunningAfter.State != "running" || o.Window <= 0 {
					t.Fatalf("incomplete overlap: %+v", o)
				}
				if report.Queries["selective_under_pressure"].Admissions["capacity_rejection"] != 1 ||
					report.Queries["retired_resume"].Admissions["capture_expired"] != 1 {
					t.Fatalf("missing query rejections: %+v", report.Queries)
				}
				if report.PinsBeforeCancel == nil || *report.PinsBeforeCancel != 1 || report.CancelToPinDrain == nil ||
					report.PinDrainToReclaim == nil || report.ReclaimedBytes == nil || *report.ReclaimedBytes != 4096 ||
					report.DrainedResources == nil || *report.DrainedResources.Reserved != 0 ||
					report.DrainedDescriptors == nil || *report.DrainedDescriptors != *report.BaselineDescriptors {
					t.Fatalf("missing cleanup evidence: %+v", report)
				}
				for phase, memory := range report.Phases {
					if memory.Samples < 2 || memory.PeakRSS == 0 || memory.CgroupPeak == nil {
						t.Errorf("missing %s memory: %+v", phase, memory)
					}
				}
			} else if err == nil || report.FailurePhase != tc.phase || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("failure phase=%q err=%v, want %q containing %q", report.FailurePhase, err, tc.phase, tc.detail)
			}
			x.mu.Lock()
			started := x.slowStarts
			x.mu.Unlock()
			for range started {
				select {
				case <-x.slowDone:
				case <-time.After(time.Second):
					t.Fatal("slow response survived workload cleanup")
				}
			}
			x.mu.Lock()
			defer x.mu.Unlock()
			if len(x.active) > 0 || (tc.fault != "cleanup_failure" && len(x.captures) > 0) {
				t.Fatalf("remaining readers=%d captures=%d", len(x.active), len(x.captures))
			}
			if tc.fault == "cleanup_failure" && report.CleanupFailure == "" {
				t.Fatal("capture cleanup failure was lost")
			}
		})
	}
}

func TestMixedLoadBoundedCaptureCleanup(t *testing.T) {
	requested := make(chan struct{}, 1)
	s := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		requested <- struct{}{}
		<-req.Context().Done()
	}))
	defer s.Close()
	probe := newService(s.URL)
	defer probe.client.CloseIdleConnections()
	r := &mixedLoadRun{service: probe, control: "/compact", cursors: map[*walk]string{{}: "held"}}
	start := time.Now()
	err := r.cleanup(t.Context())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 3*time.Second {
		t.Fatalf("unbounded cleanup or wrong error after %v: %v", time.Since(start), err)
	}
	select {
	case <-requested:
	default:
		t.Fatal("cleanup returned without attempting retirement")
	}
}

func TestMixedLoadRejectionReasons(t *testing.T) {
	for _, tc := range []struct {
		body, reason string
		status       int
		trailer      bool
		refusal      bool
	}{
		{"query capacity exhausted", "capacity_rejection", 503, false, true},
		{"query work budget exhausted", "work_budget_rejection", 503, false, true},
		{"query response capacity exhausted", "response_capacity_rejection", 503, false, true},
		{"query scan capacity exhausted", "scan_capacity_rejection", 503, false, true},
		{"query capture capacity exhausted", "capture_capacity_rejection", 503, false, true},
		{"query scan capacity exhausted", "malformed_rejection", 503, true, false},
		{"query scan capacity exhausted", "http_rejection", 500, false, false},
		{"different refusal", "http_rejection", 503, false, false},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			res := &http.Response{StatusCode: tc.status}
			if tc.trailer {
				res.Trailer = http.Header{"Bog-Next-Cursor": {"token"}}
			}
			var q queryAttempt
			q.observeHeaders(res, time.Millisecond)
			mixedLoadRejection(&q, res, []byte(tc.body+"\n"))
			if q.Admission != tc.reason || mixedLoadAdmissionRefusal(q.Admission) != tc.refusal {
				t.Fatalf("got %+v, want %s with refusal=%v", q, tc.reason, tc.refusal)
			}
		})
	}
}

func TestMixedLoadZeroWindow(t *testing.T) {
	const server = "0100007F:81F0"
	const peer = "0100007F:C002"
	for _, tc := range []struct {
		name, line       string
		blocked, invalid bool
	}{
		{"exact zero window", "  3: " + server + " " + peer + " 01 00001000:00000000 04:00001100", true, false},
		{"no queued write", "  3: " + server + " " + peer + " 01 00000000:00000000 04:00001100", false, false},
		{"different timer", "  3: " + server + " " + peer + " 01 00001000:00000000 01:00001100", false, false},
		{"different peer", "  3: " + server + " 0100007F:C003 01 00001000:00000000 04:00001100", false, false},
		{"reverse endpoints", "  3: " + peer + " " + server + " 01 00001000:00000000 04:00001100", false, false},
		{"truncated entry", "  3: " + server + " " + peer + " 01", false, true},
		{"malformed queue", "  3: " + server + " " + peer + " 01 gggg:00000000 04:00001100", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocked, err := mixedLoadZeroWindow(t.Context(), tc.line, server, peer)
			if blocked != tc.blocked || (err != nil) != tc.invalid {
				t.Fatalf("blocked=%v err=%v, want blocked=%v invalid=%v", blocked, err, tc.blocked, tc.invalid)
			}
		})
	}
}

type waitingMixedLoadObserver struct {
	samples   int
	started   chan struct{}
	cancelled bool
}

func (o *waitingMixedLoadObserver) Stalled(context.Context, *net.TCPConn) (bool, error) {
	return false, errors.New("unexpected stall probe")
}

func (o *waitingMixedLoadObserver) Sample(ctx context.Context) (mixedLoadSample, error) {
	o.samples++
	if o.samples == 2 {
		close(o.started)
		<-ctx.Done()
		o.cancelled = true
		return mixedLoadSample{}, ctx.Err()
	}
	return mixedLoadSample{RSSBytes: 1024}, ctx.Err()
}

func TestMixedLoadStopsInFlightSampler(t *testing.T) {
	observer := &waitingMixedLoadObserver{started: make(chan struct{})}
	report := mixedLoadReport{Phases: make(map[string]mixedLoadMemory)}
	r := mixedLoadRun{fixture: fixture{MemoryBytes: 128 << 20}, observer: observer, report: &report}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := r.measure(ctx, "fixture", func(ctx context.Context) error {
		select {
		case <-observer.started:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil || !observer.cancelled || report.Phases["fixture"].Samples != 2 ||
		report.Phases["fixture"].CgroupPeak != nil {
		t.Fatalf("sampler did not drain with optional cgroup measurements absent: %+v, err=%v", report, err)
	}
}
