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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestTimelineLimitKeepsBoundariesAndLatestPolls(t *testing.T) {
	timeline := newBackfillTimeline("clustered", 64, 128, 192)
	timeline.add(phaseSample{Event: "phase_start"})
	for i := 1; i <= 3*timelineSampleLimit; i++ {
		timeline.add(phaseSample{Event: "status_poll", Elapsed: time.Duration(i)})
	}
	timeline.add(phaseSample{Event: "publication", Elapsed: 200})
	timeline.add(phaseSample{Event: "phase_failure", Elapsed: 201})
	samples := timeline.Samples
	if len(samples) != timelineSampleLimit || timeline.Dropped != 2*timelineSampleLimit+3 ||
		samples[0].Event != "phase_start" || samples[1].Elapsed != 1 ||
		samples[len(samples)-3].Elapsed != 3*timelineSampleLimit ||
		samples[len(samples)-2].Event != "publication" || samples[len(samples)-1].Event != "phase_failure" {
		t.Fatalf("unexpected retained samples: %d dropped, %+v", timeline.Dropped, samples)
	}
}

func TestTimelinePreservesMemoryDuringPollRotation(t *testing.T) {
	timeline := newBackfillTimeline("clustered", 64, 128, 192)
	for range timelineSampleLimit / 2 {
		timeline.add(phaseSample{Event: "status_poll"})
	}
	memory := &memoryCurrentSample{Bytes: new(uint64(42))}
	timeline.add(phaseSample{Event: "status_poll", Memory: memory})
	for range 2 * timelineSampleLimit {
		timeline.add(phaseSample{Event: "status_poll"})
	}
	timeline.add(phaseSample{Event: "publication"})
	if len(timeline.Samples) != timelineSampleLimit || timeline.Samples[timelineSampleLimit/2].Memory != memory ||
		timeline.Samples[timelineSampleLimit-1].Event != "publication" {
		t.Fatal("timeline rotation lost memory evidence or publication")
	}
}

func TestTimelineSamplesMemoryBeforeRunningStatus(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		readErr     error
	}{
		{name: "running", state: "running"},
		{name: "already published", state: "complete"},
		{name: "read failure", state: "running", readErr: errors.New("read memory.current failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodPost {
					if reads.Load() != 0 {
						t.Error("sampled memory during the start request")
					}
					if _, err := fmt.Fprint(w, `{"state":"running","generation":3,"covered_rows":64,"committed_rows":128}`); err != nil {
						t.Error(err)
					}
					return
				}
				if reads.Load() != 1 {
					t.Error("status request was sent before the memory read")
				}
				generation, covered, pending, pin := 3, 64, 1, `"`+testPinnedGeneration+`"`
				if tc.state == "complete" {
					generation, covered, pending, pin = 4, 128, 0, "null"
				}
				if _, err := fmt.Fprintf(w, `{"state":%q,"generation":%d,"covered_rows":%d,"committed_rows":192,"metrics":{"pending_compactions":%d,"oldest_pinned_generation":%s}}`, tc.state, generation, covered, pending, pin); err != nil {
					t.Error(err)
				}
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			s.readMemoryCurrent = func(context.Context) (uint64, error) {
				reads.Add(1)
				return 42, tc.readErr
			}
			dir := t.TempDir()
			if tc.state == "complete" {
				pinRecord := filepath.Join(dir, ".bog-history-pin-"+strings.Repeat("a", 32))
				if err := os.WriteFile(pinRecord, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			p := backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
				service: s, control: "/compact", dir: dir, origin: time.Now()}
			c, sent, received, err := p.request(t.Context(), "start", false)
			if err != nil {
				t.Fatal(err)
			}
			p.startSent, p.lastRunning = sent, sent
			p.record("compaction_start", sent, received, c, nil)
			if _, err := p.pollPublication(t.Context(), 3, true); err != nil {
				t.Fatal(err)
			}
			start, status := p.Samples[0], p.Samples[1]
			m := status.Memory
			if start.Memory != nil || status.StatusError != "" || reads.Load() != 1 {
				t.Fatalf("memory sampling changed status evidence: %+v", p.Samples)
			}
			if tc.state == "complete" {
				if m != nil || status.Event != "publication" {
					t.Fatalf("recorded memory for a completed operation: %+v", status)
				}
				if c := status.Candidate; c.PendingCompactions == nil || *c.PendingCompactions != 0 ||
					c.OldestPinnedGeneration != nil || c.Unavailable[oldestPinnedGenerationMetric] != "" ||
					status.Filesystem.PinRecords == nil || *status.Filesystem.PinRecords != 1 {
					t.Fatalf("lost observed idle, empty pin set, or filesystem evidence: %+v", status)
				}
				return
			}
			if c := status.Candidate; c.PendingCompactions == nil || *c.PendingCompactions != 1 ||
				c.OldestPinnedGeneration == nil || *c.OldestPinnedGeneration != testPinnedGeneration ||
				status.Filesystem.PinRecords == nil || *status.Filesystem.PinRecords != 0 {
				t.Fatalf("lost independent candidate or filesystem metrics: %+v", status)
			}
			if m == nil || m.ReadStart < start.Elapsed || m.ReadEnd < m.ReadStart || m.ReadEnd > *status.RequestSent {
				t.Fatalf("memory read was not bracketed by running status: %+v", status)
			}
			if tc.readErr != nil {
				if m.Error != tc.readErr.Error() || m.Bytes != nil {
					t.Fatalf("lost memory read failure: %+v", m)
				}
			} else if m.Error != "" || m.Bytes == nil || *m.Bytes != 42 {
				t.Fatalf("invalid memory sample: %+v", m)
			}
		})
	}
}

func TestTimelineMemoryReadStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := newService("http://unused.invalid")
	defer s.client.CloseIdleConnections()
	reads := 0
	s.readMemoryCurrent = func(ctx context.Context) (uint64, error) {
		reads++
		cancel()
		return 0, ctx.Err()
	}
	p := backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
		service: s, control: "/compact", dir: t.TempDir(), origin: time.Now(), startSent: time.Now()}
	for range 2 {
		if _, _, _, err := p.request(ctx, "status", false); !errors.Is(err, context.Canceled) {
			t.Fatalf("status error = %v, want cancellation", err)
		}
	}
	if reads != 1 {
		t.Fatalf("memory read %d times across cancellation, want 1", reads)
	}
}

func TestTimelineMemoryReadCadenceAndLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := fmt.Fprint(w, `{"state":"running","generation":3,"covered_rows":64,"committed_rows":192}`); err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()
	s := newService(srv.URL)
	defer s.client.CloseIdleConnections()
	reads := 0
	s.readMemoryCurrent = func(context.Context) (uint64, error) {
		reads++
		return 42, nil
	}
	p := backfillProbe{backfillTimeline: newBackfillTimeline("scattered", 64, 128, 192),
		service: s, control: "/compact", dir: t.TempDir(), origin: time.Now()}
	checkStatus := func(wantSample bool) {
		t.Helper()
		c, _, _, err := p.request(t.Context(), "status", false)
		if err != nil || (c.memory != nil) != wantSample {
			t.Fatalf("status memory = %+v, error = %v, want sample = %t", c.memory, err, wantSample)
		}
	}
	checkStatus(false) // capability probe before any compaction start
	p.startSent = time.Now()
	for i := range liveMemorySampleLimit + 1 {
		p.lastPoll = time.Now().Add(-timelineSampleInterval)
		checkStatus(i < liveMemorySampleLimit)
		p.lastPoll = time.Now()
		checkStatus(false)
	}
	if reads != liveMemorySampleLimit {
		t.Fatalf("got %d reads, want %d", reads, liveMemorySampleLimit)
	}
}

func TestFilesystemTimelineSealsIndependentObservations(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1700000000, 0)
	if err := os.WriteFile(filepath.Join(dir, ".bog-checkpoint-stage"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	pin := filepath.Join(dir, ".bog-history-pin-"+strings.Repeat("a", 32))
	if err := os.WriteFile(pin, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(pin, now.Add(-time.Second), now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	sample, err := observeStorage(dir, now)
	if err == nil || sample.StagingBytes != nil || sample.Unavailable["checkpoint_staging_apparent_bytes"] == "" ||
		sample.PinRecords == nil || *sample.PinRecords != 1 || sample.OldestRecord == nil || *sample.OldestRecord != time.Second {
		t.Fatalf("invalid independent filesystem evidence: %+v, %v", sample, err)
	}
	for _, path := range []string{pin, filepath.Join(dir, ".bog-checkpoint-stage")} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	sample, err = observeStorage(dir, now)
	if err != nil || sample.StagingBytes == nil || *sample.StagingBytes != 0 ||
		sample.PinRecords == nil || *sample.PinRecords != 0 || sample.OldestRecord != nil ||
		sample.Unavailable["oldest_pin_record_mtime_age_ns"] == "" {
		t.Fatalf("expected observed empty directories: %+v, %v", sample, err)
	}
}

func TestCompactionProtocolAndPartialTimeline(t *testing.T) {
	for _, tc := range []struct {
		name, last string
		want       string
	}{
		{"missing generation", `{"state":"complete","covered_rows":128,"committed_rows":192}`, "invalid compaction"},
		{"regressed generation", `{"state":"complete","generation":3,"covered_rows":128,"committed_rows":192,"metrics":{"pending_compactions":1,"oldest_pinned_generation":"` + testPinnedGeneration + `"}}`, "unexpected compaction cutoff or generation"},
		{"wrong cutoff", `{"state":"complete","generation":4,"covered_rows":129,"committed_rows":192}`, "unexpected compaction cutoff or generation"},
		{"oversized response", strings.Repeat("x", 64<<10+1), "response exceeds"},
		{"timeout", "", "deadline exceeded"},
		{"complete", `{"state":"complete","generation":4,"covered_rows":128,"committed_rows":192,"metrics":{"retired_generation_pins":1}}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statusStarted := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.name == "timeout" {
					close(statusStarted)
					<-r.Context().Done()
					return
				}
				if _, err := fmt.Fprint(w, tc.last); err != nil && tc.name != "oversized response" {
					t.Error(err)
				}
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			ctx := t.Context()
			if tc.name == "timeout" {
				s.client.Timeout = 150 * time.Millisecond
			}
			p := backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
				service: s, control: "/compact", dir: t.TempDir(), origin: time.Now()}
			p.startSent = p.origin
			p.lastRunning = p.origin.Add(time.Nanosecond)
			p.boundary("overlap")
			_, err := p.waitPublication(ctx, 3)
			if tc.want == "" {
				if err != nil || p.Publication == nil || p.Publication.Generation != 4 ||
					p.Publication.LowerBound <= 0 || p.Publication.UpperBound < p.Publication.LowerBound ||
					p.PinDrain.Held == nil || *p.PinDrain.Held != 1 {
					t.Fatalf("invalid completion: %+v, %v", p.Publication, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) || p.Publication != nil ||
				len(p.Samples) != 2 || p.Samples[0].Event != "overlap" || p.Samples[1].StatusError == "" {
				t.Fatalf("unexpected partial evidence: %+v, %v", p.Samples, err)
			}
			for _, sample := range p.Samples {
				for _, key := range []string{pendingCompactionsMetric, oldestPinnedGenerationMetric} {
					if !strings.HasPrefix(sample.Candidate.Unavailable[key], "compaction status unavailable: ") {
						t.Fatalf("%s supplied evidence without a valid probe: %+v", key, sample)
					}
				}
				if sample.Candidate.PendingCompactions != nil || sample.Candidate.OldestPinnedGeneration != nil ||
					sample.Filesystem.PinRecords == nil || *sample.Filesystem.PinRecords != 0 {
					t.Fatalf("failed candidate probe changed independent observations: %+v", sample)
				}
			}
			if tc.name == "timeout" {
				select {
				case <-statusStarted:
				default:
					t.Fatal("request never reached candidate")
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost deadline cause: %v", err)
				}
			}

			// Atomic report serialization retains the partial timeline inside
			// the same candidate-identified envelope used by the release gate.
			out := filepath.Join(t.TempDir(), "capacity.json")
			r := report{1, strings.Repeat("a", 40), strings.Repeat("b", 64), "linux-release-v1", "live",
				map[string]outcome{capacityCase: {Status: "fail", Evidence: []result{{
					Diagnostics: &capacityDiagnostics{Backfills: []*backfillTimeline{p.backfillTimeline}},
				}}}}}
			if err := writeReport(out, r); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			var roundtrip report
			if err := json.Unmarshal(raw, &roundtrip); err != nil || !reflect.DeepEqual(roundtrip, r) {
				t.Fatalf("lost failed evidence on disk: %v\n%s", err, raw)
			}
		})
	}
}

func TestLiveFixtureBackfillPlacement(t *testing.T) {
	for _, f := range fixtures("live") {
		timelines := newBackfillTimelines(f)
		cluster, scatter := timelines[0], timelines[1]
		if f.slot(cluster.HeldCutoff)-f.slot(cluster.FinalCutoff-1) > f.BatchRows {
			t.Fatal("clustered rows left the narrow backfill band")
		}
		for ordinal := scatter.HeldCutoff; ordinal < scatter.FinalCutoff; ordinal += 8 {
			lo, hi := f.slot(ordinal), f.slot(ordinal+4)
			if lo < 1 || hi >= f.SeedRows/4 || hi-lo < f.SeedRows/9 {
				t.Fatalf("scattered rows do not alternate across the broad window: %d, %d", lo, hi)
			}
		}
	}
}

func TestPinDrainStatusTransitions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		held      *uint64
		counts    []string
		want      string
		wantError bool
	}{
		{"positive then drained", new(uint64(1)), []string{"2", "0"}, "observed", false},
		{"zero without held sample", nil, nil, "unavailable", true},
		{"zero held sample", new(uint64(0)), nil, "unavailable", true},
		{"counter unavailable", new(uint64(1)), []string{"null"}, "pending", true},
		{"gap before zero", new(uint64(1)), []string{"2", "null", "0"}, "observed", false},
		{"still pinned", new(uint64(1)), []string{"1"}, "pending", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(calls.Add(1)) - 1
				if len(tc.counts) == 0 {
					t.Error("unexpected pin status request without positive held pin evidence")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				count := tc.counts[min(i, len(tc.counts)-1)]
				// The oldest live pin can be on another generation after the
				// predecessor drains. Only its own count establishes drain.
				pin := `"` + testPinnedGeneration + `"`
				if count == "null" {
					pin = "null"
				}
				_, err := fmt.Fprintf(w, `{"state":"complete","generation":4,"covered_rows":128,"committed_rows":192,"metrics":{"retired_generation_pins":%s,"oldest_pinned_generation":%s}}`, count, pin)
				if err != nil && r.Context().Err() == nil {
					t.Error(err)
				}
			}))
			defer srv.Close()
			s := newService(srv.URL)
			defer s.client.CloseIdleConnections()
			p := backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
				service: s, control: "/compact", dir: t.TempDir(), origin: time.Now()}
			p.Publication = &publicationProof{Generation: 4}
			p.PinDrain.Held = tc.held
			limit := time.Second
			if tc.wantError {
				limit = 75 * time.Millisecond
			}
			retiredAt := time.Now()
			err := p.observeDrain(t.Context(), retiredAt, limit)
			if (err != nil) != tc.wantError || p.PinDrain.Status != tc.want ||
				len(p.Samples) < len(tc.counts) ||
				(len(p.Samples) > 0 && p.Samples[0].Event != "after_retirement") {
				t.Fatalf("drain returned %v with proof %+v", err, p.PinDrain)
			}
		})
	}
}

func TestPinDrainBudgetStartsAtCaptureRetirement(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s := newService(srv.URL)
	defer s.client.CloseIdleConnections()
	p := backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
		service: s, control: "/compact", dir: t.TempDir(), origin: time.Now().Add(-2 * drainBudget)}
	p.Publication = &publicationProof{Generation: 4}
	p.PinDrain = pinDrainProof{Status: "pending", Held: new(uint64(1))}
	// Model time spent recording diagnostics between retirement and polling.
	retiredAt := time.Now().Add(-drainBudget)
	p.record("capture_retired", time.Time{}, retiredAt, compactionReply{}, nil)
	err := p.observeDrain(t.Context(), retiredAt, drainBudget)
	if err == nil || !strings.Contains(err.Error(), "predecessor pin drain unconfirmed") {
		t.Fatalf("observeDrain error = %v, want expired drain budget", err)
	}
	if calls.Load() != 0 || len(p.Samples) != 1 || p.PinDrain.Status != "pending" || p.PinDrain.After != nil {
		t.Fatalf("expired drain issued %d requests, recorded %d samples, proof = %+v", calls.Load(), len(p.Samples), p.PinDrain)
	}
}

type drainTransport func(*http.Request) (*http.Response, error)

func (f drainTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestPinDrainRejectsLateZeroReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Leave less time than the per-request timeout so the retirement
		// deadline must be propagated to the request.
		retiredAt := time.Now().Add(-drainBudget + diagnosticRequestLimit/2)
		deadline := retiredAt.Add(drainBudget)
		s := newService("http://candidate.test")
		s.client.Transport = drainTransport(func(req *http.Request) (*http.Response, error) {
			if got, ok := req.Context().Deadline(); !ok || !got.Equal(deadline) {
				t.Errorf("request deadline = %v, want %v", got, deadline)
			}
			time.Sleep(time.Until(deadline) + time.Nanosecond)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"state":"complete","generation":4,"covered_rows":128,"committed_rows":192,"metrics":{"retired_generation_pins":0}}`)),
			}, nil
		})
		p := backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
			service: s, control: "/compact", origin: retiredAt.Add(-time.Second)}
		p.Publication = &publicationProof{Generation: 4}
		p.PinDrain.Held = new(uint64(1))
		err := p.observeDrain(t.Context(), retiredAt, drainBudget)
		if err == nil || !strings.Contains(err.Error(), "pin observation exceeded the drain budget") {
			t.Fatalf("observeDrain error = %v, want late observation error", err)
		}
		if len(p.Samples) != 1 || p.Samples[0].Elapsed != deadline.Sub(p.origin)+time.Nanosecond ||
			p.PinDrain.Status != "pending" || p.PinDrain.After != nil {
			t.Fatalf("late reply recorded samples %+v with proof %+v", p.Samples, p.PinDrain)
		}
	})
}
