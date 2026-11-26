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
	"testing"
	"testing/synctest"
	"time"
)

func TestRuntimeSamplesDistinguishZeroAndUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		err  error
		want map[string]uint64
	}{
		{"absent", "", nil, nil},
		{"null", "null", nil, nil},
		{"non-object", "[]", nil, nil},
		{"invalid individual fields", `{"publication_lag_ns":0,"staging_reserved_bytes":-1,"staging_used_bytes":2,"oldest_pin_age_ns":"3","reclaimable_bytes":null,"retired_generation_pins":18446744073709551616}`, nil, map[string]uint64{"publication_lag_ns": 0, "staging_used_bytes": 2}},
		{"all counters", `{"publication_lag_ns":100,"staging_reserved_bytes":8,"staging_used_bytes":4,"oldest_pin_age_ns":10,"reclaimable_bytes":1,"retired_generation_pins":0,"added_later":true}`, nil, map[string]uint64{
			"publication_lag_ns": 100, "staging_reserved_bytes": 8, "staging_used_bytes": 4,
			"oldest_pin_age_ns": 10, "reclaimable_bytes": 1, "retired_generation_pins": 0,
		}},
		{"failed status", `{"publication_lag_ns":9}`, errors.New("status failed"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := readRuntimeMetrics(json.RawMessage(tc.raw), tc.err)
			fields := map[string]*uint64{
				"publication_lag_ns": m.PublicationLag, "staging_reserved_bytes": m.Reserved,
				"staging_used_bytes": m.Used, "oldest_pin_age_ns": m.OldestPin,
				"reclaimable_bytes": m.Reclaimable, "retired_generation_pins": m.RetiredPins,
			}
			for key, value := range fields {
				want, present := tc.want[key]
				if present && (value == nil || *value != want || m.Unavailable[key] != "") ||
					!present && (value != nil || m.Unavailable[key] == "") {
					t.Errorf("%s = %v, reason %q", key, value, m.Unavailable[key])
				}
			}
		})
	}
}

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
		{"regressed generation", `{"state":"complete","generation":3,"covered_rows":128,"committed_rows":192}`, "unexpected compaction cutoff or generation"},
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

type statusTransport func(*http.Request) (*http.Response, error)

func (f statusTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestPinDrainStatusTransitions(t *testing.T) {
	const (
		absent  = ""
		missing = `,"metrics":{"reclaimable_bytes":0}`
		null    = `,"metrics":{"retired_generation_pins":null}`
		zero    = `,"metrics":{"retired_generation_pins":0,"reclaimable_bytes":2048}`
		one     = `,"metrics":{"retired_generation_pins":1}`
		two     = `,"metrics":{"retired_generation_pins":2}`
	)
	for _, tc := range []struct {
		name                string
		before, after       []string
		wantStatus          string
		wantHeld, wantAfter *uint64
		wantPolls           int
		wantReason          string
		wantError           string
	}{
		{"positive, missing, null, then zero", []string{one}, []string{two, missing, null, zero}, "observed", new(uint64(1)), new(uint64(0)), 4, "", ""},
		{"retirement after two seconds", []string{one}, []string{"late"}, "observed", new(uint64(1)), new(uint64(0)), 31, "", ""},
		{"missing first drain sample", []string{one}, []string{absent, zero}, "observed", new(uint64(1)), new(uint64(0)), 2, "", ""},
		{"positive supersedes publication zero", []string{zero, one}, []string{zero}, "observed", new(uint64(1)), new(uint64(0)), 1, "", ""},
		{"missing final cutoff sample preserves positive", []string{one, missing}, []string{zero}, "observed", new(uint64(1)), new(uint64(0)), 1, "", ""},
		{"zero without held sample", []string{absent}, []string{zero}, "unavailable", nil, new(uint64(0)), 1, "positive predecessor pin count", ""},
		{"positive only after exhaustion", []string{zero}, []string{one, zero}, "unavailable", new(uint64(0)), new(uint64(1)), 1, "positive predecessor pin count", ""},
		{"candidate lacks instrumentation", []string{absent}, []string{absent}, "unavailable", nil, nil, 1, "candidate did not expose metrics", ""},
		{"counter unavailable after exhaustion", []string{one}, []string{null}, "unavailable", new(uint64(1)), nil, 4, "candidate did not supply a nonnegative integer", ""},
		{"positive through deadline", []string{one}, []string{two}, "pending", new(uint64(1)), new(uint64(2)), 4, "pins still present", "did not drain within"},
		{"missing through deadline after positive", []string{one}, []string{two, missing}, "pending", new(uint64(1)), new(uint64(2)), 4, "pins still present", "did not drain within"},
		{"deadline during status request", []string{one}, []string{two, "wait"}, "pending", new(uint64(1)), new(uint64(2)), 2, "pins still present", "did not drain within"},
		{"overall fixture deadline", []string{one}, []string{two, "fixture deadline"}, "pending", new(uint64(1)), new(uint64(2)), 2, "pins still present", "context deadline exceeded"},
		{"zero from different generation", []string{one}, []string{two, "changed"}, "pending", new(uint64(1)), new(uint64(2)), 2, "pins still present", "published state changed"},
		{"status request fails while pending", []string{one}, []string{two, "failed"}, "pending", new(uint64(1)), new(uint64(2)), 2, "drain status unavailable", "HTTP 503"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				firstPageReceived := time.Now()
				retiredAt := firstPageReceived.Add(3 * time.Second)
				s := newService("http://candidate")
				s.client.Transport = statusTransport(func(req *http.Request) (*http.Response, error) {
					i := calls
					calls++
					var sample string
					if i < len(tc.before) {
						sample = tc.before[i]
					} else {
						sample = tc.after[min(i-len(tc.before), len(tc.after)-1)]
					}
					generation, status := 4, http.StatusOK
					switch sample {
					case "late":
						sample = two
						if !time.Now().Before(retiredAt) {
							sample = zero
						}
					case "wait", "fixture deadline":
						<-req.Context().Done()
						return nil, req.Context().Err()
					case "changed":
						generation, sample = 5, zero
					case "failed":
						status, sample = http.StatusServiceUnavailable, absent
					}
					body := fmt.Sprintf(`{"state":"complete","generation":%d,"covered_rows":128,"committed_rows":192%s}`, generation, sample)
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
				})
				p := backfillProbe{backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
					service: s, control: "/compact", dir: dir, origin: time.Now()}
				for i := range tc.before {
					if _, err := p.pollPublication(t.Context(), 3, i == len(tc.before)-1); err != nil {
						t.Fatal(err)
					}
				}
				deadline := time.Now().Add(350 * time.Millisecond)
				ctx := t.Context()
				switch tc.after[len(tc.after)-1] {
				case "late":
					deadline = pinDrainDeadline(firstPageReceived)
				case "wait":
					deadline = time.Now().Add(150 * time.Millisecond)
				case "fixture deadline":
					deadline = pinDrainDeadline(firstPageReceived)
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 150*time.Millisecond)
					defer cancel()
				}
				err := p.observeDrain(ctx, deadline)
				if (err == nil) != (tc.wantError == "") || err != nil && !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("drain error = %v, want %q", err, tc.wantError)
				}
				if (tc.wantError == "did not drain within" || tc.wantError == "context deadline exceeded") && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost drain deadline: %v", err)
				}
				drain := p.PinDrain
				if drain.Status != tc.wantStatus || !reflect.DeepEqual(drain.Held, tc.wantHeld) || !reflect.DeepEqual(drain.After, tc.wantAfter) ||
					(tc.wantReason == "" && drain.Reason != "") || !strings.Contains(drain.Reason, tc.wantReason) {
					t.Fatalf("unexpected drain proof: %+v", drain)
				}
				samples := p.Samples[len(tc.before):]
				if len(samples) != tc.wantPolls || samples[0].Event != "after_exhaustion" {
					t.Fatalf("unexpected drain samples: %+v", samples)
				}
				if tc.wantStatus == "observed" {
					last := samples[len(samples)-1].Candidate
					if last.Reclaimable == nil || *last.Reclaimable != 2048 {
						t.Fatalf("missing reclaimable bytes at retirement: %+v", last)
					}
				}
			})
		})
	}
}
