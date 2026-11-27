package main

import (
	"context"
	jsonv1 "encoding/json"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func cutoverTimeline(pattern string, held, requested, final int, predecessor, generation int) string {
	sample := func(event, state string, elapsed, sent, generation, covered, committed int, pins string) string {
		return fmt.Sprintf(`{"event":%q,"elapsed_ns":%d,"request_sent_ns":%d,
			"status":{"state":%q,"generation":%d,"covered_rows":%d,"committed_rows":%d},
			"candidate_status_metrics":{"retired_generation_pins":%s}}`,
			event, elapsed, sent, state, generation, covered, committed, pins)
	}
	samples := []string{
		sample("compaction_start", "running", 25, 20, predecessor, held, requested, "null"),
		sample("overlap", "running", 35, 30, predecessor, held, requested+capacityBatch, "null"),
	}
	lastAck, publishedRows, lagLower := 29, final, 10
	if pattern == "scattered" {
		lastAck, publishedRows, lagLower = 58, requested+capacityBatch, 20
		samples = append(samples, sample("status_poll", "running", 45, 40, predecessor, held, publishedRows, "null"))
	}
	samples = append(samples, sample("publication", "complete", 55, 50, generation, requested, publishedRows, "2"))
	if publishedRows != final {
		samples = append(samples, sample("final_cutoff", "complete", 65, 60, generation, requested, final, "2"))
	}
	samples = append(samples,
		`{"event":"held_exhausted","elapsed_ns":70,"status":null}`,
		`{"event":"terminal_retried","elapsed_ns":72,"status":null}`,
		sample("before_retirement", "complete", 75, 73, generation, requested, final, "2"),
		`{"event":"capture_retired","elapsed_ns":80,"status":null}`,
		sample("after_retirement", "complete", 85, 81, generation, requested, final, "1"),
		sample("drain_poll", "complete", 100000090, 100000080, generation, requested, final, "0"),
		`{"event":"fresh_visible","elapsed_ns":100000110,"status":null}`,
	)
	return fmt.Sprintf(`{"pattern":%q,"status":"pass","held_cutoff":%d,"requested_cutoff":%d,"expected_committed_rows":%d,
		"held_rows_checked":3,"held_cursor_exhausted":true,"terminal_page_retried":true,"capture_retired":true,"fresh_backfill_visible":true,"fresh_walk_exact":true,"fresh_rows_checked":%d,
		"acknowledged_backfill":{"rows":%d,"batches":%d,"first_ack_ns":18,"last_ack_ns":%d,
			"earliest_row_ts":"2020-01-01T00:00:00Z","latest_row_ts":"2020-01-01T00:00:01Z"},
		"publication":{"generation":%d,"observed_lag_lower_bound_ns":%d,"observed_lag_upper_bound_ns":35},
		"pin_drain":{"status":"observed","retired_pins_while_held":2,"retired_pins_after_exhaustion":0},
		"samples":[%s]}`,
		pattern, held, requested, final, final, final-held, (final-held)/capacityBatch, lastAck, generation, lagLower, strings.Join(samples, ","))
}

func producerCutoverReport(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "capacity.json")
	generate := exec.CommandContext(t.Context(), "go", "test", "-count=1", "-run", "^TestWriteCutoverFixture$", "../../bench/retained-history")
	generate.Env = append(os.Environ(), "BOG_CAPACITY_TEST_REPORT="+path)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("write producer capacity fixture: %v\n%s", err, output)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestValidateCutoverCapacityProof(t *testing.T) {
	id := identity{1, strings.Repeat("a", 40), strings.Repeat("b", 64), capacityFixtureID}
	cutover := validationOptions{requireCutover: true}
	archived := capacityReport(id)
	if err := validateReport(archived, id, capacityCases); err != nil {
		t.Fatalf("archived report rejected: %v", err)
	}
	if err := validateReportWithOptions(archived, id, capacityCases, cutover); err == nil {
		t.Fatal("archived report accepted as cutover")
	}

	producer := filepath.Join(t.TempDir(), "retained-history")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", producer, "../../bench/retained-history")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build capacity validator: %v\n%s", err, output)
	}
	data := producerCutoverReport(t)
	type object = map[string]any
	at := func(o object, key string) object { return o[key].(map[string]any) }
	index := func(o object, key string, i int) object { return o[key].([]any)[i].(map[string]any) }
	sample := func(b object, event string) object {
		for _, raw := range b["samples"].([]any) {
			s := raw.(map[string]any)
			if s["event"] == event {
				return s
			}
		}
		t.Fatalf("capacity fixture missing %s sample", event)
		return nil
	}
	memory := func(b object, event string) object { return at(sample(b, event), "cgroup_memory_current") }
	for _, tc := range []struct {
		name   string
		edit   func(record, timeline object)
		reject bool
	}{
		{name: "immediate and delayed drain"},
		{"forward-compatible fields", func(_, b object) { b["extension"] = object{"value": true} }, false},
		{"samples equal peak", func(r, b object) {
			peak := r["cgroup_peak_memory_bytes"]
			memory(b, "status_poll")["bytes"], memory(b, "overlap")["bytes"] = peak, peak
			summary := at(b, "live_memory_current")
			summary["min_bytes"], summary["max_bytes"] = peak, peak
		}, false},
		{"current exceeds final peak but is below cap", func(r, b object) {
			abovePeak := r["cgroup_peak_memory_bytes"].(float64) + 1
			memory(b, "overlap")["bytes"] = abovePeak
			at(b, "live_memory_current")["max_bytes"] = abovePeak
		}, true},
		{"one overlap read", func(_, b object) { delete(sample(b, "status_poll"), "cgroup_memory_current") }, true},
		{"missing overlap read", func(_, b object) { delete(sample(b, "overlap"), "cgroup_memory_current") }, true},
		{"start reply carries memory", func(_, b object) {
			sample(b, "compaction_start")["cgroup_memory_current"] = memory(b, "status_poll")
		}, true},
		{"publication reply carries memory", func(_, b object) {
			sample(b, "publication")["cgroup_memory_current"] = memory(b, "overlap")
		}, true},
		{"missing bytes", func(_, b object) { delete(memory(b, "overlap"), "bytes") }, true},
		{"zero bytes", func(_, b object) { memory(b, "overlap")["bytes"] = 0 }, true},
		{"read error with contradictory bytes", func(_, b object) { memory(b, "overlap")["error"] = "read failed" }, true},
		{"read precedes preceding running response", func(_, b object) {
			memory(b, "overlap")["read_start_ns"] = 3400000
		}, true},
		{"read ends after request begins", func(_, b object) { memory(b, "overlap")["read_end_ns"] = 5500000 }, true},
		{"zero-duration read", func(_, b object) { memory(b, "overlap")["read_end_ns"] = 3700000 }, true},
		{"missing request time", func(_, b object) { delete(sample(b, "status_poll"), "request_sent_ns") }, true},
		{"different operation generation", func(_, b object) { at(sample(b, "status_poll"), "status")["generation"] = 99 }, true},
		{"different operation cutoff", func(_, b object) { at(sample(b, "status_poll"), "status")["covered_rows"] = 1 }, true},
		{"complete status does not bracket", func(_, b object) { at(sample(b, "status_poll"), "status")["state"] = "complete" }, true},
		{"reported span disagrees", func(_, b object) { at(b, "live_memory_current")["observation_span_ns"] = 1 }, true},
		{"reported range disagrees", func(_, b object) { at(b, "live_memory_current")["min_bytes"] = 1 }, true},
		{"reported count disagrees", func(_, b object) { at(b, "live_memory_current")["sample_count"] = 3 }, true},
		{"missing summary", func(_, b object) { delete(b, "live_memory_current") }, true},
		{"reported drain without zero sample", func(_, b object) {
			at(sample(b, "drain_poll"), "candidate_status_metrics")["retired_generation_pins"] = nil
		}, true},
		{"pin drain counter contradicts unavailable status", func(_, b object) {
			at(sample(b, "drain_poll"), "candidate_status_metrics")["unavailable"] = object{"retired_generation_pins": "missing"}
		}, true},
		{"drain request precedes retirement", func(_, b object) {
			sample(b, "after_retirement")["request_sent_ns"] = 10000000
		}, true},
		{"inexact fresh walk", func(_, b object) { b["fresh_rows_checked"] = 1 }, true},
		{"inexact restart walk", func(r, _ object) { r["recovered_rows"] = 1 }, true},
		{"batch count disagrees", func(_, b object) { at(b, "acknowledged_backfill")["batches"] = 1 }, true},
		{"missing store cleanup", func(r, _ object) { delete(at(r, "cleanup"), "store_removed") }, true},
		{"reported OOM", func(r, _ object) { at(at(r, "diagnostics"), "cgroup_memory_events")["oom_kill"] = 1 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := data
			if tc.edit != nil {
				var wire object
				if err := jsonv1.Unmarshal(data, &wire); err != nil {
					t.Fatal(err)
				}
				r := index(at(at(wire, "cases"), capacityCase), "evidence", 1)
				tc.edit(r, index(at(r, "diagnostics"), "backfill_timelines", 1))
				var err error
				in, err = jsonv1.Marshal(wire)
				if err != nil {
					t.Fatal(err)
				}
			}
			reportPath := filepath.Join(t.TempDir(), "capacity.json")
			if err := os.WriteFile(reportPath, in, 0600); err != nil {
				t.Fatal(err)
			}
			if err := validateReportWithOptions(in, id, capacityCases, cutover); (err != nil) != tc.reject {
				t.Errorf("release validator error = %v, want rejection %t", err, tc.reject)
			}
			cmd := exec.CommandContext(t.Context(), producer, "-validate-cutover", reportPath)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1) || (err != nil) != tc.reject {
				t.Errorf("capacity validator error = %v, want rejection %t\n%s", err, tc.reject, output)
			}
		})
	}
}

func TestValidateCutoverTimelineSequence(t *testing.T) {
	t.Parallel()

	seed := 196608
	clustered := backfillCutoffs{seed - 2*capacityBatch, seed - capacityBatch, seed}
	scattered := backfillCutoffs{seed, seed + capacityBatch, seed + capacityTail}
	sampleIndex := func(b *backfillEvidence, event string) int {
		return slices.IndexFunc(b.Samples, func(s backfillSample) bool { return s.Event == event })
	}
	sample := func(b *backfillEvidence, event string) *backfillSample {
		return &b.Samples[sampleIndex(b, event)]
	}
	remove := func(b *backfillEvidence, event string) {
		i := sampleIndex(b, event)
		b.Samples = slices.Delete(b.Samples, i, i+1)
	}

	for _, tc := range []struct {
		name    string
		pattern string
		mutate  func(*backfillEvidence)
		problem string
	}{
		{name: "clustered final count in publication", pattern: "clustered"},
		{name: "scattered publication before last acknowledgement", pattern: "scattered"},
		{
			name: "scattered final count in publication", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				sample(b, "publication").Compaction.CommittedRows = b.FinalCutoff
				remove(b, "final_cutoff")
			},
		},
		{
			name: "missing final cutoff", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { remove(b, "final_cutoff") },
			problem: "requires a prior final cutoff observation",
		},
		{
			name: "final cutoff after exhaustion", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				i := sampleIndex(b, "final_cutoff")
				b.Samples[i].ElapsedNS, b.Samples[i].SentNS = 71, new(int64(71))
				b.Samples[i], b.Samples[i+1] = b.Samples[i+1], b.Samples[i]
			},
			problem: "requires a prior final cutoff observation",
		},
		{
			name: "final cutoff still missing rows", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "final_cutoff").Compaction.CommittedRows-- },
			problem: "final_cutoff sample must confirm its cutoff",
		},
		{
			name: "final cutoff observes different coverage", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "final_cutoff").Compaction.CoveredRows-- },
			problem: "final_cutoff sample did not observe publication",
		},
		{
			name: "final cutoff request failed", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "final_cutoff").StatusError = "connection reset" },
			problem: "final_cutoff sample failed",
		},
		{
			name: "status poll error", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "status_poll")
				s.Compaction = nil
				s.StatusError = context.DeadlineExceeded.Error()
			},
			problem: "status_poll sample failed",
		},
		{
			name: "missing terminal replay event", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { remove(b, "terminal_retried") },
			problem: "after terminal page replay",
		},
		{
			name: "held observation request precedes replay", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "before_retirement").SentNS = new(int64(71)) },
			problem: "after terminal page replay",
		},
		{
			name: "publication pins cannot replace before-retirement sample", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { remove(b, "before_retirement") },
			problem: "requires a prior before_retirement pin observation",
		},
		{
			name: "before retirement has no positive pin", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { sample(b, "before_retirement").Metrics.RetiredPins = new(uint64(0)) },
			problem: "observe the positive held pin count",
		},
		{
			name: "missing capture retirement event", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { remove(b, "capture_retired") },
			problem: "did not follow explicit capture retirement",
		},
		{
			name: "drain poll lacks initial observation", pattern: "clustered",
			mutate:  func(b *backfillEvidence) { remove(b, "after_retirement") },
			problem: "must start with after_retirement",
		},
		{
			name: "drain succeeds on first observation", pattern: "clustered",
			mutate: func(b *backfillEvidence) {
				sample(b, "after_retirement").Metrics.RetiredPins = new(uint64(0))
				remove(b, "drain_poll")
			},
		},
		{
			name: "retry missing counter", pattern: "clustered",
			mutate: func(b *backfillEvidence) { sample(b, "after_retirement").Metrics.RetiredPins = nil },
		},
		{
			name: "retry status request deadline", pattern: "scattered",
			mutate: func(b *backfillEvidence) {
				s := sample(b, "after_retirement")
				s.Compaction, s.Metrics.RetiredPins = nil, nil
				s.StatusError = context.DeadlineExceeded.Error()
				s.ElapsedNS = *s.SentNS + int64(250*time.Millisecond)
				poll := sample(b, "drain_poll")
				poll.SentNS, poll.ElapsedNS = new(s.ElapsedNS+1), s.ElapsedNS+5
				sample(b, "fresh_visible").ElapsedNS = poll.ElapsedNS + 1
			},
		},
		{
			name: "deadline cannot attest zero", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "drain_poll").StatusError = context.DeadlineExceeded.Error() },
			problem: "fresh visibility sample must follow observed pin drain",
		},
		{
			name: "drain cannot retry arbitrary error", pattern: "scattered",
			mutate:  func(b *backfillEvidence) { sample(b, "after_retirement").StatusError = "connection reset" },
			problem: "after_retirement sample failed",
		},
		{
			name: "late zero exceeds producer drain budget", pattern: "clustered",
			mutate: func(b *backfillEvidence) {
				poll := sample(b, "drain_poll")
				poll.ElapsedNS = *sample(b, "after_retirement").SentNS + int64(2*time.Second) + 1
				poll.SentNS = new(poll.ElapsedNS - 1)
				sample(b, "fresh_visible").ElapsedNS = poll.ElapsedNS + 1
			},
			problem: "exceeded the drain budget",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want, previous, generation := clustered, 38, 40
			if tc.pattern == "scattered" {
				want, previous, generation = scattered, 41, 42
			}
			var timeline backfillEvidence
			data := cutoverTimeline(tc.pattern, want.held, want.publish, want.final, previous, generation)
			if err := json.Unmarshal([]byte(data), &timeline); err != nil {
				t.Fatal(err)
			}
			if tc.mutate != nil {
				tc.mutate(&timeline)
			}
			err := timeline.validate(want)
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("wanted %q, got %v", tc.problem, err)
			}
		})
	}
}
