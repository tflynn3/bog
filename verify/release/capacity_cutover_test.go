package main

import (
	"context"
	"encoding/json/v2"
	"fmt"
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
		"held_rows_checked":3,"held_cursor_exhausted":true,"terminal_page_retried":true,"capture_retired":true,"fresh_backfill_visible":true,
		"acknowledged_backfill":{"rows":%d,"batches":%d,"first_ack_ns":18,"last_ack_ns":%d,
			"earliest_row_ts":"2020-01-01T00:00:00Z","latest_row_ts":"2020-01-01T00:00:01Z"},
		"publication":{"generation":%d,"observed_lag_lower_bound_ns":%d,"observed_lag_upper_bound_ns":35},
		"pin_drain":{"status":"observed","retired_pins_while_held":2,"retired_pins_after_exhaustion":0},
		"samples":[%s]}`,
		pattern, held, requested, final, final-held, (final-held)/capacityBatch, lastAck, generation, lagLower, strings.Join(samples, ","))
}

func cutoverCapacityReport(id identity) []byte {
	report := string(capacityReport(id))
	for _, seed := range []int{196608, 393216} {
		base := capacityRecord(seed)
		clustered := cutoverTimeline("clustered", seed-128, seed-64, seed, 38, 40)
		scattered := cutoverTimeline("scattered", seed, seed+64, seed+2048, 41, 42)
		record := strings.TrimSuffix(base, "}") + `,"diagnostics":{"backfill_timelines":[` + clustered + "," + scattered + "]}}"
		report = strings.Replace(report, base, record, 1)
	}
	return []byte(report)
}

func TestValidateCutoverCapacityProof(t *testing.T) {
	t.Parallel()
	id := identity{1, testRevision, strings.Repeat("a", 64), capacityFixtureID}
	cutover := validationOptions{requireCutover: true}
	archived, modern := capacityReport(id), cutoverCapacityReport(id)
	if err := validateReport(archived, id, capacityCases); err != nil {
		t.Fatalf("archived report rejected: %v", err)
	}
	if err := validateReportWithOptions(archived, id, capacityCases, cutover); err == nil || !strings.Contains(err.Error(), "cutover diagnostics") {
		t.Fatalf("archived report accepted as cutover: %v", err)
	}
	for _, tc := range []struct {
		name, from, to, problem string
	}{
		{"valid", "", "", ""},
		{"optional producer extension", `"held_rows_checked":3`, `"held_rows_checked":3,"extension":{"another_field":0}`, ""},
		{"null proof", `"publication":{"generation":40,"observed_lag_lower_bound_ns":10,"observed_lag_upper_bound_ns":35}`, `"publication":null`, "publication generation"},
		{"case-sensitive key", `"backfill_timelines"`, `"Backfill_Timelines"`, "backfill_timelines"},
		{"duplicate pattern", `"pattern":"scattered"`, `"pattern":"clustered"`, "duplicate cutover backfill pattern"},
		{"wrong requested boundary", `"requested_cutoff":196544`, `"requested_cutoff":196545`, "incorrect cutoffs"},
		{"incomplete continuation", `"held_cursor_exhausted":true`, `"held_cursor_exhausted":false`, "held cursor exhaustion"},
		{"missing fresh backfill", `"fresh_backfill_visible":true`, `"fresh_backfill_visible":false`, "fresh backfill visibility"},
		{"misreported acknowledgements", `"batches":2`, `"batches":1`, "acknowledged backfill"},
		{"backwards publication bounds", `"observed_lag_upper_bound_ns":35`, `"observed_lag_upper_bound_ns":9`, "publication generation or observation bounds"},
		{"publication lag mismatch", `"observed_lag_upper_bound_ns":35`, `"observed_lag_upper_bound_ns":39`, "lag bounds disagree"},
		{"wrong publication sample cutoff", `"covered_rows":196544,"committed_rows":196608`, `"covered_rows":196543,"committed_rows":196608`, "publication at the requested cutoff"},
		{"wrong compaction generation", `"compaction":{"state":"complete","generation":42`, `"compaction":{"state":"complete","generation":51`, "matching completed compaction"},
		{"drain unavailable", `"status":"observed"`, `"status":"unavailable","reason":"runtime metric is absent"`, "runtime metric is absent"},
		{"held pins missing", `"retired_pins_while_held":2`, `"retired_pins_while_held":null`, "positive held count"},
		{"pins still held", `"retired_pins_after_exhaustion":0`, `"retired_pins_after_exhaustion":1`, "zero after retirement"},
		{"missing replay summary", `"terminal_page_retried":true`, `"terminal_page_retried":false`, "terminal page replay"},
		{"missing retirement summary", `"capture_retired":true`, `"capture_retired":false`, "explicit capture retirement"},
		{"unsupported drained summary", `"retired_generation_pins":0`, `"retired_generation_pins":null`, "fresh visibility sample must follow observed pin drain"},
		{"drain request precedes retirement", `"request_sent_ns":81`, `"request_sent_ns":79`, "did not follow explicit capture retirement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.from != "" && !strings.Contains(string(modern), tc.from) {
				t.Fatalf("missing test mutation %q", tc.from)
			}
			data := []byte(strings.Replace(string(modern), tc.from, tc.to, 1))
			err := validateReportWithOptions(data, id, capacityCases, cutover)
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("wanted %q, got %v", tc.problem, err)
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
