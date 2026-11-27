package main

import (
	"fmt"
	"strings"
	"testing"
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
		sample("overlap", "running", 35, 30, predecessor, held, requested+64, "null"),
		sample("publication", "complete", 55, 50, generation, requested, final, "2"),
		`{"event":"held_exhausted","elapsed_ns":70,"status":null}`,
		sample("after_exhaustion", "complete", 75, 71, generation, requested, final, "1"),
		sample("drain_poll", "complete", 90, 80, generation, requested, final, "0"),
		`{"event":"fresh_visible","elapsed_ns":110,"status":null}`,
	}
	return fmt.Sprintf(`{"pattern":%q,"status":"pass","held_cutoff":%d,"requested_cutoff":%d,"expected_committed_rows":%d,
		"held_rows_checked":3,"held_cursor_exhausted":true,"fresh_backfill_visible":true,
		"acknowledged_backfill":{"rows":%d,"batches":%d,"first_ack_ns":18,"last_ack_ns":40,
			"earliest_row_ts":"2020-01-01T00:00:00Z","latest_row_ts":"2020-01-01T00:00:01Z"},
		"publication":{"generation":%d,"observed_lag_lower_bound_ns":10,"observed_lag_upper_bound_ns":35},
		"pin_drain":{"status":"observed","retired_pins_while_held":2,"retired_pins_after_exhaustion":0},
		"samples":[%s]}`,
		pattern, held, requested, final, final-held, (final-held)/64, generation, strings.Join(samples, ","))
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
		{"pins still held", `"retired_pins_after_exhaustion":0`, `"retired_pins_after_exhaustion":1`, "zero after exhaustion"},
		{"unsupported held summary", `"retired_generation_pins":2`, `"retired_generation_pins":null`, "exhausted before proof of a retired pin"},
		{"unsupported drained summary", `"retired_generation_pins":0`, `"retired_generation_pins":null`, "fresh visibility sample must follow observed pin drain"},
		{"drain precedes exhaustion", `"request_sent_ns":71`, `"request_sent_ns":65`, "did not follow held cursor exhaustion"},
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
