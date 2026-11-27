package capacityevidence

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"
	"testing"
)

const releaseMeasurements = `"start_to_ready_ns":1000,"restart_to_ready_ns":2000,
	"ingest_p99_ns":100,"backfill_query_p99_ns":150,"ingest_samples":32,"backfill_query_samples":120,
	"peak_rss_bytes":4096,"peak_descriptors":20,"initial_disk_free_bytes":1000,"minimum_disk_headroom_bytes":500,
	"device_io":{"253:0":{"rbytes":0,"wbytes":0,"wios":0}}`

func measurementsRecord(proof []byte, measurements string) []byte {
	return fmt.Appendf(nil, "%s,%s}", proof[:len(proof)-1], measurements)
}

func measurementsReport(first, second []byte) []byte {
	return fmt.Appendf(nil, `{"schema_version":1,"fixture":%q,"mode":"live","cases":{%q:{"status":"pass","evidence":[%s,%s]}}}`,
		FixtureID, Case, first, second)
}

func TestReleaseCapacityMeasurements(t *testing.T) {
	t.Parallel()
	small, large := observationRecord(t, SmallScale), observationRecord(t, LargeScale)
	timing := "missing or invalid timing measurements or sample counts"
	resources := "missing or invalid resource measurements"
	for _, tc := range []struct {
		name, from, to, want string
	}{
		{name: "observed zero I/O counters"},
		{"startup omitted", `"start_to_ready_ns":1000,`, "", timing},
		{"restart null", `"restart_to_ready_ns":2000`, `"restart_to_ready_ns":null`, timing},
		{"ingest latency negative", `"ingest_p99_ns":100`, `"ingest_p99_ns":-1`, timing},
		{"query latency zero", `"backfill_query_p99_ns":150`, `"backfill_query_p99_ns":0`, timing},
		{"ingest count incorrect", `"ingest_samples":32`, `"ingest_samples":31`, timing},
		{"query count zero", `"backfill_query_samples":120`, `"backfill_query_samples":0`, timing},
		{"RSS missing", `"peak_rss_bytes":4096,`, "", resources},
		{"descriptors zero", `"peak_descriptors":20`, `"peak_descriptors":0`, resources},
		{"headroom null", `"minimum_disk_headroom_bytes":500`, `"minimum_disk_headroom_bytes":null`, resources},
		{"headroom exceeds initial space", `"initial_disk_free_bytes":1000`, `"initial_disk_free_bytes":499`, resources},
		{"reported unavailable", `"device_io":`, `"unavailable":{"peak_rss_bytes":"permission denied"},"device_io":`, resources},
		{"no devices", `{"253:0":{"rbytes":0,"wbytes":0,"wios":0}}`, "{}", "missing device I/O measurements"},
		{"device number overflows", "253:0", "253:4294967296", `invalid I/O measurements for device "253:4294967296"`},
		{"write counter missing", `"wbytes":0,`, "", `invalid I/O measurements for device "253:0"`},
		{"read counter null", `"rbytes":0`, `"rbytes":null`, `invalid I/O measurements for device "253:0"`},
		{"extra counter null", `"wios":0`, `"wios":null`, `invalid I/O counter "wios" for device "253:0"`},
		{"empty counter name", `"wios":0`, `"":0`, `invalid I/O counter "" for device "253:0"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			edited := strings.Replace(releaseMeasurements, tc.from, tc.to, 1)
			second := measurementsRecord(large, edited)
			data := measurementsReport(measurementsRecord(small, releaseMeasurements), second)
			if err := ValidateRecord(second); err != nil {
				t.Fatalf("benchmark record: %v", err)
			}
			if err := ValidateReport(data); err != nil {
				t.Fatalf("benchmark report: %v", err)
			}
			for _, check := range []struct {
				name string
				err  error
				want string
			}{
				{"release cutover", ValidateReleaseCutoverReport(data), "capacity record 2: " + tc.want},
				{"historical release", ValidateHistoricalReport(data), fmt.Sprintf("capacity fixture %d: %s", LargeScale, tc.want)},
			} {
				if tc.want == "" && check.err == nil {
					continue
				}
				if tc.want == "" || check.err == nil || check.err.Error() != check.want {
					t.Errorf("%s: got %v, want %q (empty reason means success)", check.name, check.err, tc.want)
				}
			}
		})
	}
}

func TestHistoricalCapacityReportCompatibility(t *testing.T) {
	t.Parallel()
	historicalRecord := func(seed int) []byte {
		var fields map[string]jsontext.Value
		if err := json.Unmarshal(observationRecord(t, seed), &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, "cgroup_memory_limit_bytes")
		delete(fields, "cgroup_swap_max_bytes")
		delete(fields, "cgroup_oom_kill_count")
		fields["cleanup"] = jsontext.Value(`{"status":"pass","candidate_reaped":true,"subtree_empty":true,"subtree_removed":true}`)
		// Historical validation treated diagnostics as an extension.
		fields["diagnostics"] = jsontext.Value(`{"restart":"legacy extension"}`)
		raw, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return measurementsRecord(raw, releaseMeasurements)
	}
	data := measurementsReport(historicalRecord(LargeScale), historicalRecord(SmallScale))
	if err := ValidateHistoricalReport(data); err != nil {
		t.Fatalf("historical report rejected: %v", err)
	}
	if err := ValidateReleaseCutoverReport(data); err == nil || !strings.Contains(err.Error(), "decode capacity case") {
		t.Fatalf("release cutover got %v, want a diagnostic decode error", err)
	}
}

func TestBenchmarkCapacityIgnoresReleaseMeasurementTypes(t *testing.T) {
	t.Parallel()
	first := measurementsRecord(observationRecord(t, SmallScale), `"start_to_ready_ns":"unmeasured"`)
	second := measurementsRecord(observationRecord(t, LargeScale), releaseMeasurements)
	data := measurementsReport(first, second)
	if err := ValidateRecord(first); err != nil {
		t.Fatalf("benchmark record: %v", err)
	}
	if err := ValidateReport(data); err != nil {
		t.Fatalf("benchmark report: %v", err)
	}
	if err := ValidateReleaseCutoverReport(data); err == nil || !strings.Contains(err.Error(), "capacity measurements:") ||
		!strings.Contains(err.Error(), "start_to_ready_ns") {
		t.Fatalf("release cutover got %v, want a measurement decode error", err)
	}
}

func TestReleaseCutoverChecksProofBeforeMeasurements(t *testing.T) {
	t.Parallel()
	small := observationRecord(t, SmallScale)
	large := strings.Replace(string(observationRecord(t, LargeScale)), `"timed_out":false`, `"timed_out":null`, 1)
	data := measurementsReport(measurementsRecord(small, `"peak_descriptors":"unmeasured"`), []byte(large))
	const want = "capacity fixture 393216: missing or null timed_out verdict"
	if err := ValidateReleaseCutoverReport(data); err == nil || err.Error() != want {
		t.Fatalf("release cutover got %v, want %q", err, want)
	}
}
