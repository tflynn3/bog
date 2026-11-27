package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"
	"testing"
)

func capacityRecord(seed int) string {
	total := seed + 2048
	return fmt.Sprintf(`{
		"fixture":{"seed_rows":%d,"tail_rows":2048,"line_bytes":1024,"batch_rows":64,"memory_limit_bytes":134217728},
		"phase":"complete","status":"pass","workload_status":"pass","timed_out":false,
		"recovered_rows":%d,"compaction":{"state":"complete","generation":42,"covered_rows":%d,"committed_rows":%d},
		"cleanup":{"status":"pass","candidate_reaped":true,"subtree_empty":true,"subtree_removed":true},
		"start_to_ready_ns":1000,"restart_to_ready_ns":10000,"ingest_p99_ns":100,"backfill_query_p99_ns":150,
		"ingest_samples":32,"backfill_query_samples":120,
		"cgroup_peak_memory_bytes":100000000,"peak_rss_bytes":50000000,"peak_descriptors":20,
		"initial_disk_free_bytes":40000000000,"minimum_disk_headroom_bytes":20000000000,
		"device_io":{"253:0":{"rbytes":0,"wbytes":1024}}
	}`, seed, total, seed+64, total)
}

func capacityReport(id identity) []byte {
	return []byte(fmt.Sprintf(`{"schema_version":%d,"revision":%q,"binary_sha256":%q,"fixture":%q,"mode":"live",
		"cases":{"retained_history_capacity":{"status":"pass","evidence":[%s,%s]}}}`,
		id.SchemaVersion, id.Revision, id.BinarySHA256, id.Fixture, capacityRecord(196608), capacityRecord(393216)))
}

func TestValidateCapacityEvidence(t *testing.T) {
	t.Parallel()
	id := identity{1, testRevision, strings.Repeat("a", 64), capacityFixtureID}
	valid := string(capacityReport(id))
	evidence := `,"evidence":[` + capacityRecord(196608) + "," + capacityRecord(393216) + "]"
	for _, tc := range []struct{ name, from, to, problem string }{
		{"valid", "", "", ""},
		{"record order independent", evidence, `,"evidence":[` + capacityRecord(393216) + "," + capacityRecord(196608) + "]", ""},
		{"missing mode", `,"mode":"live"`, "", "live mode"},
		{"legacy mode", `"mode":"live"`, `"mode":"legacy"`, "live mode"},
		{"missing evidence", evidence, "", "evidence"},
		{"null evidence", evidence, `,"evidence":null`, "evidence"},
		{"empty evidence array", evidence, `,"evidence":[]`, "exactly one"},
		{"object instead of array", evidence, `,"evidence":{}`, "evidence"},
		{"smaller size only", evidence, `,"evidence":[` + capacityRecord(196608) + "]", "exactly one"},
		{"extra fixture", evidence, strings.TrimSuffix(evidence, "]") + "," + capacityRecord(8192) + "]", "exactly one"},
		{"duplicate size", capacityRecord(393216), capacityRecord(196608), "duplicate capacity fixture size"},
		{"unexpected size", capacityRecord(393216), capacityRecord(32768), "unexpected"},
		{"null record", capacityRecord(393216), "null", "fixture size"},
		{"wrong cap", `"memory_limit_bytes":134217728`, `"memory_limit_bytes":134217729`, "memory limit"},
		{"incorrect batch size", `"batch_rows":64`, `"batch_rows":32`, "fixture dimensions"},
		{"failed workload", `"workload_status":"pass"`, `"workload_status":"fail"`, "completed fixture"},
		{"timeout", `"timed_out":false`, `"timed_out":true`, "without timeout"},
		{"missing timeout verdict", `,"timed_out":false`, "", "without timeout"},
		{"null timeout verdict", `"timed_out":false`, `"timed_out":null`, "without timeout"},
		{"recovery count", `"recovered_rows":198656`, `"recovered_rows":196608`, "recovered"},
		{"compaction incomplete", `"state":"complete"`, `"state":"running"`, "completed compaction"},
		{"compaction missing coverage", `"covered_rows":196672,`, "", "compaction"},
		{"compaction committed count", `"committed_rows":198656`, `"committed_rows":196672`, "compaction"},
		{"compaction generation", `"generation":42`, `"generation":0`, "compaction"},
		{"no reaping proof", `"candidate_reaped":true,`, "", "cleanup"},
		{"nonempty subtree", `"subtree_empty":true`, `"subtree_empty":false`, "cleanup"},
		{"subtree removal unknown", `"subtree_removed":true`, `"subtree_removed":null`, "cleanup"},
		{"cleanup failed", `"cleanup":{"status":"pass"`, `"cleanup":{"status":"fail"`, "cleanup"},
		{"duplicate proof key", `"subtree_empty":true`, `"subtree_empty":false,"subtree_empty":true`, "duplicate"},
		{"aliased proof key", `"subtree_empty":true`, `"Subtree_Empty":true`, "cleanup"},
		{"null startup time", `"start_to_ready_ns":1000`, `"start_to_ready_ns":null`, "timing"},
		{"negative restart time", `"restart_to_ready_ns":10000`, `"restart_to_ready_ns":-1`, "timing"},
		{"malformed latency", `"ingest_p99_ns":100`, `"ingest_p99_ns":"100"`, "evidence"},
		{"bad ingest count", `"ingest_samples":32`, `"ingest_samples":31`, "sample counts"},
		{"no query samples", `"backfill_query_samples":120`, `"backfill_query_samples":0`, "sample counts"},
		{"peak above cap", `"cgroup_peak_memory_bytes":100000000`, `"cgroup_peak_memory_bytes":134217729`, "resource"},
		{"missing RSS", `"peak_rss_bytes":50000000,`, "", "resource"},
		{"zero descriptors", `"peak_descriptors":20`, `"peak_descriptors":0`, "resource"},
		{"invalid headroom", `"initial_disk_free_bytes":40000000000`, `"initial_disk_free_bytes":10000000000`, "resource"},
		{"unavailable metrics", `"device_io":`, `"unavailable":{"peak_descriptors":"permission denied"},"device_io":`, "resource"},
		{"empty device map", `{"253:0":{"rbytes":0,"wbytes":1024}}`, "{}", "device I/O"},
		{"malformed device", `"253:0"`, `"device"`, "I/O measurements"},
		{"missing device counter", `"wbytes":1024`, `"wios":1`, "I/O measurements"},
		{"null I/O counter", `"rbytes":0`, `"rbytes":null`, "I/O measurements"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := strings.Replace(valid, tc.from, tc.to, 1)
			err := validateReport([]byte(input), id, capacityCases)
			if tc.problem == "" && err != nil || tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
				t.Fatalf("wanted %q, got %v", tc.problem, err)
			}
		})
	}
}

func TestReleaseCapacityCutoverRequiresMeasurementsAtBothScales(t *testing.T) {
	data := producerCutoverReport(t)
	id := identity{schemaVersion, strings.Repeat("a", 40), strings.Repeat("b", 64), capacityFixtureID}
	opts := validationOptions{requireCutover: true}
	if err := validateReportWithOptions(data, id, capacityCases, opts); err != nil {
		t.Fatalf("producer report rejected: %v", err)
	}

	var envelope struct {
		identity
		Mode  string             `json:"mode"`
		Cases map[string]outcome `json:"cases"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	var records []map[string]jsontext.Value
	if err := json.Unmarshal(envelope.Cases[capacityCase].Evidence, &records); err != nil {
		t.Fatal(err)
	}
	delete(records[1], "peak_descriptors")
	evidence, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Cases[capacityCase] = outcome{Status: "pass", Evidence: evidence}
	mutated, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	const want = "capacity record 2: missing or invalid resource measurements"
	if err := validateReportWithOptions(mutated, id, capacityCases, opts); err == nil || err.Error() != want {
		t.Fatalf("strict release validation = %v, want %q", err, want)
	}
}
