package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const runnerTestDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func capacityFixture(status, records string) []byte {
	return fmt.Appendf(nil, `{"schema_version":1,"revision":%q,"binary_sha256":%q,"fixture":%q,"mode":"live",
		"cases":{"retained_history_capacity":{"status":%q,"detail":"fixture detail","evidence":[%s]}}}`,
		testRevision, runnerTestDigest, fixtureID, status, records)
}

func newCapacityTestReport() runnerReport {
	return runnerReport{Revision: testRevision, BinarySHA256: runnerTestDigest, Fixture: fixtureID, Capacity: newRunnerCapacity()}
}

func TestCapacityIdentityProgressAndAttribution(t *testing.T) {
	t.Parallel()
	const small = `{"fixture":{"seed_rows":196608},"status":"pass","phase":"complete","timed_out":false,
		"cgroup_peak_memory_bytes":78000,"ingest_samples":32,"query_samples":225,
		"recovered_rows":198656,"diagnostics":{"cgroup_memory_events":{"oom":0,"oom_kill":0}}}`
	const early = `{"fixture":{"seed_rows":393216},"status":"fail","phase":"seed","detail":"append at ordinal 1024",
		"workload_status":"fail","cgroup_peak_memory_bytes":134217728,
		"diagnostics":{"cgroup_memory_events":{"oom":2,"oom_kill":1,"oom_group_kill":0}},
		"cleanup":{"status":"fail","detail":"remove subtree","candidate_reaped":true,"subtree_empty":true,
		"subtree_removed":false,"store_removed":true}}`
	const large = `{"fixture":{"seed_rows":393216},"status":"pass","phase":"complete","cgroup_peak_memory_bytes":90000,
		"diagnostics":{"cgroup_memory_events":{"oom":0,"oom_kill":0}}}`
	for _, tc := range []struct {
		name, status, records, kind, detail string
		exit                                int
	}{
		{"both scales", "pass", large + "," + small, "", "", 0},
		{"candidate failure", "fail", small + "," + early, workloadFailure, "fixture detail", 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newCapacityTestReport()
			kind, err := r.readCapacity(t.Context(), capacityFixture(tc.status, tc.records), tc.exit)
			if kind != tc.kind || (err == nil) != (tc.detail == "") || err != nil && !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("classification = %q %v, want %q %q", kind, err, tc.kind, tc.detail)
			}
			first, second := r.Capacity.Scales[0], r.Capacity.Scales[1]
			if first.SeedRows != smallReleaseRows || first.Status != "pass" || first.Phase != "complete" ||
				first.TimedOut == nil || *first.TimedOut || first.RecoveredRows == nil || *first.RecoveredRows != 198656 ||
				first.PeakSource != "producer" || *first.MemoryPeak != 78000 || *first.IngestSamples != 32 ||
				*first.QuerySamples != 225 || !reflect.DeepEqual(first.OOMDelta, map[string]uint64{"oom": 0, "oom_kill": 0}) {
				t.Fatalf("small fixture %+v", first)
			}
			if second.SeedRows != largeReleaseRows {
				t.Fatalf("large fixture has seed rows %d", second.SeedRows)
			}
			if tc.exit == 20 {
				if second.Status != "fail" || second.Phase != "seed" || second.FailureKind != workloadFailure ||
					second.Detail != "append at ordinal 1024\nremove subtree" || *second.MemoryPeak != releaseMemoryLimit ||
					!reflect.DeepEqual(second.OOMDelta, map[string]uint64{"oom": 2, "oom_kill": 1, "oom_group_kill": 0}) {
					t.Fatalf("failed large fixture %+v", second)
				}
				cleanup := second.Cleanup
				if cleanup == nil || cleanup.Status != "fail" || cleanup.Detail != "remove subtree" ||
					cleanup.CandidateReaped == nil || !*cleanup.CandidateReaped ||
					cleanup.SubtreeEmpty == nil || !*cleanup.SubtreeEmpty ||
					cleanup.SubtreeRemoved == nil || *cleanup.SubtreeRemoved ||
					cleanup.StoreRemoved == nil || !*cleanup.StoreRemoved {
					t.Fatalf("cleanup = %+v", cleanup)
				}
			}
		})
	}
	r := newCapacityTestReport()
	r.BinarySHA256 = strings.Repeat("a", 64)
	if kind, err := r.readCapacity(t.Context(), capacityFixture("pass", small+","+large), 0); kind != runnerFailure ||
		err == nil || !strings.Contains(err.Error(), "identity") || r.Capacity.Scales[0].Status != "unobserved" {
		t.Fatalf("wrong pin classification = %q %v, scales %+v", kind, err, r.Capacity.Scales)
	}
	raw, err := json.Marshal(r)
	if err != nil || !strings.Contains(string(raw), r.BinarySHA256) {
		t.Fatalf("runner identity missing in %s, %v", raw, err)
	}
}

func TestCapacityProofRejectionPreservesLiveProgress(t *testing.T) {
	t.Parallel()
	const (
		failed = `{"fixture":{"seed_rows":196608},"status":"fail","phase":"seed","workload_status":"fail"}`
		setup  = `{"fixture":{"seed_rows":196608},"status":"fail","phase":"provision","workload_status":"fail"}`
		small  = `{"fixture":{"seed_rows":196608},"status":"pass","phase":"complete"}`
		large  = `{"fixture":{"seed_rows":393216},"status":"pass","phase":"complete"}`
	)
	failureProof := capacityFixture("fail", failed)
	for _, tc := range []struct {
		name, detail string
		exit         int
		proof        []byte
	}{
		{"old success requires both scales", "1 of 2 scales", 0, capacityFixture("pass", small)},
		{"zero with failed record", "expected passing live capacity evidence", 0, capacityFixture("pass", failed+","+large)},
		{"passing case", "requires a failed live case", 20, capacityFixture("pass", small+","+large)},
		{"failed case with only passes", "requires a failed live case", 20, capacityFixture("fail", small+","+large)},
		{"empty failed case", "requires a failed live case", 20, capacityFixture("fail", "")},
		{"failed seed without workload status", "requires a failed live case", 20, capacityFixture("fail",
			`{"fixture":{"seed_rows":196608},"status":"fail","phase":"seed"}`)},
		{"provisioning only", "requires a failed live case", 20, capacityFixture("fail", setup)},
		{"startup placement with OOM", "requires a failed live case", 20, capacityFixture("fail",
			`{"fixture":{"seed_rows":196608},"status":"fail","phase":"startup","workload_status":"fail",
				"detail":"open /sys/fs/cgroup/fixture/cgroup.procs: permission denied",
				"diagnostics":{"cgroup_memory_events":{"oom":1,"oom_kill":1}}}`)},
		{"restart placement with OOM", "requires a failed live case", 20, capacityFixture("fail",
			`{"fixture":{"seed_rows":196608},"status":"fail","phase":"restart","workload_status":"fail",
				"detail":"write /sys/fs/cgroup/fixture/cgroup.procs: permission denied",
				"diagnostics":{"cgroup_memory_events":{"oom":1,"oom_kill":1}}}`)},
		{"first shutdown cleanup with OOM", "requires a failed live case", 20, capacityFixture("fail",
			`{"fixture":{"seed_rows":196608},"status":"fail","phase":"first_shutdown","workload_status":"fail",
				"detail":"before restart: stop first candidate before restart: wait for empty fixture cgroup",
				"cleanup":{"status":"fail"},"diagnostics":{"cgroup_memory_events":{"oom":1,"oom_kill":1}}}`)},
		{"cleanup only", "requires a failed live case", 20, capacityFixture("fail",
			`{"fixture":{"seed_rows":196608},"status":"fail","phase":"complete","workload_status":"pass","cleanup":{"status":"fail"}}`)},
		{"duplicate scale", "duplicate capacity scale 196608", 20, capacityFixture("fail", failed+","+small)},
		{"unknown scale", "unexpected capacity scale 42", 20, capacityFixture("fail", failed+","+
			`{"fixture":{"seed_rows":42},"status":"pass","phase":"complete"}`)},
		{"unknown record status", "unexpected capacity record status", 20, capacityFixture("fail", failed+","+
			`{"fixture":{"seed_rows":393216},"status":"skip"}`)},
		{"missing phase", "failed capacity record has no phase", 20, capacityFixture("fail",
			`{"fixture":{"seed_rows":196608},"status":"fail"}`)},
		{"wrong schema", "identity", 20, []byte(strings.ReplaceAll(string(failureProof), `"schema_version":1`, `"schema_version":2`))},
		{"wrong revision", "identity", 20, []byte(strings.ReplaceAll(string(failureProof), testRevision, strings.Repeat("f", 40)))},
		{"wrong checksum", "identity", 20, []byte(strings.ReplaceAll(string(failureProof), runnerTestDigest, strings.Repeat("f", 64)))},
		{"wrong fixture", "identity", 20, []byte(strings.ReplaceAll(string(failureProof), fixtureID, "other-fixture"))},
		{"wrong mode", "identity", 20, []byte(strings.ReplaceAll(string(failureProof), `"live"`, `"legacy"`))},
		{"missing live case", "missing the live case", 20, []byte(strings.ReplaceAll(string(failureProof), capacityCaseName, "legacy_baseline"))},
		{"malformed json", "decode capacity producer report", 20, failureProof[:len(failureProof)-1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newCapacityTestReport()
			peak := uint64(17)
			r.Capacity.Scales[0] = scaleProgress{SeedRows: smallReleaseRows, Status: "observed", Phase: "candidate_in_cgroup",
				MemoryPeak: &peak, PeakSource: "live_sample", OOMDelta: map[string]uint64{"oom": 1, "oom_kill": 1}, OOMSource: "live_sample"}
			before := r.Capacity.Scales
			kind, err := r.readCapacity(t.Context(), tc.proof, tc.exit)
			if err == nil || !strings.Contains(err.Error(), tc.detail) || kind != runnerFailure {
				t.Fatalf("verdict = %q, %v, want %q, %q", kind, err, runnerFailure, tc.detail)
			}
			if !reflect.DeepEqual(r.Capacity.Scales, before) {
				t.Fatalf("rejected proof changed scale evidence: %+v, want %+v", r.Capacity.Scales, before)
			}
		})
	}
}

func TestCommittedCapacityFailurePreservesAbsentScaleSample(t *testing.T) {
	t.Parallel()
	r := newCapacityTestReport()
	peak := uint64(17)
	sample := scaleProgress{SeedRows: largeReleaseRows, Status: "observed", Phase: "candidate_in_cgroup",
		MemoryPeak: &peak, PeakSource: "live_sample", OOMDelta: map[string]uint64{"oom": 0, "oom_kill": 0}, OOMSource: "live_sample"}
	r.Capacity.Scales[1] = sample
	proof := capacityFixture("fail", `{"fixture":{"seed_rows":196608},"status":"fail","phase":"seed",
		"workload_status":"fail","detail":"ingest timeout","cleanup":{"status":"fail","detail":"remove subtree"}}`)
	kind, err := r.readCapacity(t.Context(), proof, 20)
	if kind != workloadFailure || err == nil || err.Error() != "live capacity case failed: fixture detail" {
		t.Fatalf("verdict = %q, %v", kind, err)
	}
	small, large := r.Capacity.Scales[0], r.Capacity.Scales[1]
	if small.Status != "fail" || small.Phase != "seed" || small.FailureKind != workloadFailure ||
		small.Cleanup == nil || small.Cleanup.Status != "fail" || small.Detail != "ingest timeout\nremove subtree" {
		t.Fatalf("failed scale = %+v", small)
	}
	if !reflect.DeepEqual(large, sample) {
		t.Fatalf("absent scale = %+v, want %+v", large, sample)
	}
}

func TestOomDeltaAndVerifierReserveFailure(t *testing.T) {
	t.Parallel()
	before := map[string]uint64{"oom": 5, "oom_kill": 2, "oom_group_kill": 1}
	after := map[string]uint64{"oom": 8, "oom_kill": 2, "oom_group_kill": 1, "high": 8}
	delta, err := oomDelta(before, after)
	if err != nil || !reflect.DeepEqual(delta, map[string]uint64{"oom": 3, "oom_kill": 0, "oom_group_kill": 0}) {
		t.Fatalf("oomDelta = %v, %v", delta, err)
	}
	after["oom"] = 4
	if delta, err := oomDelta(before, after); err == nil || delta != nil || !strings.Contains(err.Error(), "oom delta") {
		t.Fatalf("regressed counter = %v, %v", delta, err)
	}
	for _, tc := range []struct {
		output string
		want   bool
	}{
		{"fatal error: failed to reserve page summary memory\n\nruntime stack:", true},
		{"fatal error: memory reservation exceeds address space limit", true},
		{"fatal error: runtime: cannot remap pages in address space", true},
		{"fatal error: out of memory", false},
		{"candidate log: fatal error: failed to reserve page summary memory", false},
	} {
		if got := verifierReserveFailure(tc.output); got != tc.want {
			t.Errorf("reserve failure for %q = %t", tc.output, got)
		}
	}
}
