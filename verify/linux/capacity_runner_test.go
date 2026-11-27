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
		"cgroup_peak_memory_bytes":134217728,"diagnostics":{"cgroup_memory_events":{"oom":2,"oom_kill":1,"oom_group_kill":0}}}`
	const provision = `{"fixture":{"seed_rows":393216},"status":"fail","phase":"provision","detail":"read memory.max"}`
	const large = `{"fixture":{"seed_rows":393216},"status":"pass","phase":"complete","cgroup_peak_memory_bytes":90000,
		"diagnostics":{"cgroup_memory_events":{"oom":0,"oom_kill":0}}}`
	for _, tc := range []struct {
		name, status, records, kind, detail string
	}{
		{"both scales", "pass", large + "," + small, "", ""},
		{"candidate failure", "fail", small + "," + early, workloadFailure, "fixture detail"},
		{"runner failure", "fail", small + "," + provision, runnerFailure, "fixture detail"},
		{"incomplete progress", "fail", small, runnerFailure, "1 of 2 scales"},
		{"duplicate scale", "pass", small + "," + small, runnerFailure, "duplicate capacity scale 196608"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newCapacityTestReport()
			kind, err := r.readCapacity(capacityFixture(tc.status, tc.records))
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
			switch tc.name {
			case "candidate failure":
				if second.Status != "fail" || second.Phase != "seed" || second.FailureKind != workloadFailure ||
					second.Detail != "append at ordinal 1024" || *second.MemoryPeak != releaseMemoryLimit ||
					!reflect.DeepEqual(second.OOMDelta, map[string]uint64{"oom": 2, "oom_kill": 1, "oom_group_kill": 0}) {
					t.Fatalf("failed large fixture %+v", second)
				}
			case "incomplete progress", "duplicate scale":
				if second.Status != "unobserved" || second.MemoryPeak != nil || second.OOMDelta != nil {
					t.Fatalf("unobserved fixture contains observations: %+v", second)
				}
			}
		})
	}
	r := newCapacityTestReport()
	r.BinarySHA256 = strings.Repeat("a", 64)
	if kind, err := r.readCapacity(capacityFixture("pass", small+","+large)); kind != runnerFailure ||
		err == nil || !strings.Contains(err.Error(), "identity") || r.Capacity.Scales[0].Status != "unobserved" {
		t.Fatalf("wrong pin classification = %q %v, scales %+v", kind, err, r.Capacity.Scales)
	}
	raw, err := json.Marshal(r)
	if err != nil || !strings.Contains(string(raw), r.BinarySHA256) {
		t.Fatalf("runner identity missing in %s, %v", raw, err)
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
