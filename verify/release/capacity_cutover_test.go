package main

import (
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

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
	archiveID := identity{1, strings.Repeat("a", 40), strings.Repeat("b", 64), capacityFixtureID}
	cutover := validationOptions{requireCutover: true}
	archived := capacityReport(archiveID)
	if err := validateReport(archived, archiveID, capacityCases); err != nil {
		t.Fatalf("archived report rejected: %v", err)
	}
	if err := validateReportWithOptions(archived, archiveID, capacityCases, cutover); err == nil {
		t.Fatal("archived report accepted as cutover")
	}

	producer := filepath.Join(t.TempDir(), "retained-history")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", producer, "../../bench/retained-history")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build capacity validator: %v\n%s", err, output)
	}
	data := producerCutoverReport(t)
	t.Setenv("BOG_RELEASE_REQUIRE_RETARGET", "")
	dir := t.TempDir()
	binary := filepath.Join(dir, "pinned-candidate")
	writeFixture(t, binary, []byte("pinned candidate"))
	id, err := readIdentity(binary, testRevision, capacityFixtureID)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.NewReplacer(strings.Repeat("a", 40), id.Revision,
		strings.Repeat("b", 64), id.BinarySHA256).Replace(string(data)))
	releaseArgs := []string{"validate", "-require-cutover", "-binary", binary, "-revision", id.Revision, "-fixture", id.Fixture}
	for _, companion := range []struct {
		name     string
		required []string
		proofs   map[string]jsontext.Value
	}{
		{"admission", admissionCases, nil},
		{"durability", durabilityCases, map[string]jsontext.Value{"enospc": jsontext.Value(proofJSON)}},
		{"parity", []string{ordinaryWALCase, restoredParityCase}, map[string]jsontext.Value{
			ordinaryWALCase: jsontext.Value(ordinaryWALJSON), restoredParityCase: jsontext.Value(restoredParityJSON),
		}},
	} {
		path := filepath.Join(dir, companion.name+".json")
		result := publicationFixture(companion.required, "passing companion")
		result.identity = id
		for name, proof := range companion.proofs {
			result.Cases[name] = outcome{Status: "pass", Evidence: proof}
		}
		if err := writeReport(path, result); err != nil {
			t.Fatal(err)
		}
		releaseArgs = append(releaseArgs, "-"+companion.name, path)
	}
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
		{name: "explicit no timeout with immediate and delayed drain"},
		{"missing timeout verdict", func(r, _ object) { delete(r, "timed_out") }, true},
		{"null timeout verdict", func(r, _ object) { r["timed_out"] = nil }, true},
		{"timed-out workload", func(r, _ object) { r["timed_out"] = true }, true},
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
		{"missing raw OOM", func(r, _ object) { delete(at(at(r, "diagnostics"), "cgroup_memory_events"), "oom_kill") }, true},
		{"null raw OOM", func(r, _ object) { at(at(r, "diagnostics"), "cgroup_memory_events")["oom_kill"] = nil }, true},
		{"quoted raw OOM", func(r, _ object) { at(at(r, "diagnostics"), "cgroup_memory_events")["oom_kill"] = "0" }, true},
		{"nonzero raw OOM", func(r, _ object) { at(at(r, "diagnostics"), "cgroup_memory_events")["oom_kill"] = 1 }, true},
		{"missing summary OOM", func(r, _ object) { delete(r, "cgroup_oom_kill_count") }, true},
		{"null summary OOM", func(r, _ object) { r["cgroup_oom_kill_count"] = nil }, true},
		{"quoted summary OOM", func(r, _ object) { r["cgroup_oom_kill_count"] = "0" }, true},
		{"nonzero summary OOM", func(r, _ object) { r["cgroup_oom_kill_count"] = 1 }, true},
		{"reversed timeline order", func(r, _ object) {
			timelines := at(r, "diagnostics")["backfill_timelines"].([]any)
			timelines[0], timelines[1] = timelines[1], timelines[0]
		}, true},
		{"duplicate timeline pattern", func(_, b object) { b["pattern"] = "clustered" }, true},
		{"nonadvancing generation", func(r, b object) {
			// Keep each timeline independently valid. Both now report the
			// same successful generation from the same predecessor.
			at(b, "publication")["generation"] = 1
			at(r, "compaction")["generation"] = 1
			for _, raw := range b["samples"].([]any) {
				s := raw.(object)
				if c, present := s["status"].(object); present {
					c["generation"] = int(c["generation"].(float64)) - 1
				}
			}
		}, true},
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
			if err := run(t.Context(), append(releaseArgs, "-capacity", reportPath)); (err != nil) != tc.reject {
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
