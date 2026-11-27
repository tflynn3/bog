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
	"time"

	"bog/verify/capacityevidence"
)

// ADR 0027 uses the backup.Point JSON shape with three checkpoint rows
// followed by a committed tail. This portable fixture mirrors the Linux probe.
const capacityCrashCase = "post_restore_ack_crash"
const capacityCrashJSON = `{
	"verified_point":{
		"Generation":[212,98,240,211,29,2,110,138,122,220,32,15,139,145,212,40,117,132,153,159,157,57,76,251,121,100,143,74,217,153,210,56],
		"CoveredOffset":256,"NextOrdinal":3,"CommittedEnd":512
	},
	"append_status":204,"observed_signal":9,"replay_matched":true,"replay_rows":8,
	"crash_candidate_reaped":true,"restart_candidate_reaped":true,"fixture_removed":true
}`

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
		{"parity", []string{ordinaryWALCase, restoredParityCase, capacityCrashCase}, map[string]jsontext.Value{
			ordinaryWALCase: jsontext.Value(ordinaryWALJSON), restoredParityCase: jsontext.Value(restoredParityJSON),
			capacityCrashCase: jsontext.Value(capacityCrashJSON),
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
	keepMemory := func(b object, keep, remove string) {
		m := memory(b, keep)
		b["live_memory_current"] = object{
			"sample_count": 1, "observation_span_ns": m["read_end_ns"].(float64) - m["read_start_ns"].(float64),
			"min_bytes": m["bytes"], "max_bytes": m["bytes"],
		}
		delete(sample(b, remove), "cgroup_memory_current")
	}
	t.Run("historical live proof in standard mode", func(t *testing.T) {
		var wire object
		if err := jsonv1.Unmarshal(data, &wire); err != nil {
			t.Fatal(err)
		}
		capacity := at(at(wire, "cases"), capacityCase)
		for scale := range capacity["evidence"].([]any) {
			d := at(index(capacity, "evidence", scale), "diagnostics")
			delete(d, "restart")
			for pattern := range d["backfill_timelines"].([]any) {
				delete(index(d, "backfill_timelines", pattern), "terminal_page_bytes_equal")
			}
		}
		historical, err := jsonv1.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateReport(historical, id, capacityCases); err != nil {
			t.Fatalf("standard validation rejected historical live evidence: %v", err)
		}
	})
	restart := func(r object) object { return at(at(r, "diagnostics"), "restart") }
	for _, tc := range []struct {
		name string
		edit func(record, timeline object)
		want string
	}{
		{name: "explicit no timeout with immediate and delayed drain"},
		{"missing timeout verdict", func(r, _ object) { delete(r, "timed_out") }, "missing or null timed_out"},
		{"null timeout verdict", func(r, _ object) { r["timed_out"] = nil }, "missing or null timed_out"},
		{"timed-out workload", func(r, _ object) { r["timed_out"] = true }, "require timed_out to be false"},
		{"forward-compatible fields", func(_, b object) { b["extension"] = object{"value": true} }, ""},
		{"samples equal peak", func(r, b object) {
			peak := r["cgroup_peak_memory_bytes"]
			memory(b, "status_poll")["bytes"], memory(b, "overlap")["bytes"] = peak, peak
			summary := at(b, "live_memory_current")
			summary["min_bytes"], summary["max_bytes"] = peak, peak
		}, ""},
		{"current exceeds final peak but is below cap", func(r, b object) {
			abovePeak := r["cgroup_peak_memory_bytes"].(float64) + 1
			memory(b, "overlap")["bytes"] = abovePeak
			at(b, "live_memory_current")["max_bytes"] = abovePeak
		}, "above-final-memory.peak"},
		{"one overlap read", func(_, b object) {
			keepMemory(b, "overlap", "status_poll")
		}, "require multiple bracketed memory.current samples including overlap before publication"},
		{"missing overlap read", func(_, b object) {
			keepMemory(b, "status_poll", "overlap")
		}, "require a bracketed memory.current overlap sample"},
		{"start reply carries memory", func(_, b object) {
			sample(b, "compaction_start")["cgroup_memory_current"] = memory(b, "status_poll")
		}, "memory.current sample outside running status polls"},
		{"publication reply carries memory", func(_, b object) {
			sample(b, "publication")["cgroup_memory_current"] = memory(b, "overlap")
		}, "memory.current sample outside running status polls"},
		{"missing bytes", func(_, b object) { delete(memory(b, "overlap"), "bytes") }, "missing, zero, or above-final-memory.peak"},
		{"zero bytes", func(_, b object) { memory(b, "overlap")["bytes"] = 0 }, "missing, zero, or above-final-memory.peak"},
		{"read error with contradictory bytes", func(_, b object) {
			memory(b, "overlap")["error"] = "read failed"
		}, "live memory.current sampling failed: read failed"},
		{"read precedes preceding running response", func(_, b object) {
			memory(b, "overlap")["read_start_ns"] = 3400000
		}, "mis-timed or excessive memory.current samples"},
		{"read ends after request begins", func(_, b object) {
			memory(b, "overlap")["read_end_ns"] = 5500000
			at(b, "live_memory_current")["observation_span_ns"] = 3300000
		}, "mis-timed or excessive memory.current samples"},
		{"zero-duration read", func(_, b object) {
			memory(b, "overlap")["read_end_ns"] = 3700000
			at(b, "live_memory_current")["observation_span_ns"] = 1500000
		}, "mis-timed or excessive memory.current samples"},
		{"missing request time", func(_, b object) {
			delete(sample(b, "status_poll"), "request_sent_ns")
		}, "missing or invalid request timing for status_poll"},
		{"different operation generation", func(_, b object) {
			at(sample(b, "status_poll"), "status")["generation"] = 99
		}, "status_poll sample must observe the same running compaction"},
		{"different operation cutoff", func(_, b object) {
			at(sample(b, "status_poll"), "status")["covered_rows"] = 1
		}, "status_poll sample must observe the same running compaction"},
		{"complete status does not bracket", func(_, b object) {
			at(sample(b, "status_poll"), "status")["state"] = "complete"
		}, "status_poll sample must observe the same running compaction"},
		{"reported span disagrees", func(_, b object) {
			at(b, "live_memory_current")["observation_span_ns"] = 1
		}, "missing or inconsistent memory.current observation span or range"},
		{"reported range disagrees", func(_, b object) {
			at(b, "live_memory_current")["min_bytes"] = 1
		}, "missing or inconsistent memory.current observation span or range"},
		{"reported count disagrees", func(_, b object) {
			at(b, "live_memory_current")["sample_count"] = 3
		}, "missing or inconsistent memory.current observation span or range"},
		{"missing summary", func(_, b object) {
			delete(b, "live_memory_current")
		}, "missing or inconsistent memory.current observation span or range"},
		{"reported drain without zero sample", func(_, b object) {
			at(sample(b, "drain_poll"), "candidate_status_metrics")["retired_generation_pins"] = nil
		}, "fresh visibility sample must follow observed pin drain"},
		{"pin drain counter contradicts unavailable status", func(_, b object) {
			at(sample(b, "drain_poll"), "candidate_status_metrics")["unavailable"] = object{"retired_generation_pins": "missing"}
		}, "retired_generation_pins contradicts unavailable status"},
		{"drain request precedes retirement", func(_, b object) {
			sample(b, "after_retirement")["request_sent_ns"] = 10000000
		}, "pin drain sample did not follow explicit capture retirement"},
		{"late zero after a delayed first poll", func(_, b object) {
			deadline := sample(b, "capture_retired")["elapsed_ns"].(float64) + float64(capacityevidence.DrainBudget)
			first := sample(b, "after_retirement")
			first["request_sent_ns"], first["elapsed_ns"] = deadline-float64(2*time.Millisecond), deadline-float64(time.Millisecond)
			zero := sample(b, "drain_poll")
			zero["request_sent_ns"], zero["elapsed_ns"] = deadline, deadline+1
			sample(b, "fresh_visible")["elapsed_ns"] = deadline + 2
		}, "pin drain sample exceeded the drain budget"},
		{"last acknowledgment at fresh visibility", func(_, b object) {
			at(b, "acknowledged_backfill")["last_ack_ns"] = sample(b, "fresh_visible")["elapsed_ns"]
		}, ""},
		{"last acknowledgment after fresh visibility", func(_, b object) {
			at(b, "acknowledged_backfill")["last_ack_ns"] = sample(b, "fresh_visible")["elapsed_ns"].(float64) + 1
		}, "last backfill acknowledgment must occur by the fresh visibility observation"},
		{"inexact fresh walk", func(_, b object) { b["fresh_rows_checked"] = 1 }, "missing exact fresh walk parity"},
		{"inexact restart walk", func(r, _ object) { r["recovered_rows"] = 1 }, "missing completed fixture with exact restart parity"},
		{"missing restart proof", func(r, _ object) {
			delete(at(r, "diagnostics"), "restart")
		}, "completed restart proof"},
		{"restart directory changed", func(r, _ object) {
			at(restart(r), "after_walk_directory")["device"] = 9
		}, "restart directory device or inode changed"},
		{"recovered checkpoint has different generation", func(r, _ object) {
			at(restart(r), "recovered_checkpoint")["generation"] = 99
		}, "recovered checkpoint disagrees"},
		{"unmatched WAL suffix", func(r, _ object) {
			at(restart(r), "ledger")["matched_wal_suffix_rows"] = 0
		}, "expect and match the nonempty WAL suffix"},
		{"missing clustered terminal byte verdict", func(r, _ object) {
			delete(index(at(r, "diagnostics"), "backfill_timelines", 0), "terminal_page_bytes_equal")
		}, "clustered backfill: require byte equality on terminal page replay"},
		{"null scattered terminal byte verdict", func(_, b object) {
			b["terminal_page_bytes_equal"] = nil
		}, "scattered backfill: require byte equality on terminal page replay"},
		{"negative scattered terminal byte verdict", func(_, b object) {
			b["terminal_page_bytes_equal"] = false
		}, "scattered backfill: require byte equality on terminal page replay"},
		{"batch count disagrees", func(_, b object) { at(b, "acknowledged_backfill")["batches"] = 1 }, "invalid acknowledged backfill evidence"},
		{"missing store cleanup", func(r, _ object) { delete(at(r, "cleanup"), "store_removed") }, "missing candidate, subtree, or fixture store cleanup"},
		{"missing raw OOM", func(r, _ object) { delete(at(at(r, "diagnostics"), "cgroup_memory_events"), "oom_kill") }, "raw and summary cgroup OOM-kill counts"},
		{"null raw OOM", func(r, _ object) { at(at(r, "diagnostics"), "cgroup_memory_events")["oom_kill"] = nil }, "raw and summary cgroup OOM-kill counts"},
		{"quoted raw OOM", func(r, _ object) { at(at(r, "diagnostics"), "cgroup_memory_events")["oom_kill"] = "0" }, "/cgroup_memory_events/oom_kill"},
		{"nonzero raw OOM", func(r, _ object) { at(at(r, "diagnostics"), "cgroup_memory_events")["oom_kill"] = 1 }, "raw and summary cgroup OOM-kill counts"},
		{"missing summary OOM", func(r, _ object) { delete(r, "cgroup_oom_kill_count") }, "raw and summary cgroup OOM-kill counts"},
		{"null summary OOM", func(r, _ object) { r["cgroup_oom_kill_count"] = nil }, "raw and summary cgroup OOM-kill counts"},
		{"quoted summary OOM", func(r, _ object) { r["cgroup_oom_kill_count"] = "0" }, "/cgroup_oom_kill_count"},
		{"nonzero summary OOM", func(r, _ object) { r["cgroup_oom_kill_count"] = 1 }, "raw and summary cgroup OOM-kill counts"},
		{"reversed timeline order", func(r, _ object) {
			timelines := at(r, "diagnostics")["backfill_timelines"].([]any)
			timelines[0], timelines[1] = timelines[1], timelines[0]
		}, "require clustered followed by scattered backfill timelines"},
		{"duplicate timeline pattern", func(_, b object) {
			b["pattern"] = "clustered"
		}, "require clustered followed by scattered backfill timelines"},
		{"scattered predecessor did not advance past clustered publication", func(r, b object) {
			clustered := index(at(r, "diagnostics"), "backfill_timelines", 0)
			generation := at(clustered, "publication")["generation"]
			for _, raw := range b["samples"].([]any) {
				s := raw.(object)
				if c, present := s["status"].(object); present && c["state"] == "running" {
					c["generation"] = generation
				}
			}
		}, "scattered predecessor must advance past clustered publication at the seed checkpoint"},
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
			releaseErr := run(t.Context(), append(releaseArgs, "-capacity", reportPath))
			if tc.want == "" && releaseErr != nil ||
				tc.want != "" && (releaseErr == nil || !strings.Contains(releaseErr.Error(), tc.want)) {
				t.Errorf("release validator error = %v, want %q", releaseErr, tc.want)
			}
			cmd := exec.CommandContext(t.Context(), producer, "-validate-cutover", reportPath)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if tc.want == "" && err != nil ||
				tc.want != "" && (!errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), tc.want)) {
				t.Errorf("capacity validator error = %v, want %q\n%s", err, tc.want, output)
			}
		})
	}
}
