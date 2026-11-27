package capacityevidence

import (
	"fmt"
	"strings"
	"testing"
)

func TestDecodeCompactionStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, wire, metrics string
		want                CompactionObservation
	}{
		{"zero predecessor", `{"state":"running","generation":0,"covered_rows":0,"committed_rows":0}`, "",
			CompactionObservation{State: "running"}},
		{"publication with suffix", ` {"committed_rows":192,"covered_rows":128,"generation":18446744073709551615,"state":"complete","metrics": null} ` + "\n", "null",
			CompactionObservation{State: "complete", Generation: ^uint64(0), CoveredRows: 128, CommittedRows: 192}},
		{"extensions and raw metrics", `{"s\u0074ate":"running","generation":7,"covered_rows":8,"committed_rows":8,"State":"ignored","future":{"same":1,"same":2},"metrics": { "retired_generation_pins":1, "retired_generation_pins":0, "pending_compactions":3 }}`,
			`{ "retired_generation_pins":1, "retired_generation_pins":0, "pending_compactions":3 }`,
			CompactionObservation{State: "running", Generation: 7, CoveredRows: 8, CommittedRows: 8}},
		{"non-object diagnostic", `{"state":"running","generation":0,"covered_rows":0,"committed_rows":0,"metrics":[true]}`, "[true]",
			CompactionObservation{State: "running"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := []byte(tc.wire)
			got, metrics, err := DecodeCompactionStatus(body)
			clear(body)
			if err != nil || got != tc.want || string(metrics) != tc.metrics || (metrics == nil) != (tc.metrics == "") {
				t.Fatalf("status = %+v, metrics = %q, error = %v, want %+v and metrics %q", got, metrics, err, tc.want, tc.metrics)
			}
		})
	}
}

func TestDecodeCompactionStatusRejectsInvalidEnvelope(t *testing.T) {
	t.Parallel()
	const snapshot = `{"state":"complete","generation":7,"covered_rows":8,"committed_rows":10,"metrics":{"pending_compactions":0}}`
	for _, tc := range []struct{ name, wire, want string }{
		{"empty", "", "EOF"},
		{"null", "null", "expected compaction status JSON object"},
		{"array", "[]", "expected compaction status JSON object"},
		{"truncated", strings.TrimSuffix(snapshot, "}"), "unexpected EOF"},
		{"trailing value", snapshot + " {}", "unexpected trailing data after compaction status"},
		{"trailing junk", snapshot + "x", "invalid character"},
		{"invalid extension value", strings.TrimSuffix(snapshot, "}") + `,"extra":[1,]}`, "invalid character"},
		{"duplicate known member", strings.Replace(snapshot, `"generation":7`, `"generation":7,"generatio\u006e":8`, 1), `duplicate compaction status member "generation"`},
		{"duplicate metrics", strings.TrimSuffix(snapshot, "}") + `,"metrics":null}`, `duplicate compaction status member "metrics"`},
		{"duplicate extension", strings.TrimSuffix(snapshot, "}") + `,"extra":null,"extra":true}`, `duplicate compaction status member "extra"`},
		{"unknown state", strings.Replace(snapshot, "complete", "pending", 1), `unexpected state "pending"`},
		{"negative coverage", strings.Replace(snapshot, `"covered_rows":8`, `"covered_rows":-1`, 1), "invalid row counters: covered=-1 committed=10"},
		{"negative commit", strings.Replace(snapshot, `"committed_rows":10`, `"committed_rows":-1`, 1), "invalid row counters: covered=8 committed=-1"},
		{"commit behind coverage", strings.Replace(snapshot, `"committed_rows":10`, `"committed_rows":7`, 1), "invalid row counters: covered=8 committed=7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalidCompactionStatus(t, tc.wire, tc.want)
		})
	}
	for _, field := range []struct {
		name, valid, invalid, overflow string
	}{
		{"state", `"complete"`, "true", ""},
		{"generation", "7", "-1", "18446744073709551616"},
		{"covered_rows", "8", `"8"`, "9223372036854775808"},
		{"committed_rows", "10", "1.0", "9223372036854775808"},
	} {
		t.Run(field.name, func(t *testing.T) {
			t.Parallel()
			member := fmt.Sprintf("%q:%s", field.name, field.valid)
			missing := fmt.Sprintf("missing or null %q", field.name)
			assertInvalidCompactionStatus(t, strings.Replace(snapshot, member+",", "", 1), missing)
			assertInvalidCompactionStatus(t, strings.Replace(snapshot, member, fmt.Sprintf("%q:null", field.name), 1), missing)
			assertInvalidCompactionStatus(t, strings.Replace(snapshot, member, fmt.Sprintf("%q:%s", strings.ToUpper(field.name), field.valid), 1), missing)
			assertInvalidCompactionStatus(t, strings.Replace(snapshot, member, fmt.Sprintf("%q:%s", field.name, field.invalid), 1), field.name+":")
			if field.overflow != "" {
				assertInvalidCompactionStatus(t, strings.Replace(snapshot, member, fmt.Sprintf("%q:%s", field.name, field.overflow), 1), field.name+":")
			}
		})
	}
}

func assertInvalidCompactionStatus(t *testing.T, body, wantErr string) {
	t.Helper()
	got, metrics, err := DecodeCompactionStatus([]byte(body))
	if err == nil || !strings.Contains(err.Error(), wantErr) || got != (CompactionObservation{}) || metrics != nil {
		t.Fatalf("decoding %s gave %+v, metrics %q, error %v, want no observation or metrics and error containing %q", body, got, []byte(metrics), err, wantErr)
	}
}
