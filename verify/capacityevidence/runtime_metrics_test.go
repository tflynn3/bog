package capacityevidence

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
	"strings"
	"testing"
)

func TestRuntimeMetricsWireContract(t *testing.T) {
	t.Parallel()
	const known = `"publication_lag_ns":0,"staging_reserved_bytes":1,"staging_used_bytes":2,"oldest_pin_age_ns":3,"reclaimable_bytes":4,"retired_generation_pins":5,"pending_compactions":18446744073709551615,"oldest_pinned_generation":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"`
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{`+known+`,"future_metric":{"value":true},"unavailable":{"pending_compactions":"ignored by live decoder"}}`), &fields); err != nil {
		t.Fatal(err)
	}
	m, issues := DecodeRuntimeMetrics(fields)
	if len(issues) != 0 {
		t.Fatalf("live field issues = %v", issues)
	}
	wire, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{` + known + `}`; string(wire) != want {
		t.Fatalf("report metrics = %s, want %s", wire, want)
	}
	var report runtimeObservations
	if err := jsonv2.Unmarshal(wire, &report); err != nil {
		t.Fatal(err)
	}
	if err := report.validate(true); err != nil {
		t.Fatal(err)
	}
	if got := string(report.OldestPinnedGeneration); got != `"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"` {
		t.Errorf("report oldest generation = %s", got)
	}
	liveCounters := m.counters()
	for i, strict := range report.counters() {
		live := liveCounters[i]
		if *strict.dest == nil || *live.dest == nil || **strict.dest != **live.dest {
			t.Errorf("report lost live counter %s", live.name)
		}
	}
}

func TestRuntimeCounterInterpretation(t *testing.T) {
	t.Parallel()
	for _, key := range []string{
		"publication_lag_ns", "staging_reserved_bytes", "staging_used_bytes", "oldest_pin_age_ns",
		"reclaimable_bytes", "retired_generation_pins", "pending_compactions",
	} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				wire      string
				liveIssue RuntimeMetricIssue
				badReport bool
			}{
				{wire: "", liveIssue: RuntimeMetricMissing},
				{wire: "null", liveIssue: RuntimeMetricMalformed},
				{wire: "0"},
				{wire: "18446744073709551615"},
				{wire: "-0", liveIssue: RuntimeMetricMalformed, badReport: true},
				{wire: "-1", liveIssue: RuntimeMetricMalformed, badReport: true},
				{wire: "0.0", liveIssue: RuntimeMetricMalformed, badReport: true},
				{wire: "0e0", liveIssue: RuntimeMetricMalformed, badReport: true},
				{wire: `"0"`, liveIssue: RuntimeMetricMalformed, badReport: true},
				{wire: "18446744073709551616", liveIssue: RuntimeMetricMalformed, badReport: true},
				{wire: "false", liveIssue: RuntimeMetricMalformed, badReport: true},
				{wire: "{}", liveIssue: RuntimeMetricMalformed, badReport: true},
			} {
				wire := "{}"
				if tc.wire != "" {
					wire = fmt.Sprintf(`{%q:%s}`, key, tc.wire)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(wire), &fields); err != nil {
					t.Fatal(err)
				}
				_, issues := DecodeRuntimeMetrics(fields)
				if got := issues[key]; got != tc.liveIssue {
					t.Errorf("live metrics %s issue = %v, want %v", wire, got, tc.liveIssue)
				}
				var report runtimeObservations
				err := jsonv2.Unmarshal([]byte(wire), &report)
				if tc.badReport {
					if err == nil || !strings.Contains(err.Error(), key) {
						t.Errorf("report metrics %s error = %v, want error naming %s", wire, err, key)
					}
				} else if err != nil {
					t.Errorf("report metrics %s error = %v", wire, err)
				}
			}
		})
	}
}
