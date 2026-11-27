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
	m, issues, err := DecodeRuntimeMetrics([]byte(`{` + known + `,"future_metric":{"value":true,"value":false},
		"future_metric":[{"retired_generation_pins":0}],"unavailable":{"pending_compactions":"ignored by live decoder"}}`))
	if err != nil {
		t.Fatal(err)
	}
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
				_, issues, err := DecodeRuntimeMetrics([]byte(wire))
				if err != nil {
					t.Fatalf("live metrics %s: %v", wire, err)
				}
				if got := issues[key]; got != tc.liveIssue {
					t.Errorf("live metrics %s issue = %v, want %v", wire, got, tc.liveIssue)
				}
				var report runtimeObservations
				err = jsonv2.Unmarshal([]byte(wire), &report)
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

func TestRepeatedRuntimeMetricMembers(t *testing.T) {
	t.Parallel()
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, key := range []string{
		"publication_lag_ns", "staging_reserved_bytes", "staging_used_bytes", "oldest_pin_age_ns",
		"reclaimable_bytes", "retired_generation_pins", "pending_compactions", "oldest_pinned_generation",
	} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			good, other := "0", "1"
			sibling := fmt.Sprintf(`"oldest_pinned_generation":%q`, digest)
			if key == OldestPinnedGenerationMetric {
				good, other = fmt.Sprintf("%q", digest), "null"
				sibling = `"pending_compactions":0`
			}
			for _, tc := range []struct {
				name, first, last string
			}{
				{"different values", good, other},
				{"reverse order", other, good},
				{"equal values", good, good},
				{"malformed first", `{"nested":true,"nested":false}`, good},
				{"malformed last", good, "false"},
				{"three members", good, fmt.Sprintf("%s,%q:%s", other, key, good)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					// The second spelling decodes to exactly the first name.
					wire := fmt.Sprintf(`{%q:%s,%s,"\u%04x%s":%s}`, key, tc.first, sibling, key[0], key[1:], tc.last)
					m, issues, err := DecodeRuntimeMetrics([]byte(wire))
					if err != nil || issues[key] != RuntimeMetricDuplicate {
						t.Fatalf("decode %s = issues %v, error %v", wire, issues, err)
					}
					if key == OldestPinnedGenerationMetric {
						if m.OldestPinnedGeneration != nil || m.PendingCompactions == nil ||
							*m.PendingCompactions != 0 || issues[PendingCompactionsMetric] != 0 {
							t.Fatalf("generation or sibling misclassified: %+v, issues %v", m, issues)
						}
					} else {
						if m.OldestPinnedGeneration == nil || *m.OldestPinnedGeneration != digest ||
							issues[OldestPinnedGenerationMetric] != 0 {
							t.Fatalf("lost generation sibling: %+v, issues %v", m, issues)
						}
						for _, field := range m.counters() {
							if *field.dest != nil {
								t.Errorf("%s unexpectedly observed", field.name)
							}
						}
					}
				})
			}
		})
	}
}

func TestInvalidLiveRuntimeMetricsObject(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, wire string
		absent     bool
	}{
		{"omitted", "", true},
		{"null", " \r\nnull\t", true},
		{"whitespace only", " \n", false},
		{"array", "[]", false},
		{"scalar", "0", false},
		{"truncated", `{"retired_generation_pins":0`, false},
		{"invalid unknown value", `{"retired_generation_pins":0,"future":[1,]}`, false},
		{"trailing value", `{"retired_generation_pins":0} {}`, false},
		{"trailing garbage", `{"retired_generation_pins":0} x`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, issues, err := DecodeRuntimeMetrics([]byte(tc.wire))
			if err == nil || (err == ErrRuntimeMetricsAbsent) != tc.absent {
				t.Fatalf("decode error = %v, want absent = %t", err, tc.absent)
			}
			if len(issues) != len(m.counters())+1 || issues[OldestPinnedGenerationMetric] != RuntimeMetricMissing ||
				m.OldestPinnedGeneration != nil {
				t.Fatalf("decode = %+v, issues %v", m, issues)
			}
			for _, field := range m.counters() {
				if *field.dest != nil || issues[field.name] != RuntimeMetricMissing {
					t.Errorf("%s unexpectedly observed or misclassified: %v", field.name, issues)
				}
			}
		})
	}
}

func TestMalformedLiveGenerationKeepsCounter(t *testing.T) {
	t.Parallel()
	for _, value := range []string{`"\ud800"`, "\"\xff\""} {
		wire := fmt.Sprintf(`{"oldest_pinned_generation":%s,"\u0070ending_compactions":0}`, value)
		m, issues, err := DecodeRuntimeMetrics([]byte(wire))
		if err != nil || m.OldestPinnedGeneration != nil || issues[OldestPinnedGenerationMetric] != RuntimeMetricMalformed ||
			m.PendingCompactions == nil || *m.PendingCompactions != 0 || issues[PendingCompactionsMetric] != 0 {
			t.Errorf("decode %q = metrics %+v, issues %v, error %v", wire, m, issues, err)
		}
	}
}
