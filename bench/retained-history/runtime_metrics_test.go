package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/verify/capacityevidence"
)

func TestRuntimeSamplesDistinguishZeroAndUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		err  error
		want map[string]uint64
	}{
		{"absent", "", nil, nil},
		{"null", "null", nil, nil},
		{"whitespace around null", " null\n", nil, nil},
		{"non-object", "[]", nil, nil},
		{"invalid individual fields", `{"publication_lag_ns":0,"staging_reserved_bytes":-1,"staging_used_bytes":2,"oldest_pin_age_ns":"3","reclaimable_bytes":null,"retired_generation_pins":18446744073709551616}`, nil, map[string]uint64{"publication_lag_ns": 0, "staging_used_bytes": 2}},
		{"all counters", `{"publication_lag_ns":100,"staging_reserved_bytes":8,"staging_used_bytes":4,"oldest_pin_age_ns":10,"reclaimable_bytes":1,"retired_generation_pins":0,"added_later":true}`, nil, map[string]uint64{
			"publication_lag_ns": 100, "staging_reserved_bytes": 8, "staging_used_bytes": 4,
			"oldest_pin_age_ns": 10, "reclaimable_bytes": 1, "retired_generation_pins": 0,
		}},
		{"failed status", `{"publication_lag_ns":9}`, errors.New("status failed"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := readRuntimeMetrics(json.RawMessage(tc.raw), tc.err)
			fields := map[string]*uint64{
				"publication_lag_ns": m.PublicationLag, "staging_reserved_bytes": m.Reserved,
				"staging_used_bytes": m.Used, "oldest_pin_age_ns": m.OldestPin,
				"reclaimable_bytes": m.Reclaimable, "retired_generation_pins": m.RetiredPins,
			}
			for key, value := range fields {
				want, present := tc.want[key]
				if present && (value == nil || *value != want || m.Unavailable[key] != "") ||
					!present && (value != nil || m.Unavailable[key] == "") {
					t.Errorf("%s = %v, reason %q", key, value, m.Unavailable[key])
				}
			}
		})
	}
}

const testPinnedGeneration = "ff28c5b55587675a17bbff577adbea7f5aa1a2e9f149dcc29b95ebce4c02a7dd"

func TestOptionalRuntimeMetrics(t *testing.T) {
	const (
		missing      = "candidate did not expose metric"
		badPending   = "candidate pending compactions must be a nonnegative integer"
		badPin       = "candidate generation ID must be a lowercase hex SHA-256 digest or null"
		noMetrics    = "candidate did not expose metrics"
		wrongMetrics = "candidate metrics must be a JSON object"
		failedProbe  = "compaction status unavailable: probe failed"
		duplicate    = "candidate supplied a duplicate metric"
		pin          = `"` + testPinnedGeneration + `"`
	)
	for _, tc := range []struct {
		name, raw                string
		statusErr                error
		pending, oldest          string // encoded report values
		pendingReason, pinReason string
	}{
		{"legacy", `{"retired_generation_pins":0}`, nil, "null", "null", missing, missing},
		{"empty metrics", `{}`, nil, "null", "null", missing, missing},
		{"no metrics", "", nil, "null", "null", noMetrics, noMetrics},
		{"null metrics", "null", nil, "null", "null", noMetrics, noMetrics},
		{"invalid metrics", `[]`, nil, "null", "null", wrongMetrics, wrongMetrics},
		{"invalid JSON", `{`, nil, "null", "null", wrongMetrics, wrongMetrics},
		{"idle without pins", `{"pending_compactions":0,"oldest_pinned_generation": null }`, nil, "0", "null", "", ""},
		{"active with pins", `{"pending_compactions":2,"oldest_pinned_generation":` + pin + `}`, nil, "2", pin, "", ""},
		{"idle with pins", `{"pending_compactions":0,"oldest_pinned_generation":` + pin + `}`, nil, "0", pin, "", ""},
		{"active without pins", `{"pending_compactions":1,"oldest_pinned_generation":null}`, nil, "1", "null", "", ""},
		{"max pending count", `{"pending_compactions":18446744073709551615}`, nil, "18446744073709551615", "null", "", missing},
		{"negative count", `{"pending_compactions":-1,"oldest_pinned_generation":` + pin + `}`, nil, "null", pin, badPending, ""},
		{"overflowing count", `{"pending_compactions":18446744073709551616,"oldest_pinned_generation":null}`, nil, "null", "null", badPending, ""},
		{"fractional count", `{"pending_compactions":0.5}`, nil, "null", "null", badPending, missing},
		{"quoted count", `{"pending_compactions":"0"}`, nil, "null", "null", badPending, missing},
		{"null count", `{"pending_compactions":null}`, nil, "null", "null", badPending, missing},
		{"numeric generation", `{"pending_compactions":0,"oldest_pinned_generation":0}`, nil, "0", "null", "", badPin},
		{"array generation", `{"oldest_pinned_generation":[0]}`, nil, "null", "null", missing, badPin},
		{"empty generation", `{"oldest_pinned_generation":""}`, nil, "null", "null", missing, badPin},
		{"short digest", `{"oldest_pinned_generation":"abcd"}`, nil, "null", "null", missing, badPin},
		{"uppercase digest", `{"oldest_pinned_generation":"` + strings.ToUpper(testPinnedGeneration) + `"}`, nil, "null", "null", missing, badPin},
		{"nonhex digest", `{"oldest_pinned_generation":"` + strings.Repeat("g", 64) + `"}`, nil, "null", "null", missing, badPin},
		{"repeated pending count", `{"pending_compactions":1,"pending_compactions":0,"oldest_pinned_generation":` + pin + `}`, nil, "null", pin, duplicate, ""},
		{"repeated generation", `{"pending_compactions":0,"oldest_pinned_generation":` + pin + `,"oldest_pinned_generation":null}`, nil, "0", "null", "", duplicate},
		{"failed probe", `{"pending_compactions":0,"oldest_pinned_generation":null}`, errors.New("probe failed"), "null", "null", failedProbe, failedProbe},
		{"failed probe with duplicates", `{"pending_compactions":0,"pending_compactions":1,"oldest_pinned_generation":null}`, errors.New("probe failed"), "null", "null", failedProbe, failedProbe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := readRuntimeMetrics(json.RawMessage(tc.raw), tc.statusErr)
			if pending, pin := m.Unavailable[pendingCompactionsMetric], m.Unavailable[oldestPinnedGenerationMetric]; pending != tc.pendingReason || pin != tc.pinReason {
				t.Fatalf("unavailable pending = %q, oldest pin = %q, want %q and %q", pending, pin, tc.pendingReason, tc.pinReason)
			}
			data, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			if pending, oldest := string(wire[pendingCompactionsMetric]), string(wire[oldestPinnedGenerationMetric]); pending != tc.pending || oldest != tc.oldest {
				t.Fatalf("encoded pending = %q, oldest pin = %q, want %q and %q", pending, oldest, tc.pending, tc.oldest)
			}
		})
	}
}

func TestLiveRuntimeMetricsRoundTrip(t *testing.T) {
	t.Parallel()
	const pin = `"` + testPinnedGeneration + `"`
	for _, tc := range []struct {
		name, start, held, drained, problem string
	}{
		{
			name:    "zero pending and a live pin after predecessor drain",
			start:   `{"pending_compactions":0,"oldest_pinned_generation":null,"future_metric":42}`,
			held:    `{"retired_generation_pins":1,"oldest_pinned_generation":` + pin + `}`,
			drained: `{"retired_generation_pins":0,"oldest_pinned_generation":` + pin + `}`,
		},
		{
			name:    "malformed diagnostics beside valid pin counts",
			start:   `[]`,
			held:    `{"retired_generation_pins":1,"oldest_pinned_generation":false}`,
			drained: `{"retired_generation_pins":0,"pending_compactions":"0"}`,
		},
		{
			name:    "duplicate diagnostics beside valid pin counts",
			held:    `{"retired_generation_pins":1,"oldest_pinned_generation":` + pin + `,"oldest_pinned_generation":null}`,
			drained: `{"retired_generation_pins":0,"pending_compactions":1,"pending_compactions":0}`,
		},
		{
			name:    "omitted and malformed diagnostics cannot substitute for retired pin zero",
			held:    `{"retired_generation_pins":1}`,
			drained: `{"retired_generation_pins":-1,"pending_compactions":0,"oldest_pinned_generation":null}`,
			problem: "fresh visibility sample must follow observed pin drain",
		},
		{
			name:    "live generation cannot substitute for positive retired pin count",
			held:    `{"retired_generation_pins":null,"oldest_pinned_generation":` + pin + `}`,
			drained: `{"retired_generation_pins":0}`,
			problem: "before_retirement must observe the positive held pin count",
		},
		{
			name:    "duplicate held pins ending in a positive count",
			held:    `{"retired_generation_pins":0,"retired_generation_pins":1}`,
			drained: `{"retired_generation_pins":0}`,
			problem: "before_retirement must observe the positive held pin count",
		},
		{
			name:    "duplicate drain pins ending in zero",
			held:    `{"retired_generation_pins":1}`,
			drained: `{"retired_generation_pins":1,"retired_generation_pins":0}`,
			problem: "fresh visibility sample must follow observed pin drain",
		},
		{
			name:    "duplicate drain pins starting at zero",
			held:    `{"retired_generation_pins":1}`,
			drained: `{"retired_generation_pins":0,"retired_generation_pins":1}`,
			problem: "fresh visibility sample must follow observed pin drain",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := passingCutoverReport()
			record := &r.Cases[capacityCase].Evidence[0]
			samples := record.Diagnostics.Backfills[0].Samples
			for i := range samples {
				var raw string
				switch samples[i].Event {
				case "compaction_start":
					raw = tc.start
				case "before_retirement":
					raw = tc.held
				case "after_retirement":
					raw = tc.drained
				default:
					continue
				}
				samples[i].Candidate = readRuntimeMetrics(json.RawMessage(raw), nil)
			}
			data, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			for _, err := range []error{record.validateCutover(), capacityevidence.ValidateReport(data)} {
				if tc.problem == "" && err != nil ||
					tc.problem != "" && (err == nil || !strings.Contains(err.Error(), tc.problem)) {
					t.Errorf("strict validation error = %v, want %q", err, tc.problem)
				}
			}
		})
	}
}

func TestLivePinProbeWithRepeatedMetrics(t *testing.T) {
	t.Parallel()
	const (
		positive = `"retired_generation_pins":1`
		zero     = `"retired_generation_pins":0`
	)
	for _, tc := range []struct {
		name    string
		replies []string
	}{
		{"reject repeated held count", []string{zero + "," + positive}},
		{"skip zero as last duplicate", []string{positive, positive + "," + zero, zero}},
		{"skip zero as first duplicate", []string{positive, zero + "," + positive, zero}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(calls.Add(1)) - 1
				if r.Method != http.MethodGet || r.URL.Query().Get("action") != "status" || i >= len(tc.replies) {
					t.Errorf("unexpected status probe %d: %s %s", i, r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, err := fmt.Fprintf(w, `{"state":"complete","generation":4,"covered_rows":128,"committed_rows":192,"metrics":{%s,"pending_compactions":0}}`, tc.replies[i])
				if err != nil {
					t.Errorf("write status reply: %v", err)
				}
			}))
			defer server.Close()
			s := newService(server.URL)
			defer s.client.CloseIdleConnections()
			p := backfillProbe{
				backfillTimeline: newBackfillTimeline("clustered", 64, 128, 192),
				service:          s,
				control:          "/compact",
				dir:              t.TempDir(),
				origin:           time.Now(),
			}
			p.Publication = &publicationProof{Generation: 4}
			heldErr := p.observeHeldPin(t.Context())
			duplicateIndex := 0
			if len(tc.replies) == 1 {
				if heldErr == nil || p.PinDrain.Held != nil {
					t.Fatalf("ambiguous held count accepted: proof %+v, error %v", p.PinDrain, heldErr)
				}
			} else {
				if heldErr != nil {
					t.Fatal(heldErr)
				}
				retiredAt := p.boundary("capture_retired")
				if err := p.observeDrain(t.Context(), retiredAt, drainBudget); err != nil {
					t.Fatal(err)
				}
				if p.PinDrain.Status != "observed" || p.PinDrain.After == nil || *p.PinDrain.After != 0 {
					t.Fatalf("unambiguous zero did not establish drain: %+v", p.PinDrain)
				}
				duplicateIndex = 2 // Held status, retirement, then the first drain status.
			}
			if got := int(calls.Load()); got != len(tc.replies) {
				t.Fatalf("issued %d status probes, want %d", got, len(tc.replies))
			}
			if len(p.Samples) <= duplicateIndex {
				t.Fatalf("missing ambiguous status sample: %+v", p.Samples)
			}
			m := p.Samples[duplicateIndex].Candidate
			if m.RetiredPins != nil || m.Unavailable["retired_generation_pins"] != "candidate supplied a duplicate metric" ||
				m.PendingCompactions == nil || *m.PendingCompactions != 0 || m.Unavailable[pendingCompactionsMetric] != "" {
				t.Fatalf("ambiguous pin count or valid sibling misclassified: %+v", m)
			}
		})
	}
}
