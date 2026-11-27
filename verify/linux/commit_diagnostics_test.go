package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestUncertainWALRequiresDirectEvidence(t *testing.T) {
	t.Parallel()
	const recoveryRequired = `{"ready":false,"reason":"recovery_required","category":"eio"}`
	for _, tc := range []struct {
		name, body string
		status     int
		want       bool
	}{
		{"explicit recovery required", recoveryRequired, 503, true},
		{"explicit confirmed rollback", `{"ready":false,"reason":"wal_rollback","category":"enospc"}`, 503, false},
		{"wrong HTTP status", recoveryRequired, 200, false},
		{"truncated readiness", recoveryRequired[:len(recoveryRequired)-1], 503, false},
		{"transport unavailable", "", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diag := newCaseDiagnostics()
			diag.Candidates = []*processEvidence{{Reaped: true, Signal: "killed"}}
			diag.observeIngest("sigkill_interrupt", ingestObservation{Lifetime: 1, Outcome: ingestTransportAmbiguous}, nil)
			if diag.UncertainWAL != nil || diag.Unavailable["uncertain_wal_outcome"] == "" {
				t.Fatal("transport ambiguity was treated as WAL uncertainty")
			}
			client := &http.Client{Transport: readinessTransport(func(*http.Request) (*http.Response, error) {
				if tc.status == 0 {
					return nil, io.EOF
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}
			obs, err := readDiskReadiness(t.Context(), client, "localhost")
			obs.Lifetime = 1
			e := &readinessEvidence{}
			diag.observeReadiness(e, obs, err)
			if e.Attempts != 1 || (err != nil) != (e.FirstFailure != nil) {
				t.Fatalf("readiness evidence = %+v, err = %v", e, err)
			}
			if tc.status != 0 && (e.Last.Status == nil || *e.Last.Status != tc.status || e.Last.Headers == nil) {
				t.Fatalf("partial response metadata missing: %+v", e.Last)
			}
			if (diag.UncertainWAL != nil) != tc.want || (diag.Unavailable["uncertain_wal_outcome"] == "") != tc.want {
				t.Fatalf("unexpected WAL outcome: %+v, unavailable = %v", diag.UncertainWAL, diag.Unavailable)
			}
			if u := diag.UncertainWAL; u != nil && (u.Category != "eio" || u.Lifetime != 1) {
				t.Fatalf("incorrect uncertainty witness: %+v", u)
			}
		})
	}
}

func TestDurabilityReportDecodingWithPartialDiagnostics(t *testing.T) {
	t.Parallel()
	const historical = `{"schema_version":1,"revision":"` + testRevision + `","fixture":"linux-release-v1","cases":{"sigkill":{"status":"pass","evidence":{"acknowledged_rows":9}},"recovery":{"status":"pass"},"enospc":{"status":"fail","detail":"probe failed","evidence":{"observed_enospc":true}}}}`
	var r report
	if err := json.Unmarshal([]byte(historical), &r); err != nil {
		t.Fatal(err)
	}
	if r.Diagnostics != nil || r.Cases["sigkill"].Status != "pass" {
		t.Fatalf("historical report = %+v", r)
	}

	d := newCaseDiagnostics()
	r.Diagnostics = map[string]*caseDiagnostics{"enospc": d}
	d.Candidates = []*processEvidence{{Reaped: true}}
	const phase = "full_to_healthy/retarget_ingest"
	d.observeIngest(phase, ingestObservation{Lifetime: 1, Rows: 2, Outcome: ingestTransportAmbiguous}, io.EOF)
	d.observeIngest(phase, ingestObservation{Lifetime: 1, Rows: 2, Outcome: ingestHTTPRejected}, errors.New(strings.Repeat("x", 1024)))
	for range 1000 {
		d.observeIngest(phase, ingestObservation{Lifetime: 1, Rows: 2, Outcome: ingestAcknowledged}, nil)
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var legacy struct {
		SchemaVersion int `json:"schema_version"`
		Cases         map[string]struct {
			Status   string       `json:"status"`
			Detail   string       `json:"detail"`
			Evidence diskEvidence `json:"evidence"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.SchemaVersion != 1 || len(legacy.Cases) != 3 || legacy.Cases["sigkill"].Evidence.AcknowledgedRows != 9 ||
		legacy.Cases["recovery"].Status != "pass" || legacy.Cases["enospc"].Status != "fail" ||
		legacy.Cases["enospc"].Detail != "probe failed" || !legacy.Cases["enospc"].Evidence.ObservedENOSPC {
		t.Fatalf("legacy decoding = %+v", legacy)
	}
	var decoded report
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	e := decoded.Diagnostics["enospc"].Ingest[phase]
	if e.Attempts != 1002 || e.FirstFailure == nil || e.FirstFailure.Outcome != ingestTransportAmbiguous ||
		e.FirstFailure.Error != "EOF" || e.FirstFailure.Status != nil || e.Last.Outcome != ingestAcknowledged {
		t.Fatalf("partial evidence was lost: %+v", e)
	}
	if len(raw) > 8<<10 {
		t.Fatalf("report grew with probe attempts: %d bytes", len(raw))
	}
}
