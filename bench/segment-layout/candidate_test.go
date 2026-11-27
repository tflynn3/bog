package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestCandidateConfig(t *testing.T) {
	t.Parallel()
	revision, sha256 := strings.Repeat("a", 40), strings.Repeat("b", 64)
	collect := []string{"-cold", "-require-live", "-bog-bin", "bog", "-bog-revision", revision}
	validate := []string{"-validate-report", "live.json", "-require-live",
		"-bog-revision", revision, "-bog-sha256", sha256, "-bog-arch", "arm64"}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"collect", append(append([]string{}, collect...), "-report", "live.json"), ""},
		{"collect without binary", collect[:2], "-require-live requires"},
		{"collect with smoke", append(append([]string{}, collect...), "-smoke"), "without -smoke"},
		{"uppercase revision", []string{"-cold", "-require-live", "-bog-bin", "bog", "-bog-revision", strings.ToUpper(revision)}, "full lowercase"},
		{"ordinary validation", []string{"-validate-report", "historical.json"}, ""},
		{"candidate validation", validate, ""},
		{"missing architecture", validate[:len(validate)-2], "strict validation requires"},
		{"stray validation option", append(append([]string{}, validate...), "-smoke"), "strict validation requires"},
		{"missing strict switch", []string{"-validate-report", "live.json", "-bog-revision", revision}, "must be used alone"},
		{"report without strict switch", []string{"-cold", "-report", "live.json"}, "-report requires"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(tc.args, "benchmark-revision", io.Discard)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("parseConfig error = %v, want %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "candidate validation" && cfg.candidate != (candidateIdentity{revision, sha256, "arm64"}) {
				t.Fatalf("candidate = %+v", cfg.candidate)
			}
		})
	}
}

func TestStrictCandidateReport(t *testing.T) {
	t.Parallel()
	expected := candidateIdentity{strings.Repeat("a", 40), strings.Repeat("b", 64), "arm64"}
	report := testColdMatrix(t)
	report.Environment["arch"] = expected.Arch
	for i := range report.Profiles {
		p := &report.Profiles[i]
		p.Live = liveReport{Status: "measured", Revision: expected.Revision, BinarySHA256: expected.SHA256}
		for _, phase := range p.Production.Phases[1:] {
			initial := p.Production.Phases[0].Rows
			p.Live.Measurements = append(p.Live.Measurements, publicationMeasurement{
				Scenario: phase.Phase, CoveredRows: phase.Rows, ElapsedNS: 100, HTTPQueryParity: true,
				WriteAmplification: writeAmplification(4096, phase.Rewrite.AppendedWALBytes, liveBoundary),
				Publication: &livePublication{
					Before:          controlReply{"complete", new(uint64(0)), &initial, &initial},
					After:           controlReply{"complete", new(uint64(1)), new(phase.Rows), new(phase.Rows)},
					PersistedParity: true,
				},
			})
		}
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*coldMatrixReport)
		want string
	}{
		{"both profiles measured", nil, ""},
		{"v7", func(r *coldMatrixReport) { r.SchemaVersion = historicalColdMatrixSchema }, "schema v8"},
		{"wrong architecture", func(r *coldMatrixReport) {
			r.Environment["arch"] = "amd64"
			for i := range r.Profiles {
				r.Profiles[i].Environment["arch"] = "amd64"
			}
		}, "expected native Linux candidate architecture"},
		{"both unavailable", func(r *coldMatrixReport) {
			for i := range r.Profiles {
				r.Profiles[i].Live.Status = "unavailable"
				r.Profiles[i].Live.Reason = "loopback compaction control returned HTTP 404"
				r.Profiles[i].Live.Measurements = nil
			}
		}, "mixed: live candidate control unavailable: loopback compaction control returned HTTP 404"},
		{"entropy unavailable", func(r *coldMatrixReport) {
			r.Profiles[1].Live.Status = "unavailable"
			r.Profiles[1].Live.Reason = "loopback compaction control returned HTTP 501"
			r.Profiles[1].Live.Measurements = nil
		}, "profile 2 disagrees on live candidate"},
		{"different revision in both profiles", func(r *coldMatrixReport) {
			for i := range r.Profiles {
				r.Profiles[i].Live.Revision = strings.Repeat("c", 40)
			}
		}, "differs from the expected identity"},
		{"different digest in both profiles", func(r *coldMatrixReport) {
			for i := range r.Profiles {
				r.Profiles[i].Live.BinarySHA256 = strings.Repeat("c", 64)
			}
		}, "differs from the expected identity"},
		{"entropy has a different binary", func(r *coldMatrixReport) {
			r.Profiles[1].Live.BinarySHA256 = strings.Repeat("c", 64)
		}, "profile 2 disagrees on live candidate"},
		{"historical live evidence", func(r *coldMatrixReport) {
			r.Profiles[1].Live.Measurements[1].Publication = nil
		}, "entropy: scattered: missing or invalid ordered live publication"},
		{"unverified persisted rows", func(r *coldMatrixReport) {
			r.Profiles[0].Live.Measurements[0].Publication.PersistedParity = false
		}, "mixed: clustered: missing or invalid ordered live publication"},
		{"missing initial generation", func(r *coldMatrixReport) {
			r.Profiles[0].Live.Measurements[0].Publication.Before.Generation = nil
		}, "mixed: clustered: missing or invalid ordered live publication"},
		{"same generation", func(r *coldMatrixReport) {
			r.Profiles[0].Live.Measurements[0].Publication.After.Generation = new(uint64(0))
		}, "mixed: clustered: missing or invalid ordered live publication"},
		{"wrong committed cutoff", func(r *coldMatrixReport) {
			*r.Profiles[0].Live.Measurements[0].Publication.After.CommittedRows++
		}, "mixed: clustered: missing or invalid ordered live publication"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var r coldMatrixReport
			if err := json.Unmarshal(data, &r); err != nil {
				t.Fatal(err)
			}
			if tc.edit != nil {
				tc.edit(&r)
			}
			raw, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			err = readBenchmarkReport(bytes.NewReader(raw), &expected)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("strict validation = %v, want %q", err, tc.want)
			}
			if tc.name == "historical live evidence" || tc.name == "both unavailable" {
				if err := validateBenchmarkReport(bytes.NewReader(raw)); err != nil {
					t.Fatalf("ordinary validation = %v", err)
				}
			}
		})
	}
}
