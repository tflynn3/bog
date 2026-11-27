package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestQualificationValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*incrementalReport)
		want   string
	}{
		{"complete", func(*incrementalReport) {}, ""},
		{"version", func(r *incrementalReport) { r.SchemaVersion = 3 }, "header"},
		{"missing revision", func(r *incrementalReport) { r.Revision = "" }, "header"},
		{"unknown revision", func(r *incrementalReport) { r.Revision = "unknown" }, "header"},
		{"cache", func(r *incrementalReport) { delete(r.Environment, "cache_policy") }, "header"},
		{"baseline", func(r *incrementalReport) { r.Baseline.Name = "" }, "production v1"},
		{"missing target", func(r *incrementalReport) { r.Layouts = r.Layouts[:8] }, "nine"},
		{"duplicate target", func(r *incrementalReport) { r.Layouts[0] = r.Layouts[1] }, "duplicate"},
		{"unknown codec", func(r *incrementalReport) { r.Layouts[0].Codec = "snappy" }, "unexpected"},
		{"wrong phase", func(r *incrementalReport) { r.Baseline.Publications[2].Phase = "forward" }, "clustered"},
		{"wrong cutoff", func(r *incrementalReport) { r.Layouts[0].Publications[3].Rows++ }, "scattered"},
		{"missing dense", func(r *incrementalReport) {
			p := &r.Layouts[0].Publications[0]
			p.Queries = p.Queries[:1]
		}, "sparse and dense"},
		{"missing decode", func(r *incrementalReport) { r.Layouts[0].Publications[0].Decode = nil }, "decode sample"},
		{"run count", func(r *incrementalReport) { r.Fixture.Runs++ }, "run count"},
		{"oracle mismatch", func(r *incrementalReport) { r.Layouts[1].Publications[2].Queries[1].MatchingRows++ }, "query identity"},
		{"wrong p95", func(r *incrementalReport) { r.Baseline.Publications[2].Queries[0].QueryP95NS++ }, "sample summary"},
		{"wrong peak RSS", func(r *incrementalReport) { r.Baseline.Publications[0].Queries[1].PeakRSSBytes++ }, "sample summary"},
		{"idle overlap", func(r *incrementalReport) { sampleAt(r, 0).OverlapNS = 1 }, "invalid sample"},
		{"no overlap", func(r *incrementalReport) { sampleAt(r, 1).OverlapNS = 0 }, "invalid sample"},
		{"impossible overlap", func(r *incrementalReport) { sampleAt(r, 1).OverlapNS = 21 }, "invalid sample"},
		{"zero write accounting", func(r *incrementalReport) { sampleAt(r, 2).WriteAmplification.WrittenBytes = 0 }, "invalid sample"},
		{"unmatched WAL", func(r *incrementalReport) {
			sampleAt(r, 2).WriteAmplification = writeAmplification(4096, 128, physicalBoundary)
		}, "invalid sample"},
		{"wrong physical ratio", func(r *incrementalReport) { sampleAt(r, 3).WriteAmplification.Ratio++ }, "invalid sample"},
		{"stale sparse summary", func(r *incrementalReport) {
			r.Layouts[0].Publications[3].QueryCompaction.OverlapNS[0]--
		}, "summary differs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			report := validQualification()
			tc.change(&report)
			data, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			err = validateQualificationJSON(bytes.NewReader(data))
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("validation %v, want %q", err, tc.want)
			}
		})
	}
}

func TestQualificationRejectsTrailingJSON(t *testing.T) {
	var data bytes.Buffer
	for range 2 {
		if err := json.NewEncoder(&data).Encode(validQualification()); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateQualificationJSON(&data); err == nil {
		t.Fatal("accepted two reports")
	}
}

func TestQualificationRequiresPositiveDecodeMeasurements(t *testing.T) {
	data, err := json.Marshal(validQualification())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"allocations", "allocation_bytes", "peak_rss_bytes", "oversized_segments",
		"peak_ordinary_working_bytes", "ordinary_working_bound_bytes",
		"peak_oversized_working_bytes", "peak_working_bytes", "working_bound_bytes",
	} {
		for _, tc := range []struct {
			name  string
			value any
		}{
			{name: "negative", value: -1},
			{name: "zero", value: 0},
			{name: "omitted"},
		} {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				var report map[string]any
				if err := json.Unmarshal(data, &report); err != nil {
					t.Fatal(err)
				}
				layout := report["layouts"].([]any)[0].(map[string]any)
				pub := layout["publications"].([]any)[0].(map[string]any)
				decode := pub["decode"].(map[string]any)
				if _, ok := decode[field]; !ok {
					t.Fatalf("valid fixture omits %s", field)
				}
				if tc.value == nil {
					delete(decode, field)
				} else {
					decode[field] = tc.value
				}
				mutated, err := json.Marshal(report)
				if err != nil {
					t.Fatal(err)
				}
				if err := validateQualificationJSON(bytes.NewReader(mutated)); err == nil {
					t.Fatalf("accepted %s %s", tc.name, field)
				}
			})
		}
	}
}

func TestQualificationDecodeWorkingSetAccounting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*decodeWorkingSet)
		valid  bool
	}{
		{"aggregate below oversized peak", func(d *decodeWorkingSet) { d.PeakWorkingBytes-- }, false},
		{"aggregate above both peaks", func(d *decodeWorkingSet) { d.PeakWorkingBytes++ }, false},
		{"aggregate below ordinary peak", func(d *decodeWorkingSet) {
			d.PeakOversizedBytes = d.PeakOrdinaryBytes - 1
			d.PeakWorkingBytes = d.PeakOversizedBytes
		}, false},
		{"ordinary bound differs from target bound", func(d *decodeWorkingSet) { d.OrdinaryBoundBytes-- }, false},
		{"ordinary peak above ordinary bound", func(d *decodeWorkingSet) {
			d.PeakOrdinaryBytes = d.OrdinaryBoundBytes + 1
		}, false},
		{"overall bound below measured peak", func(d *decodeWorkingSet) {
			d.WorkingBoundBytes = d.PeakWorkingBytes - 1
		}, false},
		{"overall bound below ordinary bound", func(d *decodeWorkingSet) {
			d.PeakOversizedBytes = d.OrdinaryBoundBytes - 1
			d.PeakWorkingBytes = d.PeakOversizedBytes
			d.WorkingBoundBytes = d.PeakWorkingBytes
		}, false},
		{"too many oversized segments", func(d *decodeWorkingSet) { d.OversizedSegments++ }, false},
		{"oversized peak at overall bound", func(d *decodeWorkingSet) {
			d.WorkingBoundBytes = d.PeakWorkingBytes
		}, true},
		{"ordinary peak at both bounds", func(d *decodeWorkingSet) {
			d.PeakOversizedBytes = d.OrdinaryBoundBytes - 1
			d.PeakOrdinaryBytes = d.OrdinaryBoundBytes
			d.PeakWorkingBytes = d.PeakOrdinaryBytes
			d.WorkingBoundBytes = d.OrdinaryBoundBytes
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			report := validQualification()
			// Check a later phase and layout so the rule covers the entire matrix.
			tc.change(&report.Layouts[8].Publications[3].Decode.decodeWorkingSet)
			data, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			err = validateQualificationJSON(bytes.NewReader(data))
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "zstd/262144: scattered: missing or invalid decode sample") {
				t.Fatalf("expected a decode sample error for the scattered publication, got %v", err)
			}
		})
	}
}

func sampleAt(r *incrementalReport, phase int) *qualificationSample {
	return &r.Layouts[0].Publications[phase].Queries[0].Samples[0]
}

func validQualification() incrementalReport {
	const batches = 1
	initial := 5 + batchPattern[0]
	r := incrementalReport{
		SchemaVersion: 4, Mode: "incremental", Revision: "revision",
		Environment: map[string]any{"os": "linux", "cache_policy": qualificationCache},
		Fixture:     incrementalFixtureReport{"mixed-successive-v1", "custom", 44, batches, 1},
	}
	publications := func(target int) (pubs []publicationResult) {
		for i, phase := range qualificationPhases {
			p := publicationResult{
				Phase: phase, Rows: initial + 4*i, AppendedRows: 4, Segments: i + 1, ManifestBytes: 100,
				segmentBytes: segmentBytes{ReusedBytes: int64(20 * i), RewrittenBytes: 20, RetainedBytes: int64(20 * (i + 1))},
			}
			if target != 0 {
				p.Decode = &decodeSample{
					decodeWorkingSet: decodeWorkingSet{
						PeakOrdinaryBytes:  target / 2,
						OrdinaryBoundBytes: decodeBound(target),
						PeakOversizedBytes: 4 << 20,
						PeakWorkingBytes:   4 << 20,
						WorkingBoundBytes:  12 << 20,
						OversizedSegments:  1,
					},
					Allocations: 100, AllocationBytes: 8 << 20, PeakRSSBytes: 16 << 20,
				}
			}
			overlap := int64(12)
			if i == 0 {
				overlap = 0
				p.AppendedRows = initial
			} else {
				p.QueryCompaction = &compactionLatency{"sparse", 1, []int64{20}, []int64{overlap}, 20}
			}
			for j, name := range qualificationQueries {
				sample := qualificationSample{20, 25, overlap, 8192, writeAmplification(4096, 256, physicalBoundary)}
				scan := qualificationScan{Query: name, MatchingRows: j + 1, Samples: []qualificationSample{sample}}
				scan.summarize()
				p.Queries = append(p.Queries, scan)
			}
			pubs = append(pubs, p)
		}
		return pubs
	}
	r.Baseline = productionLayout{productionV1, publications(0)}
	for _, target := range segmentTargets {
		for c := rawCodec; c <= zstdCodec; c++ {
			r.Layouts = append(r.Layouts, incrementalLayout{(layout{codec: c}).name(), target, publications(target)})
		}
	}
	return r
}
