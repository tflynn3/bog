package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestColdMatrixReport(t *testing.T) {
	t.Parallel()
	report := testColdMatrix(t)
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*coldMatrixReport)
	}{
		{"complete", nil},
		{"single profile", func(r *coldMatrixReport) { r.Profiles = r.Profiles[:1] }},
		{"duplicate profile", func(r *coldMatrixReport) { r.Profiles[1] = r.Profiles[0] }},
		{"swapped profiles", func(r *coldMatrixReport) { r.Profiles[0], r.Profiles[1] = r.Profiles[1], r.Profiles[0] }},
		{"different batch count", func(r *coldMatrixReport) { r.Profiles[1].Fixture.Batches++ }},
		{"different seed", func(r *coldMatrixReport) { r.Profiles[1].Fixture.Seed++ }},
		{"different sample count", func(r *coldMatrixReport) { r.Profiles[1].Fixture.SparseRuns++ }},
		{"changed cache procedure", func(r *coldMatrixReport) { r.Profiles[1].Environment["cache_policy"] = "warm" }},
		{"changed selectivity schedule", func(r *coldMatrixReport) { r.Protocol.Probes[1].End = r.Protocol.Probes[0].End }},
		{"changed selectivity count", func(r *coldMatrixReport) { pairedSparse(&r.Profiles[0]).Selectivity.TimeWindowRows-- }},
		{"different run identity", func(r *coldMatrixReport) { r.Profiles[1].Pairing.RunID = strings.Repeat("02", 16) }},
		{"foreign sample", func(r *coldMatrixReport) {
			pairedSparse(&r.Profiles[0]).Samples[0] = pairedSparse(&r.Profiles[1]).Samples[0]
		}},
		{"inconsistent budgets", func(r *coldMatrixReport) { r.Protocol.Budgets.RewriteBytesPerBackfillByte++ }},
		{"missing budgets", func(r *coldMatrixReport) { r.Protocol.Budgets = nil }},
		{"cold p95 outside budget", func(r *coldMatrixReport) { r.Protocol.Budgets.ColdP95NS = 10 }},
		{"rewrite outside budget", func(r *coldMatrixReport) {
			a := r.Profiles[0].Production.Phases[1].Rewrite
			*a = writeAmplification(a.AppendedWALBytes*coldRewriteLimit+1, a.AppendedWALBytes, a.Boundary)
		}},
		{"v1 decode outside budget", func(r *coldMatrixReport) {
			q := &r.Profiles[0].Production.Phases[0].Queries[0]
			for i := range q.Samples {
				q.Samples[i].DecodeBytes = new(coldV1DecodeLimit + 1)
				q.Samples[i].ReadBytes = coldV1DecodeLimit + 1
			}
			q.summarize()
		}},
		{"missing v1 decode sample", func(r *coldMatrixReport) {
			r.Profiles[0].Production.Phases[0].Queries[0].Samples[0].DecodeBytes = nil
		}},
		{"inconsistent v1 decode sample", func(r *coldMatrixReport) {
			q := &r.Profiles[0].Production.Phases[0].Queries[0]
			q.Samples[1].DecodeBytes = new(*q.Samples[0].DecodeBytes + 1)
			q.summarize()
		}},
		{"wrong stored payload inventory", func(r *coldMatrixReport) { r.Profiles[0].Experimental[1].Phases[0].PayloadSizes[0]++ }},
		{"wrong backfill timeline", func(r *coldMatrixReport) { r.Profiles[0].Backfills[1].DistinctTimestamps++ }},
		{"wrong stored bytes", func(r *coldMatrixReport) { *r.Profiles[0].Experimental[0].Phases[0].StoredBytes++ }},
		{"wrong fan-out", func(r *coldMatrixReport) { pairedSparse(&r.Profiles[0]).Samples[0].Scan.SegmentsVisited-- }},
		{"wrong index bytes", func(r *coldMatrixReport) { pairedSparse(&r.Profiles[0]).Samples[0].Scan.IndexBytes++ }},
		{"wrong decode bound", func(r *coldMatrixReport) { pairedSparse(&r.Profiles[0]).Samples[0].Scan.Decode.WorkingBoundBytes++ }},
		{"recomputed peak exceeds actual block bound", func(r *coldMatrixReport) {
			q := &r.Profiles[0].Experimental[1].Phases[0].Queries[0]
			s := &q.Samples[0]
			d := &s.Scan.Decode
			d.PeakOrdinaryBytes = d.OrdinaryBoundBytes + 1
			d.PeakWorkingBytes = max(d.PeakOrdinaryBytes, d.PeakOversizedBytes)
			*s.DecodeBytes = d.PeakWorkingBytes
			q.summarize()
		}},
		{"wrong paired p95", func(r *coldMatrixReport) { r.Profiles[0].Pairing.Comparisons[0].Tradeoffs.ColdP95++ }},
		{"wrong paired stored bytes", func(r *coldMatrixReport) { r.Profiles[0].Pairing.Comparisons[0].Tradeoffs.StoredBytes++ }},
		{"missing paired fan-out", func(r *coldMatrixReport) { r.Profiles[0].Pairing.Comparisons[0].Tradeoffs = nil }},
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
			err = validateBenchmarkReport(bytes.NewReader(raw))
			if (err == nil) != (tc.edit == nil) {
				t.Fatalf("validation = %v", err)
			}
			if strings.Contains(tc.name, "outside budget") && !strings.Contains(err.Error(), "budget") {
				t.Fatalf("expected a budget violation, got %v", err)
			}
		})
	}
	for _, counter := range []struct{ field, value string }{
		{"storage_read_bytes", "0"}, {"storage_read_bytes_p95", "0"}, {"process_read_bytes_p95", "8000"},
		{"segments_decoded_delta", "0"}, {"peak_oversized_working_bytes", "0"}, {"reused_bytes", "0"}, {"retired_bytes", "0"},
		{"bloom_metadata_bytes", "0"}, {"matching_rows", "0"}, {"decode_bytes", "0"}, {"decode_bytes", "4096"},
	} {
		field, value := counter.field, counter.value
		original := []byte(`"` + field + `":` + value + ",")
		if !bytes.Contains(data, original) {
			t.Fatalf("fixture lacks counter %s", original)
		}
		for _, edit := range []struct {
			name, replacement string
		}{
			{"missing", ""},
			{"null", `"` + field + `":null,`},
			{"duplicate", string(original) + string(original)},
			{"conflicting duplicate", `"` + field + `":1,` + string(original)},
			{"case alias", `"` + strings.ToUpper(field) + `":` + value + "," + string(original)},
		} {
			t.Run(edit.name+" "+field+"="+value, func(t *testing.T) {
				t.Parallel()
				raw := bytes.Replace(data, original, []byte(edit.replacement), 1)
				if err := validateBenchmarkReport(bytes.NewReader(raw)); err == nil {
					t.Fatal("accepted malformed counter")
				}
			})
		}
	}
	t.Run("historical v7 matrix", func(t *testing.T) {
		var r coldMatrixReport
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatal(err)
		}
		r.SchemaVersion = historicalColdMatrixSchema
		r.Protocol.Budgets = nil
		for i := range r.Profiles {
			p := &r.Profiles[i]
			p.SchemaVersion = historicalColdMatrixSchema
			p.Backfills = nil
			for _, l := range append([]coldLayout{p.Production}, p.Experimental...) {
				for j := range l.Phases {
					l.Phases[j].PayloadSizes = nil
				}
			}
			for _, phase := range p.Production.Phases {
				for i := range phase.Queries {
					q := &phase.Queries[i]
					for j := range q.Samples {
						q.Samples[j].DecodeBytes = nil
					}
					q.summarize()
				}
			}
		}
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateBenchmarkReport(bytes.NewReader(raw)); err != nil {
			t.Fatal(err)
		}
	})
}

func testColdMatrix(t *testing.T) coldMatrixReport {
	t.Helper()
	env := map[string]any{"os": "linux", "cache_policy": coldCachePolicy}
	runID := strings.Repeat("01", 16)
	report := coldMatrixReport{
		SchemaVersion: coldMatrixSchema, Mode: "cold-matrix", Revision: "test", RunID: runID,
		Environment: env, Protocol: matrixProtocol("mixed", 2),
	}
	report.Protocol.Budgets = new(matrixBudgets(time.Minute))
	for _, profile := range report.Protocol.ProfileOrder {
		p := testColdReport()
		p.SchemaVersion, p.Fixture.Profile, p.Environment = coldMatrixSchema, profile, env
		input := fixture(p.Fixture.Batches, profile == "entropy", p.Fixture.Seed)
		for _, burst := range input.backfills() {
			backfill, err := describeColdBackfill(burst)
			if err != nil {
				t.Fatal(err)
			}
			p.Backfills = append(p.Backfills, backfill)
		}
		p.Pairing = &coldPairing{RunID: runID, FixtureSHA256: coldFixtureDigest(p.Fixture, input)}
		for i, l := range append([]coldLayout{p.Production}, p.Experimental...) {
			var before snapshot
			var dir string
			if i > 0 {
				dir = t.TempDir()
			}
			for pi, burst := range append([]backfill{{name: "initial"}}, input.backfills()...) {
				pub := &l.Phases[pi]
				pub.Queries = nil
				rows, appended := input.rows, input.rows
				var denominator uint64
				if pi > 0 {
					rows = input.withBackfill(burst)
					appended = rows[len(input.rows):]
					denominator = p.Backfills[pi-1].AppendedWALBytes
					*pub.Rewrite = writeAmplification(pub.Rewrite.WrittenBytes, denominator, pub.Rewrite.Boundary)
				}
				var snap snapshot
				if i > 0 {
					layout := layouts[i-1]
					path, err := publishSegments(t.Context(), dir, burst.name, before, appended, layout, blockTarget)
					if err != nil {
						t.Fatal(err)
					}
					snap, err = readSnapshot(path)
					if err != nil {
						t.Fatal(err)
					}
					pub.Segments, pub.BloomBytes = len(snap.Segments), 0
					if layout.prune {
						pub.BloomBytes = int64(pub.Segments * len(summary{}))
					}
					pub.segmentBytes, err = accountSegments(before, snap)
					if err != nil {
						t.Fatal(err)
					}
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					pub.ManifestBytes = info.Size()
					if pi == 0 {
						before = snap
					} else {
						*pub.Rewrite = writeAmplification(uint64(pub.RewrittenBytes+pub.ManifestBytes), denominator, coldRewriteBoundary)
					}
				}
				if i > 0 {
					pub.PayloadSizes = coldPayloadSizes(snap)
				}
				pub.StoredBytes = new(pub.storedBytes())
				for _, q := range coldQueryPlan(coldMatrixSchema) {
					want := reference(rows, q)
					result := coldQueryReport{Query: q.name, Literal: q.literal, Start: q.start, End: q.end, Matches: len(want),
						Selectivity: new(measureColdSelectivity(rows, q, len(want)))}
					for n := range coldQueryRuns(coldMatrixSchema, q, p.Fixture.SparseRuns) {
						var s coldSample
						if i > 0 {
							got, sample, err := scanColdSnapshot(t.Context(), dir, snap, q)
							if err != nil {
								t.Fatal(err)
							}
							if err := checkSnapshotRows(got, want); err != nil {
								t.Fatalf("%s/%s/%s/%s: %v", profile, l.Name, burst.name, q.name, err)
							}
							s = sample
							s.ReadBytes = s.Scan.IndexBytes + s.Payload.Bytes
						}
						s.ElapsedNS, s.PeakRSSBytes, s.GoAllocationBytes = int64(10+i+n), 1000, 1000
						if i == 0 {
							s.ReadBytes = 8000
							s.DecodeBytes = new(coldV1PageBytes)
						}
						s.Proof = &coldSampleProof{Round: n + 1, PairID: p.Pairing.pairID(burst.name, q, n+1),
							Layout: l.Name, RowsSHA256: coldRowsDigest(want)}
						result.Samples = append(result.Samples, s)
					}
					result.summarize()
					pub.Queries = append(pub.Queries, result)
				}
			}
		}
		if err := p.sealPairs(t.Context(), input); err != nil {
			t.Fatal(err)
		}
		if len(p.Pairing.Comparisons) != 72 {
			t.Fatalf("incorrect comparison count: %d", len(p.Pairing.Comparisons))
		}
		report.Profiles = append(report.Profiles, p)
	}
	return report
}

func TestColdBoundsRejectMalformedBlock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	q := query{start: firstTime, end: endTime}
	rows := []checkpoint.BlockRow{
		{Row: store.Row{TS: epoch, Line: strings.Repeat("a", blockTarget)}},
		{Row: store.Row{TS: epoch, Line: "second"}, Ordinal: 1},
	}
	for _, l := range layouts[:3] {
		t.Run(l.name(), func(t *testing.T) {
			t.Parallel()
			name := l.name() + ".segment"
			path := filepath.Join(dir, name)
			n, err := writeGeneration(path, [][]checkpoint.BlockRow{rows}, l)
			if err != nil {
				t.Fatal(err)
			}
			ref := segmentRef{Name: name, FileBytes: n, RawBytes: rawBlockBytes(rows)}
			for _, tc := range []struct {
				name string
				ref  segmentRef
			}{
				{"oversized multi-row", ref},
				{"declared raw length too small", segmentRef{Name: name, FileBytes: n, RawBytes: blockTarget}},
				{"declared raw length over ceiling", segmentRef{Name: name, FileBytes: n, RawBytes: maxBlock + 1}},
				{"wrong file size", segmentRef{Name: name, FileBytes: n + 1, RawBytes: ref.RawBytes}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, sample, err := scanColdSnapshot(t.Context(), dir, snapshot{[]segmentRef{tc.ref}}, q)
					if err == nil || got != nil || sample.Scan != nil {
						t.Fatalf("malformed segment returned sample=%+v, error=%v", sample, err)
					}
				})
			}
		})
	}
}

// The index and snapshot agree on a smaller destination. The compressed
// payload still expands to the original length and must fail inside the codec.
func TestColdCompressedOutputCannotExceedDeclaredSize(t *testing.T) {
	t.Parallel()
	q := query{start: firstTime, end: endTime}
	rows := []checkpoint.BlockRow{{Row: store.Row{TS: epoch, Line: strings.Repeat("compressible", 4096)}}}
	for _, c := range []codec{lz4Codec, zstdCodec} {
		l := layout{codec: c}
		t.Run(l.name(), func(t *testing.T) {
			t.Parallel()
			raw, err := checkpoint.EncodeBlock(rows)
			if err != nil {
				t.Fatal(err)
			}
			data := layoutFixture(t, l, raw)
			index := data[headerSize : headerSize+l.indexWidth()]
			limit := len(raw) / 2
			if int(le.Uint32(index[28:])) >= limit {
				t.Fatal("fixture did not take the compressed path")
			}
			le.PutUint32(index[24:], uint32(limit))
			seal(index)
			dir := t.TempDir()
			const name = "bounded.segment"
			if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
				t.Fatal(err)
			}
			current := snapshot{[]segmentRef{{Name: name, FileBytes: int64(len(data)), RawBytes: limit}}}
			got, sample, err := scanColdSnapshot(t.Context(), dir, current, q)
			if err == nil || !strings.Contains(err.Error(), l.name()+" block") || got != nil || sample.Scan != nil {
				t.Fatalf("overlong compressed output returned rows=%d sample=%+v error=%v", len(got), sample, err)
			}
		})
	}
}
