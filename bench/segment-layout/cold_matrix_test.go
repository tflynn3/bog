package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
			if err := validateBenchmarkReport(bytes.NewReader(raw)); (err == nil) != (tc.edit == nil) {
				t.Fatalf("validation = %v", err)
			}
		})
	}
	for _, field := range []string{`"segments_decoded_delta":0,`, `"peak_oversized_working_bytes":0,`} {
		t.Run("missing "+field, func(t *testing.T) {
			t.Parallel()
			raw := bytes.Replace(data, []byte(field), nil, 1)
			if bytes.Equal(raw, data) {
				t.Fatal("missing fixture field")
			}
			if err := validateBenchmarkReport(bytes.NewReader(raw)); err == nil {
				t.Fatal("accepted a missing zero field")
			}
		})
	}
}

func testColdMatrix(t *testing.T) coldMatrixReport {
	t.Helper()
	env := map[string]any{"os": "linux", "cache_policy": coldCachePolicy}
	runID := strings.Repeat("01", 16)
	report := coldMatrixReport{
		SchemaVersion: coldMatrixSchema, Mode: "cold-matrix", Revision: "test", RunID: runID,
		Environment: env, Protocol: matrixProtocol("mixed", 2),
	}
	for _, profile := range report.Protocol.ProfileOrder {
		p := testColdReport()
		p.SchemaVersion, p.Fixture.Profile, p.Environment = coldMatrixSchema, profile, env
		input := fixture(p.Fixture.Batches, profile == "entropy", p.Fixture.Seed)
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
					var err error
					denominator, err = coldBackfillWALBytes(burst)
					if err != nil {
						t.Fatal(err)
					}
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
						s.Proof = &coldSampleProof{Round: n + 1, PairID: p.Pairing.pairID(burst.name, q, n+1),
							Layout: l.Name, RowsSHA256: coldRowsDigest(want)}
						result.Samples = append(result.Samples, s)
					}
					result.summarize()
					pub.Queries = append(pub.Queries, result)
				}
			}
		}
		if err := p.sealPairs(input); err != nil {
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
