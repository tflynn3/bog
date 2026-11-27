package main

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func testPairedReport(t *testing.T) coldReport {
	t.Helper()
	r := testColdReport()
	r.SchemaVersion = pairedColdSchema
	input := fixture(r.Fixture.Batches, false, r.Fixture.Seed)
	r.Pairing = &coldPairing{
		RunID: strings.Repeat("01", 16), FixtureSHA256: coldFixtureDigest(r.Fixture, input),
	}
	for pi, phase := range coldPhases {
		rows := input.rows
		if pi > 0 {
			rows = input.withBackfill(input.backfills()[pi-1])
		}
		for qi, q := range coldQueries() {
			want := reference(rows, q)
			digest := coldRowsDigest(want)
			all := append([]coldLayout{r.Production}, r.Experimental...)
			for _, l := range all {
				result := &l.Phases[pi].Queries[qi]
				result.Matches = len(want)
				for i := range result.Samples {
					result.Samples[i].Proof = &coldSampleProof{
						Round: i + 1, PairID: r.Pairing.pairID(phase, q, i+1), Layout: l.Name, RowsSHA256: digest,
					}
				}
			}
		}
	}
	// Two rounds with opposed physical-read deltas and consistently lower
	// LZ4 memory. Their independently measured write counts also differ.
	for pi := range coldPhases {
		raw := &r.Experimental[0].Phases[pi].Queries[0]
		lz4 := &r.Experimental[1].Phases[pi].Queries[0]
		raw.Samples[0].StorageReadBytes, raw.Samples[1].StorageReadBytes = 4096, 0
		lz4.Samples[0].StorageReadBytes, lz4.Samples[1].StorageReadBytes = 0, 8192
		lz4.Samples[0].PeakRSSBytes, lz4.Samples[1].PeakRSSBytes = 39000, 38000
		raw.summarize()
		lz4.summarize()
		if pi > 0 {
			backfill, err := describeColdBackfill(input.backfills()[pi-1])
			if err != nil {
				t.Fatal(err)
			}
			prod := r.Production.Phases[pi].Rewrite
			*prod = writeAmplification(prod.WrittenBytes, backfill.AppendedWALBytes, prod.Boundary)
			for i, l := range layouts {
				p := &r.Experimental[i].Phases[pi]
				if l.codec == lz4Codec {
					p.ManifestBytes -= 60
				}
				*p.Rewrite = writeAmplification(uint64(p.RewrittenBytes+p.ManifestBytes), backfill.AppendedWALBytes, coldRewriteBoundary)
			}
		}
	}
	if err := r.sealPairs(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPairedColdDeltas(t *testing.T) {
	t.Parallel()
	r := testPairedReport(t)
	if got := len(r.Pairing.Comparisons); got != 36 {
		t.Fatalf("got %d comparisons, want 3 phases × 2 queries × 2 Bloom settings × 3 codec pairs", got)
	}
	keys := make(map[[4]string]bool)
	for _, p := range r.Pairing.Comparisons {
		key := [4]string{p.Phase, p.Query, p.Baseline, p.Candidate}
		if keys[key] || len(p.Rounds) != 2 || (p.Phase == "initial") != (p.Rewrite == nil) ||
			strings.HasSuffix(p.Baseline, "+summary") != p.Bloom || strings.HasSuffix(p.Candidate, "+summary") != p.Bloom {
			t.Fatalf("unexpected comparison %+v", p)
		}
		keys[key] = true
		if p.Query != "sparse" || p.Baseline != "raw" || p.Candidate != "lz4" {
			continue
		}
		if got := []int64{p.Rounds[0].PhysicalRead, p.Rounds[1].PhysicalRead}; !slices.Equal(got, []int64{-4096, 8192}) {
			t.Fatalf("%s: paired physical read deltas %v", p.Phase, got)
		}
		if p.PhysicalRead != (coldDeltaSummary{P50: -4096, Lower: 1, Higher: 1}) ||
			p.PeakRSS != (coldDeltaSummary{P50: -2000, Lower: 2}) || p.StorageReadRounds != 2 ||
			p.Decision != "tradeoff_or_variable" {
			t.Fatalf("%s: unexpected summaries %+v", p.Phase, p)
		}
		if p.Rewrite != nil && (p.Rewrite.Boundary != coldRewriteBoundary || p.Rewrite.WrittenBytes != -60 ||
			p.Rewrite.Ratio != -60/float64(p.Rewrite.AppendedWALBytes)) {
			t.Fatalf("%s: unexpected rewrite delta %+v", p.Phase, p.Rewrite)
		}
	}
}

func TestPairedColdReportRejectsMismatches(t *testing.T) {
	t.Parallel()
	// Each parallel case decodes its own report so mutations cannot alter
	// the shared valid JSON or another case's samples.
	data, err := json.Marshal(testPairedReport(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*coldReport)
	}{
		{"valid", nil},
		{"missing pairing", func(r *coldReport) { r.Pairing = nil }},
		{"downgrade", func(r *coldReport) { r.SchemaVersion = 5 }},
		{"different execution", func(r *coldReport) { r.Pairing.RunID = strings.Repeat("02", 16) }},
		{"invalid execution", func(r *coldReport) { r.Pairing.RunID = strings.Repeat("z", 32) }},
		{"different fixture seed", func(r *coldReport) { r.Fixture.Seed++ }},
		{"different fixture profile", func(r *coldReport) { r.Fixture.Profile = "entropy" }},
		{"different cache", func(r *coldReport) { r.Environment["cache_policy"] = "warm" }},
		{"missing phase", func(r *coldReport) { r.Experimental[0].Phases = r.Experimental[0].Phases[:2] }},
		{"missing query", func(r *coldReport) { r.Experimental[0].Phases[0].Queries = nil }},
		{"missing run", func(r *coldReport) { pairedSparse(r).Samples = pairedSparse(r).Samples[:1] }},
		{"missing exact evidence", func(r *coldReport) { pairedSparse(r).Samples[0].Proof = nil }},
		{"swapped rounds", func(r *coldReport) {
			s := pairedSparse(r).Samples
			s[0], s[1] = s[1], s[0]
		}},
		{"duplicate round", func(r *coldReport) { pairedSparse(r).Samples[1].Proof = pairedSparse(r).Samples[0].Proof }},
		{"different codec sample", func(r *coldReport) {
			pairedSparse(r).Samples[0].Proof = r.Experimental[1].Phases[0].Queries[0].Samples[0].Proof
		}},
		{"different phase sample", func(r *coldReport) {
			pairedSparse(r).Samples[0].Proof = r.Experimental[0].Phases[1].Queries[0].Samples[0].Proof
		}},
		{"different query sample", func(r *coldReport) {
			pairedSparse(r).Samples[0].Proof = r.Experimental[0].Phases[0].Queries[1].Samples[0].Proof
		}},
		{"wrong full generation digest", func(r *coldReport) {
			r.Experimental[0].Phases[1].Queries[6].Samples[0].Proof.RowsSHA256 = strings.Repeat("0", 64)
		}},
		{"shared but wrong WAL growth", func(r *coldReport) {
			for _, l := range append([]coldLayout{r.Production}, r.Experimental...) {
				a := l.Phases[1].Rewrite
				*a = writeAmplification(a.WrittenBytes, a.AppendedWALBytes+1, a.Boundary)
			}
		}},
		{"missing comparison", func(r *coldReport) { r.Pairing.Comparisons = r.Pairing.Comparisons[:35] }},
		{"mislabelled comparison", func(r *coldReport) { r.Pairing.Comparisons[0].Bloom = true }},
		{"wrong signed read delta", func(r *coldReport) { r.Pairing.Comparisons[0].Rounds[0].PhysicalRead++ }},
		{"wrong rewrite delta", func(r *coldReport) { r.Pairing.Comparisons[12].Rewrite.Ratio++ }},
		{"wrong decision", func(r *coldReport) { r.Pairing.Comparisons[0].Decision = "candidate_lower" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var report coldReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			if tc.edit != nil {
				tc.edit(&report)
			}
			data, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateBenchmarkReport(bytes.NewReader(data)); (err == nil) != (tc.edit == nil) {
				t.Fatalf("validation = %v, want success %t", err, tc.edit == nil)
			}
		})
	}
	for _, field := range []string{`"storage_read_bytes":0,`, `"bloom":false,`, `"physical_read_bytes_delta":0,`} {
		t.Run("omitted zero "+field, func(t *testing.T) {
			t.Parallel()
			missing := bytes.Replace(data, []byte(field), nil, 1)
			if bytes.Equal(missing, data) {
				t.Fatal("test field missing from fixture")
			}
			if err := validateBenchmarkReport(bytes.NewReader(missing)); err == nil {
				t.Fatal("accepted missing field")
			}
		})
	}
}

func pairedSparse(r *coldReport) *coldQueryReport {
	return &r.Experimental[0].Phases[0].Queries[0]
}

func TestColdOrderedResultDigest(t *testing.T) {
	t.Parallel()
	rows := []checkpoint.BlockRow{
		{Row: store.Row{TS: epoch, Line: "é duplicate"}, Ordinal: 12},
		{Row: store.Row{TS: epoch, Line: "é duplicate"}, Ordinal: 13},
	}
	want := coldRowsDigest(rows)
	for _, tc := range []struct {
		name string
		edit func([]checkpoint.BlockRow) []checkpoint.BlockRow
	}{
		{"reordered ties", func(r []checkpoint.BlockRow) []checkpoint.BlockRow { return []checkpoint.BlockRow{r[1], r[0]} }},
		{"timestamp", func(r []checkpoint.BlockRow) []checkpoint.BlockRow {
			r[0].Row.TS = epoch.Add(time.Nanosecond)
			return r
		}},
		{"line bytes", func(r []checkpoint.BlockRow) []checkpoint.BlockRow { r[0].Row.Line = "è duplicate"; return r }},
		{"ordinal", func(r []checkpoint.BlockRow) []checkpoint.BlockRow { r[0].Ordinal++; return r }},
		{"missing duplicate", func(r []checkpoint.BlockRow) []checkpoint.BlockRow { return r[:1] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := coldRowsDigest(tc.edit(slices.Clone(rows))); got == want {
				t.Fatalf("changed rows retained digest %s", got)
			}
		})
	}
}

func TestColdSignedDeltasAndDecision(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		candidate, baseline uint64
		want                int64
		ok                  bool
	}{
		{5, 5, 0, true}, {3, 5, -2, true}, {5, 3, 2, true},
		{math.MaxUint64, math.MaxUint64 - 1, 1, true},
		{math.MaxInt64, 0, math.MaxInt64, true}, {0, 1 << 63, math.MinInt64, true},
		{1 << 63, 0, 0, false}, {0, 1<<63 + 1, 0, false},
	} {
		if got, err := signedColdDelta(tc.candidate, tc.baseline); got != tc.want || (err == nil) != tc.ok {
			t.Errorf("delta(%d, %d) = %d, %v, want %d with success %t", tc.candidate, tc.baseline, got, err, tc.want, tc.ok)
		}
	}
	for _, tc := range []struct {
		want          string
		lower, higher int
		writes        int64
	}{
		{"equal", 0, 0, 0}, {"candidate_lower", 1, 0, 0}, {"baseline_lower", 0, 0, 1},
		{"tradeoff_or_variable", 0, 1, -10}, {"tradeoff_or_variable", 1, 1, 0},
	} {
		p := coldCodecPair{PhysicalRead: coldDeltaSummary{Lower: tc.lower, Higher: tc.higher},
			Rewrite: &coldRewriteDelta{WrittenBytes: tc.writes}}
		if got := coldPairDecision(p); got != tc.want {
			t.Errorf("decision(%+v) = %s, want %s", p, got, tc.want)
		}
	}
}
