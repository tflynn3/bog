package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

func TestSmokeLayouts(t *testing.T) {
	if maxBlock != checkpoint.MaxBlockSize {
		t.Fatal("benchmark block limit differs from checkpoint codec")
	}
	for _, profile := range []string{"mixed", "entropy"} {
		t.Run(profile, func(t *testing.T) {
			parent := t.TempDir()
			var out bytes.Buffer
			cfg := config{dir: parent, revision: "test", profile: profile, scale: "smoke", batches: 64, runs: 2}
			if err := run(t.Context(), cfg, testExecutable(t), &out); err != nil {
				t.Fatal(err)
			}
			var got report
			if err := json.Unmarshal(out.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.SchemaVersion != 3 || got.Revision != "test" || got.Fixture.Scale != "smoke" ||
				got.Fixture.Name != profile+"-log-v2" || got.Fixture.Rows != 5253 || len(got.Layouts) != len(layouts) {
				t.Fatalf("wrong report identity: schema %d, fixture %+v, %d layouts", got.SchemaVersion, got.Fixture, len(got.Layouts))
			}

			baseline := map[codec]layoutReport{}
			for i, item := range got.Layouts {
				l := layouts[i]
				if item.Name != l.name() || item.PeakWorkerRSSBytes <= 0 || len(item.Backfills) != 2 {
					t.Fatalf("unexpected layout name or scenarios: %s", item.Name)
				}
				if !l.prune {
					baseline[l.codec] = item
				}
				base := baseline[l.codec]
				checkGeneration(t, l, item.generationReport, base.generationReport, cfg.runs, "initial")
				for j, scenario := range item.Backfills {
					wantName := []string{"clustered", "scattered"}[j]
					if scenario.Scenario != wantName || scenario.AppendedRows != 4 || scenario.Rows != got.Fixture.RewrittenRows ||
						scenario.Rows != item.Rows+4 || scenario.RewriteBytes != scenario.RetainedBytes || scenario.RewriteNS <= 0 {
						t.Fatalf("%s %s: wrong rewrite accounting", item.Name, scenario.Scenario)
					}
					if !scenario.Earliest.Equal(epoch) || (j == 0 && !scenario.Latest.Equal(epoch)) ||
						(j == 1 && !scenario.Latest.Equal(epoch.Add(62*time.Second))) {
						t.Errorf("%s %s: wrong backfill bounds", item.Name, scenario.Scenario)
					}
					checkGeneration(t, l, scenario.generationReport, base.Backfills[j].generationReport, cfg.runs, scenario.Scenario)
				}
				if item.RewriteBytes != item.Backfills[0].RewriteBytes {
					t.Errorf("%s: top-level rewrite bytes differ from clustered rewrite", item.Name)
				}
			}
			assertEmpty(t, parent)
		})
	}
}

func checkGeneration(t *testing.T, l layout, gen, baseline generationReport, runs int, phase string) {
	t.Helper()
	wantBloom := int64(l.indexWidth()-entrySize) * int64(gen.Blocks)
	if gen.BloomMetadataBytes != wantBloom || gen.RetainedBytes-baseline.RetainedBytes != wantBloom ||
		gen.BloomBytesPerRetainedByte != float64(wantBloom)/float64(gen.RetainedBytes) {
		t.Errorf("%s: wrong Bloom byte accounting", l.name())
	}
	wantQueries := queries()
	if len(gen.Queries) != len(wantQueries) {
		t.Fatalf("%s: got %d queries, want %d", l.name(), len(gen.Queries), len(wantQueries))
	}
	hits := make(map[string]int)
	for i, m := range gen.Queries {
		b := baseline.Queries[i]
		if !m.OrderedParity || m.Name != wantQueries[i].name || len(m.ColdSamplesNS) != runs || m.ColdP95NS != p95(m.ColdSamplesNS) ||
			m.MatchingFraction != float64(m.MatchingRows)/float64(gen.Rows) {
			t.Errorf("%s: unexpected query result %+v", l.name(), m)
		}
		if m.DecodedBytes < m.Bytes || m.BaselineDecodedBytes != b.DecodedBytes || m.BaselineBlocksRead != b.Blocks || m.BaselineBytesRead != b.Bytes ||
			m.BlocksAvoided != b.Blocks-m.Blocks || m.BytesAvoided != b.Bytes-m.Bytes ||
			m.BlocksAvoided < 0 || m.BytesAvoided < 0 {
			t.Errorf("%s/%s: wrong baseline accounting", l.name(), m.Name)
		}
		if (m.PeakDecodeBytes == 0) != (m.Blocks == 0) {
			t.Errorf("%s/%s: invalid decode memory %d", l.name(), m.Name, m.PeakDecodeBytes)
		}
		switch m.Name {
		case "all", "one-byte", "two-byte", "three-byte":
			if m.Blocks != gen.Blocks || m.MatchingRows == 0 {
				t.Errorf("%s: unexpected complete scan %+v", l.name(), m)
			}
		case "absent", "rare":
			if l.prune && m.BlocksAvoided == 0 {
				t.Errorf("%s: missing pruning for %s", l.name(), m.Name)
			}
		}
		hits[m.Name] = m.MatchingRows
	}
	rare := 2
	boundedSparse := 2
	switch phase {
	case "clustered":
		rare++
		boundedSparse += 3
	case "scattered":
		rare++
		boundedSparse += 2
	}
	if hits["all"] != gen.Rows || hits["rare"] != rare || hits["bounded"] != rare || hits["exclusive-end"] != 0 ||
		hits["oversized-tail"] != 1 || hits["absent"] != 0 || hits["case-mismatch"] != 0 ||
		hits["sparse"] <= 0 || hits["sparse"] >= hits["medium"] || hits["medium"] >= hits["dense"] ||
		hits["dense"] >= hits["all"] || hits["bounded-sparse"] != boundedSparse {
		t.Errorf("%s: unexpected cohort counts %v", l.name(), hits)
	}
}

type failedReportWriter struct{}

func (failedReportWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestReportFailureCleansUp(t *testing.T) {
	parent := t.TempDir()
	cfg := config{dir: parent, profile: "mixed", scale: "custom", batches: 1, runs: 1}
	if err := run(t.Context(), cfg, testExecutable(t), failedReportWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("report failure: %v", err)
	}
	assertEmpty(t, parent)
}

func init() {
	testCache = coldCache
	testPeakRSS = peakWorkerRSS
}
