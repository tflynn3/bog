package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

const testRevision = "0123456789abcdef0123456789abcdef01234567"

func testEnvironment(arch string) reportEnvironment {
	machine, digit := "x86_64", "1"
	if arch == "arm64" {
		machine, digit = "aarch64", "2"
	}
	return reportEnvironment{
		OS: "linux", Arch: arch, Machine: machine, GoVersion: "go1.27.1", BuildRevision: testRevision,
		BinarySHA256: strings.Repeat(digit, 64), CPUs: 4, GOMAXPROCS: 4, Kernel: "test",
		FilesystemMagic: "0xef53", FilesystemBlockBytes: 4096, CachePolicy: cachePolicy,
		WorkDirectory: "/test", ZstdLevel: "default", ZstdWindowBytes: zstdWindow,
	}
}

func TestAggregateMeasuredWork(t *testing.T) {
	// The cache policy is exercised by the Linux smoke test. Here the portable
	// runner still writes, reads, decodes and checks the same workload.
	for _, profile := range profiles {
		t.Run(profile, func(t *testing.T) {
			t.Parallel()
			cfg := config{revision: testRevision, profile: profile, scale: "custom", batches: 8, runs: 2}
			env := testEnvironment("arm64")
			r, err := collectIn(t.TempDir(), cfg, env, func(path string, size int64, q query, want []checkpoint.BlockRow, rows, runs int) (measurement, error) {
				data, err := os.ReadFile(path)
				if err != nil {
					return measurement{}, err
				}
				return measureQuery(bytes.NewReader(data), size, q, want, rows, runs, func() error { return nil })
			})
			if err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(t.TempDir(), "report.json")
			data, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := runAggregate([]string{path}, &out); err != nil {
				t.Fatal(err)
			}
			var combined aggregateReport
			if err := json.Unmarshal(out.Bytes(), &combined); err != nil {
				t.Fatal(err)
			}
			if combined.Recommendation != "raw" || len(combined.Missing) != 12 || len(combined.Cases) != 1 {
				t.Fatalf("custom workload qualified unexpectedly: %+v", combined)
			}
			c := combined.Cases[0]
			for phase, g := range c.Generations {
				for i, l := range g.Layouts {
					original := r.Layouts[i].generation(phase)
					if g.InputSHA256 != original.InputSHA256 || l.RetainedBytes != original.RetainedBytes {
						t.Errorf("%s/%s: summary lost generation metrics", g.Phase, l.Name)
					}
					for j, q := range l.Queries {
						m := original.Queries[j]
						if q.DecodedBytes != m.DecodedBytes || q.PeakDecodeBytes != m.PeakDecodeBytes || q.ColdP95NS != m.ColdP95NS {
							t.Errorf("%s/%s/%s: summary lost metrics", g.Phase, l.Name, q.Name)
						}
					}
				}
			}
			if err := runAggregate([]string{path}, brokenOutput{}); !errors.Is(err, io.ErrClosedPipe) {
				t.Errorf("lost output write error: %v", err)
			}
		})
	}
}

type brokenOutput struct{}

func (brokenOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// Decision tests use simulated reports to control every latency ratio. Real
// encode/decode and parity are exercised separately by TestAggregateMeasuredWork.
func decisionReport(arch, profile, scale string) report {
	batches := scaleBatches[scale]
	rows := rowsForBatches(batches)
	r := report{
		SchemaVersion: reportVersion, Revision: testRevision, Environment: testEnvironment(arch),
		Fixture: fixtureReport{
			Name: profile + fixtureVersion, Scale: scale, Seed: seed, Batches: batches,
			BatchPattern: batchPattern, Rows: rows, RewrittenRows: rows + 4,
			TargetBlockSize: blockTarget, SamplesPerQuery: minDecisionRuns, MaxLineBytes: largeLineSize,
		},
	}
	for i, l := range layouts {
		next := layoutReport{Name: l.name()}
		stored := 2000 - int64(l.codec)*500
		for phase, name := range phases {
			count := rows
			if phase > 0 {
				count += 4
			}
			bloom := int64(l.indexWidth() - entrySize)
			retained := int64(headerSize+l.indexWidth()) + stored
			g := generationReport{
				InputSHA256: strings.Repeat(string(rune('a'+phase)), 64), Rows: count, Blocks: 1,
				RetainedBytes: retained, BloomMetadataBytes: bloom, BloomBytesPerRetainedByte: float64(bloom) / float64(retained),
			}
			for j, q := range queries() {
				m := measurement{
					Name: q.name, Literal: q.literal, Start: q.start, End: q.end,
					payloadReads: payloadReads{1, stored, 2000}, PeakDecodeBytes: 5000,
					ColdP95NS: 1000, ColdSamplesNS: slices.Repeat([]int64{1000}, minDecisionRuns),
				}
				switch {
				case q.name == "all":
					m.MatchingRows, m.MatchingFraction = count, 1
				case l.prune && q.name == "absent":
					m.payloadReads, m.PeakDecodeBytes = payloadReads{}, 0
				}
				base := m
				if l.prune {
					base = r.Layouts[l.codec].generation(phase).Queries[j]
				}
				if err := m.compareReads(base); err != nil {
					panic(err)
				}
				if i == 1 && (q.name == "sparse" || q.name == "bounded-sparse") {
					setTiming(&m, 800)
				}
				g.Queries = append(g.Queries, m)
			}
			if phase == 0 {
				next.generationReport = g
				continue
			}
			latest := epoch
			if phase == 2 {
				latest = latest.Add(time.Duration(batches-2) * time.Second)
			}
			next.Backfills = append(next.Backfills, backfillReport{
				Scenario: name, AppendedRows: 4, Earliest: epoch, Latest: latest, RewriteBytes: retained, generationReport: g,
			})
		}
		next.RewriteBytes = next.Backfills[0].RewriteBytes
		r.Layouts = append(r.Layouts, next)
	}
	return r
}

func setTiming(m *measurement, ns int64) {
	m.ColdP95NS = ns
	m.ColdSamplesNS = slices.Repeat([]int64{ns}, len(m.ColdSamplesNS))
}

func TestAggregateDecision(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func([]report) []report
		want   string
		reason string
	}{
		{"complete", func(r []report) []report { return r }, "lz4", "lowest worst-case"},
		{"incomplete", func(r []report) []report { return r[1:] }, "raw", "missing native"},
		{"sparse regression in one backfill", func(r []report) []report {
			for j, m := range r[11].Layouts[1].Backfills[1].Queries {
				if m.Name == "bounded-sparse" {
					setTiming(&r[11].Layouts[1].Backfills[1].Queries[j], 901)
				}
			}
			return r
		}, "raw", "no candidate"},
		{"full-scan regression", func(r []report) []report {
			setTiming(&r[0].Layouts[1].Queries[0], 1101)
			return r
		}, "raw", "no candidate"},
		{"dirty source", func(r []report) []report {
			for i := range r {
				r[i].Environment.BuildRevision += "-dirty"
			}
			return r
		}, "raw", "clean embedded commit"},
		{"reused binary identity", func(r []report) []report {
			for i := range r {
				r[i].Environment.BinarySHA256 = r[0].Environment.BinarySHA256
			}
			return r
		}, "raw", "separate binary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var inputs []report
			for _, arch := range architectures {
				for _, profile := range profiles {
					for _, scale := range standardScales {
						inputs = append(inputs, decisionReport(arch, profile, scale))
					}
				}
			}
			inputs = tc.change(inputs)
			slices.Reverse(inputs)
			got, err := aggregate(inputs)
			if err != nil {
				t.Fatal(err)
			}
			if got.Recommendation != tc.want || !strings.Contains(got.Reason, tc.reason) ||
				len(got.Cases) != len(inputs) || got.Cases[0].Arch != "amd64" {
				t.Fatalf("got recommendation %s (%s) for %d reports", got.Recommendation, got.Reason, len(got.Cases))
			}
		})
	}
}

func TestAggregateRejectsInconsistentReports(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*report, *report)
		want   string
	}{
		{"duplicate", func(a, b *report) { *b = *a }, "duplicate report"},
		{"revision", func(_, b *report) { b.Revision = "other" }, "mix revisions"},
		{"fixture digest", func(_, b *report) {
			for i := range b.Layouts {
				b.Layouts[i].Backfills[1].InputSHA256 = strings.Repeat("f", 64)
			}
		}, "append fixture or packing differs"},
		{"p95", func(_, b *report) { b.Layouts[2].Queries[0].ColdP95NS++ }, "cold samples or p95"},
		{"decoded bytes", func(_, b *report) { b.Layouts[0].Queries[0].DecodedBytes-- }, "invalid query or decode metrics"},
		{"rewrite", func(_, b *report) { b.Layouts[2].Backfills[1].RewriteBytes++ }, "rewrite metadata"},
		{"layout missing", func(_, b *report) { b.Layouts = nil }, "expected 6 layouts"},
		{"unbounded block count", func(_, b *report) { b.Layouts[2].Blocks = int(^uint(0) >> 1) }, "generation metadata"},
		{"no samples", func(_, b *report) { b.Layouts[2].Queries[0].ColdSamplesNS = nil }, "cold samples or p95"},
		{"missing false-positive", func(_, b *report) {
			for i, m := range b.Layouts[3].Queries {
				if m.Name == falsePositiveQuery {
					m.payloadReads, m.PeakDecodeBytes = payloadReads{}, 0
					m.BlocksAvoided, m.BytesAvoided, m.DecodedBytesAvoided = 1, 2000, 2000
					b.Layouts[3].Queries[i] = m
				}
			}
		}, "missing Bloom false-positive"},
		{"mixed binaries on one architecture", func(a, b *report) {
			*b = decisionReport("amd64", "mixed", "medium")
			b.Environment.BinarySHA256 = strings.Repeat("f", 64)
		}, "mix binaries"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := decisionReport("amd64", "mixed", "small"), decisionReport("arm64", "mixed", "small")
			tc.change(&a, &b)
			if _, err := aggregate([]report{a, b}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
		})
	}
}

func TestAggregateInputErrors(t *testing.T) {
	if err := runAggregate(nil, io.Discard); err == nil {
		t.Fatal("accepted no reports")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "input.json")
	if err := runAggregate([]string{path}, io.Discard); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want missing input", err)
	}
	for _, data := range [][]byte{[]byte("{} {}"), bytes.Repeat([]byte(" "), maxReportBytes+1)} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := runAggregate([]string{path}, io.Discard); err == nil {
			t.Fatalf("accepted malformed %d-byte report", len(data))
		}
	}
}
