package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"
)

const (
	maxReportBytes   = 16 << 20
	minDecisionRuns  = 21
	sparseP95Limit   = 0.90
	fullScanP95Limit = 1.10
)

var architectures = [...]string{"amd64", "arm64"}
var profiles = [...]string{"mixed", "entropy"}
var standardScales = [...]string{"small", "medium", "large"}
var phases = [...]string{"initial", "clustered", "scattered"}

type aggregateReport struct {
	SchemaVersion  int             `json:"schema_version"`
	Revision       string          `json:"revision"`
	Recommendation string          `json:"recommended_layout"`
	Reason         string          `json:"reason"`
	Missing        []caseKey       `json:"missing_qualification_cases,omitempty"`
	Cases          []aggregateCase `json:"cases"`
}

type caseKey struct {
	Arch    string `json:"arch"`
	Profile string `json:"profile"`
	Scale   string `json:"scale"`
}

type aggregateCase struct {
	caseKey
	Environment reportEnvironment     `json:"environment"`
	Fixture     fixtureReport         `json:"fixture"`
	Generations []aggregateGeneration `json:"generations"`
}

type aggregateGeneration struct {
	Phase       string            `json:"phase"`
	InputSHA256 string            `json:"input_sha256"`
	Layouts     []aggregateLayout `json:"layouts"`
}

type aggregateLayout struct {
	Name          string           `json:"layout"`
	RetainedBytes int64            `json:"retained_bytes"`
	RewriteBytes  int64            `json:"rewrite_bytes"`
	Queries       []aggregateQuery `json:"queries"`
}

type aggregateQuery struct {
	Name            string `json:"query"`
	ColdP95NS       int64  `json:"cold_p95_ns"`
	DecodedBytes    int64  `json:"decoded_bytes"`
	PeakDecodeBytes int    `json:"peak_decode_bytes"`
	MatchingRows    int    `json:"matching_rows"`
}

func readReport(path string) (_ report, err error) {
	f, err := os.Open(path)
	if err != nil {
		return report{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	data, err := io.ReadAll(io.LimitReader(f, maxReportBytes+1))
	if err != nil {
		return report{}, err
	}
	if len(data) > maxReportBytes {
		return report{}, fmt.Errorf("benchmark report exceeds %d bytes", maxReportBytes)
	}
	var r report
	if err := json.Unmarshal(data, &r); err != nil {
		return report{}, err
	}
	return r, nil
}

func runAggregate(paths []string, out io.Writer) error {
	reports := make([]report, 0, len(paths))
	for _, path := range paths {
		r, err := readReport(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		reports = append(reports, r)
	}
	combined, err := aggregate(reports)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(combined)
}

func (r report) key() caseKey {
	return caseKey{r.Environment.Arch, strings.TrimSuffix(r.Fixture.Name, fixtureVersion), r.Fixture.Scale}
}

func isHexDigest(s string, bytes int) bool {
	if len(s) != bytes*2 || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func validateFixture(f fixtureReport, profile string) error {
	if !slices.Contains(profiles[:], profile) || f.Name != profile+fixtureVersion ||
		f.Seed != seed || f.TargetBlockSize != blockTarget || f.MaxLineBytes != largeLineSize ||
		!slices.Equal(f.BatchPattern, batchPattern) || f.Batches < 1 || f.Batches > maxBatches ||
		f.SamplesPerQuery < 1 || f.SamplesPerQuery > maxRuns ||
		f.Rows != rowsForBatches(f.Batches) || f.RewrittenRows != f.Rows+4 {
		return fmt.Errorf("invalid fixture identity or size")
	}
	switch f.Scale {
	case "custom":
	case "smoke":
		if f.Batches != scaleBatches["small"] {
			return fmt.Errorf("invalid smoke fixture size")
		}
	default:
		if f.Batches != scaleBatches[f.Scale] {
			return fmt.Errorf("invalid standard scale or batch count")
		}
	}
	return nil
}

func (r layoutReport) generation(phase int) generationReport {
	if phase == 0 {
		return r.generationReport
	}
	return r.Backfills[phase-1].generationReport
}

func validateReport(r report) error {
	if r.SchemaVersion != reportVersion || r.Revision == "" {
		return fmt.Errorf("require benchmark schema %d and a revision", reportVersion)
	}
	if err := validateFixture(r.Fixture, r.key().Profile); err != nil {
		return err
	}
	e := r.Environment
	if e.OS != "linux" || e.Arch == "" || e.Machine == "" || e.Kernel == "" || e.GoVersion == "" ||
		e.BuildRevision == "" || !isHexDigest(e.BinarySHA256, 32) || e.CachePolicy != cachePolicy ||
		e.CPUs < 1 || e.GOMAXPROCS < 1 || e.FilesystemMagic == "" || e.FilesystemBlockBytes < 1 ||
		e.WorkDirectory == "" || e.ZstdLevel == "" || e.ZstdWindowBytes != zstdWindow {
		return fmt.Errorf("invalid Linux measurement or build metadata")
	}
	if len(r.Layouts) != len(layouts) {
		return fmt.Errorf("expected %d layouts", len(layouts))
	}
	for i, l := range r.Layouts {
		if l.Name != layouts[i].name() || len(l.Backfills) != len(phases)-1 {
			return fmt.Errorf("unexpected layout or backfill count at layout %d", i)
		}
		for j, b := range l.Backfills {
			latest := epoch
			if j == 1 {
				latest = latest.Add(time.Duration(max(0, r.Fixture.Batches-2)) * time.Second)
			}
			if b.Scenario != phases[j+1] || b.AppendedRows != 4 ||
				!b.Earliest.Equal(epoch) || !b.Latest.Equal(latest) || b.RewriteBytes != b.RetainedBytes {
				return fmt.Errorf("%s: invalid %s rewrite metadata", l.Name, phases[j+1])
			}
		}
		if l.RewriteBytes != l.Backfills[0].RewriteBytes {
			return fmt.Errorf("%s: invalid clustered rewrite total", l.Name)
		}
	}
	for phase, name := range phases {
		if err := validateGeneration(r, phase); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func validateGeneration(r report, phase int) error {
	wantRows := r.Fixture.Rows
	if phase > 0 {
		wantRows = r.Fixture.RewrittenRows
	}
	raw := r.Layouts[rawCodec].generation(phase)
	workload := queries()
	// Check every generation's structure before following a codec baseline.
	for i, l := range layouts {
		g := r.Layouts[i].generation(phase)
		if g.Rows != wantRows || g.Blocks < 1 || g.Blocks > g.Rows || g.Blocks != raw.Blocks ||
			!isHexDigest(g.InputSHA256, 32) || g.InputSHA256 != raw.InputSHA256 ||
			len(g.Queries) != len(workload) {
			return fmt.Errorf("%s: invalid generation metadata", l.name())
		}
		indexBytes := headerSize + int64(l.indexWidth())*int64(g.Blocks)
		bloomBytes := int64(l.indexWidth()-entrySize) * int64(g.Blocks)
		if g.RetainedBytes <= indexBytes || g.RetainedBytes > indexBytes+int64(g.Blocks)*maxBlock ||
			g.BloomMetadataBytes != bloomBytes || g.BloomBytesPerRetainedByte != float64(bloomBytes)/float64(g.RetainedBytes) {
			return fmt.Errorf("%s: invalid retained or Bloom bytes", l.name())
		}
	}
	for i, l := range layouts {
		g := r.Layouts[i].generation(phase)
		base := r.Layouts[l.codec].generation(phase)
		samePruning := raw
		if l.prune {
			samePruning = r.Layouts[len(layouts)/2].generation(phase)
		}
		if g.RetainedBytes-base.RetainedBytes != g.BloomMetadataBytes {
			return fmt.Errorf("%s: invalid Bloom overhead", l.name())
		}
		for j, q := range workload {
			if err := validateMeasurement(g, l, g.Queries[j], base.Queries[j], raw.Queries[j], q, r.Fixture.SamplesPerQuery); err != nil {
				return fmt.Errorf("%s/%s: %w", l.name(), q.name, err)
			}
			got, want := g.Queries[j], samePruning.Queries[j]
			if got.Blocks != want.Blocks || got.DecodedBytes != want.DecodedBytes {
				return fmt.Errorf("%s/%s: codecs with the same pruning decoded different blocks", l.name(), q.name)
			}
		}
	}
	return nil
}

func validateMeasurement(g generationReport, l layout, m, base, raw measurement, q query, runs int) error {
	storedCapacity := g.RetainedBytes - headerSize - int64(l.indexWidth())*int64(g.Blocks)
	if m.Name != q.name || m.Literal != q.literal || !m.Start.Equal(q.start) || !m.End.Equal(q.end) ||
		m.MatchingRows != raw.MatchingRows || m.MatchingRows < 0 || m.MatchingRows > g.Rows ||
		m.MatchingFraction != float64(m.MatchingRows)/float64(g.Rows) ||
		m.Blocks < 0 || m.Blocks > g.Blocks || m.Bytes < int64(m.Blocks) || m.Bytes > storedCapacity ||
		m.DecodedBytes < max(m.Bytes, int64(m.Blocks)*blockHeader) || m.DecodedBytes > int64(m.Blocks)*maxBlock ||
		(l.codec == rawCodec && m.Bytes != m.DecodedBytes) ||
		(m.Blocks == 0 && (m.Bytes != 0 || m.MatchingRows != 0 || m.PeakDecodeBytes != 0)) ||
		(m.Blocks > 0 && m.PeakDecodeBytes < blockHeader) {
		return fmt.Errorf("invalid query or decode metrics")
	}
	if len(m.ColdSamplesNS) != runs || slices.Min(m.ColdSamplesNS) < 1 || m.ColdP95NS != p95(m.ColdSamplesNS) {
		return fmt.Errorf("invalid cold samples or p95")
	}
	if base.Blocks != raw.Blocks || base.DecodedBytes != raw.DecodedBytes ||
		m.BaselineBlocksRead != base.Blocks || m.BaselineBytesRead != base.Bytes ||
		m.BaselineDecodedBytes != base.DecodedBytes ||
		m.BlocksAvoided != base.Blocks-m.Blocks || m.BytesAvoided != base.Bytes-m.Bytes ||
		m.DecodedBytesAvoided != base.DecodedBytes-m.DecodedBytes ||
		m.BlocksAvoided < 0 || m.BytesAvoided < 0 || m.DecodedBytesAvoided < 0 {
		return fmt.Errorf("invalid unfiltered baseline or pruning savings")
	}
	if q.name == "all" && (m.MatchingRows != g.Rows || m.Blocks != g.Blocks || m.Bytes != storedCapacity) {
		return fmt.Errorf("incomplete full scan")
	}
	if len(q.literal) < 4 && m.payloadReads != base.payloadReads {
		return fmt.Errorf("short literal must bypass Bloom")
	}
	if q.name == falsePositiveQuery && (m.MatchingRows != 0 || m.Blocks == 0) {
		return fmt.Errorf("missing Bloom false-positive evidence")
	}
	return nil
}

func compareWork(a, b report) error {
	for i := range layouts {
		for phase, name := range phases {
			x, y := a.Layouts[i].generation(phase), b.Layouts[i].generation(phase)
			if x.InputSHA256 != y.InputSHA256 || x.Blocks != y.Blocks {
				return fmt.Errorf("append fixture or packing differs across architectures for %s/%s/%s", a.key().Profile, a.Fixture.Scale, name)
			}
			for j, m := range x.Queries {
				n := y.Queries[j]
				if m.MatchingRows != n.MatchingRows || m.Blocks != n.Blocks || m.DecodedBytes != n.DecodedBytes {
					return fmt.Errorf("query work differs across architectures for %s/%s/%s/%s/%s",
						a.key().Profile, a.Fixture.Scale, name, a.Layouts[i].Name, m.Name)
				}
			}
		}
	}
	return nil
}

func summarize(r report) aggregateCase {
	c := aggregateCase{caseKey: r.key(), Environment: r.Environment, Fixture: r.Fixture}
	for phase, name := range phases {
		g := aggregateGeneration{Phase: name, InputSHA256: r.Layouts[0].generation(phase).InputSHA256}
		for _, l := range r.Layouts {
			item := l.generation(phase)
			layout := aggregateLayout{Name: l.Name, RetainedBytes: item.RetainedBytes}
			if phase > 0 {
				layout.RewriteBytes = l.Backfills[phase-1].RewriteBytes
			}
			for _, m := range item.Queries {
				layout.Queries = append(layout.Queries, aggregateQuery{
					Name: m.Name, ColdP95NS: m.ColdP95NS, DecodedBytes: m.DecodedBytes,
					PeakDecodeBytes: m.PeakDecodeBytes, MatchingRows: m.MatchingRows,
				})
			}
			g.Layouts = append(g.Layouts, layout)
		}
		c.Generations = append(c.Generations, g)
	}
	return c
}

func aggregate(reports []report) (aggregateReport, error) {
	if len(reports) == 0 {
		return aggregateReport{}, fmt.Errorf("usage: segment-layout aggregate report.json [report.json ...]")
	}
	result := aggregateReport{SchemaVersion: 1, Revision: reports[0].Revision, Recommendation: layouts[rawCodec].name()}
	byCase := make(map[caseKey]report, len(reports))
	environments := make(map[string]reportEnvironment)
	for _, r := range reports {
		key, env := r.key(), r.Environment
		if err := validateReport(r); err != nil {
			return aggregateReport{}, fmt.Errorf("%s/%s/%s: %w", key.Arch, key.Profile, key.Scale, err)
		}
		if _, exists := byCase[key]; exists {
			return aggregateReport{}, fmt.Errorf("duplicate report for %s/%s/%s", key.Arch, key.Profile, key.Scale)
		}
		if r.Revision != result.Revision {
			return aggregateReport{}, fmt.Errorf("reports mix revisions %q and %q", result.Revision, r.Revision)
		}
		if previous, ok := environments[key.Arch]; ok && previous != env {
			return aggregateReport{}, fmt.Errorf("%s reports mix binaries or measurement environments", key.Arch)
		}
		environments[key.Arch] = env
		byCase[key] = r
		result.Cases = append(result.Cases, summarize(r))
	}
	slices.SortFunc(result.Cases, func(a, b aggregateCase) int {
		return strings.Compare(a.Arch+"/"+a.Profile+"/"+a.Scale, b.Arch+"/"+b.Profile+"/"+b.Scale)
	})

	var qualified []report
	for _, arch := range architectures {
		for _, profile := range profiles {
			for _, scale := range standardScales {
				key := caseKey{arch, profile, scale}
				r, ok := byCase[key]
				if !ok || r.Fixture.SamplesPerQuery < minDecisionRuns {
					result.Missing = append(result.Missing, key)
					continue
				}
				qualified = append(qualified, r)
			}
		}
	}
	for _, profile := range profiles {
		for _, scale := range standardScales {
			a, aOK := byCase[caseKey{"amd64", profile, scale}]
			b, bOK := byCase[caseKey{"arm64", profile, scale}]
			if aOK && bOK {
				if err := compareWork(a, b); err != nil {
					return aggregateReport{}, err
				}
				if a.Fixture.SamplesPerQuery != b.Fixture.SamplesPerQuery {
					return aggregateReport{}, fmt.Errorf("sample counts differ across architectures for %s/%s", profile, scale)
				}
			}
		}
	}
	a, b := environments["amd64"], environments["arm64"]
	switch {
	case len(result.Missing) != 0:
		result.Reason = "missing native Linux matrix cases with at least 21 samples per query"
	case !isHexDigest(result.Revision, 20) || a.BuildRevision != result.Revision || b.BuildRevision != result.Revision:
		result.Reason = "report revision must match the clean embedded commit on both binaries"
	case a.Machine != "x86_64" || b.Machine != "aarch64" || a.BinarySHA256 == b.BinarySHA256 ||
		a.GoVersion != b.GoVersion || a.ZstdLevel != b.ZstdLevel:
		result.Reason = "measurement requires matching Go and codec settings and a separate binary on each native Linux architecture"
	default:
		result.Recommendation, result.Reason = recommend(qualified)
	}
	return result, nil
}

func recommend(reports []report) (string, string) {
	best, bestRatio := layouts[rawCodec].name(), 1.0
	for i, l := range layouts[1:] {
		if ratio := candidateRatio(reports, i+1); ratio < bestRatio {
			best, bestRatio = l.name(), ratio
		}
	}
	if bestRatio == 1 {
		return best, "no candidate met the sparse p95, full-scan p95, and storage bounds in every qualifying case"
	}
	return best, "lowest worst-case sparse p95 ratio among layouts meeting all latency and storage bounds"
}

func candidateRatio(reports []report, candidate int) float64 {
	var worst float64
	for _, r := range reports {
		for phase := range phases {
			g := r.Layouts[candidate].generation(phase)
			raw := r.Layouts[rawCodec].generation(phase)
			// The whole-file rewrite writes exactly the retained bytes in each backfill.
			if g.RetainedBytes > raw.RetainedBytes {
				return 1
			}
			for j, m := range g.Queries {
				ratio := float64(m.ColdP95NS) / float64(raw.Queries[j].ColdP95NS)
				switch m.Name {
				case "sparse", "bounded-sparse":
					if ratio > sparseP95Limit {
						return 1
					}
					worst = max(worst, ratio)
				case "all":
					if ratio > fullScanP95Limit {
						return 1
					}
				}
			}
		}
	}
	return worst
}
