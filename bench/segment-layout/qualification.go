package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

const productionV1 = "production-v1"
const physicalBoundary = "Linux /proc/self/io write_bytes delta across durable publication and the paired read-only scan"
const qualificationCache = "filesystem page cache left in place; WAL appends and predecessor validation precede timing"

var qualificationPhases = [...]string{"initial", "forward", "clustered", "scattered"}
var qualificationQueries = [...]string{"sparse", "dense"}

type incrementalReport struct {
	SchemaVersion int                      `json:"schema_version"`
	Mode          string                   `json:"mode"`
	Revision      string                   `json:"revision"`
	Environment   map[string]any           `json:"environment"`
	Fixture       incrementalFixtureReport `json:"fixture"`
	Baseline      productionLayout         `json:"baseline"`
	Layouts       []incrementalLayout      `json:"layouts"`
}

type incrementalFixtureReport struct {
	Name    string `json:"name"`
	Scale   string `json:"scale"`
	Seed    uint64 `json:"seed"`
	Batches int    `json:"batches"`
	Runs    int    `json:"samples_per_compaction"`
}

type productionLayout struct {
	Name         string              `json:"layout"`
	Publications []publicationResult `json:"publications"`
}

type incrementalLayout struct {
	Codec        string              `json:"codec"`
	SegmentBytes int                 `json:"target_segment_bytes"`
	Publications []publicationResult `json:"publications"`
}

type publicationResult struct {
	Phase         string `json:"phase"`
	Rows          int    `json:"rows"`
	AppendedRows  int    `json:"appended_rows"`
	Segments      int    `json:"segments"`
	ManifestBytes int64  `json:"manifest_bytes"`
	segmentBytes
	Decode          *decodeSample       `json:"decode,omitempty"`
	QueryCompaction *compactionLatency  `json:"query_during_compaction,omitempty"`
	Queries         []qualificationScan `json:"queries"`
}

type qualificationScan struct {
	Query        string                `json:"query"`
	MatchingRows int                   `json:"matching_rows"`
	Samples      []qualificationSample `json:"samples"`
	QueryP95NS   int64                 `json:"query_p95_ns"`
	PublishP95NS int64                 `json:"publication_p95_ns"`
	PeakRSSBytes uint64                `json:"peak_rss_bytes"`
}

type qualificationSample struct {
	QueryNS            int64         `json:"query_ns"`
	PublicationNS      int64         `json:"publication_ns"`
	OverlapNS          int64         `json:"overlap_ns"`
	PeakRSSBytes       uint64        `json:"peak_rss_bytes"`
	WriteAmplification amplification `json:"physical_write_amplification"`
}

func (scan *qualificationScan) summarize() {
	var queryTimes, publicationTimes []int64
	scan.PeakRSSBytes = 0
	for _, sample := range scan.Samples {
		queryTimes = append(queryTimes, sample.QueryNS)
		publicationTimes = append(publicationTimes, sample.PublicationNS)
		scan.PeakRSSBytes = max(scan.PeakRSSBytes, sample.PeakRSSBytes)
	}
	scan.QueryP95NS, scan.PublishP95NS = p95(queryTimes), p95(publicationTimes)
}

func validateQualificationJSON(in io.Reader) error {
	dec := json.NewDecoder(in)
	var report incrementalReport
	if err := dec.Decode(&report); err != nil {
		return err
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return fmt.Errorf("expected one schema v4 report, trailing input: %v", err)
	}
	return report.validate()
}

func knownRevision(revision string) bool {
	revision = strings.TrimSpace(revision)
	return revision != "" && revision != "unknown"
}

func (r incrementalReport) validate() error {
	f := r.Fixture
	if r.SchemaVersion != 4 || r.Mode != "incremental" || !knownRevision(r.Revision) ||
		r.Environment["os"] != "linux" || r.Environment["cache_policy"] != qualificationCache ||
		(f.Name != "mixed-successive-v1" && f.Name != "entropy-successive-v1") ||
		f.Batches < 1 || f.Batches > maxBatches || f.Runs < 1 || f.Runs > maxRuns {
		return errors.New("invalid schema v4 qualification header")
	}
	switch f.Scale {
	case "custom":
	case "smoke":
		if f.Batches != scaleBatches["small"] || f.Runs != 1 {
			return errors.New("invalid smoke fixture size or run count")
		}
	default:
		if scaleBatches[f.Scale] != f.Batches {
			return errors.New("invalid fixture scale")
		}
	}
	if r.Baseline.Name != productionV1 || len(r.Layouts) != 3*len(segmentTargets) {
		return errors.New("expected production v1 and nine experimental layouts")
	}
	if err := validatePublications(f, r.Baseline.Publications, nil, 0); err != nil {
		return fmt.Errorf("%s: %w", productionV1, err)
	}
	seen := make(map[[2]int]bool)
	for _, item := range r.Layouts {
		c := slices.Index([]string{"raw", "lz4", "zstd"}, item.Codec)
		key := [2]int{c, item.SegmentBytes}
		if c < 0 || !slices.Contains(segmentTargets[:], item.SegmentBytes) || seen[key] {
			return fmt.Errorf("unexpected or duplicate layout %s/%d", item.Codec, item.SegmentBytes)
		}
		seen[key] = true
		if err := validatePublications(f, item.Publications, r.Baseline.Publications, item.SegmentBytes); err != nil {
			return fmt.Errorf("%s/%d: %w", item.Codec, item.SegmentBytes, err)
		}
	}
	return nil
}

func validatePublications(f incrementalFixtureReport, pubs, baseline []publicationResult, target int) error {
	if len(pubs) != len(qualificationPhases) {
		return errors.New("missing or extra publications")
	}
	initial := 5 // Boundary rows, oversized singleton, and duplicate sentinels.
	for i := range f.Batches {
		initial += batchPattern[i%len(batchPattern)]
	}
	var previous int64
	for i, pub := range pubs {
		appended := 4
		if i == 0 {
			appended = initial
		}
		if pub.Phase != qualificationPhases[i] || pub.Rows != initial+4*i || pub.AppendedRows != appended ||
			pub.Segments <= 0 || pub.ManifestBytes <= 0 || pub.RetainedBytes <= 0 ||
			pub.ReusedBytes < 0 || pub.RetiredBytes < 0 || pub.RewrittenBytes <= 0 ||
			pub.ReusedBytes != pub.RetainedBytes-pub.RewrittenBytes || pub.ReusedBytes != previous-pub.RetiredBytes {
			return fmt.Errorf("invalid %s publication metadata", qualificationPhases[i])
		}
		previous = pub.RetainedBytes
		if d := pub.Decode; target != 0 && (d == nil || d.PeakRSSBytes == 0 ||
			d.Allocations == 0 || d.AllocationBytes == 0 || d.OversizedSegments != 1 ||
			d.OrdinaryBoundBytes != decodeBound(target) || d.PeakOrdinaryBytes <= 0 ||
			d.PeakOrdinaryBytes > d.OrdinaryBoundBytes || d.PeakWorkingBytes > d.WorkingBoundBytes) {
			return fmt.Errorf("%s: missing or invalid decode sample", pub.Phase)
		}
		if len(pub.Queries) != len(qualificationQueries) {
			return fmt.Errorf("%s: expected sparse and dense scans", pub.Phase)
		}
		rowLimit := pub.Rows
		if i > 0 {
			rowLimit = pubs[i-1].Rows
		}
		var walBytes uint64
		for j, scan := range pub.Queries {
			if scan.Query != qualificationQueries[j] || scan.MatchingRows <= 0 || scan.MatchingRows > rowLimit ||
				len(scan.Samples) != f.Runs || (baseline != nil && scan.MatchingRows != baseline[i].Queries[j].MatchingRows) {
				return fmt.Errorf("%s: invalid %s query identity or run count", pub.Phase, qualificationQueries[j])
			}
			summary := scan
			summary.summarize()
			if scan.QueryP95NS != summary.QueryP95NS || scan.PublishP95NS != summary.PublishP95NS ||
				scan.PeakRSSBytes != summary.PeakRSSBytes {
				return fmt.Errorf("%s/%s: incorrect sample summary", pub.Phase, scan.Query)
			}
			for k, sample := range scan.Samples {
				a := sample.WriteAmplification
				if walBytes == 0 {
					walBytes = a.AppendedWALBytes
				}
				if sample.QueryNS <= 0 || sample.PublicationNS <= 0 || sample.PeakRSSBytes == 0 ||
					(i == 0 && sample.OverlapNS != 0) || (i > 0 && sample.OverlapNS <= 0) ||
					sample.OverlapNS > min(sample.QueryNS, sample.PublicationNS) ||
					a.Boundary != physicalBoundary || a.WrittenBytes == 0 || a.AppendedWALBytes == 0 ||
					a.AppendedWALBytes != walBytes || a.Ratio != float64(a.WrittenBytes)/float64(walBytes) ||
					(baseline != nil && walBytes != baseline[i].Queries[j].Samples[k].WriteAmplification.AppendedWALBytes) {
					return fmt.Errorf("%s/%s: invalid sample %d", pub.Phase, scan.Query, k+1)
				}
			}
		}
		legacy := pub.QueryCompaction
		if i == 0 {
			if legacy != nil {
				return errors.New("initial scans must be idle")
			}
		} else {
			sparse := pub.Queries[0]
			if legacy == nil || legacy.Query != sparse.Query || legacy.MatchingRows != sparse.MatchingRows ||
				legacy.P95NS != sparse.QueryP95NS || len(legacy.SamplesNS) != f.Runs || len(legacy.OverlapNS) != f.Runs {
				return fmt.Errorf("%s: invalid sparse compaction summary", pub.Phase)
			}
			for j, sample := range sparse.Samples {
				if legacy.SamplesNS[j] != sample.QueryNS || legacy.OverlapNS[j] != sample.OverlapNS {
					return fmt.Errorf("%s: sparse compaction summary differs from samples", pub.Phase)
				}
			}
		}
	}
	return nil
}
