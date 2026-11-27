package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const coldCachePolicy = "fresh query worker; load manifest and validate v1 reader before timing; FADV_DONTNEED on every referenced segment; default readahead"
const coldOracle = "complete ordered timestamps, append ordinals and line bytes matched the independent append-log oracle in every worker sample"
const coldRewriteBoundary = "logical bytes in newly written synced experimental segments and snapshot manifest"

var coldPhases = [...]string{"initial", "clustered", "scattered"}

type coldReport struct {
	SchemaVersion int            `json:"schema_version"`
	Mode          string         `json:"mode"`
	Revision      string         `json:"revision"`
	Fixture       coldFixture    `json:"fixture"`
	Environment   map[string]any `json:"environment"`
	Oracle        string         `json:"oracle"`
	Production    coldLayout     `json:"production_v1_component"`
	Experimental  []coldLayout   `json:"experimental_layouts"`
	Live          liveReport     `json:"live_server_compaction"`
}

type coldFixture struct {
	Profile     string `json:"profile"`
	Scale       string `json:"scale"`
	Seed        uint64 `json:"seed"`
	Batches     int    `json:"batches"`
	SparseRuns  int    `json:"samples_per_sparse_query"`
	TargetBytes int    `json:"experimental_target_segment_bytes"`
}

type coldLayout struct {
	Name   string      `json:"layout"`
	Phases []coldPhase `json:"phases"`
}

type coldPhase struct {
	Phase         string `json:"phase"`
	Rows          int    `json:"rows"`
	Segments      int    `json:"segments"`
	ManifestBytes int64  `json:"manifest_bytes"`
	BloomBytes    int64  `json:"bloom_metadata_bytes"`
	segmentBytes
	Rewrite *amplification    `json:"rewrite_amplification,omitempty"`
	Queries []coldQueryReport `json:"queries"`
}

type coldQueryReport struct {
	Query                         string       `json:"query"`
	Literal                       string       `json:"literal"`
	Start                         time.Time    `json:"start_inclusive"`
	End                           time.Time    `json:"end_exclusive"`
	Matches                       int          `json:"matching_rows"`
	Samples                       []coldSample `json:"samples"`
	P95NS                         int64        `json:"cold_p95_ns"`
	ReadBytesP95                  int64        `json:"process_read_bytes_p95"`
	StorageReadBytesP95           int64        `json:"storage_read_bytes_p95"`
	PeakDecodeBytes               *int         `json:"peak_decode_bytes,omitempty"`
	PeakDecodeHeapUpperBoundBytes uint64       `json:"peak_decode_go_heap_upper_bound_bytes"`
	PeakRSSBytes                  uint64       `json:"peak_worker_rss_bytes"`
}

type coldSample struct {
	ElapsedNS         int64         `json:"elapsed_ns"`
	ReadBytes         int64         `json:"process_read_bytes"`
	StorageReadBytes  int64         `json:"storage_read_bytes"`
	Payload           *payloadReads `json:"experimental_payload,omitempty"`
	DecodeBytes       *int          `json:"decode_bytes,omitempty"`
	GoAllocationBytes uint64        `json:"go_allocation_bytes"`
	PeakRSSBytes      uint64        `json:"worker_rss_bytes"`
}

func coldQueries() []query {
	all := queries()
	var result []query
	for _, name := range []string{"sparse", "bounded-sparse", "absent", "one-byte", "two-byte", "three-byte", "all"} {
		for _, q := range all {
			if q.name == name {
				result = append(result, q)
				break
			}
		}
	}
	return result
}

func coldRuns(q query, runs int) int {
	if q.literal == sparseLiteral {
		return runs
	}
	return 1
}

func (q *coldQueryReport) summarize() {
	times := make([]int64, len(q.Samples))
	reads := make([]int64, len(q.Samples))
	storage := make([]int64, len(q.Samples))
	for i, s := range q.Samples {
		times[i], reads[i], storage[i] = s.ElapsedNS, s.ReadBytes, s.StorageReadBytes
		if s.DecodeBytes != nil && (q.PeakDecodeBytes == nil || *s.DecodeBytes > *q.PeakDecodeBytes) {
			q.PeakDecodeBytes = s.DecodeBytes
		}
		q.PeakDecodeHeapUpperBoundBytes = max(q.PeakDecodeHeapUpperBoundBytes, s.GoAllocationBytes)
		q.PeakRSSBytes = max(q.PeakRSSBytes, s.PeakRSSBytes)
	}
	q.P95NS, q.ReadBytesP95, q.StorageReadBytesP95 = p95(times), p95(reads), p95(storage)
}

type readIO struct {
	logical, storage int64
	snapshotBytes    int64
}

func parseReadIO(data []byte) (readIO, error) {
	result := readIO{snapshotBytes: int64(len(data))}
	var logical, storage bool
	for line := range strings.Lines(string(data)) {
		key, value, _ := strings.Cut(line, ":")
		var field *int64
		switch key {
		case "rchar":
			field, logical = &result.logical, true
		case "read_bytes":
			field, storage = &result.storage, true
		default:
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 {
			return readIO{}, fmt.Errorf("invalid /proc/self/io %s counter %q", key, value)
		}
		*field = n
	}
	if !logical || !storage {
		return readIO{}, errors.New("missing /proc/self/io read counters")
	}
	return result, nil
}

func (after readIO) delta(before readIO) (logical, storage int64, err error) {
	// Reading the first /proc snapshot increments rchar after that snapshot
	// was formatted. Remove this measurement I/O from the query's count.
	if after.logical < before.logical || after.logical-before.logical < before.snapshotBytes || after.storage < before.storage {
		return 0, 0, errors.New("invalid /proc/self/io query counter delta")
	}
	return after.logical - before.logical - before.snapshotBytes, after.storage - before.storage, nil
}

func validateBenchmarkReport(in io.Reader) error {
	dec := json.NewDecoder(in)
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return fmt.Errorf("expected one benchmark report, trailing input: %v", err)
	}
	var header struct {
		Version int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return err
	}
	if header.Version == 5 {
		var report coldReport
		if err := json.Unmarshal(raw, &report); err != nil {
			return err
		}
		return report.validate()
	}
	return validateQualificationJSON(bytes.NewReader(raw))
}

func (r coldReport) validate() error {
	f := r.Fixture
	if r.SchemaVersion != 5 || r.Mode != "cold" || !knownRevision(r.Revision) || r.Oracle != coldOracle ||
		r.Environment["os"] != "linux" || r.Environment["cache_policy"] != coldCachePolicy ||
		(f.Profile != "mixed" && f.Profile != "entropy") || f.Batches < 1 || f.Batches > maxBatches ||
		f.SparseRuns < 1 || f.SparseRuns > maxRuns || f.TargetBytes != blockTarget {
		return errors.New("invalid schema v5 cold report header")
	}
	if (f.Scale == "smoke" && (f.Batches != scaleBatches["small"] || f.SparseRuns != 1)) ||
		(f.Scale != "smoke" && f.Scale != "custom" && scaleBatches[f.Scale] != f.Batches) {
		return errors.New("cold report fixture scale disagrees with batches or sample count")
	}
	if r.Production.Name != productionV1 || len(r.Experimental) != len(layouts) {
		return errors.New("expected production v1 and six experimental layouts")
	}
	rows := 5
	for i := range f.Batches {
		rows += batchPattern[i%len(batchPattern)]
	}
	if err := validateColdLayout(r.Production, rows, f.SparseRuns, true); err != nil {
		return err
	}
	for i, l := range layouts {
		got := r.Experimental[i]
		if got.Name != l.name() {
			return fmt.Errorf("expected experimental layout %s, got %s", l.name(), got.Name)
		}
		if err := validateColdLayout(got, rows, f.SparseRuns, false); err != nil {
			return err
		}
		for j, phase := range got.Phases {
			v1 := r.Production.Phases[j]
			if phase.Rewrite != nil && phase.Rewrite.AppendedWALBytes != v1.Rewrite.AppendedWALBytes {
				return fmt.Errorf("%s/%s: unmatched WAL growth", got.Name, phase.Phase)
			}
			for k, q := range phase.Queries {
				if q.Matches != v1.Queries[k].Matches {
					return fmt.Errorf("%s/%s/%s: unmatched row count", got.Name, phase.Phase, q.Query)
				}
			}
			if l.prune {
				unfiltered := r.Experimental[int(l.codec)].Phases[j]
				if err := validateBloomPair(phase, unfiltered); err != nil {
					return fmt.Errorf("%s/%s: %w", got.Name, phase.Phase, err)
				}
			} else if phase.BloomBytes != 0 {
				return fmt.Errorf("%s/%s: unexpected Bloom metadata", got.Name, phase.Phase)
			}
		}
	}
	return r.Live.validate(r.Production.Phases)
}

func validateColdLayout(l coldLayout, rows, runs int, production bool) error {
	if len(l.Phases) != len(coldPhases) {
		return fmt.Errorf("%s: incomplete phase matrix", l.Name)
	}
	for i, phase := range l.Phases {
		wantRows := rows
		if i > 0 {
			wantRows += 4
		}
		if phase.Phase != coldPhases[i] || phase.Rows != wantRows || phase.Segments < 1 || phase.ManifestBytes < 1 ||
			phase.ReusedBytes < 0 || phase.RewrittenBytes < 1 || phase.RetiredBytes < 0 ||
			phase.RetainedBytes != phase.ReusedBytes+phase.RewrittenBytes || (production && phase.BloomBytes != 0) {
			return fmt.Errorf("%s/%s: invalid publication counters", l.Name, phase.Phase)
		}
		if i == 0 {
			if phase.Rewrite != nil || phase.ReusedBytes != 0 || phase.RetiredBytes != 0 {
				return fmt.Errorf("%s: unexpected rewrite on initial publication", l.Name)
			}
		} else {
			boundary := coldRewriteBoundary
			if production {
				boundary = componentBoundary
			}
			if phase.ReusedBytes+phase.RetiredBytes != l.Phases[0].RetainedBytes ||
				!validColdAmplification(phase.Rewrite, boundary) ||
				(!production && phase.Rewrite.WrittenBytes != uint64(phase.RewrittenBytes+phase.ManifestBytes)) {
				return fmt.Errorf("%s/%s: invalid rewrite amplification", l.Name, phase.Phase)
			}
		}
		probes := coldQueries()
		if len(phase.Queries) != len(probes) {
			return fmt.Errorf("%s/%s: incomplete query matrix", l.Name, phase.Phase)
		}
		for j, q := range probes {
			got := phase.Queries[j]
			if err := got.validate(q, wantRows, coldRuns(q, runs), production); err != nil {
				return fmt.Errorf("%s/%s/%s: %w", l.Name, phase.Phase, q.name, err)
			}
		}
	}
	return nil
}

func validColdAmplification(a *amplification, boundary string) bool {
	return a != nil && a.Boundary == boundary && a.WrittenBytes > 0 && a.AppendedWALBytes > 0 &&
		a.Ratio == float64(a.WrittenBytes)/float64(a.AppendedWALBytes)
}

func (got coldQueryReport) validate(q query, rows, runs int, production bool) error {
	if got.Query != q.name || got.Literal != q.literal || !got.Start.Equal(q.start) || !got.End.Equal(q.end) ||
		len(got.Samples) != runs || got.Matches < 0 || got.Matches > rows ||
		(q.name == "all" && got.Matches != rows) || (q.name == "absent" && got.Matches != 0) {
		return errors.New("invalid query metadata or sample count")
	}
	for _, s := range got.Samples {
		if s.ElapsedNS <= 0 || s.ReadBytes < 0 || s.StorageReadBytes < 0 || s.PeakRSSBytes == 0 ||
			s.GoAllocationBytes == 0 || production != (s.Payload == nil) || production != (s.DecodeBytes == nil) {
			return errors.New("invalid cold sample")
		}
		if !production && (s.Payload.Blocks < 0 || s.Payload.Bytes < 0 || *s.DecodeBytes < 0 ||
			(s.Payload.Blocks == 0) != (s.Payload.Bytes == 0) || (s.Payload.Blocks == 0) != (*s.DecodeBytes == 0) ||
			s.Payload.Bytes > s.ReadBytes || *s.Payload != *got.Samples[0].Payload || (got.Matches > 0 && s.Payload.Blocks == 0)) {
			return errors.New("invalid or inconsistent experimental payload/decode counters")
		}
	}
	expected := coldQueryReport{Samples: got.Samples}
	expected.summarize()
	if got.P95NS != expected.P95NS || got.ReadBytesP95 != expected.ReadBytesP95 ||
		got.StorageReadBytesP95 != expected.StorageReadBytesP95 || got.PeakRSSBytes != expected.PeakRSSBytes ||
		got.PeakDecodeHeapUpperBoundBytes != expected.PeakDecodeHeapUpperBoundBytes || (got.PeakDecodeBytes == nil) != (expected.PeakDecodeBytes == nil) ||
		(got.PeakDecodeBytes != nil && *got.PeakDecodeBytes != *expected.PeakDecodeBytes) {
		return errors.New("cold aggregates disagree with samples")
	}
	return nil
}

func validateBloomPair(on, off coldPhase) error {
	if on.Segments != off.Segments || on.BloomBytes != int64(on.Segments)*int64(len(summary{})) ||
		on.RetainedBytes-off.RetainedBytes != on.BloomBytes {
		return errors.New("Bloom pair disagrees on segment packing or metadata size")
	}
	for i, q := range on.Queries {
		pruned, unfiltered := q.Samples[0].Payload, off.Queries[i].Samples[0].Payload
		if pruned.Blocks > unfiltered.Blocks || pruned.Bytes > unfiltered.Bytes ||
			(len(q.Literal) < 4 && *pruned != *unfiltered) {
			return fmt.Errorf("%s: invalid Bloom payload delta", q.Query)
		}
	}
	return nil
}

type liveReport struct {
	Status       string                   `json:"status"`
	Reason       string                   `json:"reason,omitempty"`
	Revision     string                   `json:"revision,omitempty"`
	BinarySHA256 string                   `json:"binary_sha256,omitempty"`
	Measurements []publicationMeasurement `json:"measurements,omitempty"`
}

type publicationMeasurement struct {
	Scenario           string        `json:"scenario"`
	CoveredRows        int           `json:"covered_rows"`
	ElapsedNS          int64         `json:"elapsed_ns"`
	PeakRSSBytes       uint64        `json:"peak_rss_bytes,omitempty"`
	HTTPQueryParity    bool          `json:"http_query_parity,omitempty"`
	WriteAmplification amplification `json:"write_amplification"`
}

func (r liveReport) validate(phases []coldPhase) error {
	if r.Status == "unavailable" && r.Reason != "" && len(r.Measurements) == 0 {
		return nil
	}
	if r.Status != "measured" || r.Reason != "" || len(r.Revision) != 40 || len(r.BinarySHA256) != 64 ||
		strings.Trim(r.Revision+r.BinarySHA256, "0123456789abcdefABCDEF") != "" ||
		len(r.Measurements) != len(phases)-1 {
		return errors.New("invalid live compaction evidence")
	}
	for i, m := range r.Measurements {
		phase := phases[i+1]
		if m.Scenario != phase.Phase || m.CoveredRows != phase.Rows || m.ElapsedNS <= 0 || !m.HTTPQueryParity ||
			!validColdAmplification(&m.WriteAmplification, liveBoundary) ||
			m.WriteAmplification.AppendedWALBytes != phase.Rewrite.AppendedWALBytes {
			return fmt.Errorf("%s: live compaction disagrees with component fixture", m.Scenario)
		}
	}
	return nil
}
