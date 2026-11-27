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
	SchemaVersion int                   `json:"schema_version"`
	Mode          string                `json:"mode"`
	Revision      string                `json:"revision"`
	Fixture       coldFixture           `json:"fixture"`
	Environment   map[string]any        `json:"environment"`
	Oracle        string                `json:"oracle"`
	Production    coldLayout            `json:"production_v1_component"`
	Experimental  []coldLayout          `json:"experimental_layouts"`
	Live          liveReport            `json:"live_server_compaction"`
	Backfills     []coldBackfillFixture `json:"backfill_scenarios,omitempty"`
	Pairing       *coldPairing          `json:"pairing,omitempty"`
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
	Phase         string  `json:"phase"`
	Rows          int     `json:"rows"`
	Segments      int     `json:"segments"`
	ManifestBytes int64   `json:"manifest_bytes"`
	BloomBytes    int64   `json:"bloom_metadata_bytes"`
	PayloadSizes  []int   `json:"experimental_segment_payload_bytes,omitempty"`
	StoredBytes   *uint64 `json:"stored_segment_and_manifest_bytes,omitempty"`
	segmentBytes
	Rewrite *amplification    `json:"rewrite_amplification,omitempty"`
	Queries []coldQueryReport `json:"queries"`
}

type coldQueryReport struct {
	Query                         string           `json:"query"`
	Literal                       string           `json:"literal"`
	Start                         time.Time        `json:"start_inclusive"`
	End                           time.Time        `json:"end_exclusive"`
	Matches                       int              `json:"matching_rows"`
	Selectivity                   *coldSelectivity `json:"selectivity,omitempty"`
	Samples                       []coldSample     `json:"samples"`
	P95NS                         int64            `json:"cold_p95_ns"`
	ReadBytesP95                  int64            `json:"process_read_bytes_p95"`
	StorageReadBytesP95           int64            `json:"storage_read_bytes_p95"`
	PeakDecodeBytes               *int             `json:"peak_decode_bytes,omitempty"`
	PeakDecodeHeapUpperBoundBytes uint64           `json:"peak_decode_go_heap_upper_bound_bytes"`
	PeakRSSBytes                  uint64           `json:"peak_worker_rss_bytes"`
}

type coldSample struct {
	ElapsedNS         int64            `json:"elapsed_ns"`
	ReadBytes         int64            `json:"process_read_bytes"`
	StorageReadBytes  int64            `json:"storage_read_bytes"`
	Payload           *payloadReads    `json:"experimental_payload,omitempty"`
	DecodeBytes       *int             `json:"decode_bytes,omitempty"`
	GoAllocationBytes uint64           `json:"go_allocation_bytes"`
	PeakRSSBytes      uint64           `json:"worker_rss_bytes"`
	Scan              *coldScanMetrics `json:"experimental_scan,omitempty"`
	Proof             *coldSampleProof `json:"pairing,omitempty"`
}

func (s *coldSample) UnmarshalJSON(data []byte) error {
	// Zero storage reads are valid. Require the field so a missing counter
	// cannot become a measured zero during report or worker validation.
	type sampleFields coldSample
	var decoded struct {
		sampleFields
		StorageReadBytes *int64 `json:"storage_read_bytes"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err != nil {
		return err
	}
	if decoded.StorageReadBytes == nil {
		return errors.New("missing cold sample storage read counter")
	}
	*s = coldSample(decoded.sampleFields)
	s.StorageReadBytes = *decoded.StorageReadBytes
	return nil
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

func (q *coldQueryReport) summarize() {
	q.PeakDecodeBytes, q.PeakDecodeHeapUpperBoundBytes, q.PeakRSSBytes = nil, 0, 0
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

func parseProcessIO(data []byte, field string) (uint64, error) {
	var value uint64
	var found bool
	for line := range strings.Lines(string(data)) {
		key, raw, _ := strings.Cut(line, ":")
		if key != field {
			continue
		}
		if found {
			return 0, fmt.Errorf("duplicate /proc io counter %s", field)
		}
		n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid /proc io counter %s: %w", field, err)
		}
		value, found = n, true
	}
	if !found {
		return 0, fmt.Errorf("missing /proc io counter %s", field)
	}
	return value, nil
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
			if logical {
				return readIO{}, errors.New("duplicate /proc/self/io rchar counter")
			}
			field, logical = &result.logical, true
		case "read_bytes":
			if storage {
				return readIO{}, errors.New("duplicate /proc/self/io read_bytes counter")
			}
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
	return readBenchmarkReport(in, nil)
}

func readBenchmarkReport(in io.Reader, candidate *candidateIdentity) error {
	dec := json.NewDecoder(in)
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return fmt.Errorf("expected one benchmark report, trailing input: %v", err)
	}
	if err := rejectDuplicateReportFields(raw); err != nil {
		return err
	}
	var header struct {
		Version int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return err
	}
	if candidate != nil && header.Version != coldMatrixSchema {
		return errors.New("live candidate qualification requires a schema v8 cold matrix")
	}
	if hasColdMatrixEvidence(header.Version) {
		return validateColdMatrixJSON(raw, candidate)
	}
	if header.Version == 5 || header.Version == pairedColdSchema {
		var report coldReport
		cold := json.NewDecoder(bytes.NewReader(raw))
		if header.Version == pairedColdSchema {
			cold.DisallowUnknownFields()
		}
		if err := cold.Decode(&report); err != nil {
			return err
		}
		if header.Version == pairedColdSchema && report.Pairing != nil {
			if err := validatePairEncoding(raw, report.Pairing); err != nil {
				return err
			}
		}
		return report.validate()
	}
	return validateQualificationJSON(bytes.NewReader(raw))
}

func (r coldReport) validate() error {
	if err := r.validateColdMeasurements(); err != nil {
		return err
	}
	if r.SchemaVersion == pairedColdSchema || hasColdMatrixEvidence(r.SchemaVersion) {
		return r.validatePairs()
	}
	if r.Pairing != nil {
		return errors.New("unexpected pairing in schema v5 cold report")
	}
	for _, l := range append([]coldLayout{r.Production}, r.Experimental...) {
		for _, phase := range l.Phases {
			for _, q := range phase.Queries {
				for _, sample := range q.Samples {
					if sample.Proof != nil {
						return errors.New("unexpected sample pairing in schema v5 cold report")
					}
				}
			}
		}
	}
	return nil
}

func (r coldReport) validateColdMeasurements() error {
	f := r.Fixture
	if (r.SchemaVersion != 5 && r.SchemaVersion != pairedColdSchema && !hasColdMatrixEvidence(r.SchemaVersion)) || r.Mode != "cold" || !knownRevision(r.Revision) || r.Oracle != coldOracle ||
		r.Environment["os"] != "linux" || r.Environment["cache_policy"] != coldCachePolicy ||
		(f.Profile != "mixed" && f.Profile != "entropy") || f.Batches < 1 || f.Batches > maxBatches ||
		f.SparseRuns < 1 || f.SparseRuns > maxRuns || f.TargetBytes != blockTarget {
		return errors.New("invalid cold report header")
	}
	if (f.Scale == "smoke" && (f.Batches != scaleBatches["small"] || f.SparseRuns != 1)) ||
		(f.Scale != "smoke" && f.Scale != "custom" && scaleBatches[f.Scale] != f.Batches) {
		return errors.New("cold report fixture scale disagrees with batches or sample count")
	}
	if (r.SchemaVersion == coldMatrixSchema && len(r.Backfills) != len(coldPhases)-1) ||
		(r.SchemaVersion != coldMatrixSchema && r.Backfills != nil) {
		return errors.New("invalid cold backfill scenario evidence")
	}
	if r.Production.Name != productionV1 || len(r.Experimental) != len(layouts) {
		return errors.New("expected production v1 and six experimental layouts")
	}
	rows := 5
	for i := range f.Batches {
		rows += batchPattern[i%len(batchPattern)]
	}
	if err := validateColdLayout(r.Production, rows, f.SparseRuns, r.SchemaVersion, true); err != nil {
		return err
	}
	for i, l := range layouts {
		got := r.Experimental[i]
		if got.Name != l.name() {
			return fmt.Errorf("expected experimental layout %s, got %s", l.name(), got.Name)
		}
		if err := validateColdLayout(got, rows, f.SparseRuns, r.SchemaVersion, false); err != nil {
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

func validateColdLayout(l coldLayout, rows, runs, schema int, production bool) error {
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
			phase.RetainedBytes < phase.ReusedBytes || phase.RetainedBytes-phase.ReusedBytes != phase.RewrittenBytes ||
			(production && phase.BloomBytes != 0) {
			return fmt.Errorf("%s/%s: invalid publication counters", l.Name, phase.Phase)
		}
		if hasColdMatrixEvidence(schema) {
			if phase.StoredBytes == nil || *phase.StoredBytes != phase.storedBytes() {
				return fmt.Errorf("%s/%s: stored bytes disagree with retained segment and manifest bytes", l.Name, phase.Phase)
			}
		} else if phase.StoredBytes != nil {
			return fmt.Errorf("%s/%s: unexpected stored byte total", l.Name, phase.Phase)
		}
		if schema == coldMatrixSchema {
			if production {
				if phase.PayloadSizes != nil || phase.Segments > coldV1MaxSegments {
					return fmt.Errorf("%s/%s: invalid v1 segment inventory", l.Name, phase.Phase)
				}
				if i > 0 && phase.Rewrite != nil && phase.Rewrite.WrittenBytes < uint64(phase.RewrittenBytes)+uint64(phase.ManifestBytes) {
					return fmt.Errorf("%s/%s: v1 publication writes omit segment or manifest bytes", l.Name, phase.Phase)
				}
			} else if len(phase.PayloadSizes) != phase.Segments {
				return fmt.Errorf("%s/%s: incomplete experimental segment payload inventory", l.Name, phase.Phase)
			}
		} else if phase.PayloadSizes != nil {
			return fmt.Errorf("%s/%s: unexpected budget evidence", l.Name, phase.Phase)
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
			if phase.ReusedBytes > l.Phases[0].RetainedBytes || l.Phases[0].RetainedBytes-phase.ReusedBytes != phase.RetiredBytes ||
				!validColdAmplification(phase.Rewrite, boundary) ||
				(!production && phase.Rewrite.WrittenBytes != uint64(phase.RewrittenBytes)+uint64(phase.ManifestBytes)) {
				return fmt.Errorf("%s/%s: invalid rewrite amplification", l.Name, phase.Phase)
			}
		}
		probes := coldQueryPlan(schema)
		if len(phase.Queries) != len(probes) {
			return fmt.Errorf("%s/%s: incomplete query matrix", l.Name, phase.Phase)
		}
		for j, q := range probes {
			got := phase.Queries[j]
			if err := got.validate(q, wantRows, coldQueryRuns(schema, q, runs), production, schema); err != nil {
				return fmt.Errorf("%s/%s/%s: %w", l.Name, phase.Phase, q.name, err)
			}
			if (got.Selectivity != nil) != hasColdMatrixEvidence(schema) {
				return fmt.Errorf("%s/%s/%s: unexpected selectivity evidence", l.Name, phase.Phase, q.name)
			}
			for _, sample := range got.Samples {
				if (sample.Scan != nil) != (hasColdMatrixEvidence(schema) && !production) {
					return fmt.Errorf("%s/%s/%s: invalid experimental scan evidence", l.Name, phase.Phase, q.name)
				}
			}
		}
	}
	return nil
}

func (p coldPhase) storedBytes() uint64 {
	return uint64(p.RetainedBytes) + uint64(p.ManifestBytes)
}

func validColdAmplification(a *amplification, boundary string) bool {
	return a != nil && a.Boundary == boundary && a.WrittenBytes > 0 && a.AppendedWALBytes > 0 &&
		a.Ratio == float64(a.WrittenBytes)/float64(a.AppendedWALBytes)
}

func (got coldQueryReport) validate(q query, rows, runs int, production bool, schema int) error {
	if got.Query != q.name || got.Literal != q.literal || !got.Start.Equal(q.start) || !got.End.Equal(q.end) ||
		len(got.Samples) != runs || got.Matches < 0 || got.Matches > rows ||
		(q.name == "all" && got.Matches != rows) || (q.name == "absent" && got.Matches != 0) {
		return errors.New("invalid query metadata or sample count")
	}
	v1Accounting := production && schema == coldMatrixSchema
	for _, s := range got.Samples {
		if s.ElapsedNS <= 0 || s.ReadBytes < 0 || s.StorageReadBytes < 0 || s.PeakRSSBytes == 0 ||
			s.GoAllocationBytes == 0 || production != (s.Payload == nil) || (v1Accounting || !production) != (s.DecodeBytes != nil) {
			return errors.New("invalid cold sample")
		}
		if v1Accounting && (*s.DecodeBytes < 0 || int64(*s.DecodeBytes) > s.ReadBytes ||
			(s.ReadBytes == 0) != (*s.DecodeBytes == 0) ||
			(got.Selectivity != nil && got.Selectivity.TimeWindowRows > 0 && *s.DecodeBytes == 0) ||
			*s.DecodeBytes != *got.Samples[0].DecodeBytes) {
			return errors.New("invalid or inconsistent v1 decode buffer bytes")
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
	Scenario           string           `json:"scenario"`
	CoveredRows        int              `json:"covered_rows"`
	ElapsedNS          int64            `json:"elapsed_ns"`
	PeakRSSBytes       uint64           `json:"peak_rss_bytes,omitempty"`
	HTTPQueryParity    bool             `json:"http_query_parity,omitempty"`
	Publication        *livePublication `json:"publication,omitempty"`
	WriteAmplification amplification    `json:"write_amplification"`
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
