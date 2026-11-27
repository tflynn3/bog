package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"bog/internal/checkpoint"
)

const (
	historicalColdMatrixSchema = 7
	coldMatrixSchema           = 8
)

func hasColdMatrixEvidence(schema int) bool {
	return schema == historicalColdMatrixSchema || schema == coldMatrixSchema
}

type coldMatrixReport struct {
	SchemaVersion int            `json:"schema_version"`
	Mode          string         `json:"mode"`
	Revision      string         `json:"revision"`
	RunID         string         `json:"run_id"`
	Environment   map[string]any `json:"environment"`
	Protocol      coldProtocol   `json:"protocol"`
	Profiles      []coldReport   `json:"profiles"`
}

type coldProtocol struct {
	FixtureVersion string       `json:"fixture_version"`
	ProfileOrder   []string     `json:"profile_order"`
	BatchPattern   []int        `json:"ordinary_batch_pattern_rows"`
	BackfillRows   int          `json:"rows_per_independent_backfill"`
	MaxRawBlock    int          `json:"max_raw_block_bytes"`
	ZstdWindow     int          `json:"zstd_max_window_bytes"`
	MaxLineBytes   int          `json:"max_line_bytes"`
	CachePolicy    string       `json:"cache_policy"`
	SampleOrder    string       `json:"sample_order"`
	Probes         []coldProbe  `json:"probes"`
	Budgets        *coldBudgets `json:"budgets,omitempty"`
}

type coldProbe struct {
	Query   string    `json:"query"`
	Literal string    `json:"literal"`
	Start   time.Time `json:"start_inclusive"`
	End     time.Time `json:"end_exclusive"`
	Rounds  int       `json:"rounds"`
}

func matrixProtocol(first string, runs int) coldProtocol {
	profiles := []string{"mixed", "entropy"}
	if first == "entropy" {
		profiles[0], profiles[1] = profiles[1], profiles[0]
	}
	p := coldProtocol{
		FixtureVersion: "cold-log-v1", ProfileOrder: profiles, BatchPattern: slices.Clone(batchPattern),
		BackfillRows: 4, MaxLineBytes: largeLineSize, MaxRawBlock: maxBlock,
		ZstdWindow: zstdWindow, CachePolicy: coldCachePolicy,
		SampleOrder: "sequential profiles, phases, probes and rounds; rotate leading layout by zero-based phase + probe + round",
	}
	for _, q := range coldQueryPlan(coldMatrixSchema) {
		p.Probes = append(p.Probes, coldProbe{q.name, q.literal, q.start, q.end, coldQueryRuns(coldMatrixSchema, q, runs)})
	}
	return p
}

func coldQueryPlan(schema int) []query {
	plan := coldQueries()
	if hasColdMatrixEvidence(schema) {
		for _, q := range queries() {
			if q.name == "medium" || q.name == "dense" {
				plan = append(plan, q)
			}
		}
	}
	return plan
}

func coldComparisonProbe(schema int, q query) bool {
	return q.literal == sparseLiteral || (hasColdMatrixEvidence(schema) && (q.name == "medium" || q.name == "dense"))
}

func coldQueryRuns(schema int, q query, runs int) int {
	if coldComparisonProbe(schema, q) {
		return runs
	}
	return 1
}

type coldSelectivity struct {
	PopulationRows int `json:"population_rows"`
	TimeWindowRows int `json:"time_window_rows"`
	MatchingRows   int `json:"matching_rows"`
}

func (s *coldSelectivity) UnmarshalJSON(data []byte) error {
	type fields coldSelectivity
	return decodeColdEvidence(data, (*fields)(s), "selectivity")
}

func decodeColdEvidence(data []byte, dst any, label string) error {
	if err := json.Unmarshal(data, dst); err != nil {
		return err
	}
	return validateColdEncoding(data, dst, label)
}

func validateColdEncoding(data []byte, value any, label string) error {
	canonical, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return validateColdFields(data, canonical, label)
}

func measureColdSelectivity(rows []checkpoint.BlockRow, q query, matches int) coldSelectivity {
	windows := len(rows)
	if !q.start.Equal(firstTime) || !q.end.Equal(endTime) {
		windows = 0
		for _, row := range rows {
			if !row.Row.TS.Before(q.start) && row.Row.TS.Before(q.end) {
				windows++
			}
		}
	}
	return coldSelectivity{len(rows), windows, matches}
}

func validateColdMatrixJSON(raw json.RawMessage, candidate *candidateIdentity) error {
	var report coldMatrixReport
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&report); err != nil {
		return err
	}
	if err := report.validateHeader(); err != nil {
		return err
	}
	var wire struct {
		Profiles []json.RawMessage `json:"profiles"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	for i, profile := range report.Profiles {
		if err := profile.validate(); err != nil {
			return fmt.Errorf("%s: %w", profile.Fixture.Profile, err)
		}
		if report.SchemaVersion == coldMatrixSchema {
			if err := report.Protocol.Budgets.validateMeasurements(profile); err != nil {
				return fmt.Errorf("%s: %w", profile.Fixture.Profile, err)
			}
		}
		if err := validateColdEncoding(wire.Profiles[i], profile, "cold profile"); err != nil {
			return fmt.Errorf("%s: %w", profile.Fixture.Profile, err)
		}
	}
	if candidate != nil {
		return report.requireCandidate(*candidate)
	}
	return nil
}

func (r coldMatrixReport) validateHeader() error {
	if !hasColdMatrixEvidence(r.SchemaVersion) || r.Mode != "cold-matrix" || !knownRevision(r.Revision) ||
		!validColdRunID(r.RunID) || len(r.Profiles) != 2 {
		return errors.New("invalid cold matrix header")
	}
	first := r.Profiles[0]
	expected := matrixProtocol(first.Fixture.Profile, first.Fixture.SparseRuns)
	if r.SchemaVersion == coldMatrixSchema {
		if r.Protocol.Budgets == nil || r.Protocol.Budgets.ColdP95NS <= 0 {
			return errors.New("missing or invalid cold matrix budgets")
		}
		expected.Budgets = new(matrixBudgets(time.Duration(r.Protocol.Budgets.ColdP95NS)))
	}
	if !reflect.DeepEqual(r.Protocol, expected) {
		return errors.New("invalid cold matrix fixture protocol or sample schedule")
	}
	for i, p := range r.Profiles {
		wantFixture := first.Fixture
		wantFixture.Profile = r.Protocol.ProfileOrder[i]
		if p.Fixture != wantFixture || p.SchemaVersion != r.SchemaVersion || p.Revision != r.Revision ||
			!reflect.DeepEqual(p.Environment, r.Environment) || p.Pairing == nil || p.Pairing.RunID != r.RunID {
			return fmt.Errorf("cold matrix profile %d disagrees on fixture, execution, revision or environment", i+1)
		}
		if p.Live.Status != first.Live.Status || p.Live.Revision != first.Live.Revision ||
			p.Live.BinarySHA256 != first.Live.BinarySHA256 {
			return fmt.Errorf("cold matrix profile %d disagrees on live candidate", i+1)
		}
	}
	return nil
}

// Reconstruct the shared append-order packing once per phase from the fixture.
func coldPhaseBlocks(blocks [][]checkpoint.BlockRow, initialRows int, rows []checkpoint.BlockRow) ([][]checkpoint.BlockRow, int) {
	if initialRows == len(rows) {
		return blocks, 0
	}
	last := blocks[len(blocks)-1]
	appended := rows[initialRows:]
	if rawBlockBytes(last)+encodedRowBytes(appended[0]) > blockTarget {
		return append(slices.Clone(blocks), partition(appended)...), len(blocks)
	}
	reused := len(blocks) - 1
	return append(slices.Clone(blocks[:reused]), partition(rows[initialRows-len(last):])...), reused
}

type coldBlockShape struct {
	rawBytes int
	rowCount int
	min, max time.Time
	bloom    summary
}

func coldBlockShapes(blocks [][]checkpoint.BlockRow) []coldBlockShape {
	shapes := make([]coldBlockShape, 0, len(blocks))
	for _, block := range blocks {
		b := coldBlockShape{rawBytes: rawBlockBytes(block), rowCount: len(block), min: block[0].Row.TS, max: block[0].Row.TS}
		for _, row := range block {
			if row.Row.TS.Before(b.min) {
				b.min = row.Row.TS
			}
			if row.Row.TS.After(b.max) {
				b.max = row.Row.TS
			}
			b.bloom.add(row.Row.Line)
		}
		shapes = append(shapes, b)
	}
	return shapes
}

type coldScanLimits struct {
	visited, decoded, oversized int
	maxRaw, ordinaryRaw         int
	ordinaryPeak, oversizedPeak int // Exact if payloads are stored raw.
	rawPayload                  int64
}

func (b coldBlockShape) matches(q query, bloom bool) bool {
	return !b.max.Before(q.start) && b.min.Before(q.end) && (!bloom || b.bloom.mayContain(q.literal))
}

func (b coldBlockShape) workingBytes() int {
	return 2*b.rawBytes - blockHeader + b.rowCount*(decodedRowBytes-blockRowWidth)
}

func coldLimits(shapes []coldBlockShape, q query, bloom bool) coldScanLimits {
	s := coldScanLimits{visited: len(shapes)}
	for _, b := range shapes {
		if !b.matches(q, bloom) {
			continue
		}
		s.decoded++
		s.maxRaw = max(s.maxRaw, b.rawBytes)
		s.rawPayload += int64(b.rawBytes)
		working := b.workingBytes()
		if b.rawBytes > blockTarget {
			s.oversized++
			s.oversizedPeak = max(s.oversizedPeak, working)
		} else {
			s.ordinaryRaw = max(s.ordinaryRaw, b.rawBytes)
			s.ordinaryPeak = max(s.ordinaryPeak, working)
		}
	}
	return s
}

func (limits coldScanLimits) validate(s coldSample, l layout, retained int64) error {
	m := s.Scan
	d := m.Decode
	indexBytes := int64(limits.visited) * int64(headerSize+l.indexWidth())
	if m.SegmentsVisited != limits.visited || m.SegmentsDecoded != limits.decoded ||
		s.Payload.Blocks != limits.decoded || m.LargestDecodedRaw != limits.maxRaw || m.IndexBytes != indexBytes ||
		s.Payload.Bytes < int64(limits.decoded) || s.Payload.Bytes > limits.rawPayload ||
		retained <= indexBytes || s.Payload.Bytes > retained-indexBytes ||
		s.ReadBytes < indexBytes || s.ReadBytes-indexBytes < s.Payload.Bytes ||
		(limits.decoded == limits.visited && s.Payload.Bytes != retained-indexBytes) {
		return errors.New("experimental scan bytes or segment fan-out disagree with fixture")
	}
	ordinaryBound := decodeBound(blockTarget)
	overallBound := ordinaryBound
	if limits.oversized > 0 {
		overallBound = max(overallBound, oversizedDecodeBound(limits.maxRaw))
	}
	if d.OversizedSegments != limits.oversized || d.WorkingBoundBytes != overallBound || d.OrdinaryBoundBytes != ordinaryBound ||
		d.PeakWorkingBytes != *s.DecodeBytes || d.PeakWorkingBytes != max(d.PeakOrdinaryBytes, d.PeakOversizedBytes) ||
		!validColdPeak(d.PeakOrdinaryBytes, limits.ordinaryPeak, limits.ordinaryRaw) ||
		!validColdPeak(d.PeakOversizedBytes, limits.oversizedPeak, limits.maxRaw) {
		return errors.New("experimental decode counters violate fixture ceilings")
	}
	if l.codec == rawCodec && (s.Payload.Bytes != limits.rawPayload ||
		d.PeakOrdinaryBytes != limits.ordinaryPeak || d.PeakOversizedBytes != limits.oversizedPeak) {
		return errors.New("raw decode counters disagree with uncompressed fixture")
	}
	return nil
}

func validColdPeak(peak, rawPeak, maxRaw int) bool {
	if rawPeak == 0 {
		return peak == 0
	}
	// Compression either falls back to raw or adds one stored buffer smaller than raw.
	return peak >= rawPeak && peak < rawPeak+maxRaw
}
