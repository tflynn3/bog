package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"bog/internal/checkpoint"
)

const pairedColdSchema = 6

type coldPairing struct {
	RunID         string          `json:"run_id"`
	FixtureSHA256 string          `json:"fixture_sha256"`
	Comparisons   []coldCodecPair `json:"comparisons"`
}

// Every layout measured in a round shares PairID. Layout identifies the
// endpoint, and RowsSHA256 covers the verified, ordered result stream.
type coldSampleProof struct {
	Round      int    `json:"round"`
	PairID     string `json:"pair_id"`
	Layout     string `json:"layout"`
	RowsSHA256 string `json:"ordered_rows_sha256"`
}

type coldCodecPair struct {
	Phase             string            `json:"phase"`
	Query             string            `json:"query"`
	CachePolicy       string            `json:"cache_policy"`
	Bloom             bool              `json:"bloom"`
	Baseline          string            `json:"baseline_layout"`
	Candidate         string            `json:"candidate_layout"`
	RowsSHA256        string            `json:"ordered_rows_sha256"`
	Rounds            []coldRoundDelta  `json:"rounds"`
	PhysicalRead      coldDeltaSummary  `json:"physical_read_bytes"`
	StorageReadRounds int               `json:"storage_read_observed_rounds"`
	PeakRSS           coldDeltaSummary  `json:"peak_worker_rss_bytes"`
	Rewrite           *coldRewriteDelta `json:"rewrite_amplification,omitempty"`
	Tradeoffs         *coldTradeoffs    `json:"tradeoffs,omitempty"`
	Decision          string            `json:"decision"`
}

type coldRoundDelta struct {
	Round        int    `json:"round"`
	PairID       string `json:"pair_id"`
	PhysicalRead int64  `json:"physical_read_bytes_delta"`
	PeakRSS      int64  `json:"peak_worker_rss_bytes_delta"`
}

type coldDeltaSummary struct {
	P50    int64 `json:"p50_delta"`
	Lower  int   `json:"candidate_lower_rounds"`
	Equal  int   `json:"equal_rounds"`
	Higher int   `json:"candidate_higher_rounds"`
}

type coldRewriteDelta struct {
	Boundary         string  `json:"boundary"`
	AppendedWALBytes uint64  `json:"appended_wal_bytes"`
	WrittenBytes     int64   `json:"written_bytes_delta"`
	Ratio            float64 `json:"ratio_delta"`
}

func coldRowsDigest(rows []checkpoint.BlockRow) string {
	h := sha256.New()
	var buf [32 << 10]byte
	for _, row := range rows {
		header := rowHeader(row)
		h.Write(header[:]) // hash.Hash.Write never returns an error.
		for line := row.Row.Line; len(line) != 0; {
			n := copy(buf[:], line)
			h.Write(buf[:n])
			line = line[n:]
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func coldFixtureDigest(f coldFixture, input fixtureInput) string {
	identity := fmt.Appendf(nil, "cold-fixture-v1/%q/%q/%d/%d/%d/%d/%s",
		f.Profile, f.Scale, f.Seed, f.Batches, f.SparseRuns, f.TargetBytes, coldRowsDigest(input.rows))
	return digestString(identity)
}

func (p coldPairing) pairID(phase string, q query, round int) string {
	identity := fmt.Appendf(nil, "cold-round-v1/%s/%s/%q/%q/%q/%d/%d/%d/%d/%q/%d",
		p.RunID, p.FixtureSHA256, phase, q.name, q.literal, q.start.Unix(), q.start.Nanosecond(),
		q.end.Unix(), q.end.Nanosecond(), coldCachePolicy, round)
	return digestString(identity)
}

func digestString(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validColdRunID(id string) bool {
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == id
}

// sealPairs runs before publication. The ordinary cold report validator
// establishes the complete layout, phase and query matrix before pairing.
func (r *coldReport) sealPairs(ctx context.Context, input fixtureInput) error {
	if err := r.validateColdMeasurements(); err != nil {
		return err
	}
	pairs, err := r.derivePairs(ctx, input)
	if err != nil {
		return err
	}
	r.Pairing.Comparisons = pairs
	return nil
}

func (r coldReport) validatePairs() error {
	if r.Pairing == nil || !validColdRunID(r.Pairing.RunID) {
		return errors.New("missing or invalid cold pairing run identity")
	}
	input := fixture(r.Fixture.Batches, r.Fixture.Profile == "entropy", r.Fixture.Seed)
	pairs, err := r.derivePairs(context.Background(), input)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(r.Pairing.Comparisons, pairs) {
		return errors.New("cold codec comparisons disagree with verified pairs")
	}
	return nil
}

// Struct decoding alone would turn an omitted zero delta (or a missing
// false Bloom flag) into a valid measurement. Check the pairing object's
// JSON representation as well as its recomputed typed values.
func validatePairEncoding(raw json.RawMessage, pairing *coldPairing) error {
	var wire struct {
		Pairing json.RawMessage `json:"pairing"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	canonical, err := json.Marshal(pairing)
	if err != nil {
		return err
	}
	return validateColdFields(wire.Pairing, canonical, "cold codec comparison")
}

func (r coldReport) derivePairs(ctx context.Context, input fixtureInput) ([]coldCodecPair, error) {
	p := r.Pairing
	if p == nil || !validColdRunID(p.RunID) || p.FixtureSHA256 != coldFixtureDigest(r.Fixture, input) {
		return nil, errors.New("cold pairing fixture or execution identity mismatch")
	}
	probes := coldQueryPlan(r.SchemaVersion)
	bursts := input.backfills()
	allLayouts := append([]coldLayout{r.Production}, r.Experimental...)
	var pairs []coldCodecPair
	var initialBlocks [][]checkpoint.BlockRow
	inventories := make([]snapshot, len(layouts))
	if hasColdMatrixEvidence(r.SchemaVersion) {
		initialBlocks = partition(input.rows)
	}
	for phaseIndex, phase := range coldPhases {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows := input.rows
		if phaseIndex > 0 {
			burst := bursts[phaseIndex-1]
			rows = input.withBackfill(burst)
			backfill, err := describeColdBackfill(burst)
			if err != nil {
				return nil, fmt.Errorf("%s: encode fixture backfill: %w", phase, err)
			}
			if r.SchemaVersion == coldMatrixSchema && r.Backfills[phaseIndex-1] != backfill {
				return nil, fmt.Errorf("%s: backfill timeline or WAL bytes disagree with fixture", phase)
			}
			// The measurement validator already matched every experimental
			// denominator to production. Verify their common value against
			// the bytes represented by the fixture's one committed frame.
			if got := r.Production.Phases[phaseIndex].Rewrite.AppendedWALBytes; got != backfill.AppendedWALBytes {
				return nil, fmt.Errorf("%s: measured WAL growth %d, fixture frame %d", phase, got, backfill.AppendedWALBytes)
			}
		}
		var shapes []coldBlockShape
		if hasColdMatrixEvidence(r.SchemaVersion) {
			blocks, reused := coldPhaseBlocks(initialBlocks, len(input.rows), rows)
			shapes = coldBlockShapes(blocks)
			all := coldLimits(shapes, query{start: firstTime, end: endTime}, false)
			for j, l := range layouts {
				pub := r.Experimental[j].Phases[phaseIndex]
				index := int64(all.visited) * int64(headerSize+l.indexWidth())
				if pub.Segments != all.visited || pub.RetainedBytes <= index ||
					pub.RetainedBytes-index > all.rawPayload ||
					(l.codec == rawCodec && pub.RetainedBytes-index != all.rawPayload) {
					return nil, fmt.Errorf("%s/%s: stored bytes or segment count disagree with fixture", l.name(), phase)
				}
				if r.SchemaVersion == coldMatrixSchema {
					current, err := coldPublicationInventory(pub, l, shapes, inventories[j], reused)
					if err != nil {
						return nil, fmt.Errorf("%s/%s: %w", l.name(), phase, err)
					}
					if phaseIndex == 0 {
						inventories[j] = current
					}
					if l.prune {
						if err := validateColdPayloadPair(r.Experimental[int(l.codec)].Phases[phaseIndex], pub); err != nil {
							return nil, fmt.Errorf("%s/%s: %w", l.name(), phase, err)
						}
					}
				}
			}
		}
		// Check every probe, including the complete generation, before using
		// this phase's timings or publication counters in the comparisons.
		digests := make([]string, len(probes))
		for qi, q := range probes {
			want, err := coldReference(ctx, rows, q)
			if err != nil {
				return nil, err
			}
			digest := coldRowsDigest(want)
			digests[qi] = digest
			pairIDs := make([]string, coldQueryRuns(r.SchemaVersion, q, r.Fixture.SparseRuns))
			for i := range pairIDs {
				pairIDs[i] = p.pairID(phase, q, i+1)
			}
			var limits [2]coldScanLimits
			var selectivity coldSelectivity
			if shapes != nil {
				selectivity = measureColdSelectivity(rows, q, len(want))
				limits = [2]coldScanLimits{coldLimits(shapes, q, false), coldLimits(shapes, q, true)}
			}
			for li, l := range allLayouts {
				got := l.Phases[phaseIndex].Queries[qi]
				if err := validateColdProofs(l.Name, got, len(want), digest, pairIDs); err != nil {
					return nil, fmt.Errorf("%s/%s/%s: %w", l.Name, phase, q.name, err)
				}
				if shapes != nil {
					if *got.Selectivity != selectivity {
						return nil, fmt.Errorf("%s/%s/%s: incorrect selectivity counts", l.Name, phase, q.name)
					}
					if li == 0 {
						continue
					}
					shape := limits[0]
					if layouts[li-1].prune {
						shape = limits[1]
					}
					pub := l.Phases[phaseIndex]
					if err := shape.validate(got.Samples[0], layouts[li-1], pub.RetainedBytes); err != nil {
						return nil, fmt.Errorf("%s/%s/%s: %w", l.Name, phase, q.name, err)
					}
					if r.SchemaVersion == coldMatrixSchema {
						if err := validateColdInventoryScan(shapes, pub.PayloadSizes, q, layouts[li-1].prune, got.Samples[0]); err != nil {
							return nil, fmt.Errorf("%s/%s/%s: %w", l.Name, phase, q.name, err)
						}
					}
					for _, s := range got.Samples[1:] {
						if s.ReadBytes < s.Scan.IndexBytes || s.ReadBytes-s.Scan.IndexBytes < s.Payload.Bytes ||
							*s.DecodeBytes != s.Scan.Decode.PeakWorkingBytes {
							return nil, fmt.Errorf("%s/%s/%s: sample counters disagree with scan evidence", l.Name, phase, q.name)
						}
						if *s.Scan != *got.Samples[0].Scan {
							return nil, fmt.Errorf("%s/%s/%s: experimental scan counters vary between rounds", l.Name, phase, q.name)
						}
					}
				}
			}
		}
		for qi, q := range probes {
			if !coldComparisonProbe(r.SchemaVersion, q) {
				continue
			}
			for i, left := range layouts {
				for j := i + 1; j < len(layouts); j++ {
					if layouts[j].prune != left.prune {
						continue
					}
					pair, err := compareColdCodecs(r.Experimental[i], r.Experimental[j], phaseIndex, qi, left.prune, digests[qi])
					if err != nil {
						return nil, err
					}
					pairs = append(pairs, pair)
				}
			}
		}
	}
	return pairs, ctx.Err()
}

func validateColdProofs(layout string, got coldQueryReport, count int, digest string, pairIDs []string) error {
	if got.Matches != count || len(got.Samples) != len(pairIDs) {
		return errors.New("paired sample count or oracle row count mismatch")
	}
	for i, s := range got.Samples {
		want := coldSampleProof{Round: i + 1, PairID: pairIDs[i], Layout: layout, RowsSHA256: digest}
		if s.Proof == nil || *s.Proof != want {
			return fmt.Errorf("round %d: paired sample identity or ordered row evidence mismatch", i+1)
		}
	}
	return nil
}

func compareColdCodecs(left, right coldLayout, phase, qi int, bloom bool, digest string) (coldCodecPair, error) {
	basePhase, candidatePhase := left.Phases[phase], right.Phases[phase]
	base, candidate := basePhase.Queries[qi], candidatePhase.Queries[qi]
	pair := coldCodecPair{
		Phase: basePhase.Phase, Query: base.Query, CachePolicy: coldCachePolicy, Bloom: bloom,
		Baseline: left.Name, Candidate: right.Name, RowsSHA256: digest,
		Rounds: make([]coldRoundDelta, len(base.Samples)),
	}
	readDeltas := make([]int64, len(pair.Rounds))
	rssDeltas := make([]int64, len(pair.Rounds))
	for i, a := range base.Samples {
		b := candidate.Samples[i]
		rss, err := signedColdDelta(b.PeakRSSBytes, a.PeakRSSBytes)
		if err != nil {
			return coldCodecPair{}, fmt.Errorf("%s vs %s/%s/%s round %d RSS: %w",
				right.Name, left.Name, pair.Phase, pair.Query, i+1, err)
		}
		readDeltas[i], rssDeltas[i] = b.StorageReadBytes-a.StorageReadBytes, rss
		if a.StorageReadBytes > 0 || b.StorageReadBytes > 0 {
			pair.StorageReadRounds++
		}
		pair.Rounds[i] = coldRoundDelta{
			Round: a.Proof.Round, PairID: a.Proof.PairID, PhysicalRead: readDeltas[i], PeakRSS: rss,
		}
	}
	pair.PhysicalRead, pair.PeakRSS = summarizeColdDeltas(readDeltas), summarizeColdDeltas(rssDeltas)
	if phase > 0 {
		a, b := basePhase.Rewrite, candidatePhase.Rewrite
		if a.AppendedWALBytes != b.AppendedWALBytes || a.Boundary != b.Boundary {
			return coldCodecPair{}, fmt.Errorf("%s vs %s/%s: rewrite boundaries or WAL growth disagree", right.Name, left.Name, pair.Phase)
		}
		delta, err := signedColdDelta(b.WrittenBytes, a.WrittenBytes)
		if err != nil {
			return coldCodecPair{}, fmt.Errorf("%s vs %s/%s rewrite: %w", right.Name, left.Name, pair.Phase, err)
		}
		pair.Rewrite = &coldRewriteDelta{
			Boundary: a.Boundary, AppendedWALBytes: a.AppendedWALBytes,
			WrittenBytes: delta, Ratio: float64(delta) / float64(a.AppendedWALBytes),
		}
	}
	if base.Selectivity != nil {
		tradeoffs, err := compareColdTradeoffs(basePhase, candidatePhase, base, candidate)
		if err != nil {
			return coldCodecPair{}, fmt.Errorf("%s vs %s/%s/%s: %w", right.Name, left.Name, pair.Phase, pair.Query, err)
		}
		pair.Tradeoffs = &tradeoffs
	}
	pair.Decision = coldPairDecision(pair)
	return pair, nil
}

func signedColdDelta(candidate, baseline uint64) (int64, error) {
	const maxDelta = uint64(1<<63 - 1)
	if candidate >= baseline && candidate-baseline <= maxDelta {
		return int64(candidate - baseline), nil
	}
	if baseline > candidate && baseline-candidate <= maxDelta+1 {
		return -int64(baseline-candidate-1) - 1, nil
	}
	return 0, errors.New("delta exceeds signed 64-bit range")
}

func summarizeColdDeltas(deltas []int64) coldDeltaSummary {
	slices.Sort(deltas)
	var result coldDeltaSummary
	for _, delta := range deltas {
		switch {
		case delta < 0:
			result.Lower++
		case delta > 0:
			result.Higher++
		default:
			result.Equal++
		}
	}
	// Nearest-rank p50 of paired differences, with an even sample count
	// taking the lower middle value.
	result.P50 = deltas[(len(deltas)-1)/2]
	return result
}

func coldPairDecision(p coldCodecPair) string {
	lower := p.PhysicalRead.Lower > 0 || p.PeakRSS.Lower > 0
	higher := p.PhysicalRead.Higher > 0 || p.PeakRSS.Higher > 0
	if p.Rewrite != nil {
		lower = lower || p.Rewrite.WrittenBytes < 0
		higher = higher || p.Rewrite.WrittenBytes > 0
	}
	if p.Tradeoffs != nil {
		for _, delta := range p.Tradeoffs.metricDeltas() {
			lower = lower || delta < 0
			higher = higher || delta > 0
		}
	}
	switch {
	case lower && higher:
		return "tradeoff_or_variable"
	case lower:
		return "candidate_lower"
	case higher:
		return "baseline_lower"
	default:
		return "equal"
	}
}

// Numeric tokens are preserved so large counters cannot round into a match.
func validateColdFields(given, canonical []byte, label string) error {
	decode := func(data []byte) (any, error) {
		var value any
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		err := dec.Decode(&value)
		return value, err
	}
	a, err := decode(given)
	if err != nil {
		return err
	}
	b, err := decode(canonical)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("incomplete or noncanonical %s fields", label)
	}
	return nil
}

type coldTradeoffs struct {
	ColdP95         int64 `json:"cold_p95_ns_delta"`
	ProcessReadP95  int64 `json:"process_read_bytes_p95_delta"`
	StorageReadP95  int64 `json:"storage_read_bytes_p95_delta"`
	PayloadBytes    int64 `json:"payload_bytes_read_delta"`
	StoredBytes     int64 `json:"stored_segment_and_manifest_bytes_delta"`
	SegmentsVisited int64 `json:"segments_visited_delta"`
	SegmentsDecoded int64 `json:"segments_decoded_delta"`
	PeakDecode      int64 `json:"peak_decode_bytes_delta"`
}

func compareColdTradeoffs(a, b coldPhase, base, candidate coldQueryReport) (coldTradeoffs, error) {
	left, right := base.Samples[0], candidate.Samples[0]
	stored, err := signedColdDelta(b.storedBytes(), a.storedBytes())
	if err != nil {
		return coldTradeoffs{}, fmt.Errorf("stored bytes: %w", err)
	}
	return coldTradeoffs{
		ColdP95: candidate.P95NS - base.P95NS, ProcessReadP95: candidate.ReadBytesP95 - base.ReadBytesP95,
		StorageReadP95: candidate.StorageReadBytesP95 - base.StorageReadBytesP95, PayloadBytes: right.Payload.Bytes - left.Payload.Bytes,
		StoredBytes: stored, SegmentsVisited: int64(right.Scan.SegmentsVisited - left.Scan.SegmentsVisited),
		SegmentsDecoded: int64(right.Scan.SegmentsDecoded - left.Scan.SegmentsDecoded),
		PeakDecode:      int64(*candidate.PeakDecodeBytes - *base.PeakDecodeBytes),
	}, nil
}

func (d coldTradeoffs) metricDeltas() []int64 {
	return []int64{d.ColdP95, d.ProcessReadP95, d.StorageReadP95, d.PayloadBytes, d.StoredBytes,
		d.SegmentsVisited, d.SegmentsDecoded, d.PeakDecode}
}
