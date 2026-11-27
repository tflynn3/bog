package main

import (
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
func (r *coldReport) sealPairs(input fixtureInput) error {
	if err := r.validateColdMeasurements(); err != nil {
		return err
	}
	pairs, err := r.derivePairs(input)
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
	pairs, err := r.derivePairs(input)
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
	var given, expected any
	if err := json.Unmarshal(wire.Pairing, &given); err != nil {
		return err
	}
	if err := json.Unmarshal(canonical, &expected); err != nil {
		return err
	}
	if !reflect.DeepEqual(given, expected) {
		return errors.New("incomplete cold codec comparison fields")
	}
	return nil
}

func (r coldReport) derivePairs(input fixtureInput) ([]coldCodecPair, error) {
	p := r.Pairing
	if p == nil || !validColdRunID(p.RunID) || p.FixtureSHA256 != coldFixtureDigest(r.Fixture, input) {
		return nil, errors.New("cold pairing fixture or execution identity mismatch")
	}
	probes := coldQueries()
	bursts := input.backfills()
	allLayouts := append([]coldLayout{r.Production}, r.Experimental...)
	var pairs []coldCodecPair
	for phaseIndex, phase := range coldPhases {
		rows := input.rows
		if phaseIndex > 0 {
			burst := bursts[phaseIndex-1]
			rows = input.withBackfill(burst)
			wantWAL, err := coldBackfillWALBytes(burst)
			if err != nil {
				return nil, fmt.Errorf("%s: encode fixture backfill: %w", phase, err)
			}
			// The measurement validator already matched every experimental
			// denominator to production. Verify their common value against
			// the bytes represented by the fixture's one committed frame.
			if got := r.Production.Phases[phaseIndex].Rewrite.AppendedWALBytes; got != wantWAL {
				return nil, fmt.Errorf("%s: measured WAL growth %d, fixture frame %d", phase, got, wantWAL)
			}
		}
		// Check every probe, including the complete generation, before using
		// this phase's timings or publication counters in the comparisons.
		digests := make([]string, len(probes))
		for qi, q := range probes {
			want := reference(rows, q)
			digest := coldRowsDigest(want)
			digests[qi] = digest
			pairIDs := make([]string, coldRuns(q, r.Fixture.SparseRuns))
			for i := range pairIDs {
				pairIDs[i] = p.pairID(phase, q, i+1)
			}
			for _, l := range allLayouts {
				got := l.Phases[phaseIndex].Queries[qi]
				if err := validateColdProofs(l.Name, got, len(want), digest, pairIDs); err != nil {
					return nil, fmt.Errorf("%s/%s/%s: %w", l.Name, phase, q.name, err)
				}
			}
		}
		for qi, q := range probes {
			if q.literal != sparseLiteral {
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
	return pairs, nil
}

func coldBackfillWALBytes(burst backfill) (uint64, error) {
	const walFrameHeaderBytes = 16
	body, _, err := encodeBatch(burst.batch)
	if err != nil {
		return 0, err
	}
	return uint64(len(body) + walFrameHeaderBytes), nil
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
