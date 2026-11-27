package main

import (
	"fmt"
	"slices"
	"time"
)

type measurement struct {
	Name             string    `json:"query"`
	Literal          string    `json:"literal"`
	Start            time.Time `json:"start_inclusive"`
	End              time.Time `json:"end_exclusive"`
	MatchingRows     int       `json:"matching_rows"`
	MatchingFraction float64   `json:"matching_fraction_of_retained_rows"`
	payloadReads
	BaselineBlocksRead int      `json:"baseline_logical_blocks_read"`
	BaselineBytesRead  int64    `json:"baseline_payload_bytes_read"`
	BlocksAvoided      int      `json:"payload_reads_avoided"`
	BytesAvoided       int64    `json:"payload_bytes_avoided"`
	PeakRSSBytes       uint64   `json:"peak_rss_bytes"`
	SampleRSSBytes     []uint64 `json:"sample_peak_rss_bytes"`
	PeakDecodeBytes    int      `json:"peak_decode_bytes"`
	ColdSamplesNS      []int64  `json:"cold_samples_ns"`
	ColdP95NS          int64    `json:"cold_p95_ns"`
}

// p95 uses nearest rank and leaves samples in collection order.
func p95(samples []int64) int64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	return sorted[(len(sorted)*95+99)/100-1]
}

func (m *measurement) compareReads(baseline measurement) error {
	if m.Name != baseline.Name || m.Literal != baseline.Literal ||
		!m.Start.Equal(baseline.Start) || !m.End.Equal(baseline.End) || m.MatchingRows != baseline.MatchingRows {
		return fmt.Errorf("baseline query does not match %s", m.Name)
	}
	if m.Blocks > baseline.Blocks || m.Bytes > baseline.Bytes {
		return fmt.Errorf("%s read more payload than its unfiltered baseline", m.Name)
	}
	m.BaselineBlocksRead, m.BaselineBytesRead = baseline.Blocks, baseline.Bytes
	m.BlocksAvoided, m.BytesAvoided = baseline.Blocks-m.Blocks, baseline.Bytes-m.Bytes
	return nil
}

func newMeasurement(q query, matches, total int) measurement {
	return measurement{
		Name: q.name, Literal: q.literal, Start: q.start, End: q.end,
		MatchingRows: matches, MatchingFraction: float64(matches) / float64(total),
	}
}

func (m *measurement) addSample(reads payloadReads, elapsed int64, decode int, rss uint64) error {
	if elapsed < 0 || decode < 0 || rss == 0 || reads.Blocks < 0 || reads.Bytes < 0 ||
		(reads.Blocks == 0) != (reads.Bytes == 0) || (reads.Blocks == 0) != (decode == 0) {
		return fmt.Errorf("invalid query sample metrics")
	}
	if len(m.ColdSamplesNS) > 0 && reads != m.payloadReads {
		return fmt.Errorf("payload reads changed from %+v to %+v", m.payloadReads, reads)
	}
	m.payloadReads = reads
	m.PeakDecodeBytes = max(m.PeakDecodeBytes, decode)
	m.PeakRSSBytes = max(m.PeakRSSBytes, rss)
	m.ColdSamplesNS = append(m.ColdSamplesNS, elapsed)
	m.SampleRSSBytes = append(m.SampleRSSBytes, rss)
	return nil
}

const benchmarkBoundary = "logical bytes in the newly written and synced experimental container"
const componentBoundary = "child wchar delta during checkpoint Publisher.PublishCaptured, including transient staging and publication metadata"
const liveBoundary = "server write_bytes delta from before the start request through observed complete status, including process-attributed filesystem writes"

type amplification struct {
	WrittenBytes     uint64  `json:"written_bytes"`
	AppendedWALBytes uint64  `json:"appended_wal_bytes"`
	Ratio            float64 `json:"ratio"`
	Boundary         string  `json:"boundary"`
}

func writeAmplification(written, appended uint64, boundary string) amplification {
	return amplification{written, appended, float64(written) / float64(appended), boundary}
}
