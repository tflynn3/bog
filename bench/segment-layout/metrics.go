package main

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

type measurement struct {
	Name             string    `json:"query"`
	Literal          string    `json:"literal"`
	Start            time.Time `json:"start_inclusive"`
	End              time.Time `json:"end_exclusive"`
	OrderedParity    bool      `json:"ordered_parity"`
	MatchingRows     int       `json:"matching_rows"`
	MatchingFraction float64   `json:"matching_fraction_of_retained_rows"`
	payloadReads
	BaselineBlocksRead   int     `json:"baseline_logical_blocks_read"`
	BaselineDecodedBytes int64   `json:"baseline_decoded_bytes"`
	BaselineBytesRead    int64   `json:"baseline_payload_bytes_read"`
	BlocksAvoided        int     `json:"payload_reads_avoided"`
	BytesAvoided         int64   `json:"payload_bytes_avoided"`
	PeakDecodeBytes      int     `json:"peak_decode_bytes"`
	ColdSamplesNS        []int64 `json:"cold_samples_ns"`
	ColdP95NS            int64   `json:"cold_p95_ns"`
}

// p95 uses nearest rank and leaves samples in collection order.
func p95(samples []int64) int64 {
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	return sorted[(len(sorted)*95+99)/100-1]
}

func (m *measurement) compareReads(baseline measurement) error {
	if m.Name != baseline.Name || m.Literal != baseline.Literal ||
		!m.Start.Equal(baseline.Start) || !m.End.Equal(baseline.End) || m.MatchingRows != baseline.MatchingRows {
		return fmt.Errorf("baseline query does not match %s", m.Name)
	}
	if m.Blocks > baseline.Blocks || m.Bytes > baseline.Bytes || m.DecodedBytes > baseline.DecodedBytes {
		return fmt.Errorf("%s read more payload than its unfiltered baseline", m.Name)
	}
	m.BaselineBlocksRead, m.BaselineBytesRead = baseline.Blocks, baseline.Bytes
	m.BaselineDecodedBytes = baseline.DecodedBytes
	m.BlocksAvoided, m.BytesAvoided = baseline.Blocks-m.Blocks, baseline.Bytes-m.Bytes
	return nil
}

// Linux reports VmHWM in KiB. Reject zero, malformed units and overflow so a
// missing high-water mark cannot be mistaken for a successful measurement.
func parsePeakRSS(status []byte) (int64, error) {
	for line := range strings.Lines(string(status)) {
		label, value, ok := strings.Cut(line, ":")
		if !ok || label != "VmHWM" {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 2 && fields[1] == "kB" {
			kib, err := strconv.ParseInt(fields[0], 10, 64)
			if err == nil && kib > 0 && kib <= math.MaxInt64/1024 {
				return kib * 1024, nil
			}
		}
		return 0, errors.New("invalid VmHWM in worker status")
	}
	return 0, errors.New("missing VmHWM in worker status")
}
