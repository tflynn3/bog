package main

import (
	"encoding/json"
	"fmt"
	"time"

	"bog/internal/checkpoint"
)

const (
	coldV1MaxSegments = checkpoint.MaxSegments
	// Allow a maximum fixture line and about 1 MiB of merge pages.
	coldV1DecodeLimit = 5 << 20
	coldRewriteLimit  = 1024
)

type coldBudgets struct {
	ColdP95NS                   int64  `json:"max_cold_p95_ns"`
	ExperimentalOrdinaryBytes   int    `json:"max_experimental_ordinary_decode_bytes"`
	ExperimentalOversizedBytes  int    `json:"max_experimental_oversized_decode_bytes"`
	V1DecodeBufferBytes         int    `json:"max_v1_decode_buffer_bytes"`
	RewriteBytesPerBackfillByte uint64 `json:"max_rewrite_bytes_per_backfill_wal_byte"`
}

func matrixBudgets(timeout time.Duration) coldBudgets {
	return coldBudgets{
		ColdP95NS: timeout.Nanoseconds(), ExperimentalOrdinaryBytes: decodeBound(blockTarget),
		ExperimentalOversizedBytes: oversizedDecodeBound(maxBlock),
		V1DecodeBufferBytes:        coldV1DecodeLimit, RewriteBytesPerBackfillByte: coldRewriteLimit,
	}
}

func (b coldBudgets) validateMeasurements(report coldReport) error {
	for _, l := range append([]coldLayout{report.Production}, report.Experimental...) {
		for _, phase := range l.Phases {
			if phase.Rewrite != nil && overRewriteBudget(*phase.Rewrite, b.RewriteBytesPerBackfillByte) {
				return fmt.Errorf("%s/%s: rewrite budget exceeded: %d written bytes / %d backfill WAL bytes, limit %d bytes/byte",
					l.Name, phase.Phase, phase.Rewrite.WrittenBytes, phase.Rewrite.AppendedWALBytes, b.RewriteBytesPerBackfillByte)
			}
			for _, q := range phase.Queries {
				if q.P95NS > b.ColdP95NS {
					return fmt.Errorf("%s/%s/%s: cold p95 %d exceeds budget %d ns", l.Name, phase.Phase, q.Query, q.P95NS, b.ColdP95NS)
				}
				for _, sample := range q.Samples {
					if l.Name == productionV1 && sample.DecodeBytes != nil && *sample.DecodeBytes > b.V1DecodeBufferBytes {
						return fmt.Errorf("%s/%s/%s: v1 decode buffer budget exceeded: %d bytes, limit %d bytes",
							l.Name, phase.Phase, q.Query, *sample.DecodeBytes, b.V1DecodeBufferBytes)
					}
					if sample.Scan == nil {
						continue
					}
					d := sample.Scan.Decode
					if d.PeakOrdinaryBytes > b.ExperimentalOrdinaryBytes || d.PeakOversizedBytes > b.ExperimentalOversizedBytes {
						return fmt.Errorf("%s/%s/%s: decode budget exceeded: ordinary %d/%d bytes, oversized %d/%d bytes",
							l.Name, phase.Phase, q.Query, d.PeakOrdinaryBytes, b.ExperimentalOrdinaryBytes,
							d.PeakOversizedBytes, b.ExperimentalOversizedBytes)
					}
				}
			}
		}
	}
	return nil
}

// Compare the exact integer ratio without overflow or float rounding at the limit.
func overRewriteBudget(a amplification, limit uint64) bool {
	if a.AppendedWALBytes == 0 {
		return true
	}
	whole, remainder := a.WrittenBytes/a.AppendedWALBytes, a.WrittenBytes%a.AppendedWALBytes
	return whole > limit || (whole == limit && remainder != 0)
}

func coldPayloadSizes(s snapshot) []int {
	sizes := make([]int, len(s.Segments))
	for i, seg := range s.Segments {
		sizes[i] = int(seg.FileBytes) - headerSize - seg.indexWidth()
	}
	return sizes
}

// Rebuild the inventory with fixture-derived raw sizes, names and reuse. The
// only measured input is the stored payload size per segment.
func coldPublicationInventory(phase coldPhase, l layout, shapes []coldBlockShape, before snapshot, reused int) (snapshot, error) {
	current := snapshot{Segments: make([]segmentRef, len(shapes))}
	for i, shape := range shapes {
		payload := phase.PayloadSizes[i]
		if payload <= 0 || payload > shape.rawBytes || (l.codec == rawCodec && payload != shape.rawBytes) {
			return snapshot{}, fmt.Errorf("segment %d: invalid stored payload size %d for %d raw bytes", i, payload, shape.rawBytes)
		}
		ref := segmentRef{
			RawBytes: shape.rawBytes, Bloom: l.prune,
			FileBytes: int64(headerSize + l.indexWidth() + payload),
		}
		if i < reused {
			ref.Name = before.Segments[i].Name
			if ref != before.Segments[i] {
				return snapshot{}, fmt.Errorf("segment %d: reused segment changed size", i)
			}
		} else {
			ref.Name = fmt.Sprintf("%s-%06d.segment", phase.Phase, i-reused)
		}
		current.Segments[i] = ref
	}
	account, err := accountSegments(before, current)
	if err != nil {
		return snapshot{}, err
	}
	manifest, err := json.Marshal(current)
	if err != nil {
		return snapshot{}, err
	}
	if phase.segmentBytes != account || phase.ManifestBytes != int64(len(manifest)+1) {
		return snapshot{}, fmt.Errorf("segment and manifest byte counters disagree with experimental inventory")
	}
	return current, nil
}

func validateColdPayloadPair(off, on coldPhase) error {
	if len(off.PayloadSizes) != len(on.PayloadSizes) {
		return fmt.Errorf("Bloom pair disagrees on inventory length")
	}
	for i, stored := range off.PayloadSizes {
		if stored != on.PayloadSizes[i] {
			return fmt.Errorf("Bloom pair disagrees on segment %d payload size", i)
		}
	}
	return nil
}

func validateColdInventoryScan(shapes []coldBlockShape, sizes []int, q query, bloom bool, sample coldSample) error {
	var reads payloadReads
	var ordinary, oversized int
	for i, shape := range shapes {
		if !shape.matches(q, bloom) {
			continue
		}
		stored := sizes[i]
		reads.Blocks++
		reads.Bytes += int64(stored)
		working := shape.workingBytes()
		if stored < shape.rawBytes {
			working += stored
		}
		if shape.rawBytes > blockTarget {
			oversized = max(oversized, working)
		} else {
			ordinary = max(ordinary, working)
		}
	}
	if reads != *sample.Payload || ordinary != sample.Scan.Decode.PeakOrdinaryBytes ||
		oversized != sample.Scan.Decode.PeakOversizedBytes {
		return fmt.Errorf("scan payload or live decode bytes disagree with experimental inventory")
	}
	return nil
}
