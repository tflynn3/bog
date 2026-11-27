package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"bog/internal/checkpoint"
)

type coldScanMetrics struct {
	SegmentsVisited   int              `json:"segments_visited"`
	IndexBytes        int64            `json:"index_bytes_read"`
	SegmentsDecoded   int              `json:"segments_decoded"`
	LargestDecodedRaw int              `json:"largest_decoded_raw_bytes"`
	Decode            decodeWorkingSet `json:"decode"`
}

func scanColdSnapshot(ctx context.Context, dir string, current snapshot, q query) ([]checkpoint.BlockRow, coldSample, error) {
	var decoder blockDecoder
	defer decoder.close()
	var matches []checkpoint.BlockRow
	var reads payloadReads
	metrics := coldScanMetrics{Decode: decodeWorkingSet{
		OrdinaryBoundBytes: decodeBound(blockTarget), WorkingBoundBytes: decodeBound(blockTarget),
	}}
	for _, ref := range current.Segments {
		if err := ctx.Err(); err != nil {
			return nil, coldSample{}, err
		}
		f, err := openSegment(dir, ref)
		if err != nil {
			return nil, coldSample{}, err
		}
		read, err := decoder.scan(f, ref.FileBytes, ref.RawBytes, ref.indexWidth(), q, func(raw []byte, buffers int) error {
			return metrics.match(raw, ref.RawBytes, buffers, q, &matches)
		})
		if err := errors.Join(err, f.Close()); err != nil {
			return nil, coldSample{}, fmt.Errorf("scan %s: %w", ref.Name, err)
		}
		metrics.SegmentsVisited++
		metrics.IndexBytes += int64(headerSize + ref.indexWidth())
		metrics.SegmentsDecoded += read.Blocks
		reads.Blocks += read.Blocks
		reads.Bytes += read.Bytes
	}
	slices.SortFunc(matches, ordered)
	return matches, coldSample{Payload: &reads, DecodeBytes: &metrics.Decode.PeakWorkingBytes, Scan: &metrics}, ctx.Err()
}

func (m *coldScanMetrics) match(raw []byte, declared, buffers int, q query, matches *[]checkpoint.BlockRow) error {
	if len(raw) != declared || len(raw) < blockHeader+blockRowWidth || len(raw) > maxBlock {
		return errLayout
	}
	oversized := len(raw) > blockTarget
	bound := decodeBound(len(raw))
	if oversized {
		if le.Uint32(raw[4:8]) != 1 {
			return fmt.Errorf("oversized block is not a singleton: %w", errLayout)
		}
		bound = oversizedDecodeBound(len(raw))
	}
	rows, err := checkpoint.DecodeBlock(raw)
	if err != nil {
		return err
	}
	working := buffers + len(rows)*decodedRowBytes + len(raw) - blockHeader - len(rows)*blockRowWidth
	if working > bound {
		return fmt.Errorf("decode working set %d exceeds ceiling %d: %w", working, bound, errLayout)
	}
	m.LargestDecodedRaw = max(m.LargestDecodedRaw, len(raw))
	m.Decode.PeakWorkingBytes = max(m.Decode.PeakWorkingBytes, working)
	if oversized {
		m.Decode.OversizedSegments++
		m.Decode.PeakOversizedBytes = max(m.Decode.PeakOversizedBytes, working)
		m.Decode.WorkingBoundBytes = max(m.Decode.WorkingBoundBytes, bound)
	} else {
		m.Decode.PeakOrdinaryBytes = max(m.Decode.PeakOrdinaryBytes, working)
	}
	for _, row := range rows {
		if !row.Row.TS.Before(q.start) && row.Row.TS.Before(q.end) && strings.Contains(row.Row.Line, q.literal) {
			*matches = append(*matches, row)
		}
	}
	return nil
}

func (m *coldScanMetrics) UnmarshalJSON(data []byte) error {
	type fields coldScanMetrics
	return decodeColdEvidence(data, (*fields)(m), "experimental scan")
}
