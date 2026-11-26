package main

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unsafe"

	"bog/internal/checkpoint"
)

func queryFile(r io.ReaderAt, size int64, q query) (matches []checkpoint.BlockRow, reads payloadReads, peak int, err error) {
	reads, err = scanBlocks(r, size, q, func(raw []byte, buffers int) error {
		rows, err := checkpoint.DecodeBlock(raw)
		if err != nil {
			return err
		}
		working := buffers + len(rows)*int(unsafe.Sizeof(checkpoint.BlockRow{}))
		for _, row := range rows {
			working += len(row.Row.Line)
			if !row.Row.TS.Before(q.start) && row.Row.TS.Before(q.end) && strings.Contains(row.Row.Line, q.literal) {
				matches = append(matches, row)
			}
		}
		peak = max(peak, working)
		return nil
	})
	if err != nil {
		return nil, payloadReads{}, 0, err
	}
	slices.SortFunc(matches, ordered)
	return matches, reads, peak, nil
}

func verifyResult(got, want []checkpoint.BlockRow) error {
	if len(got) != len(want) {
		return fmt.Errorf("exact oracle row count: got %d, want %d", len(got), len(want))
	}
	for i, row := range got {
		expected := want[i]
		if row.Ordinal != expected.Ordinal || !row.Row.TS.Equal(expected.Row.TS) || row.Row.Line != expected.Row.Line {
			return fmt.Errorf("exact oracle mismatch at row %d: got timestamp %s ordinal %d, want timestamp %s ordinal %d", i,
				row.Row.TS, row.Ordinal, expected.Row.TS, expected.Ordinal)
		}
	}
	return nil
}

func measureQuery(r io.ReaderAt, size int64, q query, want []checkpoint.BlockRow, totalRows, runs int, beforeSample func() error) (measurement, error) {
	if runs < 1 || runs > maxRuns || totalRows < 1 || len(want) > totalRows {
		return measurement{}, fmt.Errorf("invalid sample or row count")
	}
	m := measurement{
		Name: q.name, Literal: q.literal, Start: q.start, End: q.end,
		MatchingRows: len(want), MatchingFraction: float64(len(want)) / float64(totalRows),
		ColdSamplesNS: make([]int64, 0, runs),
	}
	for sample := range runs {
		if err := beforeSample(); err != nil {
			return measurement{}, fmt.Errorf("sample %d: %w", sample+1, err)
		}
		start := time.Now()
		got, reads, peak, err := queryFile(r, size, q)
		elapsed := time.Since(start)
		if err == nil {
			err = verifyResult(got, want)
		}
		if err != nil {
			return measurement{}, fmt.Errorf("sample %d: %w", sample+1, err)
		}
		if sample > 0 && reads != m.payloadReads {
			return measurement{}, fmt.Errorf("sample %d: payload reads changed from %+v to %+v", sample+1, m.payloadReads, reads)
		}
		m.payloadReads = reads
		m.PeakDecodeBytes = max(m.PeakDecodeBytes, peak)
		m.ColdSamplesNS = append(m.ColdSamplesNS, elapsed.Nanoseconds())
	}
	m.ColdP95NS = p95(m.ColdSamplesNS)
	return m, nil
}
