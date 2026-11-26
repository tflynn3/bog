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

func measureQuery(r io.ReaderAt, size int64, q query, expected *oracleReader, runs int, beforeSample func() error) (measurement, error) {
	if runs < 1 || runs > maxRuns || expected.totalRows < 1 {
		return measurement{}, fmt.Errorf("invalid sample or row count")
	}
	m := measurement{
		Name: q.name, Literal: q.literal, Start: q.start, End: q.end,
		MatchingRows: expected.matches, MatchingFraction: float64(expected.matches) / float64(expected.totalRows),
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
			err = expected.verify(got)
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
	m.OrderedParity = true
	return m, nil
}
