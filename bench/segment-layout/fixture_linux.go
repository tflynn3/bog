package main

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

const seed = 44
const largeLineSize = store.MaxBatch - len(`{"ts":"2025-01-01T00:00:00Z","line":""}`)
const blockTarget = 64 << 10
const rareLiteral = "incident-4e71"
const largeLiteral = "oversized-line-tail"

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
var batchPattern = []int{1, 7, 64, 256}

func fixture(batches int, entropy bool) (rows []checkpoint.BlockRow) {
	rng := rand.New(rand.NewPCG(seed, 0))
	appendRow := func(ts time.Time, line string) {
		rows = append(rows, checkpoint.BlockRow{
			Row: store.Row{TS: ts, Line: line}, Ordinal: uint64(len(rows)),
		})
	}
	appendRow(firstTime, "first supported timestamp")
	for b := range batches {
		for i := range batchPattern[b%len(batchPattern)] {
			ts := epoch.Add(time.Duration(b) * time.Second)
			line := "INFO worker finished request in 12ms tenant=public status=200"
			switch {
			case i%29 == 0:
				line = fmt.Sprintf("WARN retrying task id=%016x detail=réseau €", rng.Uint64())
			case i%19 == 0:
				line = "ERROR worker timed out tenant=public status=504"
			}
			if entropy {
				line += fmt.Sprintf(" payload=%016x%016x%016x%016x", rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64())
			}
			appendRow(ts, line)
		}
		if b == batches/2 {
			appendRow(epoch, strings.Repeat("x", largeLineSize-len(largeLiteral))+largeLiteral)
		}
	}
	// Appended after newer timestamps, alongside identical rows with distinct ordinals.
	appendRow(epoch, rareLiteral)
	appendRow(epoch, rareLiteral)
	appendRow(endTime.Add(-time.Nanosecond), "last supported timestamp")
	return rows
}

func ordered(a, b checkpoint.BlockRow) int {
	if order := a.Row.TS.Compare(b.Row.TS); order != 0 {
		return order
	}
	return cmp.Compare(a.Ordinal, b.Ordinal)
}

func partition(rows []checkpoint.BlockRow) (blocks [][]checkpoint.BlockRow) {
	start, size := 0, blockHeader
	for i, row := range rows {
		width := 24 + len(row.Row.Line)
		if size+width > blockTarget && i > start {
			blocks = append(blocks, rows[start:i])
			start, size = i, blockHeader
		}
		size += width
	}
	return append(blocks, rows[start:])
}

func queries() []query {
	return []query{
		{"all", "", firstTime, endTime},
		{"rare", rareLiteral, firstTime, endTime},
		{"absent", "unseen-error-8877", firstTime, endTime},
		{"one-byte", "x", firstTime, endTime},
		{"two-byte", "é", firstTime, endTime},
		{"three-byte", "€", firstTime, endTime},
		{"oversized-tail", largeLiteral, firstTime, endTime},
		{"bounded", rareLiteral, epoch, epoch.Add(time.Second)},
		{"exclusive-end", rareLiteral, firstTime, epoch},
	}
}

// The oracle walks the original append input independently of packing and pruning.
func reference(rows []checkpoint.BlockRow, q query) []checkpoint.BlockRow {
	var matches []checkpoint.BlockRow
	for _, row := range rows {
		if row.Row.TS.Before(q.start) || !row.Row.TS.Before(q.end) || !strings.Contains(row.Row.Line, q.literal) {
			continue
		}
		matches = append(matches, row)
	}
	// Stable time sort preserves input append ordering for ties.
	slices.SortStableFunc(matches, func(a, b checkpoint.BlockRow) int {
		return a.Row.TS.Compare(b.Row.TS)
	})
	return matches
}
