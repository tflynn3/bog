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
const sparseLiteral = "cohort-sparse"
const mediumLiteral = "cohort-medium"
const denseLiteral = "cohort-dense"
const falsePositiveQuery = "bloom-false-positive"
const falsePositiveLiteral = "Q7mR2"

type fixtureInput struct {
	rows    []checkpoint.BlockRow
	anchors []checkpoint.BlockRow // First ordinary row at each batch timestamp.
	rare    checkpoint.BlockRow
}

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
var batchPattern = []int{1, 7, 64, 256}

func rowsForBatches(batches int) int {
	rows := 5 // Extremes, oversized line, and two rare duplicates.
	for i := range batches {
		rows += batchPattern[i%len(batchPattern)]
	}
	return rows
}

func fixture(batches int, entropy bool) (input fixtureInput) {
	input.rows = make([]checkpoint.BlockRow, 0, rowsForBatches(batches))
	input.anchors = make([]checkpoint.BlockRow, 0, batches)
	rng := rand.New(rand.NewPCG(seed, 0))
	appendRow := func(ts time.Time, line string) {
		input.rows = append(input.rows, checkpoint.BlockRow{
			Row: store.Row{TS: ts, Line: line}, Ordinal: uint64(len(input.rows)),
		})
	}
	// Both four-byte grams of Q7mR2 are present without the literal.
	appendRow(firstTime, "first supported timestamp gram=Q7mR gram=7mR2")
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
			if i == 0 && b%31 == 0 {
				line += " " + sparseLiteral
			}
			if i%8 == 0 {
				line += " " + mediumLiteral
			}
			if i%5 != 4 {
				line += " " + denseLiteral
			}
			if entropy {
				line += fmt.Sprintf(" payload=%016x%016x%016x%016x", rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64())
			}
			appendRow(ts, line)
			if i == 0 {
				input.anchors = append(input.anchors, input.rows[len(input.rows)-1])
			}
		}
		if b == batches/2 {
			appendRow(epoch, strings.Repeat("x", largeLineSize-len(largeLiteral))+largeLiteral)
		}
	}
	// Appended after newer timestamps, alongside identical rows with distinct ordinals.
	appendRow(epoch, rareLiteral)
	input.rare = input.rows[len(input.rows)-1]
	appendRow(epoch, rareLiteral)
	appendRow(endTime.Add(-time.Nanosecond), "last supported timestamp")
	return input
}

type backfill struct {
	name  string
	batch []checkpoint.BlockRow
}

func (input fixtureInput) backfills() []backfill {
	// Reuse one append batch size for both scenarios. All burst rows
	// duplicate earlier rows. Keep the newest ordinary timestamp out of
	// the scattered burst when there is more than one ordinary batch.
	clustered := []checkpoint.BlockRow{input.rare, input.anchors[0], input.anchors[0], input.anchors[0]}
	last := max(0, len(input.anchors)-2)
	scattered := []checkpoint.BlockRow{input.anchors[last], input.anchors[last/2], input.anchors[0], input.rare}
	return []backfill{{"clustered", clustered}, {"scattered", scattered}}
}

func (input fixtureInput) withBackfill(b backfill) []checkpoint.BlockRow {
	rows := make([]checkpoint.BlockRow, len(input.rows), len(input.rows)+len(b.batch))
	copy(rows, input.rows)
	for _, duplicate := range b.batch {
		duplicate.Ordinal = uint64(len(rows))
		rows = append(rows, duplicate)
	}
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
	if start < len(rows) {
		blocks = append(blocks, rows[start:])
	}
	return blocks
}

func queries() []query {
	return []query{
		{"all", "", firstTime, endTime},
		{"rare", rareLiteral, firstTime, endTime},
		{"absent", "unseen-error-8877", firstTime, endTime},
		{falsePositiveQuery, falsePositiveLiteral, firstTime, endTime},
		{"one-byte", "x", firstTime, endTime},
		{"two-byte", "é", firstTime, endTime},
		{"three-byte", "€", firstTime, endTime},
		{"oversized-tail", largeLiteral, firstTime, endTime},
		{"bounded", rareLiteral, epoch, epoch.Add(time.Second)},
		{"exclusive-end", rareLiteral, firstTime, epoch},
		{"sparse", sparseLiteral, firstTime, endTime},
		{"medium", mediumLiteral, firstTime, endTime},
		{"dense", denseLiteral, firstTime, endTime},
		{"case-mismatch", "COHORT-DENSE", firstTime, endTime},
		{"bounded-sparse", sparseLiteral, epoch, epoch.Add(37 * time.Second)},
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
