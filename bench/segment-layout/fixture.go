package main

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

const fixtureSeed = 44
const fixtureVersion = "retained-log-v1"
const oversizedLiteral = "oversize-end"

var minTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
var endTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
var batchSizes = [...]int{1, 7, 64, 513}

type query struct {
	name, literal string
	start, end    time.Time
}

func (q query) matches(row store.Row) bool {
	return !row.TS.Before(q.start) && row.TS.Before(q.end) && strings.Contains(row.Line, q.literal)
}

var queries = []query{
	{"rare", "rare-7be1", minTime, endTime},
	{"absent", "absent-37ad", minTime, endTime},
	{"one-byte", "=", minTime, endTime},
	{"two-byte", "OK", minTime, endTime},
	{"three-byte", "fé", minTime, endTime},
	{"oversized", oversizedLiteral, minTime, endTime},
	{"ties-and-backfill", "", epoch.Add(time.Nanosecond), epoch.Add(2 * time.Nanosecond)},
	{"all", "", minTime, endTime},
}

func fixture(n, oversizedFrameBytes int) ([]checkpoint.BlockRow, error) {
	rows := make([]checkpoint.BlockRow, 0, n+5)
	appendFrame := func(batch []store.Row) error {
		var payload bytes.Buffer
		enc := json.NewEncoder(&payload)
		for _, row := range batch {
			if err := enc.Encode(row); err != nil {
				return err
			}
		}
		decoded, err := store.Decode(payload.Bytes())
		if err != nil {
			return err
		}
		for _, row := range decoded {
			rows = append(rows, checkpoint.BlockRow{Row: row, Ordinal: uint64(len(rows))})
		}
		return nil
	}
	rng := rand.New(rand.NewPCG(fixtureSeed, 0))
	for first, batch := 0, 0; first < n; batch++ {
		end := min(n, first+batchSizes[batch%len(batchSizes)])
		frame := make([]store.Row, 0, end-first)
		for i := first; i < end; i++ {
			line := fmt.Sprintf("level=INFO status=OK service=api shard=%02d message=café request completed", rng.IntN(32))
			if i >= n/2 && i < 3*n/4 {
				var entropy [192]byte
				for j := 0; j < len(entropy); j += 8 {
					binary.LittleEndian.PutUint64(entropy[j:], rng.Uint64())
				}
				line += " trace=" + base64.RawStdEncoding.EncodeToString(entropy[:])
			} else {
				line += strings.Repeat(" transport=keepalive", i%13)
			}
			if i == n/5 || i == 4*n/5 {
				line += " rare-7be1"
			}
			frame = append(frame, store.Row{TS: epoch.Add(time.Duration(i/3) * time.Nanosecond), Line: line})
		}
		if err := appendFrame(frame); err != nil {
			return nil, err
		}
		first = end
	}
	oversized := store.Row{TS: epoch, Line: oversizedLiteral}
	base, err := json.Marshal(oversized)
	if err != nil {
		return nil, err
	}
	oversized.Line = strings.Repeat("x", oversizedFrameBytes-len(base)-1) + oversized.Line
	if err := appendFrame([]store.Row{oversized}); err != nil {
		return nil, err
	}
	tie := store.Row{TS: epoch.Add(time.Nanosecond), Line: "late backfill rare-7be1"}
	if err := appendFrame([]store.Row{
		{TS: endTime.Add(-time.Nanosecond), Line: "last instant"},
		tie, tie, {TS: minTime, Line: "first instant"},
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

func partition(rows []checkpoint.BlockRow) [][]checkpoint.BlockRow {
	sorted := slices.Clone(rows)
	slices.SortFunc(sorted, func(a, b checkpoint.BlockRow) int {
		if order := a.Row.TS.Compare(b.Row.TS); order != 0 {
			return order
		}
		return cmp.Compare(a.Ordinal, b.Ordinal)
	})
	var blocks [][]checkpoint.BlockRow
	start, size := 0, checkpointHeader
	for i, row := range sorted {
		cost := checkpointRow + len(row.Row.Line)
		if i > start && size+cost > targetBlockBytes {
			blocks = append(blocks, sorted[start:i])
			start, size = i, checkpointHeader
		}
		size += cost
	}
	if start < len(sorted) {
		blocks = append(blocks, sorted[start:])
	}
	return blocks
}

func reference(rows []checkpoint.BlockRow, q query) []checkpoint.BlockRow {
	var result []checkpoint.BlockRow
	for _, row := range rows {
		if q.matches(row.Row) {
			result = append(result, row)
		}
	}
	slices.SortStableFunc(result, func(a, b checkpoint.BlockRow) int { return a.Row.TS.Compare(b.Row.TS) })
	return result
}
