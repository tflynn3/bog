package store

import (
	"container/heap"
	"sort"
	"strings"
	"time"
)

const blockSize = 128
const blockBudget = 64 << 10
const bitmapWords = 512 // 4 KiB

type blockFilter struct {
	bits [bitmapWords]uint64
}

// Gram hashes are calculated within each decoded string, never across rows.
func gramHash(s string) uint64 {
	x := uint64(s[0]) | uint64(s[1])<<8 | uint64(s[2])<<16 | uint64(s[3])<<24
	x ^= x >> 16
	x *= 0x7feb352d
	x ^= x >> 15
	x *= 0x846ca68b
	return x ^ (x >> 16)
}
func (b *blockFilter) add(hash uint64) {
	for i := uint64(0); i < 3; i++ {
		bit := (hash + i*(hash>>32|1)) % (bitmapWords * 64)
		b.bits[bit/64] |= uint64(1) << (bit % 64)
	}
}
func buildBlock(rows []Row) *blockFilter {
	size := 0
	for _, row := range rows {
		if len(row.Line) > blockBudget-size {
			return nil
		}
		size += len(row.Line)
	}
	b := new(blockFilter)
	for _, row := range rows {
		for i := 0; i+4 <= len(row.Line); i++ {
			b.add(gramHash(row.Line[i : i+4]))
		}
	}
	return b
}

// literalProbes computes bit positions once for all blocks in a query.
func literalProbes(literal string) []uint64 {
	grams := len(literal) - 3
	probes := min(grams, 32)
	bits := make([]uint64, 0, probes*3)
	for i := 0; i < probes; i++ {
		pos := i * (grams - 1) / max(1, probes-1)
		hash := gramHash(literal[pos : pos+4])
		for j := uint64(0); j < 3; j++ {
			bits = append(bits, (hash+j*(hash>>32|1))%(bitmapWords*64))
		}
	}
	return bits
}

func (b *blockFilter) mayContainProbes(bits []uint64) bool {
	if b == nil {
		return true
	}
	for _, bit := range bits {
		if b.bits[bit/64]&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

type latestHeap struct {
	rows     []Row
	ordinals []int
}

func (h latestHeap) Len() int           { return len(h.ordinals) }
func (h latestHeap) Less(i, j int) bool { return rowLess(h.rows, h.ordinals[j], h.ordinals[i]) }
func (h latestHeap) Swap(i, j int)      { h.ordinals[i], h.ordinals[j] = h.ordinals[j], h.ordinals[i] }
func (h *latestHeap) Push(x any)        { h.ordinals = append(h.ordinals, x.(int)) }
func (h *latestHeap) Pop() any {
	n := len(h.ordinals) - 1
	x := h.ordinals[n]
	h.ordinals = h.ordinals[:n]
	return x
}
func rowLess(rows []Row, a, b int) bool {
	if rows[a].TS.Equal(rows[b].TS) {
		return a < b
	}
	return rows[a].TS.Before(rows[b].TS)
}
func queryBlocks(rows []Row, blocks []int, count int, start, end time.Time, literal string, limit int, after *Position, lookahead bool, work *QueryWork) ([]Row, *Position, error) {
	capacity := limit
	if lookahead && capacity < count {
		capacity++
	}
	if capacity > count {
		capacity = count
	}
	h := &latestHeap{rows: rows}
	for _, first := range blocks {
		last := min(first+blockSize, count)
		for ordinal := first; ordinal < last; ordinal++ {
			row := rows[ordinal]
			eligible := !row.TS.Before(start) && row.TS.Before(end) &&
				(after == nil || row.TS.After(after.Time) || (row.TS.Equal(after.Time) && ordinal > after.Ordinal))
			if err := work.row(row.Line, eligible); err != nil {
				return nil, nil, err
			}
			if !eligible || !strings.Contains(row.Line, literal) {
				continue
			}
			if h.Len() < capacity {
				heap.Push(h, ordinal)
			} else if capacity > 0 && rowLess(rows, ordinal, h.ordinals[0]) {
				h.ordinals[0] = ordinal
				heap.Fix(h, 0)
			}
		}
	}
	sort.Slice(h.ordinals, func(i, j int) bool { return rowLess(rows, h.ordinals[i], h.ordinals[j]) })
	var next *Position
	if len(h.ordinals) > limit {
		h.ordinals = h.ordinals[:limit]
		ordinal := h.ordinals[len(h.ordinals)-1]
		next = &Position{rows[ordinal].TS, ordinal}
	}
	result := make([]Row, 0, len(h.ordinals))
	for _, ordinal := range h.ordinals {
		result = append(result, rows[ordinal])
	}
	return result, next, nil
}
