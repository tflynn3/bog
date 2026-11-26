package store

import (
	"math/bits"
	"sort"
	"time"
)

// indexRun covers a contiguous append-ordinal range starting at base. An
// explicit run maps timestamp-sorted ranks to ordinals through ordinals.
type indexRun struct {
	ordinals  []int
	blockMins []int
	base      int
	length    int
}

func (r indexRun) ordinal(rank int) int {
	if r.ordinals == nil {
		return r.base + rank
	}
	return r.ordinals[rank]
}

// seekStart uses one lower bound per run. In an implicit run containing
// the anchor, timestamp order and ordinal order coincide, so its successor
// rank follows directly from the ordinal. The timestamp-bearing API keeps
// its original start and resume seek charges for existing callers.
func (r indexRun) seekStart(rows []Row, start time.Time, after *Position, legacy bool, work *QueryWork) (int, error) {
	first := 0
	if after == nil || legacy {
		if err := work.seek(r.length); err != nil {
			return 0, err
		}
		first = sort.Search(r.length, func(rank int) bool {
			return !rows[r.ordinal(rank)].TS.Before(start)
		})
		if after == nil {
			return first, nil
		}
	}
	if !legacy && r.ordinals == nil && after.Ordinal >= r.base && after.Ordinal-r.base < r.length {
		return after.Ordinal - r.base + 1, nil
	}
	if err := work.seek(r.length - first); err != nil {
		return 0, err
	}
	return first + sort.Search(r.length-first, func(rank int) bool {
		ordinal := r.ordinal(first + rank)
		order := rows[ordinal].TS.Compare(after.Time)
		return order > 0 || (order == 0 && ordinal > after.Ordinal)
	}), nil
}

// Store one minimum per group of sorted ranks and per subtree of groups. This
// keeps the index compact while bounding the number of individual probes at
// either end of a gap.
const visibilityBlockSize = 16

func (r *indexRun) buildVisibility() {
	blocks := (r.length-1)/visibilityBlockSize + 1
	leaves := 1 << bits.Len(uint(blocks-1))
	r.blockMins = make([]int, 2*leaves)
	for i := range r.blockMins[leaves:] {
		lowest := r.base + r.length
		for _, ordinal := range r.ordinals[min(i*visibilityBlockSize, r.length):min((i+1)*visibilityBlockSize, r.length)] {
			lowest = min(lowest, ordinal)
		}
		r.blockMins[leaves+i] = lowest
	}
	for i := leaves - 1; i > 0; i-- {
		r.blockMins[i] = min(r.blockMins[2*i], r.blockMins[2*i+1])
	}
}

// nextVisible finds the first rank at or after offset in the snapshot. Whole
// runs outside the snapshot and implicit suffixes are removed by query setup.
func (r indexRun) nextVisible(offset, count int, work *QueryWork) (int, error) {
	if offset >= r.length || r.base+r.length <= count || r.ordinals == nil || r.ordinals[offset] < count {
		return offset, nil
	}
	if err := work.Charge(1); err != nil {
		return 0, err
	}
	// Probe the rest of this group first. Nearby visible rows do not need a
	// tree walk, even when backfill is interleaved at individual ranks.
	end := min(offset+visibilityBlockSize-offset%visibilityBlockSize, r.length)
	if rank, err := r.probeRanks(offset+1, end, count, work); err != nil || rank < r.length || end == r.length {
		return rank, err
	}
	return r.nextVisibleBlock(offset/visibilityBlockSize, count, work)
}

// nextVisibleBlock climbs from the exhausted block to locate the nearest
// right sibling that contains a visible row. Starting locally avoids probing
// the upper tree levels whenever the next visible block is nearby.
func (r indexRun) nextVisibleBlock(block, count int, work *QueryWork) (int, error) {
	leaves := len(r.blockMins) / 2
	node := leaves + block
	for node > 1 {
		if node%2 == 0 {
			if err := work.Charge(1); err != nil {
				return 0, err
			}
			if r.blockMins[node+1] < count {
				node++
				for node < leaves {
					node *= 2
					if err := work.Charge(1); err != nil {
						return 0, err
					}
					if r.blockMins[node] >= count {
						node++
					}
				}
				first := (node - leaves) * visibilityBlockSize
				return r.probeRanks(first, min(first+visibilityBlockSize, r.length), count, work)
			}
		}
		node /= 2
	}
	return r.length, nil
}

func (r indexRun) probeRanks(first, end, count int, work *QueryWork) (int, error) {
	for rank := first; rank < end; rank++ {
		if err := work.Charge(1); err != nil {
			return 0, err
		}
		if r.ordinals[rank] < count {
			return rank, nil
		}
	}
	return r.length, nil
}
