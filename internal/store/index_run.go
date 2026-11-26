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

// queryStart seeks to the first rank in the time window, or strictly past a
// validated anchor. An anchor within an implicit run already gives its rank.
func (r indexRun) queryStart(rows []Row, start time.Time, after *Position, work *QueryWork) (int, error) {
	if after != nil && r.ordinals == nil && after.Ordinal >= r.base && after.Ordinal-r.base < r.length {
		return after.Ordinal - r.base + 1, work.Charge(1)
	}
	if err := work.seek(r.length); err != nil {
		return 0, err
	}
	if after == nil {
		return sort.Search(r.length, func(rank int) bool {
			return !rows[r.ordinal(rank)].TS.Before(start)
		}), nil
	}
	return sort.Search(r.length, func(rank int) bool {
		return rowAfter(rows, r.ordinal(rank), *after)
	}), nil
}

// queryStartLegacy preserves the metered seeks of the Position-based API.
func (r indexRun) queryStartLegacy(rows []Row, start time.Time, after *Position, work *QueryWork) (int, error) {
	lo, err := r.queryStart(rows, start, nil, work)
	if err != nil || after == nil {
		return lo, err
	}
	if err := work.seek(r.length - lo); err != nil {
		return 0, err
	}
	return lo + sort.Search(r.length-lo, func(rank int) bool {
		return rowAfter(rows, r.ordinal(lo+rank), *after)
	}), nil
}

func rowAfter(rows []Row, ordinal int, after Position) bool {
	stamp := rows[ordinal].TS
	return stamp.After(after.Time) || (stamp.Equal(after.Time) && ordinal > after.Ordinal)
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
	if offset >= r.length || r.base+r.length <= count || r.ordinals == nil {
		return offset, nil
	}
	if err := work.Charge(1); err != nil {
		return 0, err
	}
	if r.ordinals[offset] < count {
		return offset, nil
	}
	// Probe the rest of this group first. Nearby visible rows do not need a
	// tree walk, even when backfill is interleaved at individual ranks.
	end := min(offset+visibilityBlockSize-offset%visibilityBlockSize, r.length)
	if rank, err := r.probeRanks(offset+1, end, count, work); err != nil || rank < r.length || end == r.length {
		return rank, err
	}
	return r.seekVisibleBlock(end/visibilityBlockSize, count, work)
}

// Starting at the next leaf lets nearby gaps avoid a walk from the root.
// If the current subtree is entirely invisible, the next subtree to inspect
// is its right sibling (or the right sibling of an ancestor).
func (r indexRun) seekVisibleBlock(block, count int, work *QueryWork) (int, error) {
	leaves := len(r.blockMins) / 2
	for node := leaves + block; node < 2*leaves; {
		if err := work.Charge(1); err != nil {
			return 0, err
		}
		if r.blockMins[node] < count {
			for node < leaves {
				node *= 2
				if err := work.Charge(1); err != nil {
					return 0, err
				}
				if r.blockMins[node] >= count {
					// The parent has a visible rank, so the right child must.
					node++
				}
			}
			first := (node - leaves) * visibilityBlockSize
			return r.probeRanks(first, min(first+visibilityBlockSize, r.length), count, work)
		}
		// Every trailing right edge means that ancestor is exhausted.
		node >>= bits.TrailingZeros(^uint(node))
		if node == 0 {
			break
		}
		node++
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
