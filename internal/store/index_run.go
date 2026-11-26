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

// seekStart locates the first rank at or after start, or strictly after the
// validated anchor. The compact page scan can seek directly from the anchor.
// Legacy queries keep their original seek charges.
func (r indexRun) seekStart(rows []Row, start time.Time, after *Position, compactPage bool, work *QueryWork) (lower, next int, err error) {
	if after != nil && compactPage {
		if r.ordinals == nil && after.Ordinal >= r.base && after.Ordinal < r.base+r.length {
			// The anchor's ordinal is also its position in an implicit run.
			next := after.Ordinal - r.base + 1
			return next, next, work.Charge(1)
		}
		next, err := r.seekAfter(rows, *after, 0, work)
		return next, next, err
	}
	if err := work.seek(r.length); err != nil {
		return 0, 0, err
	}
	first := sort.Search(r.length, func(rank int) bool {
		return !rows[r.ordinal(rank)].TS.Before(start)
	})
	if after != nil {
		next, err := r.seekAfter(rows, *after, first, work)
		return first, next, err
	}
	return first, first, nil
}

func (r indexRun) seekAfter(rows []Row, after Position, first int, work *QueryWork) (int, error) {
	if err := work.seek(r.length - first); err != nil {
		return 0, err
	}
	return first + sort.Search(r.length-first, func(i int) bool {
		ordinal := r.ordinal(first + i)
		stamp := rows[ordinal].TS
		return stamp.After(after.Time) || (stamp.Equal(after.Time) && ordinal > after.Ordinal)
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
	// Start with the remainder of this group. Interleaved backfill
	// usually leaves a visible row nearby.
	group := offset / visibilityBlockSize
	end := min((group+1)*visibilityBlockSize, r.length)
	leaves := len(r.blockMins) / 2
	scanGroup := true
	if offset%visibilityBlockSize == 0 {
		if err := work.Charge(1); err != nil {
			return 0, err
		}
		scanGroup = r.blockMins[leaves+group] < count
	}
	if scanGroup {
		if rank, err := r.probeRanks(offset+1, end, count, work); err != nil || rank < r.length {
			return rank, err
		}
	}

	// Ascend from this group until a right sibling can contain a visible
	// row. Siblings cover the remaining ranks in increasing order.
	for node := leaves + group; node > 1; node /= 2 {
		if node%2 != 0 {
			continue
		}
		if err := work.Charge(1); err != nil {
			return 0, err
		}
		if r.blockMins[node+1] < count {
			return r.firstVisibleIn(node+1, leaves, count, work)
		}
	}
	return r.length, nil
}

func (r indexRun) firstVisibleIn(node, leaves, count int, work *QueryWork) (int, error) {
	for node < leaves {
		if err := work.Charge(1); err != nil {
			return 0, err
		}
		node *= 2
		// At least one child contains a match, so probe only the left.
		if r.blockMins[node] >= count {
			node++
		}
	}
	first := (node - leaves) * visibilityBlockSize
	return r.probeRanks(first, min(first+visibilityBlockSize, r.length), count, work)
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
