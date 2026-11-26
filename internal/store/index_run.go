package store

import "math/bits"

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
	return r.seekVisible(1, 0, len(r.blockMins)/2*visibilityBlockSize, end, count, work)
}

func (r indexRun) seekVisible(node, first, width, offset, count int, work *QueryWork) (int, error) {
	if first+width <= offset || first >= r.length {
		return r.length, nil
	}
	if err := work.Charge(1); err != nil {
		return 0, err
	}
	if r.blockMins[node] >= count {
		return r.length, nil
	}
	if width == visibilityBlockSize {
		return r.probeRanks(first, min(first+width, r.length), count, work)
	}
	half := width / 2
	rank, err := r.seekVisible(2*node, first, half, offset, count, work)
	if err != nil || rank < r.length {
		return rank, err
	}
	return r.seekVisible(2*node+1, first+half, half, offset, count, work)
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
