package checkpoint

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"slices"
)

// The local packing window holds at most four decoded originals at once.
// The slack fallback can use more gaps by reading each selected block separately.
const mergeFanIn = 4

var errSegmentCapacity = errors.New("checkpoint segment limit exceeded: bounded merge has insufficient space")

// packMergeRows groups rows in place using one small assignment index per
// row. Retrying with more bins does not decode or allocate another copy of the
// rows. The window bounds both row storage and the number of placement trials.
// The caller supplies a nonempty run and an output count range within
// [1, mergeFanIn]. Returned groups alias rows, and even an unsuccessful
// trial can reorder rows.
func packMergeRows(ctx context.Context, rows []BlockRow, capacity, minimum, maximum int) ([][]BlockRow, error) {
	sortByMergeSize(rows)
	bins := make([]uint16, len(rows))
	var counts [mergeFanIn]int
	for limit := minimum; limit <= maximum; limit++ {
		var room [mergeFanIn]int
		for i := range room[:limit] {
			room[i] = capacity
		}
		clear(counts[:])
		for i, row := range rows {
			if i%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			size := blockRowSize + len(row.Row.Line)
			best := -1
			for candidate, free := range room[:limit] {
				if free >= size && (best == -1 || free < room[best]) {
					best = candidate
				}
			}
			if best == -1 {
				break
			}
			bins[i] = uint16(best)
			room[best] -= size
			counts[best]++
			if i == len(rows)-1 {
				return groupMergeRows(rows, bins, counts[:limit]), nil
			}
		}
	}
	return nil, errSegmentCapacity
}

func groupMergeRows(rows []BlockRow, bins []uint16, counts []int) [][]BlockRow {
	ends := make([]int, len(counts))
	next := make([]int, len(counts))
	pos := 0
	layout := make([][]BlockRow, 0, len(counts))
	for bin, count := range counts {
		next[bin] = pos
		ends[bin] = pos + count
		if count > 0 {
			layout = append(layout, rows[pos:pos+count:pos+count])
		}
		pos += count
	}
	for bin, end := range ends {
		for next[bin] < end {
			i := next[bin]
			dest := bins[i]
			if int(dest) == bin {
				next[bin]++
				continue
			}
			j := next[dest]
			rows[i], rows[j] = rows[j], rows[i]
			bins[i], bins[j] = bins[j], bins[i]
			next[dest]++
		}
	}
	return layout
}

func sortByMergeSize(rows []BlockRow) {
	slices.SortFunc(rows, func(a, b BlockRow) int {
		if bySize := cmp.Compare(len(b.Row.Line), len(a.Row.Line)); bySize != 0 {
			return bySize
		}
		return cmp.Compare(a.Ordinal, b.Ordinal)
	})
}

type slackBin struct {
	index int
	room  int
}

type slackQueue []slackBin

func (q slackQueue) Len() int { return len(q) }
func (q slackQueue) Less(i, j int) bool {
	if q[i].room != q[j].room {
		return q[i].room > q[j].room
	}
	return q[i].index < q[j].index
}
func (q slackQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *slackQueue) Push(x any) { *q = append(*q, x.(slackBin)) }
func (q *slackQueue) Pop() any {
	old := *q
	last := old[len(old)-1]
	*q = old[:len(old)-1]
	return last
}

// placeInSlack groups the suffix in place across a prefix of rooms, which
// must be supplied in descending order and are bounded by MaxSegments.
// Each returned group corresponds to the room at the same index. The rooms
// are read-only, and the run may be reordered even if placement fails.
func placeInSlack(ctx context.Context, run []BlockRow, rooms []int) ([][]BlockRow, error) {
	sortByMergeSize(run)
	q := make(slackQueue, 0, len(rooms))
	// Avoid spreading small backfills across more blocks than necessary.
	// Add a new candidate only when all previously used gaps are too small.
	counts := make([]int, 0, len(rooms))
	assignments := make([]uint16, len(run))
	for i, row := range run {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		size := blockRowSize + len(row.Row.Line)
		if len(q) == 0 || q[0].room < size {
			next := len(counts)
			if next == len(rooms) {
				return nil, errSegmentCapacity
			}
			room := rooms[next]
			if room < size {
				return nil, errSegmentCapacity
			}
			heap.Push(&q, slackBin{index: next, room: room})
			counts = append(counts, 0)
		}
		c := q[0].index
		counts[c]++
		assignments[i] = uint16(c)
		q[0].room -= size
		heap.Fix(&q, 0)
	}
	return groupMergeRows(run, assignments, counts), nil
}
