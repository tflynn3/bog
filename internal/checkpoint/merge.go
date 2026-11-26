package checkpoint

import (
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"

	"bog/internal/store"
)

const (
	// The local packing window holds at most four old inputs. If the
	// suffix fits only across more blocks, fillSlack visits them one at a
	// time. An attempt stages final blocks only and replaces each old
	// input at most once.
	mergeFanIn   = 4
	blockPayload = MaxBlockSize - blockHeaderSize
)

var errSegmentCapacity = errors.New("checkpoint segment limit exceeded: bounded merge has insufficient space")

type segmentBuilder struct {
	attempt       *publicationAttempt
	old           []Segment
	oldOrdinal    uint64
	outputs       []Segment
	candidates    []int // Unused old segments, smallest payload first.
	replaced      []bool
	replacedCount int
	scratch       []byte
}

func (a *publicationAttempt) buildSegments(m *Manifest, next *uint64, scan func(func(uint64, []store.Row) error) error) error {
	builder := segmentBuilder{attempt: a, old: m.Segments, oldOrdinal: m.NextOrdinal}
	var run []BlockRow
	var payload int
	flush := func() error {
		if len(run) == 0 {
			return nil
		}
		if err := builder.flush(run, payload); err != nil {
			return err
		}
		// Reuse the row slots and drop the frame strings they referenced.
		clear(run)
		run, payload = run[:0], 0
		return nil
	}
	if err := scan(func(first uint64, rows []store.Row) error {
		if first != m.NextOrdinal || len(rows) == 0 || uint64(len(rows)) > ^uint64(0)-first {
			return fmt.Errorf("%w: invalid checkpoint batch ordinals", ErrIntegrity)
		}
		for i, row := range rows {
			if len(row.Line) > blockPayload-blockRowSize {
				return errors.New("row exceeds block limit")
			}
			size := blockRowSize + len(row.Line)
			if size > blockPayload-payload {
				if err := flush(); err != nil {
					return err
				}
			}
			run = append(run, BlockRow{Row: row, Ordinal: first + uint64(i)})
			payload += size
		}
		m.NextOrdinal = first + uint64(len(rows))
		return nil
	}); err != nil {
		return err
	}
	if next != nil && m.NextOrdinal != *next {
		return fmt.Errorf("%w: checkpoint scan did not match prefix ordinal", ErrIntegrity)
	}
	if err := flush(); err != nil {
		return err
	}
	m.Segments = builder.manifestSegments()
	return nil
}

func (b *segmentBuilder) flush(run []BlockRow, payload int) error {
	if len(b.old)-b.replacedCount+len(b.outputs) < MaxSegments {
		return b.write(run)
	}
	if b.candidates == nil {
		b.rankCandidates()
	}
	if len(b.candidates) == 0 {
		return errSegmentCapacity
	}
	window := b.candidates[:min(mergeFanIn, len(b.candidates))]
	// Prefer one staged output containing the suffix and as many small
	// originals as fit. This can restore headroom without paying for a
	// separate output. If the suffix needs its own block, coalesce only
	// the two cheapest originals needed to make a slot.
	count := b.wholeInputsThatFit(window, blockPayload-payload)
	if count != 0 {
		old, err := b.readInputs(window[:count], len(run))
		if err != nil {
			return err
		}
		if err := b.write(append(old, run...)); err != nil {
			return err
		}
		b.consume(count)
		return nil
	}
	if len(window) >= 2 && b.wholeInputsThatFit(window[:2], blockPayload) == 2 {
		joined, err := b.readInputs(window[:2], 0)
		if err != nil {
			return err
		}
		if err := b.write(joined); err != nil {
			return err
		}
		if err := b.write(run); err != nil {
			return err
		}
		b.consume(2)
		return nil
	}
	err := b.repack(window, run, payload)
	if errors.Is(err, errSegmentCapacity) {
		return b.fillSlack(run, payload)
	}
	return err
}

func (b *segmentBuilder) wholeInputsThatFit(window []int, room int) int {
	for i, index := range window {
		room -= payloadSize(b.old[index])
		if room < 0 {
			return i
		}
	}
	return len(window)
}

func payloadSize(s Segment) int { return int(s.Size) - blockHeaderSize }

func (b *segmentBuilder) rankCandidates() {
	b.replaced = make([]bool, len(b.old))
	b.candidates = make([]int, len(b.old))
	for i := range b.candidates {
		b.candidates[i] = i
	}
	sort.Slice(b.candidates, func(i, j int) bool {
		a, c := b.candidates[i], b.candidates[j]
		if b.old[a].Size != b.old[c].Size {
			return b.old[a].Size < b.old[c].Size
		}
		return cmp.Less(b.old[a].Name, b.old[c].Name)
	})
}

func (b *segmentBuilder) consume(count int) {
	for _, i := range b.candidates[:count] {
		b.replaced[i] = true
	}
	b.candidates = b.candidates[count:]
	b.replacedCount += count
}

func (b *segmentBuilder) manifestSegments() []Segment {
	segments := make([]Segment, 0, len(b.old)-b.replacedCount+len(b.outputs))
	for i, old := range b.old {
		if b.replaced == nil || !b.replaced[i] {
			segments = append(segments, old)
		}
	}
	return append(segments, b.outputs...)
}

func (b *segmentBuilder) write(run []BlockRow) error {
	if err := b.attempt.ctx.Err(); err != nil {
		return err
	}
	slices.SortFunc(run, compareMergeRows)
	output, err := b.attempt.stageBlock(run)
	if err != nil {
		return err
	}
	b.outputs = append(b.outputs, output)
	return nil
}

func compareMergeRows(a, b BlockRow) int {
	if order := a.Row.TS.Compare(b.Row.TS); order != 0 {
		return order
	}
	return cmp.Compare(a.Ordinal, b.Ordinal)
}

// readInputs opens one segment at a time and reuses one encoded input buffer.
// DecodeBlock owns its returned strings. Its checks cover the version, sizes,
// CRC32C, time bounds and UTF-8 before any old row can be staged again.
func (b *segmentBuilder) readInputs(indices []int, extraRows int) ([]BlockRow, error) {
	var inputs [mergeFanIn][]BlockRow
	total := extraRows
	for j, i := range indices {
		rows, err := b.decodeInput(i)
		if err != nil {
			return nil, err
		}
		inputs[j] = rows
		total += len(rows)
	}
	if len(indices) == 1 && extraRows == 0 {
		return inputs[0], nil
	}
	joined := make([]BlockRow, 0, total)
	for _, rows := range inputs[:len(indices)] {
		joined = append(joined, rows...)
	}
	return joined, nil
}

func (b *segmentBuilder) decodeInput(index int) ([]BlockRow, error) {
	if err := b.attempt.ctx.Err(); err != nil {
		return nil, err
	}
	s := b.old[index]
	if s.Size < blockHeaderSize+blockRowSize || s.Size > MaxBlockSize {
		return nil, fmt.Errorf("%w: invalid merge input size for %s", ErrIntegrity, s.Name)
	}
	size := int(s.Size)
	if cap(b.scratch) < size {
		b.scratch = make([]byte, size)
	}
	rows, err := b.readInput(s, b.scratch[:size])
	if err != nil {
		return nil, fmt.Errorf("%w: merge input %s: %v", ErrIntegrity, s.Name, err)
	}
	return rows, nil
}

func (b *segmentBuilder) readInput(s Segment, data []byte) (rows []BlockRow, err error) {
	f, err := openPublicationFile(b.attempt.pub.dir, s.Name)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != s.Size {
		return nil, errors.New("segment size changed")
	}
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}
	if digest(data) != s.SHA256 {
		return nil, errors.New("segment checksum changed")
	}
	rows, err = DecodeBlock(data)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		if row.Ordinal >= b.oldOrdinal || (i != 0 && !lessRow(rows[i-1], row)) {
			return nil, errors.New("invalid segment row order or ordinal")
		}
	}
	return rows, nil
}

// repack handles blocks that cannot be coalesced whole. First check the
// maximum slack available in the window without opening any blocks. Then pack
// rows largest first, choosing the tightest fitting output. Every output is
// sorted by timestamp and ordinal before encoding, regardless of bin order.
// This is a bounded local policy, with an intentional refusal when the window
// cannot accommodate the payload or the row shapes defeat the packing.
func (b *segmentBuilder) repack(window []int, run []BlockRow, payload int) error {
	var slack int
	for _, index := range window {
		slack += blockPayload - payloadSize(b.old[index])
	}
	if slack < payload {
		return errSegmentCapacity
	}
	// Start with the smallest viable prefix. Only read another block if
	// the present row shapes cannot fit. Failed layouts change row order
	// in memory but do not write any outputs.
	rows := slices.Clone(run)
	total := payload
	for i, index := range window {
		old, err := b.decodeInput(index)
		if err != nil {
			return err
		}
		rows = append(rows, old...)
		total += payloadSize(b.old[index])
		minimum := (total + blockPayload - 1) / blockPayload
		if minimum > i+1 {
			continue
		}
		layout, err := packMergeRows(b.attempt, rows, minimum, i+1)
		if errors.Is(err, errSegmentCapacity) {
			continue
		}
		if err != nil {
			return err
		}
		for _, bin := range layout {
			if err := b.write(bin); err != nil {
				return err
			}
		}
		b.consume(i + 1)
		return nil
	}
	return errSegmentCapacity
}

// packMergeRows groups rows in place using one small assignment index per
// row. Retrying with more bins does not decode or allocate another copy of the
// rows. The window bounds both row storage and the number of placement trials.
func packMergeRows(a *publicationAttempt, rows []BlockRow, minimum, maximum int) ([][]BlockRow, error) {
	sortByMergeSize(rows)
	bins := make([]uint16, len(rows))
	var counts [mergeFanIn]int
	for limit := minimum; limit <= maximum; limit++ {
		var room [mergeFanIn]int
		for i := range room[:limit] {
			room[i] = blockPayload
		}
		clear(counts[:])
		for i, row := range rows {
			if i%1024 == 0 {
				if err := a.ctx.Err(); err != nil {
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

// fillSlack handles a suffix that can fit into free space spread across a
// larger number of old blocks. It plans a partition of only the suffix in
// memory, and rewrites each selected block individually. The number of
// rewritten inputs is bounded by the remaining originals and the staging
// budget still applies to every output.
func (b *segmentBuilder) fillSlack(run []BlockRow, payload int) error {
	var totalRoom int64
	for _, i := range b.candidates {
		totalRoom += int64(blockPayload - payloadSize(b.old[i]))
	}
	if int64(payload) > totalRoom {
		return errSegmentCapacity
	}

	// Place large rows before smaller ones so a small row cannot consume
	// space needed by a large row. The free space queue tracks the largest
	// remaining gap and is bounded by the original manifest size.
	sortByMergeSize(run)
	layout, err := b.placeInSlack(run)
	if err != nil {
		return err
	}
	for c, suffix := range layout {
		old, err := b.readInputs(b.candidates[c:c+1], len(suffix))
		if err != nil {
			return err
		}
		if err := b.write(append(old, suffix...)); err != nil {
			return err
		}
	}
	b.consume(len(layout))
	return nil
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

func (b *segmentBuilder) placeInSlack(run []BlockRow) ([][]BlockRow, error) {
	q := make(slackQueue, 0, len(b.candidates))
	// Avoid spreading small backfills across more blocks than necessary.
	// Add a new candidate only when all previously used gaps are too small.
	counts := make([]int, 0, len(b.candidates))
	assignments := make([]uint16, len(run))
	for i, row := range run {
		if i%1024 == 0 {
			if err := b.attempt.ctx.Err(); err != nil {
				return nil, err
			}
		}
		size := blockRowSize + len(row.Row.Line)
		if len(q) == 0 || q[0].room < size {
			next := len(counts)
			if next == len(b.candidates) {
				return nil, errSegmentCapacity
			}
			room := blockPayload - payloadSize(b.old[b.candidates[next]])
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
