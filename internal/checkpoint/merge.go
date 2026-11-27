package checkpoint

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sort"

	"bog/internal/store"
)

const blockPayload = MaxBlockSize - blockHeaderSize

type segmentBuilder struct {
	attempt       *publicationAttempt
	old           []Segment
	oldOrdinal    uint64
	candidates    []int // Unused old segments, smallest payload first.
	replaced      []bool
	replacedCount int
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
			if i%1024 == 0 {
				if err := a.ctx.Err(); err != nil {
					return err
				}
			}
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
	if len(b.old)-b.replacedCount+len(b.attempt.files) < MaxSegments {
		return b.write(run)
	}
	if b.candidates == nil {
		b.rankCandidates()
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
	segments := make([]Segment, 0, len(b.old)-b.replacedCount+len(b.attempt.files))
	for i, old := range b.old {
		if b.replaced == nil || !b.replaced[i] {
			segments = append(segments, old)
		}
	}
	for _, file := range b.attempt.files {
		segments = append(segments, file.segment())
	}
	return segments
}

func (b *segmentBuilder) write(run []BlockRow) error {
	if err := b.attempt.ctx.Err(); err != nil {
		return err
	}
	slices.SortFunc(run, compareMergeRows)
	return b.attempt.stageBlock(run)
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
		rows, err := b.attempt.readOriginal(b.old[i], b.oldOrdinal)
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
		old, err := b.attempt.readOriginal(b.old[index], b.oldOrdinal)
		if err != nil {
			return err
		}
		rows = append(rows, old...)
		total += payloadSize(b.old[index])
		minimum := (total + blockPayload - 1) / blockPayload
		if minimum > i+1 {
			continue
		}
		layout, err := packMergeRows(b.attempt.ctx, rows, blockPayload, minimum, i+1)
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

// fillSlack partitions only the suffix across eligible gaps. It decodes one
// block at a time and uses the same placement rule for originals and staged
// blocks. The attempt seals a staged slot after replacing it once.
func (b *segmentBuilder) fillSlack(run []BlockRow, payload int) error {
	candidates := make([]slackInput, 0, len(b.candidates)+len(b.attempt.files))
	var totalRoom int64
	for _, i := range b.candidates {
		room := blockPayload - payloadSize(b.old[i])
		if room > 0 {
			candidates = append(candidates, slackInput{index: i, room: room})
			totalRoom += int64(room)
		}
	}
	for i, file := range b.attempt.files {
		if room := file.reusableRoom(); room > 0 {
			candidates = append(candidates, slackInput{index: i, room: room, staged: true})
			totalRoom += int64(room)
		}
	}
	if int64(payload) > totalRoom {
		return errSegmentCapacity
	}
	// Preserve the ranking of originals with equal slack and break staged
	// ties by slot order, independent of the new file names.
	slices.SortStableFunc(candidates, func(a, b slackInput) int {
		return cmp.Compare(b.room, a.room)
	})
	// The run arrives in ordinal order. Remember its lower bound before
	// changing that order so staged inputs can be checked independently.
	first := run[0].Ordinal
	rooms := make([]int, len(candidates))
	for i, candidate := range candidates {
		rooms[i] = candidate.room
	}
	layout, err := placeInSlack(b.attempt.ctx, run, rooms)
	if err != nil {
		return err
	}
	originals := 0
	for i, suffix := range layout {
		input := candidates[i]
		if input.staged {
			rows, err := b.attempt.readStaged(input.index, first)
			if err != nil {
				return err
			}
			rows = append(rows, suffix...)
			slices.SortFunc(rows, compareMergeRows)
			if err := b.attempt.replaceBlock(input.index, rows); err != nil {
				return err
			}
		} else {
			rows, err := b.attempt.readOriginal(b.old[input.index], b.oldOrdinal)
			if err != nil {
				return err
			}
			if err := b.write(append(rows, suffix...)); err != nil {
				return err
			}
			originals++
		}
	}
	b.consume(originals)
	return nil
}

type slackInput struct {
	index  int
	room   int
	staged bool
}
