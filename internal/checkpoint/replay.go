package checkpoint

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"bog/internal/store"
)

// ReplayBatch contains a whole committed frame and its append-order rows.
type ReplayBatch struct {
	Start, End   int64
	FirstOrdinal uint64
	Rows         []store.Row
	Reservation  *Reservation
}

// ReplayedTail contains fully validated committed batches. The owner transfers
// each batch's reservation with its immutable rows and releases it when that
// batch's backing data drains. Captures can retain individual reservations.
type ReplayedTail struct {
	Batches     []ReplayBatch
	NextOrdinal uint64
}

// ReplayCapturedTail atomically admits a committed suffix before reading it.
// Offsets are logical and input is addressed in those same offsets. The WAL
// owner validates the input identity and provides the covered frame boundary.
// An error returns no batches and releases the charge.
func (b *Budget) ReplayCapturedTail(ctx context.Context, input io.ReaderAt, start, end int64, firstOrdinal uint64) (*ReplayedTail, error) {
	if input == nil || start < int64(len("BOGWAL01")) || end < start {
		return nil, fmt.Errorf("%w: invalid committed replay range", ErrIntegrity)
	}
	reservation, err := b.ReserveReplayTail(ctx, start, end)
	if err != nil {
		return nil, err
	}
	defer reservation.Release()
	tail := &ReplayedTail{NextOrdinal: firstOrdinal}
	if err := scanCapturedFrames(ctx, input, start, end, firstOrdinal, func(pos, next int64, first uint64, rows []store.Row) error {
		tail.Batches = append(tail.Batches, ReplayBatch{
			Start: pos, End: next, FirstOrdinal: first, Rows: rows,
			Reservation: reservation.split(uint64(next - pos)),
		})
		tail.NextOrdinal = first + uint64(len(rows))
		return nil
	}); err != nil {
		for _, batch := range tail.Batches {
			batch.Reservation.Release()
		}
		return nil, err
	}
	return tail, nil
}

// scanCapturedFrames reads an owner-pinned suffix at absolute WAL offsets.
// It never reads the omitted prefix of a restored WAL.
func scanCapturedFrames(ctx context.Context, input io.ReaderAt, pos, end int64, ordinal uint64, visit func(int64, int64, uint64, []store.Row) error) error {
	return scanFrames(ctx, io.NewSectionReader(input, pos, end-pos), pos, end, ordinal, visit)
}

// scanFrames reads sequentially from the start of the declared logical range.
func scanFrames(ctx context.Context, input io.Reader, pos, end int64, ordinal uint64, visit func(int64, int64, uint64, []store.Row) error) error {
	var header [16]byte
	var payload []byte
	for pos < end {
		if err := ctx.Err(); err != nil {
			return err
		}
		if end-pos < int64(len(header)) {
			return fmt.Errorf("%w: incomplete committed frame at %d", ErrIntegrity, pos)
		}
		if _, err := io.ReadFull(input, header[:]); err != nil {
			return fmt.Errorf("read committed frame header at %d: %w", pos, err)
		}
		n := binary.LittleEndian.Uint32(header[:4])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(header[4:8]) != ^n ||
			crc32.Checksum(header[:12], castagnoli) != binary.LittleEndian.Uint32(header[12:]) {
			return fmt.Errorf("%w: invalid committed frame header at %d", ErrIntegrity, pos)
		}
		if int64(n) > end-pos-int64(len(header)) {
			return fmt.Errorf("%w: incomplete committed frame at %d", ErrIntegrity, pos)
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		}
		payload = payload[:n]
		if _, err := io.ReadFull(input, payload); err != nil {
			return fmt.Errorf("read committed frame at %d: %w", pos, err)
		}
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(header[8:12]) {
			return fmt.Errorf("%w: invalid committed frame checksum at %d", ErrIntegrity, pos)
		}
		rows, err := store.Decode(payload)
		if err != nil {
			return fmt.Errorf("%w: invalid committed frame at %d: %w", ErrIntegrity, pos, err)
		}
		if uint64(len(rows)) > ^uint64(0)-ordinal {
			return fmt.Errorf("%w: append ordinal overflow", ErrIntegrity)
		}
		next := pos + int64(len(header)) + int64(n)
		if err := visit(pos, next, ordinal, rows); err != nil {
			return err
		}
		ordinal += uint64(len(rows))
		pos = next
	}
	return ctx.Err()
}

// ScanCapturedFrames validates a logical range and streams whole decoded frames.
func ScanCapturedFrames(ctx context.Context, input io.ReaderAt, start, end int64, first uint64, visit func(int64, int64, uint64, []store.Row) error) error {
	if input == nil || start < 8 || end < start {
		return fmt.Errorf("%w: invalid committed replay range", ErrIntegrity)
	}
	return scanCapturedFrames(ctx, input, start, end, first, visit)
}
