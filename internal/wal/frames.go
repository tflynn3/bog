package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"time"

	"bog/internal/store"
)

const frameHeader = 16
const MaxLiveFrames = 32768

// Frame describes one whole committed append and its verified time bounds.
type Frame struct {
	Start, End                int64
	FirstOrdinal, NextOrdinal uint64
	MinTime, MaxTime          time.Time
	MaxLineBytes              int
	Ordered                   bool
	last                      time.Time
}

func (f *Frame) add(row store.Row) error {
	ts := row.TS.UTC()
	if f.NextOrdinal == ^uint64(0) {
		return errors.New("WAL ordinal overflow")
	}
	if f.FirstOrdinal == f.NextOrdinal || ts.Before(f.MinTime) {
		f.MinTime = ts
	}
	if f.FirstOrdinal == f.NextOrdinal || ts.After(f.MaxTime) {
		f.MaxTime = ts
	}
	f.Ordered = f.FirstOrdinal == f.NextOrdinal || f.Ordered && !ts.Before(f.last)
	f.last = ts
	f.MaxLineBytes = max(f.MaxLineBytes, len(row.Line))
	f.NextOrdinal++
	return nil
}

func readFrame(input io.Reader, pos, end int64, header *[frameHeader]byte, payload *[]byte) error {
	if end-pos < frameHeader {
		return fmt.Errorf("incomplete committed frame header at %d", pos)
	}
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return fmt.Errorf("read committed frame header at %d: %w", pos, err)
	}
	n := binary.LittleEndian.Uint32(header[:])
	if crc32.Checksum(header[:12], table) != binary.LittleEndian.Uint32(header[12:]) {
		return fmt.Errorf("invalid WAL header checksum at %d", pos)
	}
	if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(header[4:]) != ^n {
		return fmt.Errorf("invalid WAL header at %d", pos)
	}
	if int64(n) > end-pos-frameHeader {
		return fmt.Errorf("incomplete committed frame payload at %d", pos)
	}
	if cap(*payload) < int(n) {
		*payload = make([]byte, n, min(store.MaxBatch, max(int(n), 2*cap(*payload))))
	}
	*payload = (*payload)[:n]
	if _, err := io.ReadFull(input, *payload); err != nil {
		return fmt.Errorf("read committed frame payload at %d: %w", pos, err)
	}
	if crc32.Checksum(*payload, table) != binary.LittleEndian.Uint32(header[8:]) {
		return fmt.Errorf("invalid WAL checksum at %d", pos)
	}
	return nil
}

func scanFrames(ctx context.Context, input io.ReaderAt, start, end int64, ordinal uint64,
	visit func([]byte) error, index func(Frame) error,
) error {
	r := bufio.NewReaderSize(io.NewSectionReader(input, start, end-start), 64<<10)
	var payload []byte
	var header [frameHeader]byte
	for pos := start; pos < end; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := readFrame(r, pos, end, &header, &payload); err != nil {
			return err
		}
		frame := Frame{Start: pos, End: pos + frameHeader + int64(len(payload)), FirstOrdinal: ordinal, NextOrdinal: ordinal}
		if err := store.VisitDecoded(payload, frame.add); err != nil {
			return fmt.Errorf("invalid WAL payload at %d: %w", pos, err)
		}
		if visit != nil {
			if err := visit(payload); err != nil {
				return err
			}
		}
		if err := index(frame); err != nil {
			return err
		}
		pos, ordinal = frame.End, frame.NextOrdinal
	}
	return ctx.Err()
}

// ValidatePrefix checks a checkpoint boundary against the ordinary WAL. The
// first free ordinal must have been derived from whole checked frames.
func ValidatePrefix(input io.ReaderAt, end int64, ordinal uint64) error {
	if end < int64(len(magic)) {
		return errors.New("invalid WAL prefix end")
	}
	var next uint64
	err := scanFrames(context.Background(), input, int64(len(magic)), end, 0, nil, func(f Frame) error {
		next = f.NextOrdinal
		return nil
	})
	if err != nil {
		return err
	}
	if next != ordinal {
		return errors.New("checkpoint WAL prefix ordinal mismatch")
	}
	return nil
}

func (c *Capture) FrameCount() int { return len(c.lease.frames) }

// ScanBounds describes the largest frame payload and decoded line in the view.
func (c *Capture) ScanBounds() (frameBytes, lineBytes int) {
	return c.lease.bounds.frameBytes, c.lease.bounds.lineBytes
}

type frameBounds struct {
	frameBytes, lineBytes int
	unordered             bool
}

func (b *frameBounds) add(frame Frame, previous *Frame) {
	b.frameBytes = max(b.frameBytes, int(frame.End-frame.Start)-frameHeader)
	b.lineBytes = max(b.lineBytes, frame.MaxLineBytes)
	b.unordered = b.unordered || !frame.Ordered || previous != nil && frame.MinTime.Before(previous.MaxTime)
}

func (c *Capture) SeekWork() uint64 {
	if c.lease.bounds.unordered {
		return 0
	}
	return uint64(2 * bits.Len(uint(len(c.lease.frames))))
}
