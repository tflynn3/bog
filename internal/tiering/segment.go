// Package tiering contains primitives for immutable tiered storage.
package tiering

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"bog/internal/store"
)

// Segment v1 stores complete committed WAL payloads in append order. All
// integers are little-endian. A block's time bounds are inclusive and only a
// pruning hint; filtering must still use the decoded rows.
const (
	segmentMagic    = "BOGSEG01"
	blockMagic      = "BLK1"
	footerMagic     = "BOGEND01"
	headerSize      = 16
	blockHeaderSize = 56
	footerSize      = 24
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)
var ErrCorrupt = errors.New("corrupt tiering segment")

type Frame struct {
	Sequence uint64
	Payload  []byte
	Min, Max time.Time
}

// SegmentWriter writes a v1 segment. The caller must sync and install the file
// before publishing a reference to it. It must discard output on any error.
type SegmentWriter struct {
	w           io.Writer
	next, count uint64
	failed      error
	closed      bool
}

func NewSegmentWriter(w io.Writer, first uint64) (*SegmentWriter, error) {
	if first == 0 {
		return nil, errors.New("sequence must be positive")
	}
	s := &SegmentWriter{w: w, next: first}
	var h [headerSize]byte
	copy(h[:8], segmentMagic)
	binary.LittleEndian.PutUint32(h[8:12], 1)
	binary.LittleEndian.PutUint32(h[12:16], crc32.Checksum(h[:12], castagnoli))
	if err := writeFull(w, h[:]); err != nil {
		return nil, err
	}
	return s, nil
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Add validates a complete batch and writes it as one checksummed block.
func (s *SegmentWriter) Add(payload []byte) error {
	if s.closed {
		return errors.New("segment closed")
	}
	if s.failed != nil {
		return s.failed
	}
	rows, err := store.Decode(payload)
	if err != nil {
		return err
	}
	if s.next == ^uint64(0) {
		return errors.New("sequence exhausted")
	}
	min, max := rows[0].TS, rows[0].TS
	for _, r := range rows[1:] {
		if r.TS.Before(min) {
			min = r.TS
		}
		if r.TS.After(max) {
			max = r.TS
		}
	}
	var h [blockHeaderSize]byte
	copy(h[:4], blockMagic)
	binary.LittleEndian.PutUint32(h[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint64(h[8:16], s.next)
	binary.LittleEndian.PutUint64(h[16:24], uint64(min.Unix()))
	binary.LittleEndian.PutUint32(h[24:28], uint32(min.Nanosecond()))
	binary.LittleEndian.PutUint64(h[28:36], uint64(max.Unix()))
	binary.LittleEndian.PutUint32(h[36:40], uint32(max.Nanosecond()))
	binary.LittleEndian.PutUint32(h[40:44], crc32.Checksum(payload, castagnoli))
	// Bytes 44..51 are reserved and included in the header checksum.
	binary.LittleEndian.PutUint32(h[52:56], crc32.Checksum(h[:52], castagnoli))
	if err = writeFull(s.w, h[:]); err == nil {
		err = writeFull(s.w, payload)
	}
	if err != nil {
		s.failed = err
		return err
	}
	s.next++
	s.count++
	return nil
}

func (s *SegmentWriter) Close() error {
	if s.closed {
		return errors.New("segment closed")
	}
	s.closed = true
	if s.failed != nil {
		return s.failed
	}
	var f [footerSize]byte
	copy(f[:8], footerMagic)
	binary.LittleEndian.PutUint64(f[8:16], s.count)
	binary.LittleEndian.PutUint32(f[20:24], crc32.Checksum(f[:20], castagnoli))
	return writeFull(s.w, f[:])
}

// ReadSegment validates every block and footer, including bounds against the
// decoded payload, before returning success. The callback must not expose
// partial results until ReadSegment has returned successfully.
func ReadSegment(r io.ReaderAt, size int64, visit func(Frame) error) error {
	bad := func(detail string) error { return fmt.Errorf("%w: %s", ErrCorrupt, detail) }
	if size < headerSize+footerSize {
		return bad("short file")
	}
	var h [headerSize]byte
	if _, err := r.ReadAt(h[:], 0); err != nil {
		return err
	}
	if string(h[:8]) != segmentMagic || binary.LittleEndian.Uint32(h[8:12]) != 1 || binary.LittleEndian.Uint32(h[12:]) != crc32.Checksum(h[:12], castagnoli) {
		return bad("header")
	}
	var footer [footerSize]byte
	if _, err := r.ReadAt(footer[:], size-footerSize); err != nil {
		return err
	}
	if string(footer[:8]) != footerMagic || binary.LittleEndian.Uint32(footer[16:20]) != 0 || binary.LittleEndian.Uint32(footer[20:]) != crc32.Checksum(footer[:20], castagnoli) {
		return bad("footer")
	}
	end := size - footerSize
	pos := int64(headerSize)
	var count, prev uint64
	for pos < end {
		if end-pos < blockHeaderSize {
			return bad("short block header")
		}
		var b [blockHeaderSize]byte
		if _, err := r.ReadAt(b[:], pos); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(b[4:8])
		seq := binary.LittleEndian.Uint64(b[8:16])
		if string(b[:4]) != blockMagic || n == 0 || n > store.MaxBatch || int64(n) > end-pos-blockHeaderSize || binary.LittleEndian.Uint64(b[44:52]) != 0 || binary.LittleEndian.Uint32(b[52:]) != crc32.Checksum(b[:52], castagnoli) || seq == 0 || (count > 0 && (prev == ^uint64(0) || seq != prev+1)) {
			return bad("block header")
		}
		minNS, maxNS := binary.LittleEndian.Uint32(b[24:28]), binary.LittleEndian.Uint32(b[36:40])
		if minNS >= 1e9 || maxNS >= 1e9 {
			return bad("nanoseconds")
		}
		min := time.Unix(int64(binary.LittleEndian.Uint64(b[16:24])), int64(minNS)).UTC()
		max := time.Unix(int64(binary.LittleEndian.Uint64(b[28:36])), int64(maxNS)).UTC()
		p := make([]byte, int(n))
		if _, err := r.ReadAt(p, pos+blockHeaderSize); err != nil {
			return err
		}
		if crc32.Checksum(p, castagnoli) != binary.LittleEndian.Uint32(b[40:44]) {
			return bad("payload checksum")
		}
		rows, err := store.Decode(p)
		if err != nil {
			return bad("payload")
		}
		actualMin, actualMax := rows[0].TS, rows[0].TS
		for _, row := range rows[1:] {
			if row.TS.Before(actualMin) {
				actualMin = row.TS
			}
			if row.TS.After(actualMax) {
				actualMax = row.TS
			}
		}
		if !min.Equal(actualMin) || !max.Equal(actualMax) {
			return bad("time bounds")
		}
		if visit != nil {
			if err = visit(Frame{seq, p, min, max}); err != nil {
				return err
			}
		}
		count++
		prev = seq
		pos += blockHeaderSize + int64(n)
	}
	if count != binary.LittleEndian.Uint64(footer[8:16]) {
		return bad("block count")
	}
	return nil
}
