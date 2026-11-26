// Package tiering contains the immutable storage formats used by tiered Bog.
package tiering

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"bog/internal/store"
)

// Segment version 1 is a stream of original committed batch payloads. Integers
// are little endian. Retaining payloads avoids narrowing Bog's timestamp range
// and retains both batch boundaries and the order of duplicate rows.
const segmentMagic = "BOGSEG01"
const headerSize = 36
const blockHeaderSize = 16

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// SegmentHeader identifies the WAL generation and the committed frame range
// represented by a segment. Start and End are byte offsets at frame boundaries.
type SegmentHeader struct {
	Generation uint64
	Start, End uint64
}

func validHeader(h SegmentHeader) bool { return h.Start >= 8 && h.End > h.Start }

// WriteSegment writes batches in commit order. Each batch is independently
// checksummed. The caller must sync and publish the result before relying on it.
func WriteSegment(w io.Writer, h SegmentHeader, next func() ([]byte, error)) error {
	if !validHeader(h) {
		return errors.New("invalid segment range")
	}
	var b [headerSize]byte
	copy(b[:8], segmentMagic)
	binary.LittleEndian.PutUint64(b[8:16], h.Generation)
	binary.LittleEndian.PutUint64(b[16:24], h.Start)
	binary.LittleEndian.PutUint64(b[24:32], h.End)
	binary.LittleEndian.PutUint32(b[32:36], crc32.Checksum(b[:32], checksumTable))
	if err := writeFull(w, b[:]); err != nil {
		return err
	}
	var pos = h.Start
	for {
		payload, err := next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(payload) == 0 || len(payload) > store.MaxBatch {
			return errors.New("invalid batch length")
		}
		if _, err := store.Decode(payload); err != nil {
			return fmt.Errorf("invalid batch: %w", err)
		}
		n := uint64(len(payload))
		if pos > h.End || h.End-pos < 16 || n > h.End-pos-16 {
			return errors.New("batch exceeds segment range")
		}
		var bh [blockHeaderSize]byte
		binary.LittleEndian.PutUint32(bh[0:4], uint32(n))
		binary.LittleEndian.PutUint32(bh[4:8], ^uint32(n))
		binary.LittleEndian.PutUint32(bh[8:12], crc32.Checksum(payload, checksumTable))
		binary.LittleEndian.PutUint32(bh[12:16], crc32.Checksum(bh[:12], checksumTable))
		if err := writeFull(w, bh[:]); err != nil {
			return err
		}
		if err := writeFull(w, payload); err != nil {
			return err
		}
		pos += 16 + n
	}
	if pos != h.End {
		return errors.New("segment range does not match batches")
	}
	return nil
}

// ReadSegment validates all framing before passing each batch to visit. The
// caller must not expose visited rows until this function succeeds. The reader
// allocates at most one MaxBatch payload buffer, excluding Decode and visit.
func ReadSegment(r io.Reader, size int64, visit func([]byte) error) (SegmentHeader, error) {
	var h SegmentHeader
	if size < headerSize {
		return h, errors.New("short segment")
	}
	var b [headerSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return h, err
	}
	if string(b[:8]) != segmentMagic || crc32.Checksum(b[:32], checksumTable) != binary.LittleEndian.Uint32(b[32:36]) {
		return h, errors.New("invalid segment version")
	}
	h = SegmentHeader{binary.LittleEndian.Uint64(b[8:16]), binary.LittleEndian.Uint64(b[16:24]), binary.LittleEndian.Uint64(b[24:32])}
	if !validHeader(h) || h.End-h.Start != uint64(size-headerSize) {
		return h, errors.New("invalid segment length or range")
	}
	pos := h.Start
	var payload []byte
	for pos < h.End {
		if h.End-pos < blockHeaderSize {
			return h, errors.New("truncated block header")
		}
		var bh [blockHeaderSize]byte
		if _, err := io.ReadFull(r, bh[:]); err != nil {
			return h, err
		}
		n := binary.LittleEndian.Uint32(bh[:4])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(bh[4:8]) != ^n || crc32.Checksum(bh[:12], checksumTable) != binary.LittleEndian.Uint32(bh[12:]) || uint64(n) > h.End-pos-blockHeaderSize {
			return h, errors.New("invalid block header")
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		} else {
			payload = payload[:n]
		}
		if _, err := io.ReadFull(r, payload); err != nil {
			return h, err
		}
		if crc32.Checksum(payload, checksumTable) != binary.LittleEndian.Uint32(bh[8:12]) {
			return h, errors.New("invalid block checksum")
		}
		if _, err := store.Decode(payload); err != nil {
			return h, fmt.Errorf("invalid batch: %w", err)
		}
		if err := visit(payload); err != nil {
			return h, err
		}
		pos += blockHeaderSize + uint64(n)
	}
	return h, nil
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
