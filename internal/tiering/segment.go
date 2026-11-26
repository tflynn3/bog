// Package tiering contains the bounded offline segment codec.
package tiering

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"bog/internal/store"
)

const headerSize = 48
const footerSize = 32
const segmentMagic = "BOGSEG01"

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Coverage identifies the exact legacy WAL prefix represented by a segment.
// Generation must identify the source WAL and must not be reused for another WAL.
type Coverage struct {
	Generation [16]byte
	End        uint64
}

// ImportLegacy validates and copies a stopped WAL's committed prefix into dst.
// The caller must hold the source directory lock for the entire operation. dst
// must be a new staging file and must be discarded if this function fails.
func ImportLegacy(dst io.Writer, source io.ReaderAt, size int64, marker []byte, generation [16]byte) (Coverage, error) {
	var c Coverage
	if len(marker) != 24 || string(marker[:8]) != "BOGCMT01" {
		return c, errors.New("invalid commit marker")
	}
	end := binary.LittleEndian.Uint64(marker[8:16])
	if binary.LittleEndian.Uint64(marker[16:24]) != ^end || size < 8 || end < 8 || end > uint64(size) {
		return c, errors.New("invalid committed boundary")
	}
	var magic [8]byte
	if _, err := source.ReadAt(magic[:], 0); err != nil || string(magic[:]) != "BOGWAL01" {
		return c, errors.New("invalid WAL magic")
	}
	c = Coverage{generation, end}
	var h [headerSize]byte
	copy(h[:8], segmentMagic)
	copy(h[8:24], generation[:])
	binary.LittleEndian.PutUint64(h[24:32], end)
	binary.LittleEndian.PutUint32(h[44:48], crc32.Checksum(h[:44], crcTable))
	digest := sha256.New()
	out := io.MultiWriter(dst, digest)
	if err := writeFull(out, h[:]); err != nil {
		return Coverage{}, err
	}
	err := walkFrames(source, 8, end, func(header, payload []byte) error {
		if err := writeFull(out, header); err != nil {
			return err
		}
		return writeFull(out, payload)
	})
	if err != nil {
		return Coverage{}, err
	}
	if err = writeFull(dst, digest.Sum(nil)); err != nil {
		return Coverage{}, err
	}
	return c, nil
}

// ReadSegment validates the complete segment before returning. visit is called
// in original commit order; callers must discard its effects on any error.
func ReadSegment(src io.ReaderAt, size int64, visit func([]byte) error) (Coverage, error) {
	var c Coverage
	if size < headerSize+footerSize {
		return c, errors.New("short segment")
	}
	var h [headerSize]byte
	if _, err := src.ReadAt(h[:], 0); err != nil {
		return c, err
	}
	if string(h[:8]) != segmentMagic || crc32.Checksum(h[:44], crcTable) != binary.LittleEndian.Uint32(h[44:]) {
		return c, errors.New("invalid segment header")
	}
	for _, b := range h[32:44] {
		if b != 0 {
			return c, errors.New("unsupported segment metadata")
		}
	}
	copy(c.Generation[:], h[8:24])
	c.End = binary.LittleEndian.Uint64(h[24:32])
	if c.End < 8 || c.End-8 != uint64(size-headerSize-footerSize) {
		return Coverage{}, errors.New("invalid segment length")
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, io.NewSectionReader(src, 0, size-footerSize)); err != nil {
		return Coverage{}, err
	}
	var sum [footerSize]byte
	if _, err := src.ReadAt(sum[:], size-footerSize); err != nil {
		return Coverage{}, err
	}
	if string(sum[:]) != string(digest.Sum(nil)) {
		return Coverage{}, errors.New("invalid segment checksum")
	}
	err := walkFrames(src, headerSize, uint64(size-footerSize), func(_, payload []byte) error {
		if visit != nil {
			return visit(payload)
		}
		return nil
	})
	if err != nil {
		return Coverage{}, err
	}
	return c, nil
}

func walkFrames(src io.ReaderAt, start, end uint64, visit func([]byte, []byte) error) error {
	var h [16]byte
	var payload []byte
	for pos := start; pos < end; {
		if end-pos < 16 {
			return errors.New("incomplete committed frame header")
		}
		if _, err := src.ReadAt(h[:], int64(pos)); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(h[:4])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:8]) != ^n || crc32.Checksum(h[:12], crcTable) != binary.LittleEndian.Uint32(h[12:]) || uint64(n) > end-pos-16 {
			return fmt.Errorf("invalid frame at %d", pos)
		}
		if cap(payload) < int(n) {
			payload = make([]byte, int(n))
		} else {
			payload = payload[:int(n)]
		}
		if _, err := src.ReadAt(payload, int64(pos+16)); err != nil {
			return err
		}
		if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(h[8:12]) {
			return fmt.Errorf("invalid frame checksum at %d", pos)
		}
		if _, err := store.Decode(payload); err != nil {
			return fmt.Errorf("invalid frame payload at %d: %w", pos, err)
		}
		if err := visit(h[:], payload); err != nil {
			return err
		}
		pos += 16 + uint64(n)
	}
	return nil
}
func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
