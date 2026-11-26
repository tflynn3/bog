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

// ImportLegacy copies the committed frames from a stopped legacy WAL into a
// segment. The caller must hold the source directory lock throughout import,
// discard the segment on failure, and retain the source until publication is
// durable. The returned digest identifies the exact committed prefix, not a
// permanent identity for a WAL which may later be extended.
func ImportLegacy(wal io.ReaderAt, size int64, marker []byte, dst io.Writer, first uint64) (end int64, digest [32]byte, err error) {
	if len(marker) != 24 || string(marker[:8]) != "BOGCMT01" {
		return 0, digest, errors.New("invalid commit marker")
	}
	boundary := binary.LittleEndian.Uint64(marker[8:16])
	if binary.LittleEndian.Uint64(marker[16:]) != ^boundary || boundary < 8 || boundary > uint64(size) || size < 0 {
		return 0, digest, errors.New("invalid committed offset")
	}
	end = int64(boundary)
	var magic [8]byte
	if err = readAtFull(wal, magic[:], 0); err != nil {
		return 0, digest, err
	}
	if string(magic[:]) != "BOGWAL01" {
		return 0, digest, errors.New("invalid WAL magic")
	}
	h := sha256.New()
	h.Write(magic[:])
	sw, err := NewSegmentWriter(dst, first)
	if err != nil {
		return 0, digest, err
	}
	var header [16]byte
	for pos := int64(8); pos < end; {
		if end-pos < 16 {
			return 0, digest, errors.New("incomplete committed frame header")
		}
		if err = readAtFull(wal, header[:], pos); err != nil {
			return 0, digest, err
		}
		n := binary.LittleEndian.Uint32(header[:4])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(header[4:8]) != ^n || crc32.Checksum(header[:12], castagnoli) != binary.LittleEndian.Uint32(header[12:]) || int64(n) > end-pos-16 {
			return 0, digest, fmt.Errorf("invalid committed frame at %d", pos)
		}
		payload := make([]byte, int(n))
		if err = readAtFull(wal, payload, pos+16); err != nil {
			return 0, digest, err
		}
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(header[8:12]) {
			return 0, digest, fmt.Errorf("invalid committed payload at %d", pos)
		}
		if err = sw.Add(payload); err != nil {
			return 0, digest, fmt.Errorf("invalid committed batch at %d: %w", pos, err)
		}
		h.Write(header[:])
		h.Write(payload)
		pos += 16 + int64(n)
	}
	if err = sw.Close(); err != nil {
		return 0, digest, err
	}
	copy(digest[:], h.Sum(nil))
	return end, digest, nil
}
