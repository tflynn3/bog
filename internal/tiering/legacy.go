// Package tiering contains building blocks for tiered storage.
package tiering

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"bog/internal/store"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ReadLegacy reads exactly the committed prefix of a stopped legacy WAL. The
// caller must hold the legacy directory lock for the entire operation. Each
// callback receives a complete, validated batch in commit order; its payload
// is valid only until the callback returns. A callback error stops the import.
// The returned offset is the exact frame boundary covered by the import.
func ReadLegacy(wal io.ReaderAt, size int64, marker []byte, visit func(offset int64, payload []byte, rows []store.Row) error) (int64, error) {
	if len(marker) != 24 || string(marker[:8]) != "BOGCMT01" {
		return 0, errors.New("invalid commit marker")
	}
	end := binary.LittleEndian.Uint64(marker[8:16])
	if binary.LittleEndian.Uint64(marker[16:]) != ^end || end < 8 || end > uint64(size) || size < 0 {
		return 0, errors.New("invalid committed offset")
	}
	var magic [8]byte
	if _, err := wal.ReadAt(magic[:], 0); err != nil || string(magic[:]) != "BOGWAL01" {
		return 0, errors.New("invalid WAL magic")
	}
	var header [16]byte
	var payload []byte
	for pos := int64(8); pos < int64(end); {
		if int64(end)-pos < int64(len(header)) {
			return 0, fmt.Errorf("incomplete committed header at %d", pos)
		}
		if _, err := wal.ReadAt(header[:], pos); err != nil {
			return 0, fmt.Errorf("read header at %d: %w", pos, err)
		}
		n := binary.LittleEndian.Uint32(header[:4])
		if crc32.Checksum(header[:12], castagnoli) != binary.LittleEndian.Uint32(header[12:]) || n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(header[4:8]) != ^n {
			return 0, fmt.Errorf("invalid frame header at %d", pos)
		}
		if int64(n) > int64(end)-pos-16 {
			return 0, fmt.Errorf("incomplete committed frame at %d", pos)
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		} else {
			payload = payload[:n]
		}
		if _, err := wal.ReadAt(payload, pos+16); err != nil {
			return 0, fmt.Errorf("read frame at %d: %w", pos, err)
		}
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(header[8:12]) {
			return 0, fmt.Errorf("invalid frame checksum at %d", pos)
		}
		rows, err := store.Decode(payload)
		if err != nil {
			return 0, fmt.Errorf("invalid frame payload at %d: %w", pos, err)
		}
		next := pos + 16 + int64(n)
		if err := visit(next, payload, rows); err != nil {
			return 0, err
		}
		pos = next
	}
	return int64(end), nil
}

// ReadLegacyFiles reads a marker and WAL already opened under the legacy lock.
// It rejects non-regular files and limits the marker read before allocating.
func ReadLegacyFiles(wal, marker *os.File, visit func(int64, []byte, []store.Row) error) (int64, error) {
	wi, err := wal.Stat()
	if err != nil {
		return 0, err
	}
	mi, err := marker.Stat()
	if err != nil {
		return 0, err
	}
	if !wi.Mode().IsRegular() || !mi.Mode().IsRegular() || mi.Size() != 24 {
		return 0, errors.New("invalid legacy files")
	}
	var b [24]byte
	if _, err := marker.ReadAt(b[:], 0); err != nil {
		return 0, err
	}
	return ReadLegacy(wal, wi.Size(), b[:], visit)
}
