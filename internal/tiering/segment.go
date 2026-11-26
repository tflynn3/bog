// Package tiering contains offline building blocks for immutable WAL segments.
package tiering

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"

	"bog/internal/store"
)

const segmentMagic = "BOGSEG01"
const walMagic = "BOGWAL01"

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Boundary is the exact end offset of committed frames in a legacy WAL.
func Boundary(marker io.Reader, walSize int64) (int64, error) {
	var b [25]byte
	n, err := io.ReadFull(marker, b[:])
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return 0, err
	}
	if n != 24 || string(b[:8]) != "BOGCMT01" {
		return 0, errors.New("invalid commit marker")
	}
	end := binary.LittleEndian.Uint64(b[8:16])
	if binary.LittleEndian.Uint64(b[16:24]) != ^end || end < 8 || end > math.MaxInt64 || int64(end) > walSize {
		return 0, errors.New("invalid committed offset")
	}
	return int64(end), nil
}

// Convert writes a segment containing exactly the committed WAL frames. The
// input must be stopped and protected by the same directory lock as Bog.
// Output is staged only: callers must sync and publish it separately.
func Convert(wal io.ReaderAt, size, end int64, out io.Writer) error {
	if end < 8 || end > size {
		return errors.New("invalid boundary")
	}
	var magic [8]byte
	if _, err := wal.ReadAt(magic[:], 0); err != nil || string(magic[:]) != walMagic {
		return errors.New("invalid WAL magic")
	}
	var header [24]byte
	copy(header[:8], segmentMagic)
	binary.LittleEndian.PutUint64(header[8:16], uint64(end))
	binary.LittleEndian.PutUint32(header[16:20], crc32.Checksum(header[:16], crcTable))
	if err := writeFull(out, header[:]); err != nil {
		return err
	}
	return scanFrames(io.NewSectionReader(wal, 8, end-8), end-8, func(h []byte, payload []byte) error {
		if err := writeFull(out, h); err != nil {
			return err
		}
		return writeFull(out, payload)
	})
}

// Read validates and visits frames in commit order. The caller must discard any
// results if an error is returned; callbacks can run before later corruption is found.
func Read(r io.ReaderAt, size int64, visit func([]byte) error) error {
	if size < 24 {
		return errors.New("short segment")
	}
	var h [24]byte
	if _, err := r.ReadAt(h[:], 0); err != nil {
		return err
	}
	end := binary.LittleEndian.Uint64(h[8:16])
	if string(h[:8]) != segmentMagic || binary.LittleEndian.Uint32(h[16:20]) != crc32.Checksum(h[:16], crcTable) || binary.LittleEndian.Uint32(h[20:24]) != 0 || end < 8 || end > math.MaxInt64 || int64(end)-8 != size-24 {
		return errors.New("invalid segment header or length")
	}
	return scanFrames(io.NewSectionReader(r, 24, size-24), size-24, func(_ []byte, p []byte) error { return visit(p) })
}

func scanFrames(r io.Reader, remaining int64, visit func([]byte, []byte) error) error {
	var h [16]byte
	var payload []byte
	for remaining > 0 {
		if remaining < int64(len(h)) {
			return errors.New("incomplete committed frame")
		}
		if _, err := io.ReadFull(r, h[:]); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(h[:4])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:8]) != ^n || binary.LittleEndian.Uint32(h[12:16]) != crc32.Checksum(h[:12], crcTable) || int64(n) > remaining-16 {
			return errors.New("invalid frame header or length")
		}
		if cap(payload) < int(n) {
			payload = make([]byte, int(n))
		} else {
			payload = payload[:int(n)]
		}
		if _, err := io.ReadFull(r, payload); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(h[8:12]) != crc32.Checksum(payload, crcTable) {
			return errors.New("invalid frame checksum")
		}
		if _, err := store.Decode(payload); err != nil {
			return fmt.Errorf("invalid frame payload: %w", err)
		}
		if err := visit(h[:], payload); err != nil {
			return err
		}
		remaining -= 16 + int64(n)
	}
	return nil
}
func writeFull(w io.Writer, b []byte) error {
	for len(b) != 0 {
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

// OpenLegacy opens a stopped WAL and marker without changing either file.
// The caller is responsible for acquiring the pinned directory lock first.
func OpenLegacy(dir string) (*os.File, int64, int64, error) {
	f, err := os.Open(dir + "/bog.wal")
	if err != nil {
		return nil, 0, 0, err
	}
	fail := func(e error) (*os.File, int64, int64, error) { f.Close(); return nil, 0, 0, e }
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		return fail(errors.New("WAL is not regular"))
	}
	m, err := os.Open(dir + "/bog.commit")
	if err != nil {
		return fail(err)
	}
	defer m.Close()
	end, err := Boundary(m, info.Size())
	if err != nil {
		return fail(err)
	}
	return f, info.Size(), end, nil
}
