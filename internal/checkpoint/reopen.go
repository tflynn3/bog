package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"syscall"

	"bog/internal/store"
)

// ReadCheckpoint validates the complete manifest, all referenced segments and
// committed WAL suffix before calling visit. Segment rows carry their original
// ordinals; callers ordering results by timestamp must use ordinal as a tie-break.
// The directory lock must remain held throughout the call.
func (l *Legacy) ReadCheckpoint(visit func(BlockRow) error) error {
	if l == nil || l.wal == nil || l.dir == nil || visit == nil {
		return errors.New("invalid checkpoint reader")
	}
	f, err := l.openFile("bog.manifest")
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if info.Size() < manifestHeaderSize+4 || info.Size() > MaxManifestBytes {
		f.Close()
		return errManifest
	}
	data, err := io.ReadAll(io.LimitReader(f, MaxManifestBytes+1))
	f.Close()
	if err != nil {
		return err
	}
	m, err := ParseManifest(data)
	if err != nil {
		return err
	}
	info, err = l.wal.Stat()
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Dev) != m.WALDevice || uint64(st.Ino) != m.WALInode || m.CoveredOffset > uint64(l.end) {
		return errors.New("checkpoint WAL identity or boundary mismatch")
	}
	// Check each bounded block's ordinal interval and reject gaps, duplicates and
	// overlap. Publication writes intervals consecutively in append order.
	var next uint64
	for _, id := range m.Segments {
		rows, e := l.readPublishedSegment(id)
		if e != nil {
			return e
		}
		if uint64(len(rows)) > m.NextOrdinal-next {
			return errSegment
		}
		seen := make([]bool, len(rows))
		for _, row := range rows {
			if row.Ordinal < next || row.Ordinal-next >= uint64(len(rows)) || seen[row.Ordinal-next] {
				return errSegment
			}
			seen[row.Ordinal-next] = true
		}
		next += uint64(len(rows))
	}
	if next != m.NextOrdinal {
		return errSegment
	}
	if err = l.scanTail(int64(m.CoveredOffset), m.NextOrdinal, nil); err != nil {
		return err
	}
	for _, id := range m.Segments {
		rows, e := l.readPublishedSegment(id)
		if e != nil {
			return e
		}
		for _, row := range rows {
			if e = visit(row); e != nil {
				return e
			}
		}
	}
	return l.scanTail(int64(m.CoveredOffset), m.NextOrdinal, visit)
}
func (l *Legacy) readPublishedSegment(id [16]byte) ([]BlockRow, error) {
	f, err := l.openFile(segmentName(id))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < int64(len(segmentMagic)+blockHeaderSize) || info.Size() > int64(len(segmentMagic)+MaxBlockSize) {
		return nil, errSegment
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(len(segmentMagic)+MaxBlockSize+1)))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != info.Size() || string(data[:len(segmentMagic)]) != segmentMagic {
		return nil, errSegment
	}
	rows, err := DecodeBlock(data[len(segmentMagic):])
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(rows); i++ {
		if !segmentRowLess(rows[i-1], rows[i]) {
			return nil, errSegment
		}
	}
	return rows, nil
}
func (l *Legacy) scanTail(pos int64, ordinal uint64, visit func(BlockRow) error) error {
	var h [16]byte
	var payload []byte
	for pos < l.end {
		if l.end-pos < 16 {
			return errors.New("incomplete committed frame")
		}
		if _, err := l.wal.ReadAt(h[:], pos); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(h[:4])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:8]) != ^n || crc32.Checksum(h[:12], castagnoli) != binary.LittleEndian.Uint32(h[12:]) || int64(n) > l.end-pos-16 {
			return fmt.Errorf("invalid committed frame at %d", pos)
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		} else {
			payload = payload[:n]
		}
		if _, err := l.wal.ReadAt(payload, pos+16); err != nil {
			return err
		}
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(h[8:12]) {
			return errors.New("invalid committed checksum")
		}
		rows, err := store.Decode(payload)
		if err != nil {
			return err
		}
		if uint64(len(rows)) > ^uint64(0)-ordinal {
			return errors.New("ordinal overflow")
		}
		if visit != nil {
			for i, row := range rows {
				if err = visit(BlockRow{Row: row, Ordinal: ordinal + uint64(i)}); err != nil {
					return err
				}
			}
		}
		ordinal += uint64(len(rows))
		pos += 16 + int64(n)
	}
	return nil
}
