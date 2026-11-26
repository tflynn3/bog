package checkpoint

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

// MaxBlockSize includes the header and permits a line as large as a legacy batch.
const MaxBlockSize = store.MaxBatch + 68
const blockHeaderSize = 44
const blockRowSize = 24

// BlockRow carries the original zero-based append identity alongside a row.
type BlockRow struct {
	Row     store.Row
	Ordinal uint64
}

var errBlock = errors.New("invalid checkpoint block")
var minBlockTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
var maxBlockTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

func validBlockTime(t time.Time) bool {
	return !t.Before(minBlockTime) && t.Before(maxBlockTime)
}

// EncodeBlock encodes rows in their supplied order; it does not sort or deduplicate.
func EncodeBlock(rows []BlockRow) ([]byte, error) {
	if len(rows) == 0 || len(rows) > (MaxBlockSize-blockHeaderSize)/blockRowSize {
		return nil, errBlock
	}
	size := blockHeaderSize
	lo, hi := rows[0].Row.TS, rows[0].Row.TS
	for _, r := range rows {
		if !validBlockTime(r.Row.TS) || !utf8.ValidString(r.Row.Line) || len(r.Row.Line) > MaxBlockSize-size-blockRowSize {
			return nil, errBlock
		}
		size += blockRowSize + len(r.Row.Line)
		if r.Row.TS.Before(lo) {
			lo = r.Row.TS
		}
		if r.Row.TS.After(hi) {
			hi = r.Row.TS
		}
	}
	b := make([]byte, size)
	binary.LittleEndian.PutUint32(b[0:4], 1)
	binary.LittleEndian.PutUint32(b[4:8], uint32(len(rows)))
	binary.LittleEndian.PutUint32(b[8:12], uint32(size-blockHeaderSize))
	putBlockTime(b[12:24], lo)
	putBlockTime(b[24:36], hi)
	p := blockHeaderSize
	for _, r := range rows {
		putBlockTime(b[p:p+12], r.Row.TS)
		binary.LittleEndian.PutUint64(b[p+12:p+20], r.Ordinal)
		binary.LittleEndian.PutUint32(b[p+20:p+24], uint32(len(r.Row.Line)))
		p += blockRowSize
		copy(b[p:], r.Row.Line)
		p += len(r.Row.Line)
	}
	binary.LittleEndian.PutUint32(b[36:40], crc32.Checksum(b[blockHeaderSize:], castagnoli))
	binary.LittleEndian.PutUint32(b[40:44], crc32.Checksum(b[:40], castagnoli))
	return b, nil
}

func putBlockTime(b []byte, t time.Time) {
	binary.LittleEndian.PutUint64(b, uint64(t.Unix()))
	binary.LittleEndian.PutUint32(b[8:], uint32(t.Nanosecond()))
}
func getBlockTime(b []byte) (time.Time, bool) {
	ns := binary.LittleEndian.Uint32(b[8:])
	if ns >= 1e9 {
		return time.Time{}, false
	}
	// Check seconds before constructing a time, avoiding time.Time's wider range.
	sec := int64(binary.LittleEndian.Uint64(b))
	if sec < minBlockTime.Unix() || sec > maxBlockTime.Unix() {
		return time.Time{}, false
	}
	t := time.Unix(sec, int64(ns)).UTC()
	return t, validBlockTime(t)
}

// DecodeBlock verifies the complete block before returning any rows.
func DecodeBlock(b []byte) ([]BlockRow, error) {
	if len(b) < blockHeaderSize || len(b) > MaxBlockSize || binary.LittleEndian.Uint32(b[:4]) != 1 || crc32.Checksum(b[:40], castagnoli) != binary.LittleEndian.Uint32(b[40:44]) {
		return nil, errBlock
	}
	count := binary.LittleEndian.Uint32(b[4:8])
	payload := binary.LittleEndian.Uint32(b[8:12])
	if count == 0 || uint64(payload) != uint64(len(b)-blockHeaderSize) || uint64(count)*blockRowSize > uint64(payload) || crc32.Checksum(b[blockHeaderSize:], castagnoli) != binary.LittleEndian.Uint32(b[36:40]) {
		return nil, errBlock
	}
	lo, ok := getBlockTime(b[12:24])
	if !ok {
		return nil, errBlock
	}
	hi, ok := getBlockTime(b[24:36])
	if !ok || hi.Before(lo) {
		return nil, errBlock
	}
	// Validate every field before allocating the result or exposing any row.
	p := blockHeaderSize
	var actualLo, actualHi time.Time
	for i := uint32(0); i < count; i++ {
		if len(b)-p < blockRowSize {
			return nil, errBlock
		}
		t, valid := getBlockTime(b[p : p+12])
		if !valid {
			return nil, errBlock
		}
		n := binary.LittleEndian.Uint32(b[p+20 : p+24])
		p += blockRowSize
		if uint64(n) > uint64(len(b)-p) || !utf8.Valid(b[p:p+int(n)]) {
			return nil, errBlock
		}
		p += int(n)
		if i == 0 || t.Before(actualLo) {
			actualLo = t
		}
		if i == 0 || t.After(actualHi) {
			actualHi = t
		}
	}
	if p != len(b) || !lo.Equal(actualLo) || !hi.Equal(actualHi) {
		return nil, errBlock
	}
	rows := make([]BlockRow, int(count))
	p = blockHeaderSize
	for i := range rows {
		t, _ := getBlockTime(b[p : p+12])
		ordinal := binary.LittleEndian.Uint64(b[p+12 : p+20])
		n := int(binary.LittleEndian.Uint32(b[p+20 : p+24]))
		p += blockRowSize
		rows[i] = BlockRow{Row: store.Row{TS: t, Line: string(b[p : p+n])}, Ordinal: ordinal}
		p += n
	}
	return rows, nil
}
