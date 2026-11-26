package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"slices"
	"sort"
	"unsafe"

	"bog/internal/checkpoint"
)

const (
	magic          = "BOGBEN01"
	fileHeader     = 20
	blockHeader    = 20
	summaryBudget  = 1024 // bytes, or 256 distinct four-byte windows
	noSummary      = ^uint32(0)
	maxStored      = checkpoint.MaxBlockSize + (64 << 10)
	checkpointHead = 44 // checkpoint block v1
	checkpointRow  = 24
)

type layout uint32

const (
	compressed layout = 1 << iota
	summarized
)

var layouts = [...]string{"raw", "compressed", "summary", "compressed+summary"}
var order = binary.LittleEndian
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// A nil summary means scan. An empty, non-nil summary proves no four-byte
// substring exists. Stop collecting as soon as the budget would be exceeded.
func summarize(rows []checkpoint.BlockRow) []byte {
	grams := make(map[uint32]struct{})
	for _, row := range rows {
		line := row.Row.Line
		for i := 0; i+4 <= len(line); i++ {
			key := gram(line[i : i+4])
			grams[key] = struct{}{}
			if len(grams) > summaryBudget/4 {
				return nil
			}
		}
	}
	keys := make([]uint32, 0, len(grams))
	for key := range grams {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	summary := make([]byte, 4*len(keys))
	for i, key := range keys {
		order.PutUint32(summary[4*i:], key)
	}
	return summary
}

func gram(s string) uint32 {
	return uint32(s[0]) | uint32(s[1])<<8 | uint32(s[2])<<16 | uint32(s[3])<<24
}

func mayContain(summary []byte, literal string) bool {
	if summary == nil || len(literal) < 4 {
		return true
	}
	n := len(summary) / 4
	for i := 0; i+4 <= len(literal); i++ {
		key := gram(literal[i : i+4])
		pos := sort.Search(n, func(j int) bool { return order.Uint32(summary[j*4:]) >= key })
		if pos == n || order.Uint32(summary[pos*4:]) != key {
			return false
		}
	}
	return true
}

func writeLayout(w io.Writer, kind layout, blocks [][]checkpoint.BlockRow) (int64, error) {
	if len(blocks) == 0 || uint64(len(blocks)) > math.MaxUint32 || kind > compressed|summarized {
		return 0, errors.New("invalid layout configuration")
	}
	var written int64
	write := func(b []byte) error {
		n, err := w.Write(b)
		written += int64(n)
		if err == nil && n != len(b) {
			return io.ErrShortWrite
		}
		return err
	}
	var header [fileHeader]byte
	copy(header[:], magic)
	order.PutUint32(header[8:], uint32(kind))
	order.PutUint32(header[12:], uint32(len(blocks)))
	order.PutUint32(header[16:], checksum(header[:16]))
	if err := write(header[:]); err != nil {
		return written, err
	}
	for i, rows := range blocks {
		if err := writeBlock(write, kind, rows); err != nil {
			return written, fmt.Errorf("write block %d: %w", i, err)
		}
	}
	return written, nil
}

func writeBlock(write func([]byte) error, kind layout, rows []checkpoint.BlockRow) error {
	raw, err := checkpoint.EncodeBlock(rows)
	if err != nil {
		return err
	}
	stored := raw
	if kind&compressed != 0 {
		var buf bytes.Buffer
		z := zlib.NewWriter(&buf)
		_, err = z.Write(raw)
		if err = errors.Join(err, z.Close()); err != nil {
			return err
		}
		stored = buf.Bytes()
		if len(stored) > maxStored {
			return errors.New("compressed block exceeds storage limit")
		}
	}
	var summary []byte
	if kind&summarized != 0 {
		summary = summarize(rows)
	}
	var hdr [blockHeader]byte
	order.PutUint32(hdr[:], uint32(len(stored)))
	order.PutUint32(hdr[4:], uint32(len(raw)))
	order.PutUint32(hdr[8:], noSummary)
	if summary != nil {
		order.PutUint32(hdr[8:], uint32(len(summary)))
	}
	order.PutUint32(hdr[12:], checksum(stored))
	order.PutUint32(hdr[16:], crc32.Update(checksum(hdr[:16]), crcTable, summary))
	for _, part := range [][]byte{hdr[:], summary, stored} {
		if len(part) != 0 {
			if err := write(part); err != nil {
				return err
			}
		}
	}
	return nil
}

type blockMeta struct {
	stored, raw, payloadCRC uint32
	summary                 []byte
}

// readMeta bounds the summary using fixed limits and the remaining file bytes
// before reading it. No field may authorize pruning until the CRC is checked.
func readMeta(r io.ReaderAt, offset, remaining int64, kind layout, scratch *[summaryBudget]byte) (blockMeta, int64, error) {
	var hdr [blockHeader]byte
	if remaining < blockHeader {
		return blockMeta{}, 0, io.ErrUnexpectedEOF
	}
	if _, err := r.ReadAt(hdr[:], offset); err != nil {
		return blockMeta{}, 0, err
	}
	meta := blockMeta{stored: order.Uint32(hdr[:]), raw: order.Uint32(hdr[4:]), payloadCRC: order.Uint32(hdr[12:])}
	n := order.Uint32(hdr[8:])
	if n != noSummary {
		if kind&summarized == 0 || n > summaryBudget || n%4 != 0 {
			return blockMeta{}, 0, errors.New("invalid summary length")
		}
		meta.summary = scratch[:n]
	} else {
		n = 0
	}
	if meta.raw < checkpointHead+checkpointRow || meta.raw > checkpoint.MaxBlockSize ||
		meta.stored == 0 || meta.stored > maxStored || (kind&compressed == 0 && meta.stored != meta.raw) ||
		int64(n)+int64(meta.stored) > remaining-blockHeader {
		return blockMeta{}, 0, errors.New("invalid block extent")
	}
	if n > 0 {
		if _, err := r.ReadAt(meta.summary, offset+blockHeader); err != nil {
			return blockMeta{}, 0, err
		}
	}
	if crc32.Update(checksum(hdr[:16]), crcTable, meta.summary) != order.Uint32(hdr[16:]) {
		return blockMeta{}, 0, errors.New("block metadata checksum mismatch")
	}
	for i := 4; i < len(meta.summary); i += 4 {
		if order.Uint32(meta.summary[i-4:]) >= order.Uint32(meta.summary[i:]) {
			return blockMeta{}, 0, errors.New("invalid summary order")
		}
	}
	return meta, offset + blockHeader + int64(n), nil
}

func decompress(stored []byte, size uint32) (raw []byte, err error) {
	if size < checkpointHead+checkpointRow || size > checkpoint.MaxBlockSize {
		return nil, errors.New("invalid decompressed length")
	}
	input := bytes.NewReader(stored)
	z, err := zlib.NewReader(input)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, z.Close()) }()
	raw = make([]byte, size)
	if _, err := io.ReadFull(z, raw); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := io.ReadFull(z, extra[:]); n != 0 || err != io.EOF || input.Len() != 0 {
		return nil, fmt.Errorf("invalid zlib stream end (extra output %d, trailing input %d, read error %v)", n, input.Len(), err)
	}
	return raw, nil
}

func readBlock(r io.ReaderAt, offset int64, meta blockMeta, kind layout) ([]checkpoint.BlockRow, int64, error) {
	stored := make([]byte, meta.stored)
	if _, err := r.ReadAt(stored, offset); err != nil {
		return nil, 0, err
	}
	if checksum(stored) != meta.payloadCRC {
		return nil, 0, errors.New("stored block checksum mismatch")
	}
	raw := stored
	peak := int64(len(stored))
	if kind&compressed != 0 {
		var err error
		raw, err = decompress(stored, meta.raw)
		if err != nil {
			return nil, 0, err
		}
		peak += int64(len(raw))
	}
	rows, err := checkpoint.DecodeBlock(raw)
	if err != nil {
		return nil, 0, err
	}
	peak += int64(len(rows)) * int64(unsafe.Sizeof(checkpoint.BlockRow{}))
	for _, row := range rows {
		peak += int64(len(row.Row.Line))
	}
	return rows, peak, nil
}

type readStats struct {
	BlocksRead int   `json:"blocks_read"`
	PeakDecode int64 `json:"peak_tracked_decode_bytes"`
}

// scan opens no other files and holds one block and one bounded summary at a time,
// apart from any results independently retained by its visitor.
func scan(f *os.File, kind layout, literal string, verify bool,
	visit func(int, []checkpoint.BlockRow) error) (readStats, error) {
	var stats readStats
	st, err := f.Stat()
	if err != nil {
		return stats, err
	}
	var header [fileHeader]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		return stats, err
	}
	count := int64(order.Uint32(header[12:]))
	if string(header[:8]) != magic || order.Uint32(header[8:]) != uint32(kind) ||
		checksum(header[:16]) != order.Uint32(header[16:]) ||
		count == 0 || count > (st.Size()-fileHeader)/(blockHeader+1) {
		return stats, errors.New("invalid layout header")
	}
	offset := int64(fileHeader)
	var scratch [summaryBudget]byte
	for i := int64(0); i < count; i++ {
		meta, payload, err := readMeta(f, offset, st.Size()-offset, kind, &scratch)
		if err != nil {
			return stats, fmt.Errorf("block %d metadata: %w", i, err)
		}
		offset = payload + int64(meta.stored)
		if !verify && !mayContain(meta.summary, literal) {
			continue
		}
		rows, peak, err := readBlock(f, payload, meta, kind)
		if err != nil {
			return stats, fmt.Errorf("block %d payload: %w", i, err)
		}
		stats.BlocksRead++
		stats.PeakDecode = max(stats.PeakDecode, peak)
		if verify && kind&summarized != 0 {
			actual := summarize(rows)
			if (actual == nil) != (meta.summary == nil) || !bytes.Equal(actual, meta.summary) {
				return stats, fmt.Errorf("block %d summary does not match payload", i)
			}
		}
		if err := visit(int(i), rows); err != nil {
			return stats, err
		}
	}
	if offset != st.Size() {
		return stats, errors.New("unexpected trailing layout bytes")
	}
	return stats, nil
}
