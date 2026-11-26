package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"time"

	"bog/internal/checkpoint"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	magic            = "BOGLAY01"
	fileHeaderSize   = 20
	blockHeaderSize  = 40
	summaryBytes     = 2048
	checkpointHeader = 44 // checkpoint version 1
	checkpointRow    = 24
	targetBlockBytes = 256 << 10
)

var le = binary.LittleEndian
var crcTable = crc32.MakeTable(crc32.Castagnoli)
var errLayout = errors.New("invalid benchmark layout")

type compression byte

const (
	raw compression = iota
	lz4Block
	zstdBlock
)

type layout struct {
	codec   compression
	summary bool
}

func (l layout) name() string {
	name := [...]string{"raw", "lz4", "zstd"}[l.codec]
	if l.summary {
		name += "+summary"
	}
	return name
}

type summary [summaryBytes]byte

// Use byte windows so matching has the same semantics as strings.Contains,
// even when a four-byte window starts or ends inside a multibyte rune.
func (s *summary) grams(text string, add bool) bool {
	for i := 0; i+4 <= len(text); i++ {
		x := uint32(text[i]) | uint32(text[i+1])<<8 | uint32(text[i+2])<<16 | uint32(text[i+3])<<24
		x ^= x >> 16
		x *= 0x7feb352d
		x ^= x >> 15
		for _, hash := range [2]uint32{x, x >> 16} {
			bit := hash & (summaryBytes*8 - 1)
			mask := byte(1 << (bit % 8))
			if add {
				s[bit/8] |= mask
			} else if s[bit/8]&mask == 0 {
				return false
			}
		}
	}
	return true
}

func checksum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

func packFile(path string, l layout, blocks [][]checkpoint.BlockRow) (written int64, err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	write := func(data []byte) error {
		n, err := f.Write(data)
		written += int64(n)
		return err // os.File.Write reports a short write as an error.
	}
	var z *zstd.Encoder
	if l.codec == zstdBlock {
		z, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return 0, err
		}
		defer func() { err = errors.Join(err, z.Close()) }()
	}
	var fileHeader [fileHeaderSize]byte
	copy(fileHeader[:], magic)
	le.PutUint32(fileHeader[8:], uint32(len(blocks)))
	fileHeader[12] = byte(l.codec)
	if l.summary {
		fileHeader[13] = 1
	}
	le.PutUint32(fileHeader[16:], checksum(fileHeader[:16]))
	if err := write(fileHeader[:]); err != nil {
		return written, err
	}
	var compressor lz4.Compressor
	for _, rows := range blocks {
		block, err := checkpoint.EncodeBlock(rows)
		if err != nil {
			return written, err
		}
		payload := block
		switch l.codec {
		case lz4Block:
			buf := make([]byte, lz4.CompressBlockBound(len(block)))
			n, err := compressor.CompressBlock(block, buf)
			if err != nil {
				return written, err
			}
			if n > 0 && n < len(block) {
				payload = buf[:n]
			}
		case zstdBlock:
			if compressed := z.EncodeAll(block, nil); len(compressed) < len(block) {
				payload = compressed
			}
		}
		var record [blockHeaderSize + summaryBytes]byte
		copy(record[:24], block[12:36]) // exact inclusive bounds from the base codec
		le.PutUint32(record[24:], uint32(len(block)))
		le.PutUint32(record[28:], uint32(len(payload)))
		le.PutUint32(record[32:], checksum(payload))
		meta := record[:blockHeaderSize]
		if l.summary {
			meta = record[:]
			filter := (*summary)(meta[blockHeaderSize:])
			for _, row := range rows {
				filter.grams(row.Row.Line, true)
			}
		}
		le.PutUint32(meta[36:], crc32.Update(checksum(meta[:36]), crcTable, meta[blockHeaderSize:]))
		if err := write(meta); err != nil {
			return written, err
		}
		if err := write(payload); err != nil {
			return written, err
		}
	}
	return written, f.Sync()
}

func readBlock(r io.ReaderAt, offset int64, stored, size uint32, crc uint32, codec compression, z **zstd.Decoder) ([]checkpoint.BlockRow, error) {
	payload := make([]byte, stored)
	if _, err := r.ReadAt(payload, offset); err != nil {
		return nil, err
	}
	if checksum(payload) != crc {
		return nil, errLayout
	}
	if stored != size {
		buf := make([]byte, size)
		switch codec {
		case lz4Block:
			n, err := lz4.UncompressBlock(payload, buf)
			if err != nil || n != len(buf) {
				return nil, fmt.Errorf("lz4 size %d, expected %d: %w", n, size, errors.Join(errLayout, err))
			}
		case zstdBlock:
			if *z == nil {
				decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
					zstd.WithDecoderMaxMemory(checkpoint.MaxBlockSize), zstd.WithDecodeAllCapLimit(true))
				if err != nil {
					return nil, err
				}
				*z = decoder
			}
			decoded, err := (*z).DecodeAll(payload, buf[:0])
			if err != nil || len(decoded) != len(buf) {
				return nil, fmt.Errorf("zstd size %d, expected %d: %w", len(decoded), size, errors.Join(errLayout, err))
			}
			buf = decoded
		default:
			return nil, errLayout
		}
		payload = buf
	}
	return checkpoint.DecodeBlock(payload)
}

type queryStats struct {
	BlocksRead int    `json:"blocks_read"`
	Rows       int    `json:"rows"`
	PeakDecode uint64 `json:"peak_decode_bytes"`
}

func readTime(b []byte) (time.Time, bool) {
	sec, ns := int64(le.Uint64(b)), le.Uint32(b[8:])
	if sec < minTime.Unix() || sec > endTime.Unix() || ns >= 1e9 {
		return time.Time{}, false
	}
	t := time.Unix(sec, int64(ns)).UTC()
	return t, t.Before(endTime)
}

func scan(r io.ReaderAt, length int64, q query, measure bool, visit func(checkpoint.BlockRow) error) (stats queryStats, err error) {
	if length < fileHeaderSize {
		return stats, errLayout
	}
	var header [fileHeaderSize]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return stats, err
	}
	codec, summaries := compression(header[12]), header[13]
	if string(header[:8]) != magic || codec > zstdBlock || summaries > 1 || le.Uint16(header[14:]) != 0 ||
		checksum(header[:16]) != le.Uint32(header[16:]) {
		return stats, errLayout
	}
	var record [blockHeaderSize + summaryBytes]byte
	meta := record[:blockHeaderSize+int(summaries)*summaryBytes]
	count := le.Uint32(header[8:])
	offset := int64(fileHeaderSize)
	if uint64(count)*uint64(len(meta)+1) > uint64(length-offset) {
		return stats, errLayout
	}
	var decoder *zstd.Decoder
	defer func() {
		if decoder != nil {
			decoder.Close()
		}
	}()
	for i := uint32(0); i < count; i++ {
		if int64(len(meta)) > length-offset {
			return stats, errLayout
		}
		if _, err := r.ReadAt(meta, offset); err != nil {
			return stats, err
		}
		offset += int64(len(meta))
		if crc32.Update(checksum(meta[:36]), crcTable, meta[blockHeaderSize:]) != le.Uint32(meta[36:]) {
			return stats, errLayout
		}
		lo, loOK := readTime(meta[:12])
		hi, hiOK := readTime(meta[12:24])
		size, stored := le.Uint32(meta[24:]), le.Uint32(meta[28:])
		if !loOK || !hiOK || hi.Before(lo) || size < checkpointHeader+checkpointRow || size > checkpoint.MaxBlockSize ||
			stored == 0 || stored > size || (codec == raw && stored != size) || int64(stored) > length-offset {
			return stats, errLayout
		}
		if lo.Before(q.end) && !hi.Before(q.start) &&
			(summaries == 0 || (*summary)(meta[blockHeaderSize:]).grams(q.literal, false)) {
			var before, after runtime.MemStats
			if measure {
				runtime.ReadMemStats(&before)
			}
			rows, err := readBlock(r, offset, stored, size, le.Uint32(meta[32:]), codec, &decoder)
			if measure {
				// A fresh decoder includes its entire workspace in this sample.
				if decoder != nil {
					decoder.Close()
					decoder = nil
				}
				runtime.ReadMemStats(&after)
				stats.PeakDecode = max(stats.PeakDecode, after.TotalAlloc-before.TotalAlloc)
			}
			if err != nil {
				return stats, fmt.Errorf("block %d: %w", i, err)
			}
			stats.BlocksRead++
			for _, row := range rows {
				if q.matches(row.Row) {
					stats.Rows++
					if visit != nil {
						if err := visit(row); err != nil {
							return stats, err
						}
					}
				}
			}
		}
		offset += int64(stored)
	}
	if offset != length {
		return stats, errLayout
	}
	return stats, nil
}
