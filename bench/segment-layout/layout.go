package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"bog/internal/store"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	magic       = "BOGLAY01"
	headerSize  = 18 // magic, codec, summary flag, block count, CRC32C
	entrySize   = 36 // checkpoint time bounds, raw and stored lengths, CRC32C
	blockHeader = 44
	maxBlock    = store.MaxBatch + 68
	zstdWindow  = 1 << 20
)

type codec byte

const (
	rawCodec codec = iota
	lz4Codec
	zstdCodec
)

type layout struct {
	codec
	prune bool
}

var layouts = []layout{
	{rawCodec, false}, {lz4Codec, false}, {zstdCodec, false},
	{rawCodec, true}, {lz4Codec, true}, {zstdCodec, true},
}

func (l layout) name() string {
	name := [...]string{"raw", "lz4", "zstd"}[l.codec]
	if l.prune {
		name += "+summary"
	}
	return name
}

func (l layout) indexWidth() int {
	if l.prune {
		return entrySize + len(summary{})
	}
	return entrySize
}

var le = binary.LittleEndian
var errLayout = errors.New("invalid benchmark layout")
var firstTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
var endTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

func seal(b []byte) { le.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], crcTable)) }

func checkedRead(r io.ReaderAt, b []byte, offset int64) error {
	if _, err := r.ReadAt(b, offset); err != nil {
		return err
	}
	if crc32.Checksum(b[:len(b)-4], crcTable) != le.Uint32(b[len(b)-4:]) {
		return errLayout
	}
	return nil
}

func writeLayout(w io.WriterAt, l layout, count int, block func(int) ([]byte, summary, error)) (err error) {
	if count < 0 || uint64(count) > 1<<32-1 || l.codec > zstdCodec {
		return errLayout
	}
	var enc *zstd.Encoder
	if l.codec == zstdCodec {
		enc, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(zstdWindow))
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, enc.Close()) }()
	}
	var lz lz4.Compressor
	var scratch []byte
	header := [headerSize]byte{}
	copy(header[:], magic)
	header[8] = byte(l.codec)
	if l.prune {
		header[9] = 1
	}
	le.PutUint32(header[10:], uint32(count))
	seal(header[:])
	if _, err := w.WriteAt(header[:], 0); err != nil {
		return err
	}

	width := l.indexWidth()
	entry := make([]byte, width)
	offset := int64(headerSize) + int64(count)*int64(width)
	for i := range count {
		raw, filter, err := block(i)
		if err != nil {
			return err
		}
		if len(raw) < blockHeader || len(raw) > maxBlock {
			return errLayout
		}
		stored := raw
		switch l.codec {
		case lz4Codec:
			n := lz4.CompressBlockBound(len(raw))
			if cap(scratch) < n {
				scratch = make([]byte, n)
			}
			n, err := lz.CompressBlock(raw, scratch[:n])
			if err != nil {
				return err
			}
			if n > 0 && n < len(raw) {
				stored = scratch[:n]
			}
		case zstdCodec:
			scratch = enc.EncodeAll(raw, scratch[:0])
			if len(scratch) < len(raw) {
				stored = scratch
			}
		}
		copy(entry[:24], raw[12:36])
		le.PutUint32(entry[24:], uint32(len(raw)))
		le.PutUint32(entry[28:], uint32(len(stored)))
		if l.prune {
			copy(entry[32:], filter[:])
		}
		seal(entry)
		if _, err := w.WriteAt(entry, int64(headerSize)+int64(i)*int64(width)); err != nil {
			return err
		}
		if _, err := w.WriteAt(stored, offset); err != nil {
			return err
		}
		offset += int64(len(stored))
	}
	return nil
}

type query struct {
	name, literal string
	start, end    time.Time
}

func indexTime(b []byte) (time.Time, bool) {
	sec, ns := int64(le.Uint64(b)), le.Uint32(b[8:])
	if sec < firstTime.Unix() || sec > endTime.Unix() || ns >= 1e9 {
		return time.Time{}, false
	}
	t := time.Unix(sec, int64(ns)).UTC()
	return t, t.Before(endTime)
}

func scanBlocks(r io.ReaderAt, size int64, q query, visit func(raw []byte, bufferBytes int) error) (read int, err error) {
	var header [headerSize]byte
	if err := checkedRead(r, header[:], 0); err != nil {
		return 0, fmt.Errorf("header: %w", err)
	}
	l := layout{codec(header[8]), header[9] == 1}
	count := int64(le.Uint32(header[10:]))
	width := int64(l.indexWidth())
	offset := headerSize + count*width
	if string(header[:8]) != magic || l.codec > zstdCodec || header[9] > 1 || offset > size {
		return 0, errLayout
	}
	var dec *zstd.Decoder
	entry := make([]byte, width)
	for i := range count {
		if err := checkedRead(r, entry, headerSize+i*width); err != nil {
			return 0, fmt.Errorf("index entry %d: %w", i, err)
		}
		rawSize, storedSize := int(le.Uint32(entry[24:])), int(le.Uint32(entry[28:]))
		lo, loOK := indexTime(entry[:12])
		hi, hiOK := indexTime(entry[12:24])
		if rawSize < blockHeader || rawSize > maxBlock || storedSize <= 0 || storedSize > rawSize ||
			int64(storedSize) > size-offset || (l.codec == rawCodec && storedSize != rawSize) ||
			!loOK || !hiOK || hi.Before(lo) {
			return 0, fmt.Errorf("index entry %d: %w", i, errLayout)
		}
		bodyOffset := offset
		offset += int64(storedSize)
		if hi.Before(q.start) || !lo.Before(q.end) {
			continue
		}
		if l.prune && !(*summary)(entry[32:32+len(summary{})]).mayContain(q.literal) {
			continue
		}
		stored := make([]byte, storedSize)
		if _, err := r.ReadAt(stored, bodyOffset); err != nil {
			return 0, fmt.Errorf("block %d: %w", i, err)
		}
		raw, bufferBytes := stored, storedSize
		if storedSize < rawSize {
			raw = make([]byte, rawSize)
			bufferBytes += rawSize
			switch l.codec {
			case lz4Codec:
				n, err := lz4.UncompressBlock(stored, raw)
				if err != nil || n != rawSize {
					return 0, fmt.Errorf("lz4 block %d: %w", i, errors.Join(errLayout, err))
				}
			case zstdCodec:
				if dec == nil {
					dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(zstdWindow),
						zstd.WithDecoderMaxMemory(maxBlock), zstd.WithDecodeAllCapLimit(true))
					if err != nil {
						return 0, err
					}
					defer dec.Close()
				}
				raw, err = dec.DecodeAll(stored, raw[:0])
				if err != nil || len(raw) != rawSize {
					return 0, fmt.Errorf("zstd block %d: %w", i, errors.Join(errLayout, err))
				}
			}
		}
		if !bytes.Equal(raw[12:36], entry[:24]) {
			return 0, fmt.Errorf("block %d time bounds: %w", i, errLayout)
		}
		if err := visit(raw, bufferBytes); err != nil {
			return 0, fmt.Errorf("block %d: %w", i, err)
		}
		read++
	}
	if offset != size {
		return 0, errLayout
	}
	return read, nil
}
