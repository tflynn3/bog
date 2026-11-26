package checkpoint

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"bog/internal/store"
)

// A BOGQSG01 segment is an eight-byte magic followed by exactly one version 1
// block. The block supplies little-endian lengths, time bounds and CRC32C for
// both its header and payload. Segment names are zero-based staging sequence
// numbers; they do not imply time order or publication.
const segmentMagic = "BOGQSG01"

var errSegment = errors.New("invalid checkpoint segment")

// StageSegments scans l serially while the caller holds its directory lock.
// It returns a newly created private scratch directory and its segment count.
// The caller owns that directory and must remove it when finished. It is not a
// published checkpoint. On any failure the directory and all output are removed.
//
// maxBlockBytes caps each encoded block, including its header, and must be at
// most MaxBlockSize. A row that cannot fit is an error; use MaxBlockSize to
// accept every row representable by the existing block codec. Working memory is
// bounded by one legacy frame and its decoded rows, one bounded row buffer and
// one encoded block; it does not grow with WAL length or segment count.
func StageSegments(l *Legacy, scratchParent string, maxBlockBytes int) (dir string, count uint64, err error) {
	if l == nil || maxBlockBytes < blockHeaderSize+blockRowSize || maxBlockBytes > MaxBlockSize {
		return "", 0, errors.New("invalid segment staging parameters")
	}
	dir, err = os.MkdirTemp(scratchParent, ".bog-segments-")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		if err != nil {
			if cleanupErr := os.RemoveAll(dir); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("remove staging directory: %w", cleanupErr))
			}
			dir, count = "", 0
		}
	}()
	var buffer []BlockRow
	size := blockHeaderSize
	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		sort.Slice(buffer, func(i, j int) bool { return segmentRowLess(buffer[i], buffer[j]) })
		block, e := EncodeBlock(buffer)
		if e != nil {
			return e
		}
		name := filepath.Join(dir, fmt.Sprintf("%020d.seg", count))
		f, e := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return e
		}
		_, e = io.WriteString(f, segmentMagic)
		if e == nil {
			_, e = f.Write(block)
		}
		if closeErr := f.Close(); e == nil {
			e = closeErr
		}
		if e != nil {
			return e
		}
		count++
		clear(buffer)
		buffer = buffer[:0]
		size = blockHeaderSize
		return nil
	}
	err = l.Scan(func(_, _ int64, first uint64, rows []store.Row) error {
		for i, row := range rows {
			if len(row.Line) > maxBlockBytes-blockHeaderSize-blockRowSize {
				return errors.New("row exceeds segment block limit")
			}
			rowSize := blockRowSize + len(row.Line)
			if rowSize > maxBlockBytes-size {
				if e := flush(); e != nil {
					return e
				}
			}
			buffer = append(buffer, BlockRow{Row: row, Ordinal: first + uint64(i)})
			size += rowSize
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	return dir, count, err
}

func segmentRowLess(a, b BlockRow) bool {
	if a.Row.TS.Equal(b.Row.TS) {
		return a.Ordinal < b.Ordinal
	}
	return a.Row.TS.Before(b.Row.TS)
}

// readSegment validates the complete file, including ordering, before returning
// rows. The size check precedes allocation and a bounded read detects growth.
func readSegment(path string) ([]BlockRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < int64(len(segmentMagic)+blockHeaderSize) || info.Size() > int64(len(segmentMagic)+MaxBlockSize) {
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
