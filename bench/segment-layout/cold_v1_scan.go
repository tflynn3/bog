package main

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"bog/internal/checkpoint"
)

// Mirror history's v1 seek and read schedule in an accounting pass. The
// production reader still supplies the timed scan and its I/O counters.
const (
	coldV1PageBytes  = 4 << 10
	coldV1SeekPoints = 128
)

type coldV1Buffers struct {
	live, peak int
	readBytes  int64
}

func (m *coldV1Buffers) read(path string, offset int64, buf *[]byte, size int) error {
	if old := cap(*buf); old < size {
		*buf = make([]byte, size)
		m.peak = max(m.peak, m.live+cap(*buf))
		m.live += cap(*buf) - old
	}
	*buf = (*buf)[:size]
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	n, err := f.ReadAt(*buf, offset)
	m.readBytes += int64(n)
	return errors.Join(err, f.Close())
}

type coldV1Cursor struct {
	path         string
	size, offset int64
	ts           time.Time
	ordinal      uint64
	lineSize     int
	pageOffset   int64
	page         []byte
}

func (c *coldV1Cursor) window(offset int64, size int) []byte {
	start := offset - c.pageOffset
	if start < 0 || start+int64(size) > int64(len(c.page)) {
		return nil
	}
	return c.page[start : start+int64(size)]
}

func (c *coldV1Cursor) advance(ctx context.Context, m *coldV1Buffers, end time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if c.offset == c.size {
		return false, nil
	}
	if c.size-c.offset < blockRowWidth {
		return false, io.ErrUnexpectedEOF
	}
	header := c.window(c.offset, blockRowWidth)
	if header == nil {
		c.pageOffset = c.offset
		if err := m.read(c.path, c.offset, &c.page, int(min(coldV1PageBytes, c.size-c.offset))); err != nil {
			return false, err
		}
		header = c.page[:blockRowWidth]
	}
	c.ts = coldV1Time(header)
	c.ordinal = le.Uint64(header[12:20])
	c.lineSize = int(le.Uint32(header[20:24]))
	if int64(c.lineSize) > c.size-c.offset-blockRowWidth {
		return false, io.ErrUnexpectedEOF
	}
	return c.ts.Before(end), nil
}

func coldV1Time(b []byte) time.Time {
	return time.Unix(int64(le.Uint64(b[:8])), int64(le.Uint32(b[8:12]))).UTC()
}

// Read the seek points from the validated immutable segment. The scratch
// buffer belongs to index setup and is outside the query buffer accounting.
func coldV1SeekOffset(ctx context.Context, data []byte, start time.Time) (int64, error) {
	count := int(le.Uint32(data[4:8]))
	if count < 1 || count > (len(data)-blockHeader)/blockRowWidth {
		return 0, errors.New("invalid v1 segment row count")
	}
	step := (count + coldV1SeekPoints - 1) / coldV1SeekPoints
	offset, previous := blockHeader, blockHeader
	for i := 0; i < count; i++ {
		if i%step == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if len(data)-offset < blockRowWidth {
				return 0, io.ErrUnexpectedEOF
			}
			if !coldV1Time(data[offset:]).Before(start) {
				break
			}
			previous = offset
		}
		if len(data)-offset < blockRowWidth {
			return 0, io.ErrUnexpectedEOF
		}
		offset += blockRowWidth + int(le.Uint32(data[offset+20:offset+24]))
	}
	return int64(previous), nil
}

func measureColdV1(ctx context.Context, dir string, segments []checkpoint.Segment, q query, want []checkpoint.BlockRow) (coldV1Buffers, error) {
	var m coldV1Buffers
	var cursors coldV1Heap
	var scratch []byte
	for _, seg := range segments {
		if err := ctx.Err(); err != nil {
			return coldV1Buffers{}, err
		}
		if seg.Size < blockHeader+blockRowWidth || seg.Size > checkpoint.MaxBlockSize {
			return coldV1Buffers{}, errors.New("invalid v1 segment size")
		}
		path := filepath.Join(dir, seg.Name)
		// Use one scratch block for the entire index pass.
		f, err := os.Open(path)
		if err != nil {
			return coldV1Buffers{}, err
		}
		if cap(scratch) < int(seg.Size) {
			scratch = make([]byte, seg.Size)
		}
		scratch = scratch[:seg.Size]
		_, err = io.ReadFull(f, scratch)
		if err := errors.Join(err, f.Close()); err != nil {
			return coldV1Buffers{}, err
		}
		if coldV1Time(scratch[24:36]).Before(q.start) || !coldV1Time(scratch[12:24]).Before(q.end) {
			continue
		}
		offset, err := coldV1SeekOffset(ctx, scratch, q.start)
		if err != nil {
			return coldV1Buffers{}, err
		}
		c := &coldV1Cursor{path: path, size: seg.Size, offset: offset}
		for {
			ready, err := c.advance(ctx, &m, q.end)
			if err != nil {
				return coldV1Buffers{}, err
			}
			if !ready {
				m.live -= cap(c.page)
				break
			}
			if !c.ts.Before(q.start) {
				cursors = append(cursors, c)
				break
			}
			c.offset += int64(blockRowWidth + c.lineSize)
		}
	}
	heap.Init(&cursors)
	needle := []byte(q.literal)
	var overflow []byte
	for len(cursors) > 0 {
		if err := ctx.Err(); err != nil {
			return coldV1Buffers{}, err
		}
		c := cursors[0]
		lineOffset := c.offset + blockRowWidth
		line := c.window(lineOffset, c.lineSize)
		if line == nil {
			if err := m.read(c.path, lineOffset, &overflow, c.lineSize); err != nil {
				return coldV1Buffers{}, err
			}
			line = overflow
		}
		if bytes.Contains(line, needle) {
			if len(want) == 0 || !c.ts.Equal(want[0].Row.TS) || c.ordinal != want[0].Ordinal || string(line) != want[0].Row.Line {
				return coldV1Buffers{}, errors.New("v1 buffer accounting disagrees with production rows")
			}
			want = want[1:]
		}
		c.offset = lineOffset + int64(c.lineSize)
		ready, err := c.advance(ctx, &m, q.end)
		if err != nil {
			return coldV1Buffers{}, err
		}
		if ready {
			heap.Fix(&cursors, 0)
		} else {
			m.live -= cap(c.page)
			heap.Pop(&cursors)
		}
	}
	if err := ctx.Err(); err != nil {
		return coldV1Buffers{}, err
	}
	if len(want) != 0 {
		return coldV1Buffers{}, fmt.Errorf("v1 buffer accounting omitted %d production rows", len(want))
	}
	return m, nil
}

type coldV1Heap []*coldV1Cursor

func (h coldV1Heap) Len() int      { return len(h) }
func (h coldV1Heap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h coldV1Heap) Less(i, j int) bool {
	if byTime := h[i].ts.Compare(h[j].ts); byTime != 0 {
		return byTime < 0
	}
	return h[i].ordinal < h[j].ordinal
}
func (h *coldV1Heap) Push(x any) { *h = append(*h, x.(*coldV1Cursor)) }
func (h *coldV1Heap) Pop() any {
	last := len(*h) - 1
	cursor := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return cursor
}
