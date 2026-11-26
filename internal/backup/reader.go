package backup

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"bog/internal/store"
)

const (
	// MaxPageSize matches the live query API's page limit.
	MaxPageSize  = 1000
	readPageSize = 4096
)

// Position orders equal timestamps by their original append ordinals.
type Position struct {
	Time    time.Time
	Ordinal uint64
}

func (p Position) less(q Position) bool {
	return p.Time.Before(q.Time) || (p.Time.Equal(q.Time) && p.Ordinal < q.Ordinal)
}

// Row retains both the decoded row and its append identity.
type Row struct {
	store.Row
	Ordinal uint64
}

func (r Row) Position() Position { return Position{r.TS, r.Ordinal} }

// Query describes a half-open range and an exact case-sensitive substring.
// After is an optional exclusive lower bound into the same recovery point.
type Query struct {
	Start, End time.Time
	Contains   string
	After      *Position
}

func (q Query) includes(p Position) bool {
	return !p.Time.Before(q.Start) && p.Time.Before(q.End) && (q.After == nil || q.After.less(p))
}

// Reader serves a verified restore image. Keep its files immutable until Close.
// Separate queries and replays can run concurrently.
type Reader struct {
	root   *os.Root
	image  image
	bounds []bounds
}

// OpenRestored validates the entire image before admitting queries or replay.
func OpenRestored(ctx context.Context, directory string) (*Reader, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	img, bounds, err := verifyImage(ctx, root)
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return &Reader{root, img, bounds}, nil
}

func (r *Reader) Point() Point { return r.image.Point }

// Close releases the directory. Complete active operations before calling Close.
func (r *Reader) Close() error { return r.root.Close() }

// ReplayTail calls visit in append order once per complete WAL frame, starting
// at Point().NextOrdinal. A visitor error stops replay.
func (r *Reader) ReplayTail(ctx context.Context, visit func(firstOrdinal uint64, rows []store.Row) error) error {
	if visit == nil {
		return readTail(ctx, r.root, r.image.metadata, nil)
	}
	return readTail(ctx, r.root, r.image.metadata, func(_ int64, first uint64, rows []store.Row) error {
		return visit(first, rows)
	})
}

// QueryPage returns up to limit matching rows and, if more rows exist, an
// exclusive continuation. The reader itself pins the query's recovery point.
func (r *Reader) QueryPage(ctx context.Context, q Query, limit int) (rows []Row, next *Position, err error) {
	if limit < 1 || limit > MaxPageSize || q.End.Before(q.Start) {
		return nil, nil, errors.New("invalid backup query range or page size")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if q.Start.Equal(q.End) {
		return nil, nil, nil
	}
	f, err := openTail(r.root, r.image.metadata)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			rows, next, err = nil, nil, errors.Join(err, closeErr)
		}
	}()
	tail, err := r.tailPage(ctx, f, q, limit+1)
	if err != nil {
		return nil, nil, err
	}
	runs := make(segmentHeap, 0, len(r.image.Segments))
	for i, s := range r.image.Segments {
		bounds := r.bounds[i]
		if bounds.max.Before(q.Start) || !bounds.min.Before(q.End) ||
			(q.After != nil && bounds.max.Before(q.After.Time)) {
			continue
		}
		cursor := &segmentCursor{segment: s, next: blockHeader}
		if err := cursor.advance(ctx, r.root, q); err != nil {
			return nil, nil, err
		}
		if cursor.valid {
			runs = append(runs, cursor)
		}
	}
	heap.Init(&runs)
	rows = make([]Row, 0, limit)
	var decoder walDecoder
	var frameOffset int64
	var frameRows []store.Row
	for len(runs) > 0 || len(tail) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if len(runs) == 0 || (len(tail) > 0 && tail[0].key.less(runs[0].key)) {
			if len(rows) == limit {
				break
			}
			ref := tail[0]
			if frameRows == nil || frameOffset != ref.offset {
				frameRows = nil
				frameOffset = ref.offset
				section := io.NewSectionReader(f, frameOffset-r.image.Point.CoveredOffset, r.image.CommittedEnd-frameOffset)
				frameRows, _, err = decoder.frame(section, frameOffset, r.image.CommittedEnd)
				if err != nil {
					return nil, nil, err
				}
			}
			if ref.index >= len(frameRows) || !frameRows[ref.index].TS.Equal(ref.key.Time) {
				return nil, nil, corrupt("WAL row changed at %d", ref.offset)
			}
			rows = append(rows, Row{frameRows[ref.index], ref.key.Ordinal})
			tail = tail[1:]
			continue
		}
		cursor := runs[0]
		line, err := cursor.line(r.root)
		if err != nil {
			return nil, nil, err
		}
		if strings.Contains(line, q.Contains) {
			if len(rows) == limit {
				break
			}
			rows = append(rows, Row{store.Row{TS: cursor.key.Time, Line: line}, cursor.key.Ordinal})
		}
		if err := cursor.advance(ctx, r.root, q); err != nil {
			return nil, nil, err
		}
		if cursor.valid {
			heap.Fix(&runs, 0)
		} else {
			heap.Pop(&runs)
		}
	}
	if len(runs) > 0 || len(tail) > 0 {
		position := rows[limit-1].Position()
		return rows, &position, nil
	}
	return rows, nil, nil
}

type tailRef struct {
	key    Position
	offset int64
	index  int
}

// Keep only the smallest matching keys and frame locations. The heap root is
// the largest retained key.
func (r *Reader) tailPage(ctx context.Context, f *os.File, q Query, count int) ([]tailRef, error) {
	rows := make(tailHeap, 0, count)
	err := scanTail(ctx, f, r.image.metadata, func(offset int64, first uint64, batch []store.Row) error {
		for i, row := range batch {
			key := Position{row.TS, first + uint64(i)}
			if !q.includes(key) || !strings.Contains(row.Line, q.Contains) {
				continue
			}
			ref := tailRef{key, offset, i}
			if len(rows) < count {
				heap.Push(&rows, ref)
			} else if key.less(rows[0].key) {
				rows[0] = ref
				heap.Fix(&rows, 0)
			}
		}
		return ctx.Err()
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b tailRef) int {
		if order := a.key.Time.Compare(b.key.Time); order != 0 {
			return order
		}
		return cmp.Compare(a.key.Ordinal, b.key.Ordinal)
	})
	return rows, nil
}

type segmentCursor struct {
	segment
	key               Position
	start, size, next int64
	valid             bool
	pageOffset        int64
	page              []byte
}

func (c *segmentCursor) advance(ctx context.Context, root *os.Root, q Query) error {
	c.valid = false
	for c.next < c.Size {
		if err := ctx.Err(); err != nil {
			return err
		}
		var header [rowHeader]byte
		if c.Size-c.next < rowHeader {
			return corrupt("truncated row in %s", c.Name)
		}
		if err := c.readAt(root, header[:], c.next); err != nil {
			return err
		}
		start := c.next + rowHeader
		key, n, ok := rowKey(header[:], c.Size-start)
		if !ok {
			return corrupt("invalid row in %s", c.Name)
		}
		c.next = start + n
		if !key.Time.Before(q.End) {
			return nil
		}
		if q.includes(key) {
			c.key, c.start, c.size, c.valid = key, start, n, true
			return nil
		}
	}
	return nil
}

func (c *segmentCursor) line(root *os.Root) (string, error) {
	p := make([]byte, c.size)
	if err := c.readAt(root, p, c.start); err != nil {
		return "", err
	}
	return string(p), nil
}

func (c *segmentCursor) readAt(root *os.Root, b []byte, offset int64) (err error) {
	if len(b) == 0 {
		return nil
	}
	if offset >= c.pageOffset && offset+int64(len(b)) <= c.pageOffset+int64(len(c.page)) {
		copy(b, c.page[offset-c.pageOffset:])
		return nil
	}
	f, size, err := openRegular(root, c.Name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if size != c.Size {
		return corrupt("segment size changed for %s", c.Name)
	}
	if len(b) > readPageSize {
		_, err := io.ReadFull(io.NewSectionReader(f, offset, int64(len(b))), b)
		return err
	}
	if c.page == nil {
		c.page = make([]byte, readPageSize)
	}
	c.page = c.page[:min(readPageSize, size-offset)]
	if _, err := io.ReadFull(io.NewSectionReader(f, offset, int64(len(c.page))), c.page); err != nil {
		return err
	}
	c.pageOffset = offset
	copy(b, c.page)
	return nil
}

type segmentHeap []*segmentCursor

func (h segmentHeap) Len() int           { return len(h) }
func (h segmentHeap) Less(i, j int) bool { return h[i].key.less(h[j].key) }
func (h segmentHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *segmentHeap) Push(v any)        { *h = append(*h, v.(*segmentCursor)) }
func (h *segmentHeap) Pop() any {
	last := len(*h) - 1
	v := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return v
}

type tailHeap []tailRef

func (h tailHeap) Len() int           { return len(h) }
func (h tailHeap) Less(i, j int) bool { return h[j].key.less(h[i].key) }
func (h tailHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *tailHeap) Push(v any)        { *h = append(*h, v.(tailRef)) }
func (h *tailHeap) Pop() any {
	last := len(*h) - 1
	v := (*h)[last]
	(*h)[last] = tailRef{}
	*h = (*h)[:last]
	return v
}
