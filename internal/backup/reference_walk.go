package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"bog/internal/store"
)

const referencePageBytes = 8 << 20

// ErrReferenceWalk identifies a failure preparing the query walk, such as
// being unable to write scratch data. Other open errors are image verification
// failures. Callers can check for cancellation separately.
var ErrReferenceWalk = errors.New("prepare backup reference walk")

// ReferenceWalk emits one query over a verified, pinned image. It owns its
// directory and temporary files. Use it from one goroutine at a time and
// call Close after success, failure, or cancellation.
type ReferenceWalk struct {
	reader   *Reader
	point    Point
	query    Query
	limit    int
	segments segmentHeap
	tail     *referenceRun
	scratch  string

	pending    Row
	hasPending bool
	terminal   bool
	failed     error
}

// OpenReferenceWalk validates the complete image before returning any rows.
// Keep the image immutable until Close. The walk collects matching WAL rows
// during the verified scan and sorts them in private temporary files.
func OpenReferenceWalk(ctx context.Context, directory string, q Query, limit int) (_ *ReferenceWalk, err error) {
	return openReferenceWalk(ctx, directory, q, limit, scanTail)
}

type referenceTailScan func(context.Context, *os.File, metadata, walFrameVisitor) error

func openReferenceWalk(ctx context.Context, directory string, q Query, limit int, scan referenceTailScan) (_ *ReferenceWalk, err error) {
	if limit < 1 || limit > MaxPageSize || q.End.Before(q.Start) {
		return nil, fmt.Errorf("%w: invalid backup query range or page size", ErrReferenceWalk)
	}
	if q.After != nil {
		after := *q.After
		q.After = &after
	}
	sorter := referenceSorter{ctx: ctx}
	w := &ReferenceWalk{query: q, limit: limit}
	defer func() {
		w.scratch = sorter.directory
		if err != nil {
			err = errors.Join(err, w.Close())
		}
	}()
	collect := func(_ int64, first uint64, batch []store.Row) error {
		for i, row := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := Position{Time: row.TS, Ordinal: first + uint64(i)}
			if q.includes(key) && strings.Contains(row.Line, q.Contains) {
				if err := sorter.add(Row{row, key.Ordinal}); err != nil {
					return fmt.Errorf("%w: %w", ErrReferenceWalk, err)
				}
			}
		}
		return nil
	}
	w.reader, err = openRestored(ctx, directory, func(ctx context.Context, root *os.Root) (image, []bounds, error) {
		return verifyReferenceImage(ctx, root, scan, collect)
	})
	if err != nil {
		return nil, err
	}
	w.point = w.reader.Point()
	if !q.Start.Equal(q.End) {
		w.segments, err = w.reader.segmentRuns(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrReferenceWalk, err)
		}
	}
	w.tail, err = sorter.finish()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReferenceWalk, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return w, nil
}

// Run the same image validators as Verify, collecting WAL rows during its
// digest scan. The collected rows are only usable after every check passes.
func verifyReferenceImage(ctx context.Context, root *os.Root, scan referenceTailScan, visit walFrameVisitor) (image, []bounds, error) {
	if err := ctx.Err(); err != nil {
		return image{}, nil, err
	}
	img, err := inspect(root)
	if err != nil {
		return image{}, nil, err
	}
	bounds, err := validateSegments(ctx, root, img.manifest)
	if err != nil {
		return image{}, nil, err
	}
	f, err := openTail(root, img.metadata)
	if err != nil {
		return image{}, nil, err
	}
	if err := errors.Join(scan(ctx, f, img.metadata, visit), f.Close()); err != nil {
		return image{}, nil, err
	}
	return img, bounds, nil
}

// Point returns the verified recovery point.
func (w *ReferenceWalk) Point() Point { return w.point }

// Close releases the image and removes all scratch files. It is idempotent
// after successful cleanup, and can retry a failed scratch removal.
func (w *ReferenceWalk) Close() (err error) {
	w.segments = nil
	w.pending, w.hasPending = Row{}, false
	w.terminal = true
	if w.tail != nil {
		err = w.tail.file.Close()
		w.tail = nil
	}
	if w.reader != nil {
		err = errors.Join(err, w.reader.Close())
		w.reader = nil
	}
	if w.scratch != "" {
		if removeErr := os.RemoveAll(w.scratch); removeErr != nil {
			err = errors.Join(err, removeErr)
		} else {
			w.scratch = ""
		}
	}
	return err
}

// NextPage returns up to the requested row limit. It may cut a page early to
// bound decoded reference line bytes. The terminal page may contain rows.
// After an error the walk cannot resume, and after exhaustion it returns EOF.
func (w *ReferenceWalk) NextPage(ctx context.Context) (_ []Row, terminal bool, err error) {
	if w.terminal {
		return nil, false, io.EOF
	}
	if w.failed != nil {
		return nil, false, w.failed
	}
	defer func() { w.failed = err }()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	var rows []Row
	var size int
	for {
		row, haveRow := w.pending, w.hasPending
		w.pending, w.hasPending = Row{}, false
		if !haveRow {
			row, haveRow, err = w.nextRow(ctx)
			if err != nil {
				return nil, false, err
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if !haveRow {
			w.terminal = true
			return rows, true, nil
		}
		if len(rows) == w.limit || (len(rows) > 0 && size+len(row.Line) > referencePageBytes) {
			w.pending, w.hasPending = row, true
			return rows, false, nil
		}
		rows = append(rows, row)
		size += len(row.Line)
	}
}

func (w *ReferenceWalk) nextRow(ctx context.Context) (Row, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Row{}, false, err
		}
		if w.tail != nil && w.tail.valid && (len(w.segments) == 0 || w.tail.key.less(w.segments[0].key)) {
			line := make([]byte, w.tail.size)
			if _, err := io.ReadFull(w.tail.input, line); err != nil {
				return Row{}, false, fmt.Errorf("read reference WAL row: %w", err)
			}
			row := Row{store.Row{TS: w.tail.key.Time, Line: string(line)}, w.tail.key.Ordinal}
			if err := w.tail.advance(); err != nil {
				return Row{}, false, err
			}
			return row, true, nil
		}
		if len(w.segments) == 0 {
			return Row{}, false, nil
		}
		cursor := w.segments[0]
		line, err := cursor.line(w.reader.root)
		if err != nil {
			return Row{}, false, err
		}
		key := cursor.key
		if err := w.reader.advanceSegment(ctx, w.query, &w.segments); err != nil {
			return Row{}, false, err
		}
		if strings.Contains(line, w.query.Contains) {
			return Row{store.Row{TS: key.Time, Line: line}, key.Ordinal}, true, nil
		}
	}
}
