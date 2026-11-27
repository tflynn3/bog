package client

import (
	"bytes"
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"unicode/utf8"

	"bog/internal/store"
)

// queryRecords assembles physical NDJSON lines across arbitrary transport
// boundaries. Only an unfinished record needs encoded storage beyond the read
// block. The decoded rows have page lifetime and preserve wire order, including
// ties and duplicates. No decoded value borrows bytes from the read block or
// the unfinished record buffer.
type queryRecords struct {
	rows     []Row
	pending  []byte
	maxBytes int64
	maxRows  int
}

// add processes a nonempty block of in-budget bytes. The LF is a delimiter
// even if the preceding bytes are invalid JSON. JSON escapes such as \n are
// ordinary bytes at this layer. A trailing LF does not start another record.
func (r *queryRecords) add(ctx context.Context, block []byte) error {
	for len(block) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, rest, complete := bytes.Cut(block, []byte{'\n'})
		if !complete {
			r.extend(line)
			return nil
		}
		if len(r.pending) > 0 {
			r.extend(line)
			line = r.pending
		}
		if err := r.addLine(line); err != nil {
			return err
		}
		r.resetPending()
		block = rest
	}
	return nil
}

// extend keeps spare capacity for a fragmented record without allocating
// according to the declared page length. The caller supplies disjoint pieces
// of in-budget payload, so their sum is already within maxBytes. Clamp spare
// capacity to that bound and to the largest representable slice size.
func (r *queryRecords) extend(fragment []byte) {
	r.pending = appendQueryBytes(r.pending, fragment, r.maxBytes)
}

func appendQueryBytes(dst, fragment []byte, maxBytes int64) []byte {
	if len(fragment) > cap(dst)-len(dst) {
		needed := len(dst) + len(fragment)
		ceiling := min(maxBytes, int64(int(^uint(0)>>1)))
		capacity := int64(cap(dst))
		// Add at most 50% spare capacity, without overflowing at a large limit.
		capacity += min(capacity/2, ceiling-capacity)
		capacity = max(capacity, int64(needed))
		grown := make([]byte, len(dst), int(capacity))
		copy(grown, dst)
		dst = grown
	}
	return append(dst, fragment...)
}

func (r *queryRecords) resetPending() {
	if cap(r.pending) > queryReadBlock {
		// A large row should not keep its encoded scratch alive while a
		// slow response delivers the rest of the page.
		r.pending = nil
		return
	}
	r.pending = r.pending[:0]
}

func (r *queryRecords) addLine(line []byte) error {
	row, err := decodeQueryRecord(line)
	if err != nil {
		return err
	}
	if r.maxRows > 0 && len(r.rows) == r.maxRows {
		return fmt.Errorf("unexpected row %d: exceeds requested limit %d", len(r.rows)+1, r.maxRows)
	}
	r.rows = append(r.rows, row)
	return nil
}

// finish is only meaningful after a verified body boundary. At that point
// leftover bytes are a malformed record, even if they form a complete JSON
// value. If the HTTP frame was truncated, the caller reports the delivery
// error instead, regardless of whether the last NDJSON record ended at LF.
func (r *queryRecords) finish() error {
	if len(r.pending) != 0 {
		return errQueryRecordFraming
	}
	return nil
}

var errQueryRecordFraming = errors.New("invalid query record framing or UTF-8")

func decodeQueryRecord(line []byte) (Row, error) {
	if len(line) == 0 || !utf8.Valid(line) {
		return Row{}, errQueryRecordFraming
	}
	var record struct {
		TS   *string `json:"ts"`
		Line *string `json:"line"`
	}
	if err := jsonv2.Unmarshal(line, &record, jsonv2.RejectUnknownMembers(true)); err != nil {
		return Row{}, fmt.Errorf("decode query record: %w", err)
	}
	if record.TS == nil || record.Line == nil {
		return Row{}, errors.New("missing query record field")
	}
	ts, err := store.ParseTime(*record.TS)
	if err != nil {
		return Row{}, fmt.Errorf("decode query timestamp: %w", err)
	}
	return Row{TS: ts, Line: *record.Line}, nil
}
