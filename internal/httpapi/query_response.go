package httpapi

import (
	"bytes"
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	queryStringChunk          = 4 << 10
	queryDeliveryBytesPerUnit = 16
)

// queryResponse buffers complete records until the status and cursor can be
// committed. Each record's allocation is exact and never copies earlier rows.
type queryResponse struct {
	rows     [][]byte
	size     int
	maxBytes int
	scratch  bytes.Buffer
	encoder  jsontext.Encoder
}

// appendRow reports the exact encoded size when the first row doesn't fit.
// For later rows, the encoder can stop as soon as it proves the row won't fit.
func (b *queryResponse) appendRow(row store.Row, work *store.QueryWork) (bool, int, error) {
	if err := work.Charge(); err != nil {
		return false, 0, err
	}
	stamp, size, complete, err := b.measureRow(row, b.maxBytes-b.size, b.size != 0, work, false)
	if err != nil || !complete || size > b.maxBytes-b.size {
		return false, size, err
	}

	if err := b.appendMeasured(row.Line, stamp, size, work); err != nil {
		return false, 0, err
	}
	return true, 0, nil
}

func (b *queryResponse) appendMeasured(line string, stamp []byte, size int, work *store.QueryWork) error {
	record := make([]byte, 0, size)
	record = append(record, queryRecordPrefix...)
	record = append(record, stamp...)
	record = append(record, queryRecordField...)
	for len(line) > 0 {
		// The measurement pass reserved the work for these bytes.
		if err := work.Charge(); err != nil {
			return err
		}
		n := queryChunkEnd(line)
		payload, err := b.escapeChunk(line[:n])
		if err != nil {
			return err
		}
		record = append(record, payload...)
		line = line[n:]
	}
	record = append(record, queryRecordSuffix...)
	b.rows = append(b.rows, record)
	b.size += len(record)
	return nil
}

const (
	queryRecordPrefix = `{"ts":`
	queryRecordField  = `,"line":"`
	queryRecordSuffix = "\"}\n"
)

// measureRow uses the same encoding as delivery. Certification measures
// complete records independently of how the client will split the pages.
func (b *queryResponse) measureRow(row store.Row, remaining int, allowEarlyExit bool, work *store.QueryWork, prepaid bool) ([]byte, int, bool, error) {
	stamp, err := row.TS.MarshalJSON()
	if err != nil {
		return nil, 0, false, err
	}

	size := len(queryRecordPrefix) + len(stamp) + len(queryRecordField) + len(queryRecordSuffix)
	for line := row.Line; ; {
		// Escaping can only expand the remaining input.
		if allowEarlyExit && len(line) > remaining-size {
			return nil, 0, false, nil
		}
		if len(line) == 0 {
			break
		}
		n := queryChunkEnd(line)
		units := uint64(n)
		if prepaid {
			units = 0
		}
		if err := work.Charge(units); err != nil {
			return nil, 0, false, err
		}
		payload, err := b.escapeChunk(line[:n])
		if err != nil {
			return nil, 0, false, err
		}
		size += len(payload)
		line = line[n:]
	}
	return stamp, size, true, nil
}

type oversizedQueryRecord struct{ cap, required int }

func (e *oversizedQueryRecord) Error() string {
	return fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", e.cap, e.required)
}

func (b *queryResponse) appendRows(rows []PageRow, work *store.QueryWork) (int, error) {
	for i, row := range rows {
		fits, required, err := b.appendRow(row.Row, work)
		if err != nil {
			return i, err
		}
		if !fits {
			if b.size == 0 {
				return i, &oversizedQueryRecord{b.maxBytes, required}
			}
			return i, nil
		}
	}
	return len(rows), nil
}

func queryDeliveryWork(size int) uint64 {
	return (uint64(size) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit
}

func (b *queryResponse) reserveDelivery(work *store.QueryWork) error {
	return work.Charge(queryDeliveryWork(b.size))
}

func (b *queryResponse) write(w http.ResponseWriter, token string) {
	setQueryPageHeaders(w.Header(), b.size, token)
	w.WriteHeader(http.StatusOK)
	for _, record := range b.rows {
		if n, err := w.Write(record); err != nil || n != len(record) {
			// The client can detect the interrupted body using Content-Length.
			return
		}
	}
}

// escapeChunk returns the escaped payload without the quotes and newline
// written by the encoder. The buffer is reused across chunks and rows.
// The payload is valid until the next call.
func (b *queryResponse) escapeChunk(line string) ([]byte, error) {
	b.scratch.Reset()
	// An input byte can expand to six bytes. Leave room for the quotes,
	// newline, and the encoder's buffer growth threshold.
	maxEncoded := 6*len(line) + 3
	b.scratch.Grow(maxEncoded + maxEncoded/4)
	b.encoder.Reset(&b.scratch, jsontext.EscapeForHTML(true), jsontext.EscapeForJS(true), jsontext.AllowInvalidUTF8(true))
	if err := b.encoder.WriteToken(jsontext.String(line)); err != nil {
		return nil, err
	}
	quoted := b.scratch.Bytes()
	return quoted[1 : len(quoted)-2], nil
}

// queryChunkEnd avoids splitting a valid UTF-8 rune. Malformed bytes must
// still receive the same per-byte replacement as encoding the whole string.
func queryChunkEnd(line string) int {
	if len(line) <= queryStringChunk {
		return len(line)
	}
	end := queryStringChunk
	if !utf8.RuneStart(line[end]) {
		for i := end - 1; i > end-utf8.UTFMax; i-- {
			if utf8.RuneStart(line[i]) {
				_, size := utf8.DecodeRuneInString(line[i:])
				if i+size > end {
					return i
				}
				break
			}
		}
	}
	return end
}
