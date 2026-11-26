package httpapi

import (
	"bytes"
	"encoding/json/jsontext"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	queryStringChunk          = 4 << 10
	queryDeliveryBytesPerUnit = 16
	queryRecordPrefix         = `{"ts":`
	queryRecordField          = `,"line":"`
	queryRecordSuffix         = "\"}\n"
)

// queryResponse buffers complete records until the status and cursor can be
// committed. Each record's allocation is exact and never copies earlier rows.
type queryResponse struct {
	rows        [][]byte
	size        int
	maxBytes    int
	measureOnly bool
	scratch     bytes.Buffer
	encoder     jsontext.Encoder
}

// appendRow reports the exact encoded size when the first row doesn't fit.
// For later rows, the encoder can stop as soon as it proves the row won't fit.
func (b *queryResponse) appendRow(row store.Row, work *store.QueryWork) (bool, int, error) {
	if err := work.Charge(); err != nil {
		return false, 0, err
	}
	stamp, err := row.TS.MarshalJSON()
	if err != nil {
		return false, 0, err
	}

	remaining := b.maxBytes - b.size
	size := len(queryRecordPrefix) + len(stamp) + len(queryRecordField) + len(queryRecordSuffix)
	for line := row.Line; ; {
		// Escaping can only expand the remaining input.
		if b.size != 0 && len(line) > remaining-size {
			return false, 0, nil
		}
		if len(line) == 0 {
			break
		}
		n := queryChunkEnd(line)
		if err := work.Charge(uint64(n)); err != nil {
			return false, 0, err
		}
		payload, err := b.escapeChunk(line[:n])
		if err != nil {
			return false, 0, err
		}
		size += len(payload)
		line = line[n:]
	}
	if size > remaining {
		return false, size, nil
	}

	if b.measureOnly {
		b.size += size
		return true, 0, nil
	}

	record := make([]byte, 0, size)
	record = append(record, queryRecordPrefix...)
	record = append(record, stamp...)
	record = append(record, queryRecordField...)
	for line := row.Line; len(line) > 0; {
		// The measurement pass reserved the work for these bytes.
		if err := work.Charge(); err != nil {
			return false, 0, err
		}
		n := queryChunkEnd(line)
		payload, err := b.escapeChunk(line[:n])
		if err != nil {
			return false, 0, err
		}
		record = append(record, payload...)
		line = line[n:]
	}
	record = append(record, queryRecordSuffix...)
	b.rows = append(b.rows, record)
	b.size += len(record)
	return true, 0, nil
}

// reserveRecord bounds the preparation and delivery cost of a row on any
// future page. Only rows that might exceed the cap need escaping here.
func (b *queryResponse) reserveRecord(row store.Row, work, resume *store.QueryWork) error {
	const maxStampBytes = len(`"2006-01-02T15:04:05.000000000+00:00"`)
	const framingBytes = len(queryRecordPrefix) + maxStampBytes + len(queryRecordField) + len(queryRecordSuffix)
	if err := work.Charge(1); err != nil {
		return err
	}
	if _, offset := row.TS.Zone(); offset%60 == 0 && offset > -24*60*60 && offset < 24*60*60 &&
		row.TS.Year() >= 0 && row.TS.Year() < 10000 && len(row.Line) <= (b.maxBytes-framingBytes)/6 && b.maxBytes >= framingBytes {
		size := framingBytes + 6*len(row.Line)
		return resume.Charge(uint64(len(row.Line)), queryDeliveryWork(size))
	}

	b.size = 0
	fits, required, err := b.appendRow(row, work)
	if err != nil {
		return err
	}
	if !fits {
		return oversizedQueryRecord{b.maxBytes, required}
	}
	return resume.Charge(uint64(len(row.Line)), queryDeliveryWork(b.size))
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

// add distinguishes an oversized first record from an ordinary byte cut.
func (b *queryResponse) add(row store.Row, work *store.QueryWork) (bool, error) {
	fits, required, err := b.appendRow(row, work)
	if err == nil && !fits && len(b.rows) == 0 {
		err = oversizedQueryRecord{b.maxBytes, required}
	}
	return fits, err
}

func queryDeliveryWork(size int) uint64 {
	return (uint64(size) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit
}
