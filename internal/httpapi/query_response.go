package httpapi

import (
	"encoding/json"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	queryStringChunk          = 4 << 10
	queryDeliveryBytesPerUnit = 16
)

// queryResponse buffers complete records so a rejection can be sent before
// committing any NDJSON or cursor. Its capacity is capped at the body limit.
type queryResponse struct {
	body     []byte
	maxBytes int
}

func (b *queryResponse) append(data string) {
	b.grow(len(data))
	b.body = append(b.body, data...)
}

func (b *queryResponse) grow(size int) {
	needed := len(b.body) + size
	if needed > cap(b.body) {
		capacity := cap(b.body)
		capacity += min(capacity, b.maxBytes-capacity)
		grown := make([]byte, len(b.body), max(needed, capacity))
		copy(grown, b.body)
		b.body = grown
	}
}

// appendRow adds one record using encoding/json's escaping on bounded pieces
// of the line. If the record cannot fit, it restores the previous body.
func (b *queryResponse) appendRow(row store.Row, work *store.QueryWork) (bool, error) {
	if err := work.Charge(); err != nil {
		return false, err
	}
	const prefix, field, suffix = `{"ts":`, `,"line":"`, "\"}\n"
	stamp, err := row.TS.MarshalJSON()
	if err != nil {
		return false, err
	}
	mark := len(b.body)
	remaining := b.maxBytes - mark - len(prefix) - len(stamp) - len(field) - len(suffix)
	// JSON escaping can only expand the byte length.
	if len(row.Line) > remaining {
		return false, nil
	}
	b.append(prefix)
	b.append(string(stamp))
	b.append(field)
	for line := row.Line; len(line) > 0; {
		n := queryChunkEnd(line)
		if err := work.Charge(uint64(n)); err != nil {
			b.body = b.body[:mark]
			return false, err
		}
		quoted, err := json.Marshal(line[:n])
		if err != nil {
			b.body = b.body[:mark]
			return false, err
		}
		payload := quoted[1 : len(quoted)-1]
		remaining -= len(payload)
		line = line[n:]
		if len(line) > remaining {
			b.body = b.body[:mark]
			return false, nil
		}
		b.grow(len(payload))
		b.body = append(b.body, payload...)
	}
	b.append(suffix)
	return true, nil
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
