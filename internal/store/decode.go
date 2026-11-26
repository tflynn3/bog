package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
	"unicode/utf8"
)

const MaxBatch = 4 << 20

var timestamp = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)$`)

func ParseTime(s string) (time.Time, error) {
	if !timestamp.MatchString(s) {
		return time.Time{}, errors.New("invalid timestamp")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	// Go's parser accepts out-of-range zone components; RFC3339 does not.
	if s[len(s)-1] != 'Z' {
		z := s[len(s)-5:]
		if z[:2] > "23" || z[3:] > "59" {
			return time.Time{}, errors.New("invalid zone")
		}
	}
	t = t.UTC()
	if t.Year() < 1 || t.Year() > 9999 {
		return time.Time{}, errors.New("timestamp outside supported range")
	}
	return t, nil
}
func Decode(data []byte) ([]Row, error) {
	return decodeAppend(nil, data)
}

// DecodeIngest validates a new NDJSON batch, including paired UTF-16 surrogate
// escapes. Row errors identify the physical line and contain no input values.
// Decode retains compatibility with previously committed WAL payloads.
func DecodeIngest(data []byte) ([]Row, error) {
	return decodeBatch(nil, data, true)
}

// decodeAppend appends a complete batch to rows, retaining Decode validation.
func decodeAppend(rows []Row, data []byte) ([]Row, error) {
	return decodeBatch(rows, data, false)
}

func decodeBatch(rows []Row, data []byte, ingest bool) ([]Row, error) {
	if len(data) > MaxBatch {
		return nil, errors.New("batch too large")
	}
	if !ingest && !utf8.Valid(data) {
		return nil, errors.New("invalid UTF-8")
	}
	initialLen := len(rows)
	for number := 1; len(data) > 0; number++ {
		line, rest, _ := bytes.Cut(data, []byte{'\n'})
		data = rest
		if ingest && !utf8.Valid(line) {
			return nil, fmt.Errorf("line %d: invalid UTF-8", number)
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		row, err := decodeRow(line, ingest)
		if err != nil {
			if ingest {
				return nil, fmt.Errorf("line %d: %w", number, err)
			}
			return nil, err
		}
		rows = append(rows, row)
	}
	if len(rows) == initialLen {
		return nil, errors.New("empty batch")
	}
	return rows, nil
}

func decodeRow(line []byte, ingest bool) (Row, error) {
	ts, text, plain := plainRow(line)
	if !plain {
		var err error
		ts, text, err = decodeJSONRow(line, ingest)
		if err != nil {
			return Row{}, err
		}
	}
	t, err := checkedTime(ts)
	if err != nil {
		return Row{}, err
	}
	return Row{t, text}, nil
}

func decodeJSONRow(line []byte, ingest bool) (ts, text string, err error) {
	d := json.NewDecoder(bytes.NewReader(line))
	tok, err := d.Token()
	if err != nil {
		return "", "", errors.New("invalid JSON")
	}
	if tok != json.Delim('{') {
		return "", "", errors.New("expected JSON object")
	}
	var seenTS, seenLine bool
	for d.More() {
		tok, err := d.Token()
		if err != nil {
			return "", "", errors.New("invalid JSON")
		}
		var field string
		var value *string
		var seen *bool
		switch tok {
		case "ts":
			field, value, seen = "ts", &ts, &seenTS
		case "line":
			field, value, seen = "line", &text, &seenLine
		default:
			return "", "", errors.New("unknown field: expected ts or line")
		}
		if *seen {
			return "", "", fmt.Errorf("duplicate %s field", field)
		}
		*seen = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return "", "", errors.New("invalid JSON")
		}
		if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, value) != nil {
			return "", "", fmt.Errorf("%s field must be a string", field)
		}
	}
	if _, err := d.Token(); err != nil {
		return "", "", errors.New("invalid JSON")
	}
	var extra json.RawMessage
	if err := d.Decode(&extra); err != io.EOF {
		return "", "", errors.New("trailing data after JSON object")
	}
	if ingest && !validSurrogates(line) {
		return "", "", errors.New("unpaired Unicode surrogate escape")
	}
	if !seenTS {
		return "", "", errors.New("missing ts field")
	}
	if !seenLine {
		return "", "", errors.New("missing line field")
	}
	return ts, text, nil
}

// plainRow accepts only the unescaped, two-field form. All other forms go
// through the JSON decoder, including malformed input and escaped keys.
func plainRow(b []byte) (ts, line string, ok bool) {
	i := 0
	space := func() {
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\r' || b[i] == '\n') {
			i++
		}
	}
	take := func(c byte) bool {
		space()
		if i >= len(b) || b[i] != c {
			return false
		}
		i++
		return true
	}
	str := func() (string, bool) {
		space()
		if i >= len(b) || b[i] != '"' {
			return "", false
		}
		i++
		start := i
		for i < len(b) && b[i] != '"' {
			if b[i] < 0x20 || b[i] == '\\' {
				return "", false
			}
			i++
		}
		if i == len(b) {
			return "", false
		}
		v := string(b[start:i])
		i++
		return v, true
	}
	if !take('{') {
		return "", "", false
	}
	var seen uint8
	for n := 0; n < 2; n++ {
		if n > 0 && !take(',') {
			return "", "", false
		}
		key, valid := str()
		if !valid || !take(':') {
			return "", "", false
		}
		value, valid := str()
		if !valid {
			return "", "", false
		}
		switch key {
		case "ts":
			if seen&1 != 0 {
				return "", "", false
			}
			seen |= 1
			ts = value
		case "line":
			if seen&2 != 0 {
				return "", "", false
			}
			seen |= 2
			line = value
		default:
			return "", "", false
		}
	}
	if seen != 3 || !take('}') {
		return "", "", false
	}
	space()
	return ts, line, i == len(b)
}

func checkedTime(ts string) (time.Time, error) {
	t, err := ParseTime(ts)
	if err != nil {
		return time.Time{}, errors.New("invalid ts: expected an RFC3339 timestamp")
	}
	if t.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
		return time.Time{}, errors.New("timestamp reserved for exclusive query end")
	}
	return t, nil
}

// validSurrogates examines raw JSON strings after the decoder has verified JSON syntax.
// Escaped backslashes are skipped, so their following text is not an escape.
func validSurrogates(b []byte) bool {
	inString := false
	for i := 0; i < len(b); i++ {
		if !inString {
			if b[i] == '"' {
				inString = true
			}
			continue
		}
		if b[i] == '"' {
			inString = false
			continue
		}
		if b[i] != '\\' {
			continue
		}
		i++
		if b[i] != 'u' {
			continue
		}
		n := hexQuad(b[i+1 : i+5])
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			low := hexQuad(b[i+3 : i+7])
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

func hexQuad(b []byte) uint16 {
	var n uint16
	for _, c := range b {
		n <<= 4
		switch {
		case c >= '0' && c <= '9':
			n += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			n += uint16(c - 'a' + 10)
		default:
			n += uint16(c - 'A' + 10)
		}
	}
	return n
}
