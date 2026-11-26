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

// decodeAppend appends a complete batch to rows, retaining Decode validation.
func decodeAppend(rows []Row, data []byte) ([]Row, error) {
	initialLen := len(rows)
	if len(data) == 0 || len(data) > MaxBatch || !utf8.Valid(data) {
		return nil, errors.New("invalid batch")
	}
	for len(data) > 0 {
		line, rest, found := bytes.Cut(data, []byte{'\n'})
		if found {
			data = rest
		} else {
			data = nil
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if ts, text, ok := plainRow(line); ok {
			t, err := checkedTime(ts)
			if err != nil {
				return nil, err
			}
			rows = append(rows, Row{t, text})
			continue
		}
		d := json.NewDecoder(bytes.NewReader(line))
		tok, err := d.Token()
		if err != nil || tok != json.Delim('{') {
			return nil, errors.New("expected object")
		}
		var ts, text string
		seen := map[string]bool{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return nil, e
			}
			key, ok := k.(string)
			if !ok || seen[key] || (key != "ts" && key != "line") {
				return nil, errors.New("invalid field")
			}
			seen[key] = true
			var v json.RawMessage
			if e = d.Decode(&v); e != nil {
				return nil, e
			}
			var s string
			if len(v) == 0 || v[0] != '"' || json.Unmarshal(v, &s) != nil {
				return nil, errors.New("field must be string")
			}
			if key == "ts" {
				ts = s
			} else {
				text = s
			}
		}
		if _, err = d.Token(); err != nil {
			return nil, err
		}
		var extra any
		if err = d.Decode(&extra); err != io.EOF {
			return nil, errors.New("trailing data")
		}
		if !seen["ts"] || !seen["line"] {
			return nil, errors.New("missing field")
		}
		t, err := checkedTime(ts)
		if err != nil {
			return nil, err
		}
		rows = append(rows, Row{t, text})
	}
	if len(rows) == initialLen {
		return nil, errors.New("empty batch")
	}
	return rows, nil
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
		return time.Time{}, fmt.Errorf("timestamp: %w", err)
	}
	if t.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
		return time.Time{}, errors.New("timestamp reserved for exclusive query end")
	}
	return t, nil
}
