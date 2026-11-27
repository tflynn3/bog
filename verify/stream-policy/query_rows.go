package streampolicy

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"
)

func decodeQueryRows(body []byte) ([]Row, error) {
	if !utf8.Valid(body) {
		return nil, ErrViolation
	}
	var rows []Row
	for len(body) > 0 {
		record, rest, terminated := bytes.Cut(body, []byte("\n"))
		if !terminated {
			return nil, ErrViolation
		}
		row, ok := decodeQueryRow(record)
		if !ok {
			return nil, ErrViolation
		}
		rows = append(rows, row)
		body = rest
	}
	return rows, nil
}

func decodeQueryRow(record []byte) (Row, bool) {
	record = bytes.Trim(record, " \t\r")
	decoder := json.NewDecoder(bytes.NewReader(record))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return Row{}, false
	}
	var row Row
	var previousKey string
	// Two distinct decoded keys from this pair guarantee both fields appear.
	for range 2 {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok || key == previousKey {
			return Row{}, false
		}
		previousKey = key
		valueToken, err := decoder.Token()
		value, ok := valueToken.(string)
		if err != nil || !ok {
			return Row{}, false
		}
		switch key {
		case "ts":
			row.Timestamp = value
		case "line":
			row.Line = value
		default:
			return Row{}, false
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || decoder.InputOffset() != int64(len(record)) {
		return Row{}, false
	}
	return row, true
}
