package ingest_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

type referenceTimestamp struct {
	time.Time
}

func (ts *referenceTimestamp) UnmarshalJSON(data []byte) error {
	s, err := unmarshalRequiredString(data)
	if err != nil {
		return err
	}
	// Timestamp parsing is shared with queries. Keep the decoder-specific
	// reserved-end rule explicit in the oracle.
	value, err := store.ParseTime(s)
	if err != nil {
		return err
	}
	if value.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
		return errors.New("timestamp reserved for exclusive query end")
	}
	ts.Time = value
	return nil
}

type referenceLine string

func (line *referenceLine) UnmarshalJSON(data []byte) error {
	s, err := unmarshalRequiredString(data)
	if err != nil {
		return err
	}
	*line = referenceLine(s)
	return nil
}

func unmarshalRequiredString(data []byte) (string, error) {
	var value *string
	if err := json.Unmarshal(data, &value); err != nil {
		return "", err
	}
	if value == nil {
		return "", errors.New("expected a JSON string, got null")
	}
	return *value, nil
}

type referenceRecord struct {
	store.Row
}

// UnmarshalJSON receives one complete value whose syntax json.Unmarshal has
// already checked. Member tokens retain duplicates and decode escaped field
// names before comparison. A map would discard duplicates, and a struct would
// match case variants and ignore unknown fields.
func (record *referenceRecord) UnmarshalJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	first, err := d.Token()
	if err != nil {
		return err
	}
	if first != json.Delim('{') {
		return errors.New("expected a JSON object")
	}

	var ts referenceTimestamp
	var line referenceLine
	fields := map[string]json.Unmarshaler{"ts": &ts, "line": &line}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok {
			return errors.New("expected a member name")
		}
		field, ok := fields[name]
		if !ok {
			return fmt.Errorf("duplicate or unknown member %q", name)
		}
		if err := d.Decode(field); err != nil {
			return fmt.Errorf("member %q: %w", name, err)
		}
		delete(fields, name)
	}
	last, err := d.Token()
	if err != nil {
		return err
	}
	if last != json.Delim('}') || len(fields) != 0 {
		return errors.New("incomplete record")
	}

	record.Row = store.Row{TS: ts.Time, Line: string(line)}
	return nil
}

func referenceDecode(data []byte) ([]store.Row, error) {
	if len(data) == 0 || len(data) > store.MaxBatch || !utf8.Valid(data) {
		return nil, errors.New("invalid batch")
	}
	var rows []store.Row
	lineNumber := 0
	for physicalLine := range bytes.SplitSeq(data, []byte("\n")) {
		lineNumber++
		if len(bytes.TrimSpace(physicalLine)) == 0 {
			continue
		}
		var record referenceRecord
		if err := json.Unmarshal(physicalLine, &record); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		rows = append(rows, record.Row)
	}
	if len(rows) == 0 {
		return nil, errors.New("empty batch")
	}
	return rows, nil
}

func assertDecoderParity(t *testing.T, data []byte) []store.Row {
	t.Helper()
	want, refErr := referenceDecode(data)
	got, err := store.Decode(data)
	if (err == nil) != (refErr == nil) {
		t.Fatalf("batch (%d bytes) %.256q: optimized error %.256v, reference error %.256v",
			len(data), data, err, refErr)
	}
	if refErr != nil {
		if got != nil {
			t.Fatalf("rejected batch %.256q returned %d rows", data, len(got))
		}
		return nil
	}
	assertRows(t, data, got, want)
	return got
}

func assertRows(t *testing.T, input []byte, got, want []store.Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("batch %.256q: got %d rows, want %d", input, len(got), len(want))
	}
	for i, row := range got {
		if row != want[i] {
			t.Fatalf("batch %.256q: row %d got (%#v, %.128q), want (%#v, %.128q)",
				input, i, row.TS, row.Line, want[i].TS, want[i].Line)
		}
	}
}
