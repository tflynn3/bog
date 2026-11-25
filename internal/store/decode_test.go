package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func referenceDecode(data []byte) ([]Row, error) {
	if len(data) == 0 || len(data) > MaxBatch || !utf8.Valid(data) {
		return nil, errors.New("invalid batch")
	}
	var rows []Row
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
		t, err := ParseTime(ts)
		if err != nil {
			return nil, fmt.Errorf("timestamp: %w", err)
		}
		if t.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
			return nil, errors.New("timestamp reserved for exclusive query end")
		}
		rows = append(rows, Row{t, text})
	}
	if len(rows) == 0 {
		return nil, errors.New("empty batch")
	}
	return rows, nil
}

func TestDecodeDifferential(t *testing.T) {
	valid := `{"ts":"2025-01-01T00:00:00Z","line":"hello"}`
	cases := []string{valid, "  " + valid + "\r\n" + valid, `{"line":"é","ts":"0001-01-01T00:00:00Z"}`, `{"ts":"9999-12-31T23:59:59.999999999Z","line":"x"}`, `{"ts":"9999-12-31T23:59:59.999999998Z","line":"x"}`, `{"ts":"2025-01-01T00:00:00Z","ts":"2025-01-01T00:00:00Z","line":"x"}`, `{"t\u0073":"2025-01-01T00:00:00Z","line":"x"}`, `{"ts":"2025-01-01T00:00:00Z","line":"\ud800"}`, `{"ts":"2025-01-01T00:00:00Z","line":"a\nb"}`, "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"a\x01b\"}", valid + " trailing", valid + "\n{}", "\xff", "\n\r\n", strings.Repeat(" ", MaxBatch-len(valid)) + valid, strings.Repeat(" ", MaxBatch-len(valid)+1) + valid, `{"ts":"2025-01-01T00:00:00Z","line":"x",}`, `{"ts":"2025-01-01T00:00:00Z","line":null}`}
	for _, input := range cases {
		got, err := Decode([]byte(input))
		want, refErr := referenceDecode([]byte(input))
		if (err == nil) != (refErr == nil) || !reflect.DeepEqual(got, want) {
			t.Fatalf("input %.100q: got %v, %v; want %v, %v", input, got, err, want, refErr)
		}
	}
}
func TestDecodeOwnsPayload(t *testing.T) {
	payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"retain me"}`)
	rows, err := Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	clear(payload)
	if rows[0].Line != "retain me" {
		t.Fatalf("line changed: %q", rows[0].Line)
	}
}
func BenchmarkDecode500(b *testing.B) {
	for _, mixed := range []bool{false, true} {
		name := "plain"
		if mixed {
			name = "mixed"
		}
		var payload strings.Builder
		for i := 0; i < 500; i++ {
			if mixed && i%2 == 0 {
				payload.WriteString(`{"ts":"2025-01-01T00:00:00Z","line":"escaped\nline"}` + "\n")
			} else {
				payload.WriteString(`{"ts":"2025-01-01T00:00:00Z","line":"plain benchmark row"}` + "\n")
			}
		}
		data := []byte(payload.String())
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				if _, err := Decode(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
