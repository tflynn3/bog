package ingest_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

const (
	sampleTimestamp = "2025-01-01T00:00:00Z"
	sampleRecord    = `{"ts":"2025-01-01T00:00:00Z","line":"hello"}`
)

func recordWithLine(literal string) string {
	return `{"ts":"2025-01-01T00:00:00Z","line":` + literal + "}"
}

func TestDecodeValidBatches(t *testing.T) {
	t.Parallel()
	sampleTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		input string
		lines []string
	}{
		{"plain", sampleRecord, []string{"hello"}},
		{"reversed fields", `{"line":"hello","ts":"2025-01-01T00:00:00Z"}`, []string{"hello"}},
		{"empty string", recordWithLine(`""`), []string{""}},
		{"structural whitespace", " \t{ \r\"ts\"\t: \"" + sampleTimestamp + "\" , \t\"line\" : \"hello\" } \r", []string{"hello"}},
		{"CRLF and blank lines", "\r\n" + sampleRecord + "\r\n \t\r\n" + recordWithLine(`"bye"`) + "\r\n", []string{"hello", "bye"}},
		{"Unicode blank lines", "\u0085\u00a0\u2003\u2028\u2029\n" + sampleRecord + "\n\v\f", []string{"hello"}},
		{"Unicode inside string", recordWithLine(`"é 世界 🙂 � ` + "\u2028\u2029" + `"`), []string{"é 世界 🙂 � \u2028\u2029"}},
		{"escapes", recordWithLine(`"\"\\\/\b\f\n\r\t\u0000"`), []string{"\"\\/\b\f\n\r\t\x00"}},
		{"escaped keys and timestamp", `{"\u0074s":"2025-01-01\u005400:00:00Z","l\u0069ne":"hello"}`, []string{"hello"}},
		{"surrogate pair", recordWithLine(`"\uD83d\uDe03"`), []string{"😃"}},
		{"surrogate endpoints", recordWithLine(`"\uD800\uDC00\uDBFF\uDFFF"`), []string{"\U00010000\U0010ffff"}},
		{"unpaired high surrogate", recordWithLine(`"a\uD800b"`), []string{"a�b"}},
		{"unpaired low surrogate", recordWithLine(`"a\uDC00b"`), []string{"a�b"}},
		{"reversed surrogates", recordWithLine(`"\uDC00\uD800"`), []string{"��"}},
		{"unpaired high before pair", recordWithLine(`"\uD800\uD83D\uDE03"`), []string{"�😃"}},
		{"escaped surrogate spelling", recordWithLine(`"\\uD800"`), []string{`\uD800`}},
		{"duplicate rows stay ordered", sampleRecord + "\n" + recordWithLine(`"middle"`) + "\n" + sampleRecord, []string{"hello", "middle", "hello"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := make([]store.Row, len(tc.lines))
			for i, line := range tc.lines {
				want[i] = store.Row{TS: sampleTime, Line: line}
			}
			input := []byte(tc.input)
			assertRows(t, input, assertDecoderParity(t, input), want)
		})
	}
}

func malformedRecords() []struct{ name, record string } {
	return []struct{ name, record string }{
		{"empty object", "{}"},
		{"array", "[" + sampleRecord + "]"},
		{"top-level null", "null"},
		{"top-level string", `"hello"`},
		{"top-level number", "123"},
		{"missing timestamp", `{"line":"hello"}`},
		{"missing line", `{"ts":"2025-01-01T00:00:00Z"}`},
		{"case variant", strings.Replace(sampleRecord, `"ts"`, `"TS"`, 1)},
		{"unknown field", sampleRecord[:len(sampleRecord)-1] + `,"extra":""}`},
		{"duplicate timestamp", `{"ts":"2025-01-01T00:00:00Z","ts":"2025-01-01T00:00:00Z","line":"hello"}`},
		{"duplicate line", sampleRecord[:len(sampleRecord)-1] + `,"line":"hello"}`},
		{"escaped duplicate", sampleRecord[:len(sampleRecord)-1] + `,"lin\u0065":"hello"}`},
		{"unpaired surrogate key", strings.Replace(sampleRecord, `"line"`, `"lin\ud800"`, 1)},
		{"numeric key", strings.Replace(sampleRecord, `"line"`, "1", 1)},
		{"null line", recordWithLine("null")},
		{"boolean line", recordWithLine("false")},
		{"number line", recordWithLine("0")},
		{"array line", recordWithLine(`["hello"]`)},
		{"object line", recordWithLine(`{"nested":"hello"}`)},
		{"null timestamp", strings.Replace(sampleRecord, `"2025-01-01T00:00:00Z"`, "null", 1)},
		{"missing colon", strings.Replace(sampleRecord, `"line":`, `"line"`, 1)},
		{"missing comma", strings.Replace(sampleRecord, `,"line"`, `"line"`, 1)},
		{"trailing comma", sampleRecord[:len(sampleRecord)-1] + ",}"},
		{"trailing object", sampleRecord + sampleRecord},
		{"trailing scalar", sampleRecord + " 0"},
		{"trailing punctuation", sampleRecord + ","},
		{"trailing Unicode whitespace", sampleRecord + "\u00a0"},
		{"leading non-JSON whitespace", "\f" + sampleRecord},
		{"record split across lines", strings.Replace(sampleRecord, ",", ",\n", 1)},
		{"raw newline inside string", recordWithLine("\"a\nb\"")},
		{"raw tab inside string", recordWithLine("\"a\tb\"")},
		{"raw NUL inside string", recordWithLine("\"a\x00b\"")},
		{"invalid escape", recordWithLine(`"\x41"`)},
		{"truncated escape", recordWithLine(`"\u00"`)},
		{"invalid hex escape", recordWithLine(`"\u00zz"`)},
		{"invalid UTF-8", recordWithLine("\"\xff\"")},
		{"raw UTF-8 surrogate", recordWithLine("\"\xed\xa0\x80\"")},
		{"UTF-8 BOM", "\ufeff" + sampleRecord},
	}
}

func TestDecodeMalformedBatches(t *testing.T) {
	t.Parallel()
	for _, tc := range malformedRecords() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// A malformed record anywhere must reject the entire batch.
			for _, batch := range []string{
				tc.record,
				tc.record + "\n" + sampleRecord,
				sampleRecord + "\r\n" + tc.record + "\n" + sampleRecord,
				sampleRecord + "\n" + tc.record,
			} {
				if rows := assertDecoderParity(t, []byte(batch)); rows != nil {
					t.Fatalf("accepted malformed batch %.256q", batch)
				}
			}
		})
	}
	for _, batch := range []string{"", "\n", " \t\r\n\u00a0\n", "\u2003\u0085"} {
		if rows := assertDecoderParity(t, []byte(batch)); rows != nil {
			t.Fatalf("accepted batch with no records %q", batch)
		}
	}
}

func TestDecodeTimestampBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		timestamp string
		wantUTC   string
	}{
		{"0001-01-01T00:00:00Z", "0001-01-01T00:00:00Z"},
		{"0001-01-01T00:01:00+00:01", "0001-01-01T00:00:00Z"},
		{"0001-01-01T00:00:00+00:01", ""},
		{"0000-12-31T23:59:59-00:01", "0001-01-01T00:00:59Z"},
		{"0000-12-31T23:59:59Z", ""},
		{"9999-12-31T23:59:59.999999998Z", "9999-12-31T23:59:59.999999998Z"},
		{"9999-12-31T23:59:59.999999999Z", ""},
		{"9999-12-31T23:58:59.999999999-00:01", ""},
		{"9999-12-31T23:59:59-00:01", ""},
		{"2000-02-29T01:02:03.004005006+23:59", "2000-02-28T01:03:03.004005006Z"},
		{"1900-02-29T01:02:03Z", ""},
		{"2024-12-31T23:59:59.1-00:30", "2025-01-01T00:29:59.1Z"},
		{"2025-01-01T00:00:00-00:00", sampleTimestamp},
		{"2025-01-01T00:00:00.1234567890Z", ""},
		{"2025-01-01T00:00:00,1Z", ""},
		{"2025-01-01T00:00:00+24:00", ""},
		{"2025-01-01T00:00:00-00:60", ""},
		{"2025-01-01T00:00:00+0000", ""},
		{"2025-01-01T00:00:00", ""},
		{"2025-01-01t00:00:00z", ""},
		{"2025-01-01T0:00:00Z", ""},
		{"2025-01-01T24:00:00Z", ""},
		{"2025-01-01T00:00:60Z", ""},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.timestamp, func(t *testing.T) {
			t.Parallel()
			for _, input := range []string{
				`{"ts":"` + tc.timestamp + `","line":"hello"}`,
				`{"line":"h\u0065llo","t\u0073":"` + tc.timestamp + `"}`,
			} {
				rows := assertDecoderParity(t, []byte(input))
				if tc.wantUTC == "" {
					if rows != nil {
						t.Fatalf("accepted invalid timestamp in %q", input)
					}
					continue
				}
				wantTS, err := time.Parse(time.RFC3339Nano, tc.wantUTC)
				if err != nil {
					t.Fatal(err)
				}
				assertRows(t, []byte(input), rows, []store.Row{{TS: wantTS, Line: "hello"}})
			}
		})
	}
}

func TestDecodeBatchSize(t *testing.T) {
	t.Parallel()
	const head = `{"ts":"2025-01-01T00:00:00Z","line":"`
	const tail = `"}`
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("limit%+d", delta), func(t *testing.T) {
			size := store.MaxBatch + delta
			line := strings.Repeat("x", size-len(head)-len(tail))
			input := []byte(head + line + tail)
			rows := assertDecoderParity(t, input)
			if delta > 0 {
				if rows != nil {
					t.Fatal("accepted oversized batch")
				}
				return
			}
			assertRows(t, input, rows, []store.Row{
				{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: line},
			})
		})
	}

	// The limit includes blank lines and the physical record terminator.
	input := bytes.Repeat([]byte{'\n'}, store.MaxBatch-len(sampleRecord))
	input = append(input, sampleRecord...)
	if rows := assertDecoderParity(t, input); len(rows) != 1 {
		t.Fatal("batch padded to exactly the limit did not return its row")
	}
	if rows := assertDecoderParity(t, append(input, '\n')); rows != nil {
		t.Fatal("accepted oversized batch with extra line terminator")
	}
}

func TestDecodeConcurrent(t *testing.T) {
	t.Parallel()
	data := []byte(
		`{"ts":"2025-01-01T00:00:00.123456789Z","line":"first"}` + "\r\n" +
			`{"line":"second\n\uD83D\uDE03","t\u0073":"2024-11-30T00:00:00+01:00"}` + "\n\u00a0\n" +
			sampleRecord,
	)
	invalid := append(bytes.Clone(data), "\n{}"...)
	original := bytes.Clone(data)
	want, err := referenceDecode(data)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	const iterations = 24
	// The enclosing subtest joins all parallel workers before input is checked.
	t.Run("shared input", func(t *testing.T) {
		for worker := range workers {
			t.Run(fmt.Sprintf("worker-%d", worker), func(t *testing.T) {
				t.Parallel()
				for range iterations {
					assertRows(t, data, assertDecoderParity(t, data), want)
					assertDecoderParity(t, invalid)
				}
			})
		}
	})
	if !bytes.Equal(data, original) {
		t.Fatal("decode modified shared input")
	}

	// The caller may reuse its receive buffer after Decode returns.
	reusable := bytes.Clone(data)
	rows := assertDecoderParity(t, reusable)
	clear(reusable)
	assertRows(t, data, rows, want)
}
