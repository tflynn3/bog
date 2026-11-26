package ingest_test

import (
	"encoding/json"
	"testing"
)

// FuzzDecodeDifferential mutates complete batches, including invalid UTF-8 and
// arbitrary framing bytes. Its seed corpus also runs under ordinary go test.
func FuzzDecodeDifferential(f *testing.F) {
	for _, batch := range []string{
		"",
		"\xff\x00\x80",
		"\n\r\n\t",
		"\u0085\u2003\u00a0\n" + sampleRecord,
		sampleRecord,
		sampleRecord + "\n",
		"\t " + sampleRecord + "\r\n\r\n" + sampleRecord,
		`{"line":"café","ts":"0001-01-01T00:00:00Z"}`,
		`{"t\u0073":"2025-01-01\u005400:00:00Z","\u006cine":"escaped member names"}`,
		`{"ts":"9999-12-31T23:59:59.999999998Z","line":"valid end"}`,
		`{"ts":"9999-12-31T23:58:59.999999999-00:01","line":"reserved end"}`,
		`{"ts":"0001-01-01T00:00:00+23:59","line":"UTC underflow"}`,
		recordWithLine(`"\uD800\uDC00\uDBff\udFfF"`),
		recordWithLine(`"a\uD800b\uDFFFc"`),
		recordWithLine(`"\\uD800 \n \r \t \u0000 \\ \" \/"`),
		recordWithLine(`"literal � and 😃"`),
		recordWithLine("\"a\u2028b\u2029c\""),
		sampleRecord + "\n" + recordWithLine(`"\uD83D\uDE03"`) + "\n" + sampleRecord,
		sampleRecord + "\n" + `{"ts":null,"line":"invalid second row"}`,
		sampleRecord + "\n" + sampleRecord[:len(sampleRecord)-1],
	} {
		f.Add([]byte(batch))
	}
	for _, tc := range malformedRecords() {
		f.Add([]byte(tc.record))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		assertDecoderParity(t, data)
	})
}

// FuzzDecodeStructured keeps enough record structure intact for mutations to
// reach member decoding. Both strings are also used as raw JSON fragments in
// some layouts so mutations can introduce invalid escape sequences and types.
func FuzzDecodeStructured(f *testing.F) {
	seeds := []struct {
		line      string
		timestamp string
		layout    uint8
	}{
		{"plain", sampleTimestamp, 0},
		{"cafè 😃", "2025-01-01T01:30:00+01:30", 1},
		{"a\r\nb\x00\t\"\\", sampleTimestamp, 2 | structuredBatch},
		{`"\uD83d\uDe03"`, sampleTimestamp, 3},
		{`"\uD800"`, sampleTimestamp, 3 | structuredBatch},
		{`"\uDC00"`, sampleTimestamp, 3},
		{`"\\uD800"`, sampleTimestamp, 3},
		{"null", sampleTimestamp, 3},
		{"number", "false", 4},
		{"escaped timestamp", `"2025-01-01\u005400:00:00Z"`, 4},
		{"duplicate", sampleTimestamp, 5},
		{"duplicate", sampleTimestamp, 6 | structuredBatch},
		{"missing line", sampleTimestamp, 7},
		{"\xff\xc0\x80", sampleTimestamp, 8 | structuredBatch},
		{"nested", sampleTimestamp, 9},
		{"trailing document", sampleTimestamp, 10},
		{"trailing whitespace", sampleTimestamp, 11},
		{"first instant", "0001-01-01T00:00:00Z", 0},
		{"last usable instant", "9999-12-31T23:59:59.999999998Z", 1},
		{"reserved instant", "9999-12-31T23:59:59.999999999Z", 2},
		{"too precise", "2025-01-01T00:00:00.1234567890Z", 0},
		{"bad zone", "2025-01-01T00:00:00+24:00", 1},
		{"pinned timestamp", "", 0 | structuredFixedTimestamp},
	}
	for _, seed := range seeds {
		f.Add(seed.line, seed.timestamp, seed.layout)
	}
	f.Fuzz(func(t *testing.T, line, timestamp string, layout uint8) {
		// Exercise oversized batches in TestDecodeBatchSize. Cap generated
		// string inputs before JSON escaping can multiply their allocation.
		if len(line) > 64<<10 || len(timestamp) > 64<<10 {
			return
		}
		assertDecoderParity(t, buildStructuredBatch(t, line, timestamp, layout))
	})
}

const (
	structuredBatch          uint8 = 1 << 4
	structuredFixedTimestamp uint8 = 1 << 5
)

func buildStructuredBatch(t *testing.T, line, timestamp string, layout uint8) []byte {
	t.Helper()
	if layout&structuredFixedTimestamp != 0 {
		timestamp = sampleTimestamp
	}
	quotedTS, err := json.Marshal(timestamp)
	if err != nil {
		t.Fatal(err)
	}
	quotedLine, err := json.Marshal(line)
	if err != nil {
		t.Fatal(err)
	}
	ts, value := string(quotedTS), string(quotedLine)
	record := `{"ts":` + ts + `,"line":` + value + "}"
	switch layout & 0x0f {
	case 1:
		record = "{ \t\"line\" : " + value + ",\r \"ts\" : " + ts + " \t}"
	case 2:
		record = `{"\u0074s":` + ts + `,"l\u0069ne":` + value + "}"
	case 3:
		record = `{"ts":` + ts + `,"line":` + line + "}"
	case 4:
		record = `{"ts":` + timestamp + `,"line":` + value + "}"
	case 5:
		record = `{"ts":` + ts + `,"line":` + value + `,"l\u0069ne":` + value + "}"
	case 6:
		record = `{"ts":` + ts + `,"t\u0073":` + ts + `,"line":` + value + "}"
	case 7:
		record = `{"ts":` + ts + "}"
	case 8:
		record = `{"ts":` + ts + `,"line":"` + line + `"}`
	case 9:
		record = `{"ts":` + ts + `,"line":[` + value + "]}"
	case 10:
		record += " " + sampleRecord
	case 11:
		record += "\u2003"
	}
	if layout&structuredBatch != 0 {
		record = "\r\n" + sampleRecord + "\r\n\u00a0\n" + record +
			"\n" + recordWithLine(`"tail\nrow"`) + "\r\n"
	}
	return []byte(record)
}

func TestDecodeStructuredCorpus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		layout uint8
		line   string
		want   string
	}{
		{"plain", 0, "plain", "plain"},
		{"reordered", 1, "plain", "plain"},
		{"escaped keys", 2, "a\r\nb", "a\r\nb"},
		{"literal pair", 3, `"\uD800\uDC00"`, "\U00010000"},
		{"raw invalid UTF-8", 8, "\xff", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, flags := range []uint8{structuredFixedTimestamp, structuredFixedTimestamp | structuredBatch} {
				data := buildStructuredBatch(t, tc.line, "ignored", tc.layout|flags)
				rows := assertDecoderParity(t, data)
				if tc.want == "" {
					if rows != nil {
						t.Fatal("accepted malformed generated record")
					}
					continue
				}
				wantCount, index := 1, 0
				if flags&structuredBatch != 0 {
					wantCount, index = 3, 1
				}
				if len(rows) != wantCount || rows[index].Line != tc.want {
					t.Fatalf("generated batch %.256q returned %v", data, rows)
				}
			}
		})
	}
}

func TestDecodeTruncatedRecordAfterValidRow(t *testing.T) {
	t.Parallel()
	const prefix = sampleRecord + "\r\n"
	for _, record := range []string{sampleRecord, recordWithLine(`"é\uD83D\uDE03\n"`)} {
		input := []byte(prefix + record)
		for end := len(prefix) + 1; end < len(input); end++ {
			if rows := assertDecoderParity(t, input[:end]); rows != nil {
				t.Fatalf("accepted truncated batch at byte %d: %q", end, input[:end])
			}
		}
		if rows := assertDecoderParity(t, input); len(rows) != 2 {
			t.Fatalf("complete batch returned %d rows", len(rows))
		}
	}
}
