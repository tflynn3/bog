package store

import (
	"reflect"
	"strings"
	"testing"
)

func TestDecodeIngestSurrogateCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, value, line string
		strict            bool
	}{
		{"literal replacement rune", `"�"`, "�", true},
		{"escaped replacement rune", `"\uFFFD"`, "�", true},
		{"surrogate pair", `"\uD83d\uDe03"`, "😃", true},
		{"pair endpoints", `"\uD800\uDC00\uDBFF\uDFFF"`, "\U00010000\U0010ffff", true},
		{"escaped backslash", `"\\uD800"`, `\uD800`, true},
		{"unpaired high at end", `"\uD800"`, "�", false},
		{"unpaired low", `"a\uDC00b"`, "a�b", false},
		{"reversed pair", `"\uDC00\uD800"`, "��", false},
		{"high followed by regular escape", `"\uD800\u0061"`, "�a", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte("\n\t\r\n" + `{"t\u0073":"2025-01-01T00:00:00Z","line":` + tc.value + "}")
			historical, err := Decode(data)
			if err != nil || len(historical) != 1 || historical[0].Line != tc.line {
				t.Fatalf("historical decode: rows=%v err=%v", historical, err)
			}
			strict, err := DecodeIngest(data)
			if tc.strict {
				if err != nil || !reflect.DeepEqual(strict, historical) {
					t.Fatalf("strict decode: rows=%v err=%v", strict, err)
				}
			} else if strict != nil || err == nil || err.Error() != "line 3: unpaired Unicode surrogate escape" {
				t.Fatalf("strict rejection: rows=%v err=%v", strict, err)
			}
		})
	}
}

func TestDecodeIngestReportsFirstInvalidPhysicalLine(t *testing.T) {
	data := []byte("\n" + `{"ts":"2025-01-01T00:00:00Z","line":"first"}` + "\r\n \n{}\n\xff")
	rows, err := DecodeIngest(data)
	if rows != nil || err == nil || err.Error() != "line 4: missing ts field" {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	rows, err = DecodeIngest([]byte(strings.Repeat("\n", MaxBatch+1)))
	if rows != nil || err == nil || err.Error() != "batch too large" {
		t.Fatalf("over limit: rows=%v err=%v", rows, err)
	}
}
