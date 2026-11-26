package main

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func row(ts time.Time, ordinal uint64, line string) checkpoint.BlockRow {
	return checkpoint.BlockRow{Row: store.Row{TS: ts, Line: line}, Ordinal: ordinal}
}

func TestSummaries(t *testing.T) {
	rows := []checkpoint.BlockRow{row(epoch, 0, "abcdeé😀abc"), row(epoch, 1, "only")}
	summary := summarize(rows)
	for _, r := range rows {
		for i := range len(r.Row.Line) {
			for j := i + 1; j <= len(r.Row.Line); j++ {
				literal := r.Row.Line[i:j]
				if !mayContain(summary, literal) {
					t.Errorf("pruned contained bytes %q", literal)
				}
			}
		}
	}
	short := summarize([]checkpoint.BlockRow{row(epoch, 0, "abc")})
	for _, tc := range []struct {
		name    string
		literal string
		summary []byte
		want    bool
	}{
		{"absent", "abcdf", summary, false},
		{"case sensitive", "ABCD", summary, false},
		{"row boundary", "abconly", summary, false},
		{"empty probe", "", short, true},
		{"short probe", "abc", short, true},
		{"empty summary", "abcd", short, false},
		{"overflow", "anything", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mayContain(tc.summary, tc.literal); got != tc.want {
				t.Errorf("mayContain = %v, want %v", got, tc.want)
			}
		})
	}

	// Each four-byte line contributes one distinct window.
	for _, n := range []int{summaryBudget / 4, summaryBudget/4 + 1} {
		var rows []checkpoint.BlockRow
		for i := range n {
			rows = append(rows, row(epoch, uint64(i), fmt.Sprintf("%04x", i)))
		}
		got := summarize(rows)
		if n == summaryBudget/4 && len(got) != summaryBudget || n > summaryBudget/4 && got != nil {
			t.Fatalf("%d grams produced %d summary bytes, nil %v", n, len(got), got == nil)
		}
	}
}

func TestQueries(t *testing.T) {
	upper := epoch.Add(time.Second)
	tied := row(epoch, 1, "abcde é GET")
	rows := []checkpoint.BlockRow{
		row(upper, 0, "abcde é GET"),
		tied,
		row(epoch, 2, "abcd"),
		row(epoch, 3, "bcde"),
		row(epoch.In(time.FixedZone("west", -7*3600)), 4, tied.Row.Line),
		row(first, 5, "GET"),
		row(end.Add(-time.Nanosecond), 6, "é GET"),
		row(epoch, 7, "abc"),
	}
	blocks := partition(rows, 2)
	p := probe{"literal", "abcde", epoch, upper}
	for kind, name := range layouts {
		t.Run(name, func(t *testing.T) {
			f := openBytes(t, encode(t, layout(kind), blocks))
			for _, probe := range append(slices.Clone(probes), p, probe{"partial UTF-8", "é"[:1], first, end}) {
				matches, stats, err := query(f, layout(kind), probe)
				if err != nil || !slices.EqualFunc(matches, oracle(rows, probe), equalRow) {
					t.Fatalf("%s query got %v, err %v", probe.Name, matches, err)
				}
				if probe == p {
					// The second block is a summary candidate even though neither
					// row contains the complete literal. The upper bound excludes ordinal 0.
					wantReads := 4
					if layout(kind)&summarized != 0 {
						wantReads = 3
					}
					if len(matches) != 2 || matches[0].Ordinal != 1 || matches[1].Ordinal != 4 || stats.BlocksRead != wantReads {
						t.Fatalf("half-open query got %v, stats %+v", matches, stats)
					}
				}
			}
		})
	}
}

func TestDecompressBounds(t *testing.T) {
	raw := bytes.Repeat([]byte("abcd"), checkpoint.MaxBlockSize/4-1)
	var buf bytes.Buffer
	z := zlib.NewWriter(&buf)
	if _, err := z.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	payload := buf.Bytes()
	for _, tc := range []struct {
		name    string
		data    []byte
		size    uint32
		wantErr bool
	}{
		{"exact", payload, uint32(len(raw)), false},
		{"zero", payload, 0, true},
		{"over limit", payload, checkpoint.MaxBlockSize + 1, true},
		{"expansion bomb", payload, 100, true},
		{"underflow", payload, uint32(len(raw) + 1), true},
		{"truncated trailer", payload[:len(payload)-1], uint32(len(raw)), true},
		{"trailing bytes", append(bytes.Clone(payload), 0), uint32(len(raw)), true},
		{"second stream", append(bytes.Clone(payload), payload...), uint32(len(raw)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decompress(tc.data, tc.size)
			if tc.wantErr {
				if err == nil || got != nil {
					t.Fatalf("invalid stream returned %d bytes and %v", len(got), err)
				}
			} else if err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("valid stream returned %d bytes and %v", len(got), err)
			}
		})
	}
}

func TestCorruption(t *testing.T) {
	kind := compressed | summarized
	blocks := [][]checkpoint.BlockRow{{row(epoch, 0, "abcde")}, {row(epoch, 1, "WXYZ")}}
	valid := encode(t, kind, blocks)
	hdr := fileHeader
	payload := hdr + blockHeader + int(order.Uint32(valid[hdr+8:]))
	block2 := payload + int(order.Uint32(valid[hdr:]))
	fixMeta := func(b []byte) {
		order.PutUint32(b[hdr+16:], crc32.Update(checksum(b[hdr:hdr+16]), crcTable, b[hdr+blockHeader:payload]))
	}
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"short file header", func(b []byte) []byte { return b[:fileHeader-1] }},
		{"header checksum", func(b []byte) []byte { b[16] ^= 1; return b }},
		{"wrong format version", func(b []byte) []byte {
			b[7] = '2'
			order.PutUint32(b[16:], checksum(b[:16]))
			return b
		}},
		{"short block header", func(b []byte) []byte { return b[:block2+blockHeader-1] }},
		{"truncated summary", func(b []byte) []byte { return b[:payload-1] }},
		{"missing last block", func(b []byte) []byte { return b[:block2] }},
		{"truncated payload", func(b []byte) []byte { return b[:len(b)-1] }},
		{"trailing bytes", func(b []byte) []byte { return append(b, 0) }},
		{"summary checksum", func(b []byte) []byte { b[hdr+blockHeader] ^= 1; return b }},
		{"summary length", func(b []byte) []byte { order.PutUint32(b[hdr+8:], summaryBudget+4); return b }},
		{"summary unsorted", func(b []byte) []byte {
			copy(b[hdr+blockHeader:], b[hdr+blockHeader+4:payload])
			fixMeta(b)
			return b
		}},
		{"summary semantically wrong", func(b []byte) []byte {
			b[hdr+blockHeader] = 0
			fixMeta(b)
			return b
		}},
		{"oversized stored length", func(b []byte) []byte { order.PutUint32(b[hdr:], maxStored+1); fixMeta(b); return b }},
		{"oversized raw length", func(b []byte) []byte {
			order.PutUint32(b[hdr+4:], checkpoint.MaxBlockSize+1)
			fixMeta(b)
			return b
		}},
		{"payload checksum", func(b []byte) []byte { b[payload] ^= 1; return b }},
		{"invalid zlib with valid payload CRC", func(b []byte) []byte {
			b[payload] ^= 1
			order.PutUint32(b[hdr+12:], checksum(b[payload:block2]))
			fixMeta(b)
			return b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := openBytes(t, tc.change(bytes.Clone(valid)))
			if _, err := scan(f, kind, "absent", true, func(int, []checkpoint.BlockRow) error { return nil }); err == nil {
				t.Fatal("full verifier accepted invalid file")
			}
		})
	}
	t.Run("verifies pruned payload", func(t *testing.T) {
		b := bytes.Clone(valid)
		b[payload] ^= 1
		f := openBytes(t, b)
		_, stats, err := query(f, kind, probe{"absent", "absent", first, end})
		if err != nil || stats.BlocksRead != 0 {
			t.Fatalf("pruned query got stats %+v, err %v", stats, err)
		}
		_, err = scan(f, kind, "", true, func(int, []checkpoint.BlockRow) error { return nil })
		if err == nil {
			t.Fatal("full verifier missed pruned corruption")
		}
		b[hdr+blockHeader] ^= 1
		f = openBytes(t, b)
		if _, _, err := query(f, kind, probe{"absent", "absent", first, end}); err == nil {
			t.Fatal("query pruned using corrupt metadata")
		}
	})
}

func TestReport(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out, t.TempDir(), 16, 4); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Layouts []layoutResult `json:"layouts"`
		Blocks  int            `json:"blocks"`
		Rows    int            `json:"rows"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Layouts) != len(layouts) || report.Blocks != 5 || report.Rows != 17 {
		t.Fatalf("unexpected fixture report %+v", report)
	}
	for i, r := range report.Layouts {
		if r.Layout != layouts[i] || r.Rewrite != r.Retained || r.Retained <= 0 ||
			r.Verified.BlocksRead != report.Blocks || len(r.Queries) != len(probes) {
			t.Fatalf("invalid layout report %+v", r)
		}
		for j, q := range r.Queries {
			if q.Cache == "" || q.Probe != probes[j].Name || q.FirstReadNS <= 0 ||
				q.Matches != report.Layouts[0].Queries[j].Matches {
				t.Fatalf("invalid query report %+v", q)
			}
			if layout(i)&summarized != 0 && (q.Probe == "rare" || q.Probe == "absent" || q.Probe == "oversized") {
				if q.BlocksRead >= report.Blocks || (q.Probe != "rare" && q.BlocksRead != 1) {
					t.Fatalf("expected pruning with oversized overflow fallback, got %+v", q)
				}
			} else if q.BlocksRead != report.Blocks {
				t.Fatalf("expected full scan, got %+v", q)
			}
		}
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestShortWrite(t *testing.T) {
	if n, err := writeLayout(shortWriter{}, 0, [][]checkpoint.BlockRow{{row(epoch, 0, "test")}}); n != fileHeader-1 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeLayout returned %d, %v", n, err)
	}
}

func encode(t *testing.T, kind layout, blocks [][]checkpoint.BlockRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := writeLayout(&buf, kind, blocks); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openBytes(t *testing.T, data []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "layout")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}
