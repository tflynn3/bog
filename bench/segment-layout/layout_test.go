package main

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestSummary(t *testing.T) {
	lines := []string{"abcd", "efgh", "café 🙂", "abc\x00def", "aaaaaa", "Seven"}
	var s summary
	for _, line := range lines {
		s.grams(line, true)
		for start := range len(line) {
			for end := start; end <= len(line); end++ {
				if !s.grams(line[start:end], false) {
					t.Fatalf("rejected byte substring %q", line[start:end])
				}
			}
		}
	}
	for _, literal := range []string{"bcde", "seven", "zzzz"} {
		if s.grams(literal, false) {
			t.Errorf("unexpected candidate for %q", literal)
		}
	}
	rng := rand.New(rand.NewPCG(6, 44))
	random := make([]byte, 4096)
	for i := range random {
		random[i] = byte(rng.Uint32())
	}
	s.grams(string(random), true)
	for i := 0; i+17 <= len(random); i++ {
		if !s.grams(string(random[i:i+17]), false) {
			t.Fatalf("rejected random substring at %d", i)
		}
	}
}

func packed(t *testing.T, l layout, blocks [][]checkpoint.BlockRow) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "layout")
	n, err := packFile(path, l, blocks)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || int64(len(data)) != n {
		t.Fatalf("read %d bytes after writing %d: %v", len(data), n, err)
	}
	return data
}

func TestQueryParity(t *testing.T) {
	rows, err := fixture(6000, store.MaxBatch)
	if err != nil {
		t.Fatal(err)
	}
	blocks := partition(rows)
	for _, codec := range []compression{raw, lz4Block, zstdBlock} {
		for _, withSummary := range []bool{false, true} {
			l := layout{codec, withSummary}
			t.Run(l.name(), func(t *testing.T) {
				t.Parallel()
				data := packed(t, l, blocks)
				for _, q := range queries {
					t.Run(q.name, func(t *testing.T) {
						var got []checkpoint.BlockRow
						stats, err := scan(bytes.NewReader(data), int64(len(data)), q, false, func(row checkpoint.BlockRow) error {
							got = append(got, row)
							return nil
						})
						if want := reference(rows, q); err != nil || !slices.Equal(got, want) {
							t.Fatalf("got %d rows, want %d: %v", len(got), len(want), err)
						}
						switch q.name {
						case "rare", "absent", "oversized":
							if withSummary != (stats.BlocksRead < len(blocks)) {
								t.Fatalf("read %d of %d blocks", stats.BlocksRead, len(blocks))
							}
						case "all", "one-byte", "two-byte", "three-byte":
							if stats.BlocksRead != len(blocks) {
								t.Fatalf("short or empty literal read %d of %d blocks", stats.BlocksRead, len(blocks))
							}
						}
					})
				}
			})
		}
	}
}

func TestTruncationAndCorruption(t *testing.T) {
	line := strings.Repeat("abcdef", 50)
	blocks := [][]checkpoint.BlockRow{{{Row: store.Row{TS: epoch, Line: line}}}}
	all, absent := queries[len(queries)-1], queries[1]
	for _, codec := range []compression{raw, lz4Block, zstdBlock} {
		l := layout{codec, true}
		t.Run(l.name(), func(t *testing.T) {
			data := packed(t, l, blocks)
			for n := range len(data) {
				for _, q := range []query{all, absent} {
					_, err := scan(bytes.NewReader(data[:n]), int64(n), q, false, nil)
					if err == nil {
						t.Fatalf("accepted %d-byte prefix with query %s", n, q.name)
					}
				}
			}
			for _, offset := range []int{0, 8, fileHeaderSize + 24, fileHeaderSize + blockHeaderSize, len(data) - 1} {
				bad := slices.Clone(data)
				bad[offset] ^= 1
				stats, err := scan(bytes.NewReader(bad), int64(len(bad)), all, false, nil)
				if !errors.Is(err, errLayout) || stats.Rows != 0 {
					t.Fatalf("corruption at offset %d returned %+v, %v", offset, stats, err)
				}
			}
			bad := slices.Clone(data)
			meta := bad[fileHeaderSize : fileHeaderSize+blockHeaderSize+summaryBytes]
			le.PutUint32(meta[24:], checkpoint.MaxBlockSize+1)
			le.PutUint32(meta[36:], checksum(append(slices.Clone(meta[:36]), meta[blockHeaderSize:]...)))
			if _, err := scan(bytes.NewReader(bad), int64(len(bad)), absent, false, nil); !errors.Is(err, errLayout) {
				t.Fatalf("accepted oversized raw length: %v", err)
			}
			if _, err := scan(bytes.NewReader(data), int64(len(data))+1, all, false, nil); !errors.Is(err, errLayout) {
				t.Fatalf("accepted inconsistent file size: %v", err)
			}
		})
	}
}

func TestReferenceMatchesRecoveredStore(t *testing.T) {
	rows, err := fixture(6000, 320<<10)
	if err != nil {
		t.Fatal(err)
	}
	batch := make([]store.Row, len(rows))
	for i, row := range rows {
		batch[i] = row.Row
	}
	builder := store.NewRecoveryBuilder()
	builder.Add(batch[:6000])
	builder.Add(batch[6000:])
	var recovered store.Store
	builder.Install(&recovered)
	for _, q := range queries {
		want := reference(rows, q)
		got := recovered.Query(q.start, q.end, q.literal, len(rows))
		if len(got) != len(want) {
			t.Fatalf("%s: got %d rows, want %d", q.name, len(got), len(want))
		}
		for i, row := range got {
			if row != want[i].Row {
				t.Fatalf("%s: result %d differs", q.name, i)
			}
		}
	}
}
