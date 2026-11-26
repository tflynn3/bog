package main

import (
	"bytes"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
)

func TestSummaryNoFalseNegatives(t *testing.T) {
	lines := []string{"", "ab", "cd", "repeated repeated", "Καλημέρα 世界 €", "x\x00z🧪"}
	var s summary
	for _, line := range lines {
		s.add(line)
	}
	for _, line := range lines {
		for start := range len(line) + 1 {
			for end := start; end <= len(line); end++ {
				if !s.mayContain(line[start:end]) {
					t.Fatalf("lost byte substring %q of %q", line[start:end], line)
				}
			}
		}
	}
	for _, literal := range []string{"abcd", "unseen-error-8877"} {
		if s.mayContain(literal) {
			t.Errorf("expected fixture filter to prune %q", literal)
		}
	}
	for _, literal := range []string{"", "!", "é", "€"} {
		if !s.mayContain(literal) {
			t.Errorf("short literal %q must bypass the filter", literal)
		}
	}
}

func layoutFixture(t *testing.T, l layout, raw []byte) []byte {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "layout")
	if err != nil {
		t.Fatal(err)
	}
	var filter summary
	filter.add("expected-literal")
	writeErr := writeLayout(f, l, 1, func(int) ([]byte, summary, error) { return raw, filter, nil })
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("write %v, close %v", writeErr, closeErr)
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestLayoutRoundTripAndRejection(t *testing.T) {
	q := query{start: firstTime, end: endTime}
	for _, l := range layouts {
		t.Run(l.name(), func(t *testing.T) {
			raw := []byte(strings.Repeat("x", 2048))
			clear(raw[:blockHeader])
			data := layoutFixture(t, l, raw)
			stored := int(le.Uint32(data[headerSize+28:]))
			if (stored < len(raw)) != (l.codec != rawCodec) {
				t.Fatalf("unexpected stored length %d", stored)
			}
			visits := 0
			visit := func(got []byte, working int) error {
				visits++
				want := len(raw)
				if stored < len(raw) {
					want += stored
				}
				if !bytes.Equal(got, raw) || working != want {
					t.Fatal("wrong decoded bytes or buffer accounting")
				}
				return nil
			}
			n, err := scanBlocks(bytes.NewReader(data), int64(len(data)), q, visit)
			if err != nil || n.Blocks != 1 || n.Bytes != int64(stored) || n.DecodedBytes != int64(len(raw)) || visits != 1 {
				t.Fatalf("scan returned %+v reads, %d visits, %v", n, visits, err)
			}
			reject := func(data []byte, q query) {
				t.Helper()
				_, err := scanBlocks(bytes.NewReader(data), int64(len(data)), q, func([]byte, int) error {
					t.Fatal("exposed damaged block")
					return nil
				})
				if err == nil {
					t.Fatal("accepted damaged layout")
				}
			}
			for end := range len(data) {
				reject(data[:end], q)
			}
			if _, err := scanBlocks(bytes.NewReader(data[:len(data)-1]), int64(len(data)), q, visit); err == nil {
				t.Fatal("accepted a short payload read")
			}
			absent := q
			absent.literal = "absent-query"
			reject(data[:len(data)-1], absent)
			for _, field := range []int{headerSize + 24, headerSize + 28} {
				bad := bytes.Clone(data)
				le.PutUint32(bad[field:], 1<<32-1)
				seal(bad[headerSize : headerSize+l.indexWidth()])
				reject(bad, absent)
			}
			bad := bytes.Clone(data)
			bad[headerSize+l.indexWidth()-5] ^= 1
			reject(bad, absent)
		})
	}
}

func TestIncompressibleBlocksFallBackToRaw(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	raw := make([]byte, 4096)
	for i := range raw {
		raw[i] = byte(rng.Uint32())
	}
	for _, c := range []codec{lz4Codec, zstdCodec} {
		data := layoutFixture(t, layout{codec: c}, raw)
		if !bytes.Equal(data[headerSize+entrySize:], raw) {
			t.Errorf("codec %d expanded an incompressible block", c)
		}
	}
}
