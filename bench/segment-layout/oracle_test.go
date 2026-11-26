package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"bog/internal/checkpoint"
)

func TestStreamingOracle(t *testing.T) {
	input := fixture(4, false)
	q := queries()[0]
	want := reference(input.rows, q)
	path := filepath.Join(t.TempDir(), "expected")
	if err := writeOracle(t.Context(), path, len(input.rows), want, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		change  func([]byte, []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow)
		wantErr bool
	}{
		{"exact", func(b []byte, r []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow) { return b, r }, false},
		{"header CRC", func(b []byte, r []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow) { b[8] ^= 1; return b, r }, true},
		{"truncated", func(b []byte, r []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow) { return b[:len(b)-1], r }, true},
		{"trailing", func(b []byte, r []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow) { return append(b, 0), r }, true},
		{"wrong line", func(b []byte, r []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow) {
			b[oracleHeaderSize+oracleRowSize] ^= 1
			return b, r
		}, true},
		{"wrong ordinal", func(b []byte, r []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow) { r[1].Ordinal++; return b, r }, true},
		{"missing row", func(b []byte, r []checkpoint.BlockRow) ([]byte, []checkpoint.BlockRow) { return b, r[1:] }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, got := tc.change(slices.Clone(data), slices.Clone(want))
			expected, err := readOracle(bytes.NewReader(data), int64(len(data)))
			if err == nil {
				err = expected.verify(got)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("verify error = %v", err)
			}
		})
	}
}

func TestEachSampleChecksOracle(t *testing.T) {
	input := fixture(4, false)
	dir := t.TempDir()
	path := filepath.Join(dir, "raw")
	size, err := writeGeneration(t.Context(), path, partition(input.rows), layout{codec: rawCodec})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	q := queries()[1] // Equal timestamps and literal, distinct append ordinals.
	oraclePath := filepath.Join(dir, "expected")
	if err := writeOracle(t.Context(), oraclePath, len(input.rows), reference(input.rows, q), nil); err != nil {
		t.Fatal(err)
	}
	oracleData, err := os.ReadFile(oraclePath)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := readOracle(bytes.NewReader(oracleData), int64(len(oracleData)))
	if err != nil {
		t.Fatal(err)
	}

	samples := 0
	_, err = measureQuery(bytes.NewReader(data), size, q, expected, 3, func() error {
		samples++
		if samples == 2 {
			rowWidth := oracleRowSize + len(rareLiteral)
			first, second := oracleData[oracleHeaderSize:oracleHeaderSize+rowWidth], oracleData[oracleHeaderSize+rowWidth:]
			saved := slices.Clone(first)
			copy(first, second)
			copy(second, saved)
		}
		return nil
	})
	if err == nil || samples != 2 {
		t.Fatalf("oracle failed to catch wrong duplicate order after %d samples: %v", samples, err)
	}

	samples = 0
	_, err = measureQuery(bytes.NewReader(data), size, q, expected, 3, func() error {
		samples++
		return io.ErrClosedPipe
	})
	if !errors.Is(err, io.ErrClosedPipe) || samples != 1 {
		t.Fatalf("failed cache preparation should terminate after one attempt: %d, %v", samples, err)
	}
}
