package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"slices"
	"testing"
)

func TestEachSampleChecksOracle(t *testing.T) {
	input := fixture(4, false)
	path := t.TempDir() + "/raw"
	size, err := writeGeneration(path, partition(input.rows), layout{codec: rawCodec})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	q := queries()[1] // Equal timestamps and literal, distinct append ordinals.
	want := slices.Clone(reference(input.rows, q))
	samples := 0
	_, err = measureQuery(bytes.NewReader(data), size, q, want, len(input.rows), 3, func() error {
		samples++
		if samples == 2 {
			want[0], want[1] = want[1], want[0]
		}
		return nil
	})
	if err == nil || samples != 2 {
		t.Fatalf("oracle failed to catch wrong duplicate order after %d samples: %v", samples, err)
	}

	samples = 0
	_, err = measureQuery(bytes.NewReader(data), size, q, reference(input.rows, q), len(input.rows), 3, func() error {
		samples++
		return io.ErrClosedPipe
	})
	if !errors.Is(err, io.ErrClosedPipe) || samples != 1 {
		t.Fatalf("failed cache preparation should terminate after one attempt: %d, %v", samples, err)
	}
}

func TestCorruptionDiscardsEarlierMatches(t *testing.T) {
	input := fixture(4, false)
	blocks := partition(input.rows)
	path := t.TempDir() + "/raw"
	size, err := writeGeneration(path, blocks, layout{codec: rawCodec, prune: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) < 2 {
		t.Fatal("fixture did not span enough blocks")
	}
	data[len(data)-1] ^= 1
	got, reads, peak, err := queryFile(bytes.NewReader(data), size, queries()[0])
	if err == nil || got != nil || reads != (payloadReads{}) || peak != 0 {
		t.Fatalf("corrupt final block exposed %d rows and metrics %+v/%d, error %v", len(got), reads, peak, err)
	}
}
