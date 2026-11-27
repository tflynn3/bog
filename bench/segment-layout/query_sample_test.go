package main

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestQuerySampleRequiresExactOrderedStream(t *testing.T) {
	t.Parallel()
	wide := strings.Repeat("x", 8192)
	want := []checkpoint.BlockRow{
		{Row: store.Row{TS: epoch, Line: wide + "é"}, Ordinal: 4},
		{Row: store.Row{TS: epoch, Line: wide + "é"}, Ordinal: 8},
	}
	metrics := coldSample{ElapsedNS: 10, ReadBytes: 30000, StorageReadBytes: 0, PeakRSSBytes: 12345}
	for _, tc := range []struct {
		name string
		rows func() []checkpoint.BlockRow
		ok   bool
	}{
		{"duplicate lines with separate ordinals", func() []checkpoint.BlockRow { return want }, true},
		{"reordered tie", func() []checkpoint.BlockRow { return []checkpoint.BlockRow{want[1], want[0]} }, false},
		{"dropped duplicate", func() []checkpoint.BlockRow { return want[:1] }, false},
		{"extra row", func() []checkpoint.BlockRow { return append(slices.Clone(want), want[1]) }, false},
		{"same length with different bytes beyond multiple chunks", func() []checkpoint.BlockRow {
			rows := slices.Clone(want)
			rows[0].Row.Line = wide + "è"
			return rows
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			if err := writeQuerySample(&output, metrics, tc.rows()); err != nil {
				t.Fatal(err)
			}
			var decoded coldSample
			if err := readQuerySample(bytes.NewReader(output.Bytes()), want, &decoded); (err == nil) != tc.ok {
				t.Fatalf("readQuerySample = %v, want success %t", err, tc.ok)
			}
			if tc.ok && decoded != metrics {
				t.Fatalf("decoded metrics %+v, want %+v", decoded, metrics)
			}
		})
	}
	var output bytes.Buffer
	if err := writeQuerySample(&output, metrics, want); err != nil {
		t.Fatal(err)
	}
	data := output.Bytes()
	if err := readQuerySample(bytes.NewReader(data[:len(data)-1]), want, &coldSample{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("partial line returned %v", err)
	}
}
