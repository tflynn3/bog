package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestColdV1LiveDecodeBuffers(t *testing.T) {
	t.Parallel()
	makeRow := func(seconds, size int, char string, ordinal uint64) checkpoint.BlockRow {
		return checkpoint.BlockRow{
			Row:     store.Row{TS: epoch.Add(time.Duration(seconds) * time.Second), Line: strings.Repeat(char, size)},
			Ordinal: ordinal,
		}
	}
	rows := []checkpoint.BlockRow{makeRow(0, 4600, "a", 0), makeRow(1, 7000, "b", 1), makeRow(2, 4200, "c", 2)}
	dir := t.TempDir()
	var segments []checkpoint.Segment
	for i, run := range [][]checkpoint.BlockRow{rows[:2], rows[2:]} {
		data, err := checkpoint.EncodeBlock(run)
		if err != nil {
			t.Fatal(err)
		}
		name := []string{"first", "second"}[i]
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
		segments = append(segments, checkpoint.Segment{Name: name, Size: int64(len(data))})
	}
	end := epoch.Add(3 * time.Second)
	for _, tc := range []struct {
		name        string
		q           query
		peak, reads int
	}{
		{"dense", query{start: epoch, end: end}, 2*coldV1PageBytes + 4600 + 7000, 3*coldV1PageBytes + 4600 + 7000 + 4200},
		{"sparse literal still checks every line", query{start: epoch, end: end, literal: "ccc"}, 2*coldV1PageBytes + 4600 + 7000, 3*coldV1PageBytes + 4600 + 7000 + 4200},
		{"bounded scan", query{start: rows[2].Row.TS, end: end, literal: "ccc"}, coldV1PageBytes + 4200, coldV1PageBytes + 4200},
		{"disjoint scan", query{start: end, end: end.Add(time.Second)}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := measureColdV1(t.Context(), dir, segments, tc.q, reference(rows, tc.q))
			if err != nil || got.peak != tc.peak || got.readBytes != int64(tc.reads) {
				t.Fatalf("accounting = %+v, %v, want peak %d, read %d", got, err, tc.peak, tc.reads)
			}
		})
	}
	q := query{start: epoch, end: end}
	if got, err := measureColdV1(t.Context(), dir, segments, q, rows[1:]); err == nil || got != (coldV1Buffers{}) {
		t.Fatalf("oracle mismatch produced evidence %+v, %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := measureColdV1(ctx, dir, segments, q, rows); !errors.Is(err, context.Canceled) || got != (coldV1Buffers{}) {
		t.Fatalf("canceled scan produced evidence %+v, %v", got, err)
	}
}
