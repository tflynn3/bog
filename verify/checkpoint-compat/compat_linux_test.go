//go:build linux

package checkpointcompat_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

var batches = [][]byte{
	[]byte("{\"ts\":\"2025-05-02T12:00:00Z\",\"line\":\"duplicate\"}\n{\"ts\":\"2025-05-02T12:00:00Z\",\"line\":\"tie-a\"}\n"),
	[]byte("{\"ts\":\"2020-01-01T00:00:00.123456789Z\",\"line\":\"backfill\"}\n{\"ts\":\"2025-05-02T12:00:00Z\",\"line\":\"duplicate\"}\n"),
	[]byte("{\"ts\":\"2025-05-02T12:00:00Z\",\"line\":\"tie-b\"}\n{\"ts\":\"2020-01-01T00:00:00.123456789Z\",\"line\":\"backfill\"}\n"),
}

func openWriter(t *testing.T, dir string) *wal.Log {
	t.Helper()
	w, err := wal.Open(dir, &store.Store{})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	return w
}

func closeWriter(t *testing.T, w *wal.Log) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
}

func privateDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func populate(t *testing.T, w *wal.Log) [][]store.Row {
	t.Helper()
	var expected [][]store.Row
	for _, payload := range batches {
		rows, err := store.Decode(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(payload, rows); err != nil {
			t.Fatalf("append: %v", err)
		}
		expected = append(expected, rows)
	}
	return expected
}

func readFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestProductionWALScan(t *testing.T) {
	dir := privateDir(t)
	w := openWriter(t, dir)
	expected := populate(t, w)
	closeWriter(t, w)

	beforeWAL := readFile(t, dir, "bog.wal")
	beforeMarker := readFile(t, dir, "bog.commit")
	var end int64 = 8
	for _, payload := range batches {
		end += 16 + int64(len(payload))
	}
	if len(beforeMarker) != 24 || int64(binary.LittleEndian.Uint64(beforeMarker[8:16])) != end || int64(len(beforeWAL)) != end {
		t.Fatalf("unexpected production boundary: marker %x, WAL size %d, want %d", beforeMarker, len(beforeWAL), end)
	}

	scanner, err := checkpoint.OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	if got := scanner.CommittedOffset(); got != end {
		t.Fatalf("committed offset = %d, want %d", got, end)
	}
	frame := 0
	var offset int64 = 8
	var ordinal uint64
	err = scanner.Scan(func(start, next int64, first uint64, rows []store.Row) error {
		if frame >= len(expected) {
			t.Fatalf("unexpected frame %d", frame)
		}
		wantNext := offset + 16 + int64(len(batches[frame]))
		if start != offset || next != wantNext || first != ordinal {
			t.Fatalf("frame %d: offsets %d..%d ordinal %d; want %d..%d ordinal %d", frame, start, next, first, offset, wantNext, ordinal)
		}
		if !reflect.DeepEqual(rows, expected[frame]) {
			t.Fatalf("frame %d rows = %#v, want %#v", frame, rows, expected[frame])
		}
		for i := range rows {
			if !rows[i].TS.Equal(expected[frame][i].TS) {
				t.Fatalf("frame %d row %d timestamp = %v, want %v", frame, i, rows[i].TS, expected[frame][i].TS)
			}
		}
		offset, ordinal = next, ordinal+uint64(len(rows))
		frame++
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if frame != len(batches) || offset != end || ordinal != 6 {
		t.Fatalf("scan ended at frame %d, offset %d, ordinal %d", frame, offset, ordinal)
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeWAL, readFile(t, dir, "bog.wal")) || !bytes.Equal(beforeMarker, readFile(t, dir, "bog.commit")) {
		t.Fatal("scan changed production WAL or commit marker")
	}
}

func TestMutualExclusion(t *testing.T) {
	dir := privateDir(t)
	w := openWriter(t, dir)
	if scanner, err := checkpoint.OpenLegacy(dir); !errors.Is(err, syscall.EWOULDBLOCK) {
		if scanner != nil {
			scanner.Close()
		}
		t.Fatalf("scanner while writer holds lock: %v", err)
	}
	closeWriter(t, w)
	scanner, err := checkpoint.OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	if other, err := wal.Open(dir, &store.Store{}); !errors.Is(err, syscall.EWOULDBLOCK) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("writer while scanner holds lock: %v", err)
	}
}

func TestFailedOpenReleasesLocks(t *testing.T) {
	dir := privateDir(t)
	w := openWriter(t, dir)
	populate(t, w)
	closeWriter(t, w)
	markerPath := filepath.Join(dir, "bog.commit")
	marker := readFile(t, dir, "bog.commit")
	invalid := bytes.Clone(marker)
	invalid[0] ^= 1
	if err := os.WriteFile(markerPath, invalid, 0600); err != nil {
		t.Fatal(err)
	}
	if scanner, err := checkpoint.OpenLegacy(dir); err == nil || !strings.Contains(err.Error(), "invalid commit marker") {
		if scanner != nil {
			scanner.Close()
		}
		t.Fatalf("invalid marker: %v", err)
	}
	if err := os.WriteFile(markerPath, marker, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWriterSubprocess$")
	cmd.Env = append(os.Environ(), "BOG_COMPAT_WRITER_DIR="+dir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("writer subprocess: %v (context: %v)\n%s", err, ctx.Err(), output)
	}
}

func TestWriterSubprocess(t *testing.T) {
	dir := os.Getenv("BOG_COMPAT_WRITER_DIR")
	if dir == "" {
		return
	}
	w := openWriter(t, dir)
	closeWriter(t, w)
}
