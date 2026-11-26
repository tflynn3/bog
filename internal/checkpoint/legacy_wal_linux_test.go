package checkpoint

import (
	"bytes"
	"errors"
	"slices"
	"syscall"
	"testing"

	"bog/internal/store"
	"bog/internal/wal"
)

func TestProductionWALCompatibility(t *testing.T) {
	dir := privateDir(t)
	writer, err := wal.Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, writer)
	if scanner, err := OpenLegacy(dir); !errors.Is(err, syscall.EWOULDBLOCK) {
		if scanner != nil {
			closeOnCleanup(t, scanner)
		}
		t.Fatalf("open scanner while writer holds lock: %v", err)
	}
	payloads := []string{
		"{\"ts\":\"9999-12-31T23:59:59.999999998Z\",\"line\":\"same\"}\n" +
			"{\"ts\":\"9999-12-31T23:59:59.999999998Z\",\"line\":\"same\"}\n",
		"{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"backfill\"}\n" +
			"{\"line\":\"literal \\u2603\",\"ts\":\"2024-01-01T03:00:00+03:00\"}\n",
	}
	var expected [][]store.Row
	for _, payload := range payloads {
		rows, err := store.Decode([]byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Append([]byte(payload), rows); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, rows)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	walBytes := readFixture(t, dir, "bog.wal")
	marker := readFixture(t, dir, "bog.commit")
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, l)
	if other, err := wal.Open(dir, &store.Store{}); !errors.Is(err, syscall.EWOULDBLOCK) {
		if other != nil {
			closeOnCleanup(t, other)
		}
		t.Fatalf("open writer while scanner holds lock: %v", err)
	}
	frame := 0
	var ordinal uint64
	pos := int64(8)
	if err := l.Scan(func(offset, next int64, first uint64, rows []store.Row) error {
		if frame >= len(expected) {
			t.Fatal("unexpected extra frame")
		}
		wantNext := pos + 16 + int64(len(payloads[frame]))
		if offset != pos || next != wantNext || first != ordinal || !slices.Equal(rows, expected[frame]) {
			t.Fatalf("frame %d = (%d, %d, %d, %v), want (%d, %d, %d, %v)",
				frame, offset, next, first, rows, pos, wantNext, ordinal, expected[frame])
		}
		frame++
		pos = wantNext
		ordinal += uint64(len(rows))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if frame != len(expected) || pos != l.CommittedOffset() || pos != int64(len(walBytes)) {
		t.Fatalf("scanned %d frames through %d, committed offset %d, file length %d", frame, pos, l.CommittedOffset(), len(walBytes))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(walBytes, readFixture(t, dir, "bog.wal")) ||
		!bytes.Equal(marker, readFixture(t, dir, "bog.commit")) {
		t.Fatal("scan modified the writer's committed files")
	}
}
