package checkpoint

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishAndReopen(t *testing.T) {
	dir, wal, marker := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	m, err := l.Publish(MaxBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if m.NextOrdinal != 2 || len(m.Segments) != 1 {
		t.Fatalf("manifest: %+v", m)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var rows []BlockRow
	if err = l.ReadCheckpoint(func(r BlockRow) error { rows = append(rows, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Ordinal != 0 || rows[1].Ordinal != 1 || rows[0].Row != rows[1].Row {
		t.Fatalf("rows: %+v", rows)
	}
	w, _ := os.ReadFile(filepath.Join(dir, "bog.wal"))
	c, _ := os.ReadFile(filepath.Join(dir, "bog.commit"))
	if !bytes.Equal(w, wal) || !bytes.Equal(c, marker) {
		t.Fatal("legacy input changed")
	}
	if err = os.Remove(filepath.Join(dir, segmentName(m.Segments[0]))); err != nil {
		t.Fatal(err)
	}
	called := false
	if err = l.ReadCheckpoint(func(BlockRow) error { called = true; return nil }); err == nil || called {
		t.Fatalf("missing segment: %v, called=%v", err, called)
	}
}

func TestCheckpointRejectsChangedWALAndInvalidMarker(t *testing.T) {
	dir, wal, marker := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.Publish(MaxBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	// Replacing the file must not be accepted even if it has the same bytes.
	if err = os.Rename(filepath.Join(dir, "bog.wal"), filepath.Join(dir, "old.wal")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "bog.wal"), wal, 0600); err != nil {
		t.Fatal(err)
	}
	l, err = OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.ReadCheckpoint(func(BlockRow) error { t.Fatal("exposed row"); return nil }); err == nil {
		t.Fatal("accepted replaced WAL")
	}
	l.Close()
	// Restore the original WAL and move the marker into its final frame.
	if err = os.Remove(filepath.Join(dir, "bog.wal")); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(filepath.Join(dir, "old.wal"), filepath.Join(dir, "bog.wal")); err != nil {
		t.Fatal(err)
	}
	end := uint64(len(wal) - 1)
	binary.LittleEndian.PutUint64(marker[8:], end)
	binary.LittleEndian.PutUint64(marker[16:], ^end)
	if err = os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	l, err = OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = l.ReadCheckpoint(func(BlockRow) error { t.Fatal("exposed row"); return nil }); err == nil {
		t.Fatal("accepted marker inside frame")
	}
}

func TestReadWithoutManifest(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var count int
	if err = l.ReadCheckpoint(func(BlockRow) error { count++; return nil }); err != nil || count != 2 {
		t.Fatalf("legacy fallback: count=%d err=%v", count, err)
	}
}
