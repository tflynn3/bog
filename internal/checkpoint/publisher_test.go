package checkpoint

import (
	"bytes"
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
