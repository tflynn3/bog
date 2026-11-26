package checkpoint

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishAndInterruptedGeneration(t *testing.T) {
	dir, wal, marker := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = l.OpenPublished(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing manifest: %v", err)
	}
	if err = os.WriteFile(filepath.Join(dir, "bog-segment-abandoned"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := l.Publish()
	if err != nil {
		t.Fatal(err)
	}
	if m.CoveredOffset != int64(len(wal)-len("uncommitted")) || m.NextOrdinal != 2 {
		t.Fatalf("manifest: %+v", m)
	}
	p, err := l.OpenPublished()
	if err != nil {
		t.Fatal(err)
	}
	var got []BlockRow
	if err = p.Scan(func(r BlockRow) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Ordinal != 0 || got[1].Ordinal != 1 || got[0].Row != got[1].Row {
		t.Fatalf("rows: %+v", got)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "bog.wal"))
	afterMarker, _ := os.ReadFile(filepath.Join(dir, "bog.commit"))
	if !bytes.Equal(wal, after) || !bytes.Equal(marker, afterMarker) {
		t.Fatal("legacy inputs changed")
	}
	l, err = OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err = l.OpenPublished(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, m.Segments[0].Name)); err != nil {
		t.Fatal(err)
	}
	if _, err = l.OpenPublished(); err == nil {
		t.Fatal("missing referenced segment accepted")
	}
}

func TestRejectCorruptSegment(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	m, err := l.Publish()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, m.Segments[0].Name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = l.OpenPublished(); err == nil {
		t.Fatal("corruption accepted")
	}
}
