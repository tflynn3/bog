package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
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
	if _, err = l.OpenPublished(); !errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing referenced segment: %v", err)
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

func TestManySmallFrames(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"x\"}\n")
	var h [16]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[4:], ^uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(payload, castagnoli))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], castagnoli))
	wal := []byte("BOGWAL01")
	for i := 0; i < 1025; i++ {
		wal = append(wal, h[:]...)
		wal = append(wal, payload...)
	}
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:], uint64(len(wal)))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(wal)))
	if err := os.WriteFile(filepath.Join(dir, "bog.wal"), wal, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	m, err := l.Publish()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Segments) != 1 {
		t.Fatalf("segments: %d", len(m.Segments))
	}
	p, err := l.OpenPublished()
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	err = p.Scan(func(r BlockRow) error {
		if r.Ordinal != n {
			t.Fatalf("ordinal %d, want %d", r.Ordinal, n)
		}
		n++
		return nil
	})
	if err != nil || n != 1025 {
		t.Fatalf("rows %d: %v", n, err)
	}
}

func TestRenameFailureStopsUntilReopen(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(dir, manifestName), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err = l.Publish(); !errors.Is(err, ErrPublicationUncertain) {
		t.Fatalf("publish: %v", err)
	}
	if _, err = l.Publish(); !errors.Is(err, ErrPublicationUncertain) {
		t.Fatalf("retry: %v", err)
	}
	if _, err = l.OpenPublished(); !errors.Is(err, ErrPublicationUncertain) {
		t.Fatalf("open: %v", err)
	}
	l.Close()
	if err = os.Remove(filepath.Join(dir, manifestName)); err != nil {
		t.Fatal(err)
	}
	l, err = OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err = l.OpenPublished(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reopen: %v", err)
	}
}
