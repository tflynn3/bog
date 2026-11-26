package checkpoint

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"bog/internal/store"
)

func TestManifestFromLegacy(t *testing.T) {
	dir := t.TempDir()
	wal := []byte("BOGWAL01")
	for _, payload := range []string{
		"{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"same\"}\n{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"same\"}\n",
		"{\"ts\":\"2023-01-01T00:00:00Z\",\"line\":\"backfill\"}\n",
	} {
		var h [16]byte
		n := uint32(len(payload))
		binary.LittleEndian.PutUint32(h[:], n)
		binary.LittleEndian.PutUint32(h[4:], ^n)
		binary.LittleEndian.PutUint32(h[8:], crc32.Checksum([]byte(payload), castagnoli))
		binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], castagnoli))
		wal = append(wal, h[:]...)
		wal = append(wal, payload...)
	}
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:], uint64(len(wal)))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(wal)))
	for name, data := range map[string][]byte{"bog.wal": wal, "bog.commit": marker} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	m := Manifest{Generation: [16]byte{1}, CoveredOffset: 8}
	var rows []store.Row
	if err := l.Scan(func(_, next int64, first uint64, batch []store.Row) error {
		if first != uint64(len(rows)) {
			t.Fatalf("ordinal %d", first)
		}
		rows = append(rows, batch...)
		m.CoveredOffset, m.NextOrdinal = uint64(next), first+uint64(len(batch))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0] != rows[1] || !rows[2].TS.Before(rows[0].TS) || m.CoveredOffset != uint64(l.CommittedOffset()) || m.NextOrdinal != 3 {
		t.Fatal("unexpected scan", m, rows)
	}
	m.Segments = [][16]byte{{2}, {3}}
	encoded, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(encoded)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatalf("round trip: %#v %v", got, err)
	}
}

func TestManifestEmptyWAL(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	m := Manifest{Generation: [16]byte{1}, CoveredOffset: uint64(l.CommittedOffset())}
	if err := l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error { t.Fatal("unexpected frame"); return nil }); err != nil {
		t.Fatal(err)
	}
	b, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(b)
	if err != nil || got.CoveredOffset != 8 || got.NextOrdinal != 0 || len(got.Segments) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestManifestRejectsInvalid(t *testing.T) {
	valid := Manifest{Generation: [16]byte{1}, CoveredOffset: 100, NextOrdinal: 3, Segments: [][16]byte{{2}, {3}}}
	b, err := valid.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	rechecksum := func(b []byte) { binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], castagnoli)) }
	cases := map[string]func([]byte) []byte{
		"magic":     func(b []byte) []byte { b[0]++; return b },
		"version":   func(b []byte) []byte { b[8]++; rechecksum(b); return b },
		"checksum":  func(b []byte) []byte { b[12]++; return b },
		"truncated": func(b []byte) []byte { return b[:len(b)-1] },
		"trailing":  func(b []byte) []byte { return append(b, 0) },
		"count": func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[44:], MaxManifestSegments+1)
			rechecksum(b)
			return b
		},
		"generation":     func(b []byte) []byte { clear(b[12:28]); rechecksum(b); return b },
		"offset low":     func(b []byte) []byte { binary.LittleEndian.PutUint64(b[28:], 7); rechecksum(b); return b },
		"offset high":    func(b []byte) []byte { binary.LittleEndian.PutUint64(b[28:], math.MaxUint64); rechecksum(b); return b },
		"empty boundary": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[28:], 8); rechecksum(b); return b },
		"zero identity":  func(b []byte) []byte { clear(b[48:64]); rechecksum(b); return b },
		"duplicate":      func(b []byte) []byte { copy(b[64:80], b[48:64]); rechecksum(b); return b },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseManifest(change(append([]byte(nil), b...))); err == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
	for i := 0; i < len(b); i++ {
		if _, err := ParseManifest(b[:i]); err == nil {
			t.Fatalf("accepted prefix %d", i)
		}
	}
	if _, err := ParseManifest(make([]byte, MaxManifestBytes+1)); err == nil {
		t.Fatal("accepted oversized manifest")
	}
	for _, bad := range []Manifest{{}, {Generation: [16]byte{1}, CoveredOffset: 8, NextOrdinal: 1}, {Generation: [16]byte{1}, CoveredOffset: 9, Segments: [][16]byte{{1}, {1}}}, {Generation: [16]byte{1}, CoveredOffset: 9, Segments: make([][16]byte, MaxManifestSegments+1)}} {
		if _, err := bad.MarshalBinary(); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
}
