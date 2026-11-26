package checkpoint

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

func segmentFixture(t *testing.T, payloads ...string) string {
	t.Helper()
	dir := t.TempDir()
	wal := []byte("BOGWAL01")
	for _, payload := range payloads {
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
	if err := os.WriteFile(filepath.Join(dir, "bog.wal"), wal, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestStageSegments(t *testing.T) {
	payload := strings.Join([]string{
		`{"ts":"2024-01-01T00:00:00.000000001Z","line":"dup"}`,
		`{"ts":"2024-01-01T00:00:00.000000001Z","line":"dup"}`,
		`{"ts":"0001-01-01T00:00:00Z","line":"late"}`,
		`{"ts":"9999-12-31T23:59:59.999999998Z","line":"last"}`,
	}, "\n")
	l, err := OpenLegacy(segmentFixture(t, payload))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	parent := t.TempDir()
	dir, count, err := StageSegments(l, parent, blockHeaderSize+3*(blockRowSize+4))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if count != 2 {
		t.Fatalf("count %d", count)
	}
	var got []BlockRow
	for i := uint64(0); i < count; i++ {
		rows, err := readSegment(filepath.Join(dir, fmt.Sprintf("%020d.seg", i)))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, rows...)
	}
	if len(got) != 4 || got[0].Ordinal != 2 || got[1].Ordinal != 0 || got[2].Ordinal != 1 || got[3].Ordinal != 3 || got[1].Row.Line != got[2].Row.Line || got[1].Row.TS.Nanosecond() != 1 {
		t.Fatalf("unexpected rows: %+v", got)
	}
	if !got[0].Row.TS.Equal(minBlockTime) || !got[3].Row.TS.Equal(maxBlockTime.Add(-time.Nanosecond)) {
		t.Fatal("timestamp range lost")
	}
}

func TestSegmentRejectsInvalidFiles(t *testing.T) {
	rows := []BlockRow{{Row: store.Row{TS: time.Unix(1, 0), Line: "a"}, Ordinal: 1}, {Row: store.Row{TS: time.Unix(2, 0), Line: "b"}, Ordinal: 2}}
	block, err := EncodeBlock(rows)
	if err != nil {
		t.Fatal(err)
	}
	valid := append([]byte(segmentMagic), block...)
	path := filepath.Join(t.TempDir(), "segment")
	check := func(data []byte) {
		t.Helper()
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if got, err := readSegment(path); err == nil || got != nil {
			t.Fatalf("accepted invalid segment: %+v", got)
		}
	}
	for i := range valid {
		check(valid[:i])
	}
	bad := append([]byte(nil), valid...)
	bad[0] ^= 1
	check(bad)
	bad = append([]byte(nil), valid...)
	bad[len(bad)-1] ^= 1
	check(bad)
	check(append(append([]byte(nil), valid...), 0))
	for _, unordered := range [][]BlockRow{{rows[1], rows[0]}, {rows[0], rows[0]}} {
		b, err := EncodeBlock(unordered)
		if err != nil {
			t.Fatal(err)
		}
		check(append([]byte(segmentMagic), b...))
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(len(segmentMagic) + MaxBlockSize + 1)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if rows, err := readSegment(path); err == nil || rows != nil {
		t.Fatal("accepted oversized file")
	}
}

func TestStageFailureRemovesPartialOutput(t *testing.T) {
	payload := `{"ts":"2024-01-01T00:00:00Z","line":"one"}` + "\n" + `{"ts":"2024-01-01T00:00:00Z","line":"two"}`
	dir := segmentFixture(t, payload, payload)
	walPath := filepath.Join(dir, "bog.wal")
	wal, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal[len(wal)-1] ^= 1
	if err := os.WriteFile(walPath, wal, 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	parent := t.TempDir()
	staged, count, err := StageSegments(l, parent, blockHeaderSize+blockRowSize+3)
	if err == nil || staged != "" || count != 0 {
		t.Fatalf("stage: %q %d %v", staged, count, err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch remains: %v %v", entries, err)
	}
}

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
