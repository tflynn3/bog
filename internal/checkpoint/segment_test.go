package checkpoint

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
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
