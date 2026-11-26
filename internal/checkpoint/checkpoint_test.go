package checkpoint

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"bog/internal/store"
)

func appendFrame(t *testing.T, dir, payload string) {
	t.Helper()
	f, e := os.OpenFile(filepath.Join(dir, "bog.wal"), os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	var h [16]byte
	n := uint32(len(payload))
	binary.LittleEndian.PutUint32(h[:], n)
	binary.LittleEndian.PutUint32(h[4:], ^n)
	binary.LittleEndian.PutUint32(h[8:], crc32.Checksum([]byte(payload), crc))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], crc))
	if _, e = f.Write(h[:]); e != nil {
		t.Fatal(e)
	}
	if _, e = f.WriteString(payload); e != nil {
		t.Fatal(e)
	}
	s, e := f.Stat()
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
	var marker [24]byte
	copy(marker[:], "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:], uint64(s.Size()))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(s.Size()))
	if e = os.WriteFile(filepath.Join(dir, "bog.commit"), marker[:], 0600); e != nil {
		t.Fatal(e)
	}
}
func TestBuildAndScan(t *testing.T) {
	d := t.TempDir()
	if e := os.WriteFile(filepath.Join(d, "bog.wal"), []byte("BOGWAL01"), 0600); e != nil {
		t.Fatal(e)
	}
	appendFrame(t, d, "{\"ts\":\"2020-01-02T00:00:00Z\",\"line\":\"same\"}\n{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"early\"}\n{\"ts\":\"2020-01-02T00:00:00Z\",\"line\":\"same\"}\n")
	before, e := os.ReadFile(filepath.Join(d, "bog.wal"))
	if e != nil {
		t.Fatal(e)
	}
	if e = Build(d, 1); e != nil {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(filepath.Join(d, "bog.wal"))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("input changed")
	}
	appendFrame(t, d, "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"late\"}\n")
	var lines []string
	var ord []uint64
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(48 * time.Hour)
	e = Scan(d, start, end, "", 1<<20, func(r store.Row, n uint64) error { lines = append(lines, r.Line); ord = append(ord, n); return nil })
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(lines, []string{"early", "late", "same", "same"}) || !reflect.DeepEqual(ord, []uint64{1, 3, 0, 2}) {
		t.Fatalf("%v %v", lines, ord)
	}
	if e = Scan(d, start, end, "", 0, func(store.Row, uint64) error { return nil }); e == nil {
		t.Fatal("tail limit ignored")
	}
	m, e := readManifest(d)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Truncate(filepath.Join(d, m.Segment), 8); e != nil {
		t.Fatal(e)
	}
	if e = Scan(d, start, end, "", 1<<20, func(store.Row, uint64) error { return nil }); e == nil {
		t.Fatal("truncated segment accepted")
	}
}
