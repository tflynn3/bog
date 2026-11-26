package checkpoint

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	markerBefore, e := os.ReadFile(filepath.Join(d, "bog.commit"))
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(d, "bog.wal"))
	if e != nil {
		t.Fatal(e)
	}
	if e = Build(d, 1); e != nil {
		t.Fatal(e)
	}
	markerAfter, e := os.ReadFile(filepath.Join(d, "bog.commit"))
	if e != nil || !reflect.DeepEqual(markerBefore, markerAfter) {
		t.Fatal("marker changed")
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

func TestLargerThanSortBudgetAndFailedRebuild(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
		t.Fatal(err)
	}
	// Shuffled timestamps, duplicate payloads, and more retained data than the sort budget.
	line := strings.Repeat("x", 16<<10)
	for i := 0; i < 100; i++ {
		appendFrame(t, d, fmt.Sprintf("{\"ts\":\"2020-01-%02dT00:00:00Z\",\"line\":\"%s\"}\n", 1+(99-i)%28, line))
	}
	walBefore, err := os.ReadFile(filepath.Join(d, "bog.wal"))
	if err != nil {
		t.Fatal(err)
	}
	markerBefore, err := os.ReadFile(filepath.Join(d, "bog.commit"))
	if err != nil {
		t.Fatal(err)
	}
	if len(walBefore) <= 1<<20 {
		t.Fatal("corpus did not exceed budget")
	}
	if err := Build(d, 1<<20); err != nil {
		t.Fatal(err)
	}
	appendFrame(t, d, "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"tail\"}\n")
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	var count int
	var prev time.Time
	var last uint64
	err = Scan(d, start, start.AddDate(0, 1, 0), "", 1024, func(r store.Row, n uint64) error {
		if count > 0 && (r.TS.Before(prev) || (r.TS.Equal(prev) && n <= last)) {
			t.Fatal("order")
		}
		prev, last = r.TS, n
		count++
		return nil
	})
	if err != nil || count != 101 {
		t.Fatalf("scan: count=%d err=%v", count, err)
	}
	// A bad committed suffix must not replace the authoritative manifest.
	manifestBefore, err := os.ReadFile(filepath.Join(d, "bog.checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	appendFrame(t, d, "bad payload")
	if err = Build(d, 1<<20); err == nil {
		t.Fatal("accepted corrupt frame")
	}
	manifestAfter, err := os.ReadFile(filepath.Join(d, "bog.checkpoint"))
	if err != nil || !reflect.DeepEqual(manifestBefore, manifestAfter) {
		t.Fatal("manifest changed on failure")
	}
	// The original committed inputs were untouched by the successful build.
	if !bytes.Equal(walBefore, mustReadPrefix(t, filepath.Join(d, "bog.wal"), len(walBefore))) {
		t.Fatal("WAL prefix changed")
	}
	if len(markerBefore) != 24 {
		t.Fatal("original marker invalid")
	}
}

func mustReadPrefix(t *testing.T, path string, n int) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || len(b) < n {
		t.Fatal(err)
	}
	return b[:n]
}
