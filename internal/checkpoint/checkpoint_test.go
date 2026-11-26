package checkpoint

import (
	"encoding/binary"
	"fmt"
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
	f, e := os.OpenFile(filepath.Join(dir, "bog.wal"), os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	b := []byte(payload)
	var h [16]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(b)))
	binary.LittleEndian.PutUint32(h[4:], ^uint32(len(b)))
	binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(b, crc))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], crc))
	if _, e = f.Write(append(h[:], b...)); e != nil {
		t.Fatal(e)
	}
	s, e := f.Stat()
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
	m := make([]byte, 24)
	copy(m, "BOGCMT01")
	binary.LittleEndian.PutUint64(m[8:], uint64(s.Size()))
	binary.LittleEndian.PutUint64(m[16:], ^uint64(s.Size()))
	if e = os.WriteFile(filepath.Join(dir, "bog.commit"), m, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestBuildTailAndCorruption(t *testing.T) {
	d := t.TempDir()
	if e := os.WriteFile(filepath.Join(d, "bog.wal"), []byte("BOGWAL01"), 0600); e != nil {
		t.Fatal(e)
	}
	payload := "{\"ts\":\"2020-01-02T00:00:00Z\",\"line\":\"same\"}\n{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"first\"}\n{\"ts\":\"2020-01-02T00:00:00Z\",\"line\":\"same\"}\n"
	appendFrame(t, d, payload)
	before, _ := os.ReadFile(filepath.Join(d, "bog.wal"))
	if e := Build(d); e != nil {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(filepath.Join(d, "bog.wal"))
	if !reflect.DeepEqual(before, after) {
		t.Fatal("WAL changed")
	}
	appendFrame(t, d, "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"late\"}\n")
	r, e := Open(d, 1024)
	if e != nil {
		t.Fatal(e)
	}
	var got []string
	err := r.Query(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), "", func(row store.Row, n uint64) error { got = append(got, fmt.Sprintf("%s:%d", row.Line, n)); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"first:1", "late:3", "same:0", "same:2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	r.Close()
	if _, e = Open(d, 1); e == nil {
		t.Fatal("expected tail limit error")
	}
	f, e := os.OpenFile(filepath.Join(d, "bog.wal"), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	s, _ := f.Stat()
	f.WriteAt([]byte{0}, s.Size()-1)
	f.Close()
	if _, e = Open(d, 1024); e == nil {
		t.Fatal("expected committed checksum error")
	}
}
