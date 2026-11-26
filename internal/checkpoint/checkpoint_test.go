package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"bog/internal/store"
)

func TestTruncatedRecordBodyIsNotEndOfRun(t *testing.T) {
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], 24)
	if _, err := get(bytes.NewReader(length[:])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want unexpected EOF", err)
	}
	d := t.TempDir()
	root, err := os.OpenRoot(d)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.WriteFile(filepath.Join(d, "bad"), append([]byte("BOGSEG01"), length[:]...), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "empty"), []byte("BOGSEG01"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := merge(root, "bad", "empty"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("merge accepted truncated run: %v", err)
	}
}

func TestMissingCheckpointAndTailLimit(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(d, 0); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("missing checkpoint: %v", err)
	}
	if err := Build(d); err != nil {
		t.Fatal(err)
	}
	appendFrame(t, d, "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"tail\"}\n")
	if _, err := Open(d, 0); !errors.Is(err, ErrTailTooLarge) {
		t.Fatalf("tail limit: %v", err)
	}
	if err := Build(d); err != nil {
		t.Fatal(err)
	}
	r, err := Open(d, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}

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

func TestSegmentCorruptionFailsOpenAndInputsSurvive(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
		t.Fatal(err)
	}
	appendFrame(t, d, "{\"ts\":\"9999-12-31T23:59:59.999999998Z\",\"line\":\"x\"}\n")
	wal, _ := os.ReadFile(filepath.Join(d, "bog.wal"))
	marker, _ := os.ReadFile(filepath.Join(d, "bog.commit"))
	if err := Build(d); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(d, "segment-*.bog"))
	if err != nil || len(files) != 1 {
		t.Fatalf("segments: %v %v", files, err)
	}
	if err := os.Truncate(files[0], 8); err != nil {
		t.Fatal(err)
	}
	if r, err := Open(d, 0); err == nil {
		r.Close()
		t.Fatal("accepted truncated segment")
	}
	gotWAL, _ := os.ReadFile(filepath.Join(d, "bog.wal"))
	gotMarker, _ := os.ReadFile(filepath.Join(d, "bog.commit"))
	if !reflect.DeepEqual(wal, gotWAL) || !reflect.DeepEqual(marker, gotMarker) {
		t.Fatal("inputs changed")
	}
	l, err := OpenLegacy(d)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	count := 0
	if err := l.Scan(func(_, _ int64, rows []store.Row) error { count += len(rows); return nil }); err != nil || count != 1 {
		t.Fatalf("legacy scan: %d %v", count, err)
	}
}

func TestConcurrentQueriesAndCoveredCorruption(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		appendFrame(t, d, fmt.Sprintf("{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"row-%d\"}\n", i))
	}
	if err := Build(d); err != nil {
		t.Fatal(err)
	}
	r, err := Open(d, 0)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(2, 0, 0)
	// Block both callbacks after their first result, forcing their buffered reads to interleave.
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for j := 0; j < 2; j++ {
		go func() {
			count := 0
			err := r.Query(start, end, "", func(_ store.Row, ordinal uint64) error {
				if count == 0 {
					ready <- struct{}{}
					<-release
				}
				if ordinal != uint64(count) {
					return fmt.Errorf("ordinal %d at %d", ordinal, count)
				}
				count++
				return nil
			})
			if err == nil && count != 200 {
				err = fmt.Errorf("count %d", count)
			}
			results <- err
		}()
	}
	<-ready
	<-ready
	close(release)
	for j := 0; j < 2; j++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(d, "bog.wal"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err = f.ReadAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 1
	if _, err = f.WriteAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if r, err = Open(d, 0); err == nil {
		r.Close()
		t.Fatal("accepted corruption in covered WAL prefix")
	}
}
