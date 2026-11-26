package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"bog/internal/store"
)

func fixture(t *testing.T) (string, []byte, []byte) {
	t.Helper()
	dir := t.TempDir()
	payload := []byte("{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"same\"}\n{\"ts\":\"2024-01-01T00:00:00Z\",\"line\":\"same\"}\n")
	var h [16]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[4:], ^uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(payload, castagnoli))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], castagnoli))
	wal := append([]byte("BOGWAL01"), h[:]...)
	wal = append(wal, payload...)
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:], uint64(len(wal)))
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(len(wal)))
	wal = append(wal, []byte("uncommitted")...)
	if err := os.WriteFile(filepath.Join(dir, "bog.wal"), wal, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	return dir, wal, marker
}

func TestScanCommittedReadOnly(t *testing.T) {
	dir, wal, marker := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	err = l.Scan(func(offset, next int64, ordinal uint64, rows []store.Row) error {
		if ordinal != 0 || offset != 8 || next != l.CommittedOffset() || len(rows) != 2 || rows[0] != rows[1] {
			t.Fatalf("unexpected frame: %d %d %#v", offset, next, rows)
		}
		count++
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("scan: %v, frames %d", err, count)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	gotWal, _ := os.ReadFile(filepath.Join(dir, "bog.wal"))
	gotMarker, _ := os.ReadFile(filepath.Join(dir, "bog.commit"))
	if !bytes.Equal(gotWal, wal) || !bytes.Equal(gotMarker, marker) {
		t.Fatal("input changed")
	}
}

func TestRejectCommittedCorruption(t *testing.T) {
	dir, wal, _ := fixture(t)
	wal[24] ^= 1
	if err := os.WriteFile(filepath.Join(dir, "bog.wal"), wal, 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	called := false
	if err := l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error { called = true; return nil }); err == nil || called {
		t.Fatalf("corruption accepted: %v", err)
	}
}

func TestRejectMarkerInsideFrame(t *testing.T) {
	dir, _, marker := fixture(t)
	binary.LittleEndian.PutUint64(marker[8:], 25)
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(25))
	if err := os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error { t.Fatal("partial frame delivered"); return nil }); err == nil {
		t.Fatal("accepted partial committed frame")
	}
}

func TestMagicOnlyAndLocks(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.CommittedOffset() != 8 {
		t.Fatal(l.CommittedOffset())
	}
	if err := l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error { t.Fatal("unexpected frame"); return nil }); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenLegacy(dir); err == nil {
		other.Close()
		t.Fatal("lock not held")
	}
	if _, err := os.Stat(filepath.Join(dir, "bog.commit")); !os.IsNotExist(err) {
		t.Fatalf("marker created: %v", err)
	}
}

func TestOrdinalsAcrossFrames(t *testing.T) {
	dir, wal, marker := fixture(t)
	end := int(binary.LittleEndian.Uint64(marker[8:]))
	frame := append([]byte(nil), wal[8:end]...)
	wal = append(wal[:end], frame...)
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
	var ordinals []uint64
	if err := l.Scan(func(_, _ int64, first uint64, rows []store.Row) error {
		for i := range rows {
			ordinals = append(ordinals, first+uint64(i))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ordinals) != 4 || ordinals[0] != 0 || ordinals[1] != 1 || ordinals[2] != 2 || ordinals[3] != 3 {
		t.Fatal(ordinals)
	}
}

func TestCloseWaitsForAcceptedScan(t *testing.T) {
	for _, visitorFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("visitorFails=%t", visitorFails), func(t *testing.T) {
			dir, _, _ := fixture(t)
			l, err := OpenLegacy(dir)
			if err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			visitorErr := errors.New("visitor failed")
			scanDone := make(chan error, 1)
			go func() {
				scanDone <- l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error {
					close(entered)
					<-release
					if visitorFails {
						return visitorErr
					}
					return nil
				})
			}()
			<-entered
			closeDone := make(chan error, 2)
			go func() { closeDone <- l.Close() }()
			// Observe admission closing before checking rejection or starting another close.
			deadline := time.Now().Add(5 * time.Second)
			for {
				l.mu.Lock()
				closing := l.closing
				l.mu.Unlock()
				if closing {
					break
				}
				if time.Now().After(deadline) {
					close(release)
					t.Fatal("Close did not begin")
				}
				runtime.Gosched()
			}
			go func() { closeDone <- l.Close() }()
			if err := l.Scan(nil); !errors.Is(err, os.ErrClosed) {
				t.Errorf("scan during close: %v", err)
			}
			if other, err := OpenLegacy(dir); err == nil {
				other.Close()
				t.Error("directory lock released during scan")
			}
			select {
			case err := <-closeDone:
				t.Errorf("Close returned during scan: %v", err)
			default:
			}
			close(release)
			if err := <-scanDone; visitorFails && !errors.Is(err, visitorErr) || !visitorFails && err != nil {
				t.Errorf("scan: %v", err)
			}
			for i := 0; i < 2; i++ {
				if err := <-closeDone; err != nil {
					t.Errorf("close: %v", err)
				}
			}
			if err := l.Close(); err != nil {
				t.Errorf("repeated close: %v", err)
			}
			other, err := OpenLegacy(dir)
			if err != nil {
				t.Fatal("reacquire lock:", err)
			}
			other.Close()
		})
	}
}

func TestScanAfterClose(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprintf("populated=%t", populated), func(t *testing.T) {
			var dir string
			if populated {
				dir, _, _ = fixture(t)
			} else {
				dir = t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			l, err := OpenLegacy(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if err := l.Scan(nil); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("scan after close: %v", err)
			}
		})
	}
}

func TestFailedOpenReleasesLocks(t *testing.T) {
	dir, _, marker := fixture(t)
	if err := os.WriteFile(filepath.Join(dir, "bog.commit"), []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if l, err := OpenLegacy(dir); err == nil {
		l.Close()
		t.Fatal("accepted invalid marker")
	}
	if err := os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal("reopen after failure:", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
