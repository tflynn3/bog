package checkpoint

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"testing/iotest"
	"testing/synctest"

	"bog/internal/store"
)

func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func readFixture(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func closeOnCleanup(t *testing.T, c io.Closer) {
	t.Helper()
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
}

func fixture(t *testing.T) (string, []byte, []byte) {
	t.Helper()
	dir := privateDir(t)
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
	gotWal := readFixture(t, dir, "bog.wal")
	gotMarker := readFixture(t, dir, "bog.commit")
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
	closeOnCleanup(t, l)
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
	closeOnCleanup(t, l)
	if err := l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error { t.Fatal("partial frame delivered"); return nil }); err == nil {
		t.Fatal("accepted partial committed frame")
	}
}

func TestCommitMarkerRead(t *testing.T) {
	_, _, marker := fixture(t)
	badMagic := bytes.Clone(marker)
	badMagic[0] ^= 1
	badComplement := bytes.Clone(marker)
	badComplement[16] ^= 1
	for _, tc := range []struct {
		name string
		r    io.Reader
	}{
		{"empty", bytes.NewReader(nil)},
		{"truncated", bytes.NewReader(marker[:23])},
		{"overlong", bytes.NewReader(append(bytes.Clone(marker), 0))},
		{"magic", bytes.NewReader(badMagic)},
		{"complement", bytes.NewReader(badComplement)},
		{"EIO", iotest.ErrReader(syscall.EIO)},
		{"EIO after marker", io.MultiReader(bytes.NewReader(marker), iotest.ErrReader(syscall.EIO))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := readCommitMarker(tc.r, 1024); err == nil ||
				errors.Is(err, syscall.EIO) != (tc.name == "EIO" || tc.name == "EIO after marker") {
				t.Fatalf("readCommitMarker: %v", err)
			}
		})
	}
}

func TestFailedOpenReleasesLocks(t *testing.T) {
	dir, _, marker := fixture(t)
	path := filepath.Join(dir, "bog.commit")
	if err := os.WriteFile(path, marker[:23], 0600); err != nil {
		t.Fatal(err)
	}
	if l, err := OpenLegacy(dir); err == nil {
		closeOnCleanup(t, l)
		t.Fatal("opened with a truncated commit marker")
	}
	if err := os.WriteFile(path, marker, 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, l)
	var n int
	if err := l.Scan(func(_, _ int64, _ uint64, rows []store.Row) error {
		n += len(rows)
		return nil
	}); err != nil || n != 2 {
		t.Fatalf("scan repaired fixture: %d rows, %v", n, err)
	}
}

func TestRejectUnsafeDirectory(t *testing.T) {
	for _, mode := range []os.FileMode{0720, 0702} {
		dir, _, _ := fixture(t)
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
		if l, err := OpenLegacy(dir); err == nil {
			closeOnCleanup(t, l)
			t.Fatalf("opened unsafe directory with mode %04o", mode)
		}
	}
}

func TestMagicOnlyAndLocks(t *testing.T) {
	dir := privateDir(t)
	if err := os.WriteFile(filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, l)
	if l.CommittedOffset() != 8 {
		t.Fatal(l.CommittedOffset())
	}
	if err := l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error { t.Fatal("unexpected frame"); return nil }); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenLegacy(dir); err == nil {
		closeOnCleanup(t, other)
		t.Fatal("lock not held")
	}
	if _, err := os.Stat(filepath.Join(dir, "bog.commit")); !os.IsNotExist(err) {
		t.Fatalf("marker created: %v", err)
	}
}

func TestScanAfterClose(t *testing.T) {
	for _, name := range []string{"empty", "populated"} {
		t.Run(name, func(t *testing.T) {
			var dir string
			if name == "empty" {
				dir = privateDir(t)
				if err := os.WriteFile(filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				dir, _, _ = fixture(t)
			}
			l, err := OpenLegacy(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if err := l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error {
				t.Fatal("closed scanner invoked visitor")
				return nil
			}); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("scan after close = %v, want os.ErrClosed", err)
			}
		})
	}
}

func TestCloseWaitsForAcceptedScans(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		closeOnCleanup(t, l)
		entered := make(chan struct{}, 2)
		var release [2]func()
		scanned := make(chan error, 2)
		for i := range release {
			unblock := make(chan struct{})
			release[i] = sync.OnceFunc(func() { close(unblock) })
			defer release[i]()
			go func() {
				scanned <- l.Scan(func(_, _ int64, _ uint64, _ []store.Row) error {
					entered <- struct{}{}
					<-unblock
					return nil
				})
			}()
		}
		synctest.Wait()
		if len(entered) != 2 {
			t.Fatal("both scans must reach their visitors")
		}
		closed := make(chan error, 2)
		go func() { closed <- l.Close() }()
		go func() { closed <- l.Close() }()
		synctest.Wait()
		if len(closed) != 0 {
			t.Fatal("Close returned while visitors were blocked")
		}
		if err := l.Scan(nil); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("scan during close = %v, want os.ErrClosed", err)
		}
		if other, err := OpenLegacy(dir); !errors.Is(err, syscall.EWOULDBLOCK) {
			if other != nil {
				if err := other.Close(); err != nil {
					t.Error(err)
				}
			}
			t.Fatalf("second open during close = %v, want lock contention", err)
		}
		for i, unblock := range release {
			unblock()
			if err := <-scanned; err != nil {
				t.Error(err)
			}
			synctest.Wait()
			if i == 0 && len(closed) != 0 {
				t.Fatal("Close returned before the last visitor finished")
			}
		}
		for range 2 {
			if err := <-closed; err != nil {
				t.Error(err)
			}
		}
	})
	other, err := OpenLegacy(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
}
