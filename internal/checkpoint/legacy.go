// Package checkpoint contains the offline checkpoint tooling.
package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"

	"bog/internal/store"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Legacy is a read-only, exclusively locked view of a stopped legacy WAL and
// its commit marker. Close it before starting a writer on the same directory.
type Legacy struct {
	root     *os.Root
	dir, wal *os.File
	end      int64

	mu        sync.Mutex
	active    sync.WaitGroup
	closeDone chan struct{}
	closeErr  error

	publicationMu   sync.Mutex
	uncertain       error
	manifestRename  func(string, string) error
	manifestDirSync func() error
}

// OpenLegacy pins and locks the data directory, and reads the committed boundary.
// It never truncates the uncommitted suffix or modifies either input file.
func OpenLegacy(path string) (_ *Legacy, err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	l := &Legacy{root: root}
	defer func() {
		if err != nil {
			if closeErr := l.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	l.dir, err = root.Open(".")
	if err != nil {
		return nil, err
	}
	info, err := l.dir.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		return nil, errors.New("unsafe data directory")
	}
	if err = syscall.Flock(int(l.dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	l.wal, err = l.openFile("bog.wal")
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(l.wal.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	info, err = l.wal.Stat()
	if err != nil {
		return nil, err
	}
	var magic [8]byte
	if _, err = l.wal.ReadAt(magic[:], 0); err != nil {
		return nil, fmt.Errorf("read WAL magic: %w", err)
	}
	if string(magic[:]) != "BOGWAL01" {
		return nil, errors.New("invalid WAL magic")
	}
	marker, e := l.openFile("bog.commit")
	if errors.Is(e, os.ErrNotExist) {
		if info.Size() != 8 {
			return nil, errors.New("missing commit marker for populated WAL")
		}
		l.end = 8
		return l, nil
	}
	if e != nil {
		return nil, e
	}
	l.end, err = readCommitMarker(marker, info.Size())
	if closeErr := marker.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if err != nil {
		return nil, err
	}
	return l, nil
}

func readCommitMarker(r io.Reader, walSize int64) (int64, error) {
	b, err := io.ReadAll(io.LimitReader(r, 25))
	if err != nil {
		return 0, fmt.Errorf("read commit marker: %w", err)
	}
	if len(b) != 24 || string(b[:8]) != "BOGCMT01" {
		return 0, errors.New("invalid commit marker")
	}
	end := binary.LittleEndian.Uint64(b[8:16])
	if binary.LittleEndian.Uint64(b[16:24]) != ^end || end < 8 || end > uint64(walSize) {
		return 0, errors.New("invalid committed offset")
	}
	return int64(end), nil
}

func (l *Legacy) openFile(name string) (*os.File, error) {
	// O_NOFOLLOW prevents a final-component symlink from escaping the pinned directory.
	fd, err := syscall.Openat(int(l.dir.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		return nil, errors.Join(fmt.Errorf("unsafe file %s", name), f.Close())
	}
	return f, nil
}

// CommittedOffset is the exact boundary from the legacy commit marker. Scan
// must still succeed before this value can be used as a checkpoint boundary.
func (l *Legacy) CommittedOffset() int64 { return l.end }

// Scan validates every committed frame and calls visit in append order after a
// whole frame has decoded successfully. firstOrdinal is zero-based and each row
// has ordinal firstOrdinal plus its index. The rows are only valid during visit.
// Memory is bounded by one maximum-size frame and its decoded rows, independent
// of WAL length. An error in a committed frame is never treated as a torn tail.
// Concurrent scans are independent. Once Close begins, new scans return
// os.ErrClosed. A visitor must return before calling Close on the same Legacy.
func (l *Legacy) Scan(visit func(offset, next int64, firstOrdinal uint64, rows []store.Row) error) error {
	if err := l.begin(); err != nil {
		return err
	}
	defer l.active.Done()
	return l.scanFrom(8, 0, visit)
}

// scanFrom requires an accepted operation and a known committed frame boundary.
func (l *Legacy) scanFrom(pos int64, ordinal uint64, visit func(offset, next int64, firstOrdinal uint64, rows []store.Row) error) error {
	var h [16]byte
	var payload []byte
	for pos < l.end {
		if l.end-pos < 16 {
			return fmt.Errorf("incomplete committed frame at %d", pos)
		}
		if _, err := l.wal.ReadAt(h[:], pos); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(h[:4])
		if crc32.Checksum(h[:12], castagnoli) != binary.LittleEndian.Uint32(h[12:]) || n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:8]) != ^n {
			return fmt.Errorf("invalid WAL header at %d", pos)
		}
		if int64(n) > l.end-pos-16 {
			return fmt.Errorf("incomplete committed frame at %d", pos)
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		} else {
			payload = payload[:n]
		}
		if _, err := l.wal.ReadAt(payload, pos+16); err != nil {
			return err
		}
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(h[8:12]) {
			return fmt.Errorf("invalid WAL checksum at %d", pos)
		}
		rows, err := store.Decode(payload)
		if err != nil {
			return fmt.Errorf("invalid WAL payload at %d: %w", pos, err)
		}
		next := pos + 16 + int64(n)
		if uint64(len(rows)) > ^uint64(0)-ordinal {
			return errors.New("append ordinal overflow")
		}
		if err = visit(pos, next, ordinal, rows); err != nil {
			return err
		}
		ordinal += uint64(len(rows))
		pos = next
	}
	return nil
}

func (l *Legacy) begin() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeDone != nil || l.wal == nil {
		return os.ErrClosed
	}
	l.active.Add(1)
	return nil
}

// Close stops admitting operations and waits for accepted operations, including
// their visitors, before releasing the files and directory lock. Concurrent and
// repeated calls wait for the same result.
func (l *Legacy) Close() error {
	l.mu.Lock()
	if done := l.closeDone; done != nil {
		l.mu.Unlock()
		<-done
		return l.closeErr
	}
	done := make(chan struct{})
	l.closeDone = done
	l.mu.Unlock()

	l.active.Wait()
	if l.wal != nil {
		l.closeErr = l.wal.Close()
	}
	if l.root != nil {
		l.closeErr = errors.Join(l.closeErr, l.root.Close())
	}
	if l.dir != nil {
		l.closeErr = errors.Join(l.closeErr, l.dir.Close())
	}
	close(done)
	return l.closeErr
}
