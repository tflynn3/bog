// Package checkpoint contains the offline checkpoint tooling.
package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
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
			l.Close()
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
	if _, err = l.wal.ReadAt(magic[:], 0); err != nil || string(magic[:]) != "BOGWAL01" {
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
	defer marker.Close()
	var b [25]byte
	n, e := io.ReadFull(marker, b[:])
	if e != io.ErrUnexpectedEOF || n != 24 || string(b[:8]) != "BOGCMT01" {
		return nil, errors.New("invalid commit marker")
	}
	end := binary.LittleEndian.Uint64(b[8:16])
	if binary.LittleEndian.Uint64(b[16:24]) != ^end || end < 8 || end > uint64(info.Size()) {
		return nil, errors.New("invalid committed offset")
	}
	l.end = int64(end)
	return l, nil
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
		f.Close()
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		f.Close()
		return nil, fmt.Errorf("unsafe file %s", name)
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
func (l *Legacy) Scan(visit func(offset, next int64, firstOrdinal uint64, rows []store.Row) error) error {
	var h [16]byte
	var payload []byte
	var ordinal uint64
	for pos := int64(8); pos < l.end; {
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

// Close releases the input files and directory lock.
func (l *Legacy) Close() error {
	var err error
	if l.wal != nil {
		err = l.wal.Close()
		l.wal = nil
	}
	if l.dir != nil {
		if e := l.dir.Close(); err == nil {
			err = e
		}
		l.dir = nil
	}
	if l.root != nil {
		if e := l.root.Close(); err == nil {
			err = e
		}
		l.root = nil
	}
	return err
}
