package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"bog/internal/store"
)

const (
	walName    = "bog.wal"
	markerName = "bog.commit"
)

// Open takes ownership of the pinned directory for the lifetime of the Log.
// Recovery builds private state and installs it only after all disk work succeeds.
func Open(dir string, s *store.Store) (_ *Log, err error) {
	if s == nil {
		return nil, errors.New("WAL requires a store")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, root.Close())
		}
	}()
	d, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.Close())
		}
	}()

	info, err := d.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		return nil, errors.New("data directory must be owned by the service user and not group- or world-writable")
	}
	// Lock the pinned directory inode so replacing a pathname cannot bypass it.
	if err := syscall.Flock(int(d.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock data directory: %w", err)
	}
	if err := rejectRestoreImage(root); err != nil {
		return nil, err
	}
	l := &Log{s: s, root: root, dir: d, syncDir: d.Sync}
	f, err := l.openWAL()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	// Keep the inode lock for compatibility with existing owners.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock WAL: %w", err)
	}
	l.captures, err = openCaptureFile(d, f)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, l.captures.release())
		}
	}()
	l.f = f
	if err := l.recover(); err != nil {
		return nil, err
	}
	return l, nil
}

func rejectRestoreImage(root *os.Root) error {
	// These names are reserved by the v1 backup format. A checkpoint alone
	// can coexist with a complete legacy WAL and is not a restore indicator.
	for _, name := range []string{"bog.backup", "bog.wal.tail"} {
		if _, err := root.Lstat(name); err == nil {
			return fmt.Errorf("found %s: backup/restore image requires checkpoint-aware recovery; use a restore-capable Bog reader", name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect restore entry %s: %w", name, err)
		}
	}
	return nil
}

func (l *Log) openWAL() (*os.File, error) {
	f, err := openRegular(l.dir, walName, os.O_RDWR)
	if !errors.Is(err, os.ErrNotExist) {
		return f, err
	}
	// A marker without a WAL cannot be recovered by bootstrapping a new WAL.
	if _, err := l.root.Lstat(markerName); err == nil {
		return nil, errors.New("commit marker without WAL")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// With neither authoritative name present, every staging file is orphaned.
	if err := l.cleanOrphans(); err != nil {
		return nil, err
	}
	name, err := l.writeStaged(walStagePrefix, []byte(magic))
	if err != nil {
		return nil, err
	}
	if err := renameNoReplace(l.dir, name, walName); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("install WAL: %w", err)
		}
		if err := l.removeStage(name); err != nil {
			return nil, err
		}
	}
	return openRegular(l.dir, walName, os.O_RDWR)
}

func (l *Log) recover() error {
	// Persist the data directory's own entry, using the pinned directory's
	// parent even if the caller opened it through a symlink.
	parent, err := openParent(l.dir)
	if err != nil {
		return err
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return fmt.Errorf("sync data directory parent: %w", err)
	}

	size, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("size WAL: %w", err)
	}
	end, hasMarker, err := l.committedEnd(size)
	if err != nil {
		return err
	}
	builder := store.NewRecoveryBuilder()
	if err := l.replay(end, builder); err != nil {
		return err
	}
	if hasMarker && size > end {
		if err := l.f.Truncate(end); err != nil {
			return fmt.Errorf("discard uncommitted WAL suffix: %w", err)
		}
		if err := l.f.Sync(); err != nil {
			return fmt.Errorf("sync recovered WAL: %w", err)
		}
	}
	// The final names and every committed frame have been validated. Staging
	// entries cannot be referenced by the marker and no writer owns them.
	if err := l.cleanOrphans(); err != nil {
		return err
	}
	if !hasMarker {
		// Also repairs a magic-only WAL left before first marker publication.
		if err := l.f.Sync(); err != nil {
			return fmt.Errorf("sync bootstrap WAL: %w", err)
		}
		if err := l.syncDir(); err != nil {
			return fmt.Errorf("sync bootstrap WAL entry: %w", err)
		}
		if _, err := l.commit(end); err != nil {
			return err
		}
	} else if err := l.syncDir(); err != nil {
		// Persist directory metadata without allocating a new marker.
		return fmt.Errorf("sync recovered data directory: %w", err)
	}
	builder.Install(l.s)
	// QueryPage with a zero row limit captures the installed row count
	// without scanning or decoding the recovered prefix again.
	_, count, _, _ := l.s.QueryPage(time.Time{}, time.Time{}, "", 0, nil, nil)
	l.committed = captureBoundary{end: end, nextOrdinal: uint64(count)}
	return nil
}

func (l *Log) committedEnd(size int64) (int64, bool, error) {
	marker, err := openRegular(l.dir, markerName, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		if size != int64(len(magic)) {
			return 0, false, errors.New("missing commit marker for populated WAL")
		}
		return size, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	end, err := readCommitMarker(marker, size)
	return end, true, errors.Join(err, marker.Close())
}

func readCommitMarker(r io.Reader, size int64) (int64, error) {
	b, err := io.ReadAll(io.LimitReader(r, markerSize+1))
	if err != nil {
		return 0, fmt.Errorf("read commit marker: %w", err)
	}
	if len(b) != markerSize || string(b[:8]) != markerMagic ||
		binary.LittleEndian.Uint64(b[16:]) != ^binary.LittleEndian.Uint64(b[8:16]) {
		return 0, errors.New("invalid commit marker")
	}
	end := binary.LittleEndian.Uint64(b[8:16])
	if size < int64(len(magic)) || end < uint64(len(magic)) || end > uint64(size) {
		return 0, errors.New("invalid committed offset")
	}
	return int64(end), nil
}

func (l *Log) replay(end int64, builder *store.RecoveryBuilder) error {
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL header: %w", err)
	}
	reader := bufio.NewReaderSize(io.LimitReader(l.f, end), 64*1024)
	var header [len(magic)]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return fmt.Errorf("read WAL magic: %w", err)
	}
	if string(header[:]) != magic {
		return errors.New("invalid WAL magic")
	}

	var payload []byte
	var h [16]byte
	for pos := int64(len(magic)); pos < end; {
		if end-pos < int64(len(h)) {
			return fmt.Errorf("incomplete committed frame header at %d", pos)
		}
		if _, err := io.ReadFull(reader, h[:]); err != nil {
			return fmt.Errorf("read committed frame header at %d: %w", pos, err)
		}
		if crc32.Checksum(h[:12], table) != binary.LittleEndian.Uint32(h[12:]) {
			return fmt.Errorf("invalid WAL header checksum at %d", pos)
		}
		n := binary.LittleEndian.Uint32(h[:])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:]) != ^n {
			return fmt.Errorf("invalid WAL header at %d", pos)
		}
		if int64(n) > end-pos-int64(len(h)) {
			return fmt.Errorf("incomplete committed frame payload at %d", pos)
		}
		if cap(payload) < int(n) {
			capacity := min(store.MaxBatch, max(int(n), 2*cap(payload)))
			payload = make([]byte, n, capacity)
		} else {
			payload = payload[:n]
		}
		if _, err := io.ReadFull(reader, payload); err != nil {
			return fmt.Errorf("read committed frame payload at %d: %w", pos, err)
		}
		if crc32.Checksum(payload, table) != binary.LittleEndian.Uint32(h[8:]) {
			return fmt.Errorf("invalid WAL checksum at %d", pos)
		}
		if err := builder.DecodeFrame(payload); err != nil {
			return fmt.Errorf("invalid WAL payload at %d: %w", pos, err)
		}
		pos += int64(len(h)) + int64(n)
	}
	if _, err := l.f.Seek(end, io.SeekStart); err != nil {
		return fmt.Errorf("seek committed WAL boundary: %w", err)
	}
	l.offset = end
	return nil
}

func (l *Log) cleanOrphans() error {
	// Use a fresh directory stream rooted at the pinned inode. Read a bounded
	// page at a time even when earlier startups left many staging files.
	dir, err := l.root.Open(".")
	if err != nil {
		return err
	}
	err = l.scanOrphans(dir)
	return errors.Join(err, dir.Close())
}

func (l *Log) scanOrphans(dir *os.File) error {
	for {
		entries, err := dir.ReadDir(64)
		for _, entry := range entries {
			name := entry.Name()
			if !isStageName(name) {
				continue
			}
			info, err := l.root.Lstat(name)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return fmt.Errorf("inspect staging entry: %w", err)
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if info.Mode().IsRegular() && ok && st.Uid == uint32(os.Geteuid()) && st.Nlink == 1 {
				if err := l.removeStage(name); err != nil {
					return err
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("scan WAL staging entries: %w", err)
		}
	}
}

func isStageName(name string) bool {
	for _, prefix := range []string{walStagePrefix, markerStagePrefix} {
		suffix, ok := strings.CutPrefix(name, prefix)
		if !ok || len(suffix) != stageIDSize*2 {
			continue
		}
		for _, c := range suffix {
			if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
				return false
			}
		}
		return true
	}
	return false
}
