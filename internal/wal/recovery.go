package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	return openOwner(dir, s, func(l *Log) error {
		if _, err := l.root.Lstat(AuthorityName); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return err
			}
			return errors.New("retained writable authority requires checkpoint-aware recovery")
		}
		if err := rejectRestoreImage(l.root); err != nil {
			return err
		}
		return l.OpenOrdinary(nil)
	})
}

// OpenRuntime holds the owner lock through recovery and until Close. The
// callback must install one writable file before returning successfully.
func OpenRuntime(dir string, recover func(*Log) error) (*Log, error) {
	return openOwner(dir, nil, recover)
}

func openOwner(dir string, s *store.Store, recover func(*Log) error) (_ *Log, err error) {
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
	l := &Log{s: s, root: root, dir: d, syncDir: d.Sync}
	parent, err := openParent(d)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return nil, fmt.Errorf("sync data directory parent: %w", err)
	}
	if err := recover(l); err != nil {
		return nil, errors.Join(err, l.closeFiles())
	}
	if l.f == nil {
		return nil, errors.New("missing writable WAL after recovery")
	}
	return l, nil
}

// Directory returns the borrowed handles held for the owner's lifetime.
func (l *Log) Directory() (*os.Root, *os.File) { return l.root, l.dir }

// OpenOrdinary recovers the ordinary WAL during initialization. check can
// validate an existing checkpoint against the committed file before truncation.
func (l *Log) OpenOrdinary(check func(*os.File, int64, uint64) error) (err error) {
	f, err := l.openWAL()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lock WAL: %w", err)
	}
	capture, err := openCaptureFile(l.dir, f, walName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, capture.release())
		}
	}()
	if err := l.recoverOrdinary(f, check); err != nil {
		return err
	}
	l.f, l.captures = f, capture
	return nil
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

type recoveryFile interface {
	file
	io.ReaderAt
	Stat() (os.FileInfo, error)
}

func (l *Log) recoverOrdinary(f recoveryFile, check func(*os.File, int64, uint64) error) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	end, hasMarker, err := l.committedEnd(size)
	if err != nil {
		return err
	}
	var header [len(magic)]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		return fmt.Errorf("read WAL magic: %w", err)
	}
	if string(header[:]) != magic {
		return errors.New("invalid WAL magic")
	}

	var builder *store.RecoveryBuilder
	if l.s != nil {
		builder = store.NewRecoveryBuilder()
	}
	next, err := l.scanRecovery(f, int64(len(magic)), end, 0, builder)
	if err != nil {
		return err
	}
	if check != nil {
		if err := check(f.(*os.File), end, next); err != nil {
			return err
		}
	}
	if err := finishReplay(f, size, end); err != nil {
		return err
	}
	if err := l.cleanOrphans(); err != nil {
		return err
	}
	if !hasMarker {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync bootstrap WAL: %w", err)
		}
		if err := l.syncDir(); err != nil {
			return fmt.Errorf("sync bootstrap WAL entry: %w", err)
		}
		if err := l.syncDir(); err != nil {
			return fmt.Errorf("sync bootstrap WAL entry: %w", err)
		}
		if _, err := l.commit(end); err != nil {
			return err
		}
	} else if err := l.syncDir(); err != nil {
		return fmt.Errorf("sync recovered data directory: %w", err)
	}
	if builder != nil {
		builder.Install(l.s)
	}
	l.offset = end
	l.committed = captureBoundary{end: end, nextOrdinal: next}
	return nil
}

func finishReplay(f file, size, end int64) error {
	if size > end {
		if err := f.Truncate(end); err != nil {
			return fmt.Errorf("discard uncommitted WAL suffix: %w", err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync recovered WAL: %w", err)
		}
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		return fmt.Errorf("seek committed WAL boundary: %w", err)
	}
	return nil
}

func (l *Log) scanRecovery(f io.ReaderAt, start, end int64, ordinal uint64, builder *store.RecoveryBuilder) (uint64, error) {
	next := ordinal
	var visit func([]byte) error
	if builder != nil {
		visit = builder.DecodeFrame
	}
	err := scanFrames(context.Background(), f, start, end, ordinal, visit, func(frame Frame) error {
		next = frame.NextOrdinal
		if l.s == nil && len(l.frames) <= MaxLiveFrames {
			frame.last = time.Time{}
			l.addFrame(frame)
		}
		return nil
	})
	return next, err
}

func (l *Log) addFrame(frame Frame) {
	var previous *Frame
	if len(l.frames) != 0 {
		previous = &l.frames[len(l.frames)-1]
	}
	l.bounds.add(frame, previous)
	l.frames = append(l.frames, frame)
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
