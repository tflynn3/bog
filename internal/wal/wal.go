package wal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"

	"bog/internal/store"
)

const magic = "BOGWAL01"

var table = crc32.MakeTable(crc32.Castagnoli)
var ErrUncertain = errors.New("WAL rollback uncertain")

type file interface {
	io.Reader
	io.Writer
	io.Seeker
	Sync() error
	Truncate(int64) error
	Close() error
}
type Log struct {
	mu        sync.Mutex
	cond      *sync.Cond
	queue     []*appendRequest
	running   bool
	f         file
	s         *store.Store
	offset    int64
	poisoned  bool
	closed    bool
	closeDone bool
	closeErr  error
	committed bool
	root      *os.Root
	dir       *os.File
	syncDir   func() error
}

const maxGroup = 16

type appendRequest struct {
	payload []byte
	rows    []store.Row
	done    chan error
}

func openRegular(dir *os.File, name string, flags int) (*os.File, error) {
	fd, err := openat(int(dir.Fd()), name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0600)
	var f *os.File
	if err == nil {
		f = os.NewFile(uintptr(fd), name)
	}
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%s is not a regular file", name)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		f.Close()
		return nil, fmt.Errorf("%s has unsafe ownership, permissions, or link count", name)
	}
	return f, nil
}

// openParent resolves the parent of the pinned directory, not the parent of
// the spelling used to reach it (which may be "." or a symlink).
func openParent(dir *os.File) (*os.File, error) {
	fd, err := openat(int(dir.Fd()), "..", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "data directory parent"), nil
}

func Open(dir string, s *store.Store) (*Log, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			root.Close()
		}
	}()
	d, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			d.Close()
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
	// Lock the pinned directory inode, not a replaceable pathname in it.
	if err = syscall.Flock(int(d.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	f, err := openRegular(d, "bog.wal", os.O_RDWR)
	if errors.Is(err, os.ErrNotExist) {
		// Never create a replacement WAL when a commit marker already exists.
		if _, markerErr := root.Lstat("bog.commit"); markerErr == nil {
			return nil, errors.New("commit marker without WAL")
		} else if !errors.Is(markerErr, os.ErrNotExist) {
			return nil, markerErr
		}
		var id [16]byte
		_, err = rand.Read(id[:])
		if err == nil {
			name := ".bog-wal-init-" + hex.EncodeToString(id[:])
			var tmp *os.File
			tmp, err = openRegular(d, name, os.O_CREATE|os.O_EXCL|os.O_RDWR)
			if err == nil {
				defer root.Remove(name)
				err = writeAll(tmp, []byte(magic))
				if err == nil {
					err = tmp.Sync()
				}
				if e := tmp.Close(); err == nil {
					err = e
				}
				if err == nil {
					err = renameNoReplace(d, name, "bog.wal")
				}
				if err == nil || errors.Is(err, os.ErrExist) {
					f, err = openRegular(d, "bog.wal", os.O_RDWR)
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	l := &Log{f: f, s: s, root: root, dir: d, syncDir: d.Sync}
	// The parent directory must persist the data directory's own entry as well.
	parent, err := openParent(d)
	if err == nil {
		err = parent.Sync()
		if e := parent.Close(); err == nil {
			err = e
		}
	}
	if err != nil {
		l.Close()
		return nil, err
	}
	var committedEnd int64 = -1
	marker, err := openRegular(d, "bog.commit", os.O_RDONLY)
	hasMarker := err == nil
	l.committed = hasMarker
	if err == nil {
		b, e := io.ReadAll(io.LimitReader(marker, 25))
		marker.Close()
		if e != nil || len(b) != 24 || string(b[:8]) != "BOGCMT01" || binary.LittleEndian.Uint64(b[16:]) != ^binary.LittleEndian.Uint64(b[8:16]) {
			l.Close()
			return nil, errors.New("invalid commit marker")
		}
		end := binary.LittleEndian.Uint64(b[8:])
		size, e := f.Seek(0, io.SeekEnd)
		if e != nil || end < uint64(len(magic)) || end > uint64(size) {
			l.Close()
			return nil, errors.New("invalid committed offset")
		}
		committedEnd = int64(end)
	} else if !errors.Is(err, os.ErrNotExist) {
		l.Close()
		return nil, err
	}
	if !hasMarker {
		size, e := f.Seek(0, io.SeekEnd)
		if e != nil || size != int64(len(magic)) {
			l.Close()
			return nil, errors.New("missing commit marker for populated WAL")
		}
		committedEnd = int64(len(magic))
	}
	if err = l.replay(committedEnd); err != nil {
		l.Close()
		return nil, err
	}
	if hasMarker {
		size, e := f.Seek(0, io.SeekEnd)
		if e == nil && size > committedEnd {
			e = f.Truncate(committedEnd)
			if e == nil {
				e = f.Sync()
			}
		}
		if e == nil {
			_, e = f.Seek(committedEnd, io.SeekStart)
		}
		if e != nil {
			l.Close()
			return nil, e
		}
	}
	if !hasMarker {
		// Persist the WAL and its entry before establishing the first marker.
		// This also repairs a magic-only bootstrap left by an earlier crash.
		if err = f.Sync(); err == nil {
			err = d.Sync()
		}
		if err == nil {
			_, err = l.commit(l.offset)
		}
		if err != nil {
			l.Close()
			return nil, err
		}
	}
	// Persist directory metadata before readiness without replacing an existing
	// marker, so an intact store can reopen even when no blocks are free.
	if err = d.Sync(); err != nil {
		l.Close()
		return nil, err
	}
	success = true
	return l, nil
}
func (l *Log) replay(end int64) error {
	info, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if end >= 0 {
		info = end
	}
	if _, err = l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header := make([]byte, len(magic))
	if _, err = io.ReadFull(l.f, header); err != nil || string(header) != magic {
		return errors.New("invalid WAL magic")
	}
	pos := int64(len(magic))
	for pos < info {
		if info-pos < 16 {
			return l.recoverTail(pos)
		}
		h := make([]byte, 16)
		_, err = io.ReadFull(l.f, h)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return l.recoverTail(pos)
		}
		if err != nil {
			return err
		}
		if crc32.Checksum(h[:12], table) != binary.LittleEndian.Uint32(h[12:]) {
			return fmt.Errorf("invalid WAL header checksum at %d", pos)
		}
		n := binary.LittleEndian.Uint32(h)
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:]) != ^n {
			return fmt.Errorf("invalid WAL header at %d", pos)
		}
		if int64(n) > info-pos-16 {
			return l.recoverTail(pos)
		}
		payload := make([]byte, n)
		_, err = io.ReadFull(l.f, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return l.recoverTail(pos)
		}
		if err != nil {
			return err
		}
		if crc32.Checksum(payload, table) != binary.LittleEndian.Uint32(h[8:]) {
			return fmt.Errorf("invalid WAL checksum at %d", pos)
		}
		rows, err := store.Decode(payload)
		if err != nil {
			return fmt.Errorf("invalid WAL payload at %d: %w", pos, err)
		}
		l.s.Publish(rows)
		pos += 16 + int64(n)
	}
	l.offset = pos
	_, err = l.f.Seek(pos, io.SeekStart)
	return err
}
func (l *Log) recoverTail(pos int64) error {
	if l.committed {
		return errors.New("incomplete committed frame")
	}
	if err := l.f.Truncate(pos); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.offset = pos
	_, err := l.f.Seek(pos, io.SeekStart)
	return err
}
func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
func (l *Log) initCond() {
	if l.cond == nil {
		l.cond = sync.NewCond(&l.mu)
	}
}

// Append waits for admission and for its entire batch to become durable and visible.
func (l *Log) Append(payload []byte, rows []store.Row) error {
	l.mu.Lock()
	l.initCond()
	for len(l.queue) >= maxGroup && !l.closed && !l.poisoned {
		l.cond.Wait()
	}
	if l.poisoned {
		l.mu.Unlock()
		return ErrUncertain
	}
	if l.closed {
		l.mu.Unlock()
		return os.ErrClosed
	}
	req := &appendRequest{payload: payload, rows: rows, done: make(chan error, 1)}
	l.queue = append(l.queue, req)
	if !l.running {
		l.running = true
		go l.run()
	}
	l.mu.Unlock()
	return <-req.done
}

func (l *Log) run() {
	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.running = false
			l.cond.Broadcast()
			l.mu.Unlock()
			return
		}
		group := l.queue
		l.queue = nil
		l.cond.Broadcast()
		l.mu.Unlock()

		err := l.appendGroup(group)
		l.mu.Lock()
		if errors.Is(err, ErrUncertain) {
			l.poisoned = true
			for _, req := range l.queue {
				req.done <- ErrUncertain
			}
			l.queue = nil
			l.cond.Broadcast()
		}
		for _, req := range group {
			req.done <- err
		}
		l.mu.Unlock()
	}
}

func (l *Log) appendGroup(group []*appendRequest) error {
	next := l.offset
	for _, req := range group {
		var h [16]byte
		n := uint32(len(req.payload))
		binary.LittleEndian.PutUint32(h[:], n)
		binary.LittleEndian.PutUint32(h[4:], ^n)
		binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(req.payload, table))
		binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], table))
		if err := writeAll(l.f, h[:]); err != nil {
			return l.rollback(err)
		}
		if err := writeAll(l.f, req.payload); err != nil {
			return l.rollback(err)
		}
		next += int64(len(h) + len(req.payload))
	}
	if err := l.f.Sync(); err != nil {
		return l.rollback(err)
	}
	if l.root != nil {
		replaced, err := l.commit(next)
		if err != nil {
			if !replaced {
				return l.rollback(err)
			}
			return ErrUncertain
		}
	}
	l.offset = next
	for _, req := range group {
		l.s.Publish(req.rows)
	}
	return nil
}

func (l *Log) rollback(err error) error {
	if e := l.f.Truncate(l.offset); e != nil {
		return ErrUncertain
	}
	if e := l.f.Sync(); e != nil {
		return ErrUncertain
	}
	if _, e := l.f.Seek(l.offset, io.SeekStart); e != nil {
		return ErrUncertain
	}
	return err
}

// commit atomically replaces the durable marker only after the WAL is synced.
func (l *Log) commit(offset int64) (bool, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return false, err
	}
	name := ".bog-commit-" + hex.EncodeToString(id[:])
	f, err := openRegular(l.dir, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY)
	if err != nil {
		return false, err
	}
	defer l.root.Remove(name)
	b := make([]byte, 24)
	copy(b, "BOGCMT01")
	binary.LittleEndian.PutUint64(b[8:], uint64(offset))
	binary.LittleEndian.PutUint64(b[16:], ^uint64(offset))
	err = writeAll(f, b)
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return false, err
	}
	// A rename failure is treated as uncertain: do not risk rolling back a
	// WAL whose marker may already have been replaced.
	if err = l.root.Rename(name, "bog.commit"); err != nil {
		return true, err
	}
	return true, l.syncDir()
}
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.initCond()
	if l.closed {
		for !l.closeDone {
			l.cond.Wait()
		}
		return l.closeErr
	}
	l.closed = true
	l.cond.Broadcast()
	for l.running {
		l.cond.Wait()
	}
	err := l.f.Close()
	if l.dir != nil {
		if e := l.dir.Close(); err == nil {
			err = e
		}
	}
	if l.root != nil {
		if e := l.root.Close(); err == nil {
			err = e
		}
	}
	l.closeErr = err
	l.closeDone = true
	l.cond.Broadcast()
	return err
}
