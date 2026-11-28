package wal

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"sync/atomic"
	"syscall"
)

// Owner holds the writer lease. Its caller serializes appends, selection and
// capture acquisition. Tails and independent readers must close before Owner.
type Owner struct {
	files   *Log
	outcome atomic.Pointer[CommitOutcome]
	fence   error
}

// Tail owns an open file. ReadAt uses logical WAL offsets and supports
// concurrent reads of immutable committed bytes. Callers supply their own
// section bounds and keep the tail open for the duration of each read.
type Tail struct {
	file      *os.File
	base      int64
	end       int64
	sum       hash.Cloner
	authority Authority
}

func OpenOwner(path string) (_ *Owner, err error) {
	root, dir, err := lockDirectory(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, dir.Close(), root.Close())
		}
	}()
	parent, err := openParent(dir)
	if err != nil {
		return nil, fmt.Errorf("open data directory parent: %w", err)
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return nil, fmt.Errorf("sync data directory parent: %w", err)
	}
	return &Owner{files: &Log{root: root, dir: dir, syncDir: dir.Sync}}, nil
}

func (o *Owner) Directory() *os.File { return o.files.dir }
func (o *Owner) Root() *os.Root      { return o.files.root }
func (o *Owner) Close() error        { return errors.Join(o.files.dir.Close(), o.files.root.Close()) }
func (o *Owner) CleanStages() error  { return o.files.cleanOrphans() }

func (o *Owner) OpenLegacy() (_ *Tail, err error) {
	if err := rejectRestoreImage(o.Root()); err != nil {
		return nil, err
	}
	f, err := o.files.openWAL()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock WAL: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	var hdr [len(magic)]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil || string(hdr[:]) != magic {
		return nil, errors.Join(errors.New("invalid WAL magic"), err)
	}
	end, marker, err := o.files.committedEnd(info.Size())
	if err != nil {
		return nil, err
	}
	if !marker {
		if err := errors.Join(f.Sync(), o.Directory().Sync()); err != nil {
			return nil, err
		}
		if _, err := o.files.commit(end); err != nil {
			return nil, err
		}
	}
	return &Tail{file: f, end: end}, nil
}

func (o *Owner) OpenSelected(a Authority, maxBytes uint64) (_ *Tail, err error) {
	if !a.valid() || uint64(a.End-a.Base) > maxBytes {
		return nil, errors.New("invalid or oversized selected suffix")
	}
	f, err := openRegular(o.Directory(), a.Tail, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st := info.Sys().(*syscall.Stat_t)
	if uint64(st.Dev) != a.Device || st.Ino != a.Inode || info.Size() < a.End-a.Base {
		return nil, errors.New("selected WAL tail identity or size mismatch")
	}
	h := sha256.New().(hash.Cloner)
	if _, err := io.CopyN(h, io.NewSectionReader(f, 0, a.End-a.Base), a.End-a.Base); err != nil {
		return nil, err
	}
	if digestString(h.Sum(nil)) != a.SuffixSHA256 {
		return nil, errors.New("selected WAL tail checksum mismatch")
	}
	return &Tail{file: f, base: a.Base, end: a.End, sum: h, authority: a}, nil
}

func (t *Tail) Name() string { return t.file.Name() }
func (t *Tail) End() int64   { return t.end }
func (t *Tail) Close() error { return t.file.Close() }
func (t *Tail) ReadAt(p []byte, off int64) (int, error) {
	if off < t.base {
		return 0, io.EOF
	}
	return t.file.ReadAt(p, off-t.base)
}

// FinishRecovery discards uncommitted bytes only after the caller has
// validated the generation and all committed frames.
func (t *Tail) FinishRecovery() error {
	info, err := t.file.Stat()
	if err != nil {
		return err
	}
	if size := t.end - t.base; size != info.Size() {
		if err := t.file.Truncate(size); err != nil {
			return err
		}
	}
	return t.file.Sync()
}

func (o *Owner) NewTail(source io.ReaderAt, a Authority) (_ *Tail, err error) {
	if a.CoveredOffset < int64(len(magic)) || a.End < a.CoveredOffset {
		return nil, errors.New("invalid WAL rebase range")
	}
	var id [stageIDSize]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	a.Tail, a.Base, a.Version = tailPrefix+digestString(id[:]), a.CoveredOffset, 2
	f, err := openRegular(o.Directory(), a.Tail, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close(), o.remove(a.Tail))
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return nil, err
	}
	h := sha256.New().(hash.Cloner)
	size := a.End - a.Base
	if _, err := io.CopyN(io.MultiWriter(f, h), io.NewSectionReader(source, a.Base, size), size); err != nil {
		return nil, err
	}
	if sum := digestString(h.Sum(nil)); a.SuffixSHA256 != "" && a.SuffixSHA256 != sum {
		return nil, errors.New("rebased WAL suffix checksum mismatch")
	} else {
		a.SuffixSHA256 = sum
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st := info.Sys().(*syscall.Stat_t)
	a.Device, a.Inode = uint64(st.Dev), st.Ino
	if err := errors.Join(f.Sync(), o.Directory().Sync()); err != nil {
		return nil, err
	}
	return &Tail{file: f, base: a.Base, end: a.End, sum: h, authority: a}, nil
}

func (o *Owner) Select(t *Tail) error {
	if o.fence != nil {
		return o.fence
	}
	operation, err := o.selectAuthority(t.authority)
	if errors.Is(err, ErrUncertain) {
		o.fence = err
		outcome := newCommitOutcome(operation, err)
		o.outcome.Store(&outcome)
	}
	return err
}

func (o *Owner) LastCommitOutcome() CommitOutcome {
	if p := o.outcome.Load(); p != nil {
		return *p
	}
	return CommitOutcome{}
}

func (o *Owner) Append(t *Tail, payload []byte, ordinal uint64) (err error) {
	if o.fence != nil {
		return o.fence
	}
	if len(payload) == 0 || len(payload) > math.MaxUint32 || int64(len(payload))+FrameHeaderSize > math.MaxInt64-t.end {
		return errors.New("invalid WAL append extent")
	}
	op := "wal_write"
	defer func() {
		outcome := newCommitOutcome(op, err)
		o.outcome.Store(&outcome)
		switch outcome.State {
		case CommitUncertain:
			o.fence = err
		case CommitRolledBack:
			o.files.diagnoseRollback(outcome)
		}
	}()
	start := t.end - t.base
	rollback := func(cause error) error {
		if undo := errors.Join(t.file.Truncate(start), t.file.Sync()); undo != nil {
			return errors.Join(ErrUncertain, cause, undo)
		}
		return cause
	}
	hdr := frameHeader(payload)
	w := io.NewOffsetWriter(t.file, start)
	if err := writeAll(w, hdr[:]); err != nil {
		return rollback(err)
	}
	if err := writeAll(w, payload); err != nil {
		return rollback(err)
	}
	op = "wal_sync"
	if err := t.file.Sync(); err != nil {
		return rollback(err)
	}
	end := t.end + int64(FrameHeaderSize+len(payload))
	if t.sum == nil {
		op, err = o.files.commit(end)
	} else {
		a := t.authority
		next, cloneErr := t.sum.Clone()
		if cloneErr != nil {
			return rollback(cloneErr)
		}
		// SHA-256's Write never returns an error.
		next.Write(hdr[:])
		next.Write(payload)
		a.End, a.Ordinal, a.SuffixSHA256 = end, ordinal, digestString(next.Sum(nil))
		op, err = o.selectAuthority(a)
		if err == nil {
			t.authority, t.sum = a, next
		}
	}
	if err != nil {
		if errors.Is(err, ErrUncertain) {
			return err
		}
		return rollback(err)
	}
	t.end, op = end, "commit"
	return nil
}

func (o *Owner) remove(name string) error {
	if err := o.Root().Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return o.Directory().Sync()
}

// Retire closes a predecessor. It removes the exact owned inode only after
// the caller has drained its pins and moved durable authority away from it.
func (o *Owner) Retire(t *Tail) error {
	if t.Name() == walName {
		if err := o.remove(markerName); err != nil {
			return err
		}
	}
	have, err := t.file.Stat()
	if err != nil {
		return err
	}
	named, err := o.Root().Lstat(t.Name())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && !os.SameFile(have, named) {
		return errors.New("retired WAL name changed identity")
	}
	if err := o.remove(t.Name()); err != nil {
		return err
	}
	return t.Close()
}

// InstallImmutable syncs a new immutable name or verifies an identical file
// left by an interrupted activation.
func (o *Owner) InstallImmutable(name string, data []byte) error {
	existing, err := o.OpenFile(name)
	if err == nil {
		b, err := io.ReadAll(io.LimitReader(existing, int64(len(data))+1))
		if err := errors.Join(err, existing.Close()); err != nil {
			return err
		}
		if string(b) != string(data) {
			return errors.New("immutable file already exists with different contents")
		}
		return o.Directory().Sync()
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	staged, err := o.files.writeStaged(walStagePrefix, data)
	if err != nil {
		return err
	}
	if err := renameNoReplace(o.Directory(), staged, name); err != nil {
		return errors.Join(err, o.files.removeStage(staged))
	}
	return o.Directory().Sync()
}

// CleanUnselectedTails runs after recovery makes the selected authority and
// tail durable. Random protocol-owned names can remain after a power loss.
func (o *Owner) CleanUnselectedTails(selected string) (err error) {
	dir, err := o.Root().Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	for {
		names, readErr := dir.Readdirnames(64)
		for _, name := range names {
			if name == selected || (!ownedTail(name) && name != walName) {
				continue
			}
			f, err := o.OpenFile(name)
			if err != nil {
				return err
			}
			if err := o.Retire(&Tail{file: f}); err != nil {
				return errors.Join(err, f.Close())
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}
