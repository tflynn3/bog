package wal

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"syscall"

	"bog/internal/store"
)

var ErrCapacity = errors.New("WAL tail capacity exhausted")

// Owner is the directory lease and commit serializer for the retained runtime.
// The command owns generation selection and admission to its decoded views.
type Owner struct {
	log      *Log
	tail     *Tail
	point    Point
	digest   hash.Hash
	selected selector
}

type Locked struct{ owner *Owner }

// Tail maps a pinned WAL file into logical offsets. Its owner keeps the file
// open until all callers that borrowed it under Locked have drained.
type Tail struct {
	closed   bool
	closeErr error
	file     *os.File
	base     int64
	id       fileID
}

// Close drops the descriptor without unlinking it (for fenced shutdown).
func (t *Tail) Close() error {
	if t == nil {
		return nil
	}
	if !t.closed {
		t.closed = true
		t.closeErr = t.file.Close()
	}
	return t.closeErr
}

func (t *Tail) Section(start, end int64) *io.SectionReader {
	return io.NewSectionReader(t.file, start-t.base, end-start)
}

func OpenOwner(path string) (*Owner, error) {
	root, dir, err := lockDirectory(path)
	if err != nil {
		return nil, err
	}
	if err := syncParent(dir); err != nil {
		return nil, errors.Join(err, dir.Close(), root.Close())
	}
	var fs syscall.Statfs_t
	if err := syscall.Fstatfs(int(dir.Fd()), &fs); err != nil {
		return nil, errors.Join(fmt.Errorf("probe data directory filesystem: %w", err), dir.Close(), root.Close())
	}
	return &Owner{log: &Log{root: root, dir: dir, syncDir: dir.Sync}}, nil
}

func (o *Owner) Directory() (*os.Root, *os.File) { return o.log.root, o.log.dir }

func (o *Owner) OpenProbeDirectory() (*os.File, error) {
	probe, err := o.log.root.Open(".")
	if err != nil {
		return nil, err
	}
	want, wantErr := o.log.dir.Stat()
	got, gotErr := probe.Stat()
	if err := errors.Join(wantErr, gotErr); err != nil {
		return nil, errors.Join(err, probe.Close())
	}
	if !os.SameFile(want, got) {
		return nil, errors.Join(errors.New("readiness directory identity mismatch"), probe.Close())
	}
	return probe, nil
}

func (o *Owner) WithLock(fn func(*Locked) error) error {
	o.log.mu.Lock()
	defer o.log.mu.Unlock()
	if err := o.log.admissionError(); err != nil {
		return err
	}
	return fn(&Locked{owner: o})
}

// Synchronize allows release of previously admitted references after fencing.
func (o *Owner) Synchronize(fn func(*Locked)) {
	o.log.mu.Lock()
	defer o.log.mu.Unlock()
	fn(&Locked{owner: o})
}

func (l *Locked) Err() error { return l.owner.log.admissionError() }

func (o *Owner) Fence(cause error) {
	o.log.mu.Lock()
	defer o.log.mu.Unlock()
	(&Locked{owner: o}).Fence(cause)
}

// Fence bars new work before the caller releases owner synchronization.
func (l *Locked) Fence(cause error) error {
	err := errors.Join(ErrUncertain, cause)
	l.owner.log.poisoned = err
	outcome := newCommitOutcome("authority_selection", err)
	l.owner.log.lastOutcome.Store(&outcome)
	return err
}

func (o *Owner) LastCommitOutcome() CommitOutcome { return o.log.LastCommitOutcome() }

// OpenSelected opens only the named authority. Absence is distinct from an
// invalid selector or a missing referenced file.
func (l *Locked) OpenSelected() (bool, error) {
	o := l.owner
	b, err := o.log.readSmall(authorityName, authorityMax)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s, p, err := decodeSelector(b)
	if err != nil {
		return false, fmt.Errorf("decode %s: %w", authorityName, err)
	}
	for name, expected := range map[string]*fileID{walName: s.LegacyWAL, markerName: s.LegacyMarker} {
		if err := o.log.checkPredecessor(name, expected, false); err != nil {
			return false, err
		}
	}
	if !s.FromRestore {
		if err := rejectRestoreImageForMigration(o.log.root); err != nil {
			return false, err
		}
	}
	t, size, err := o.openTail(s.Tail, s.Base, os.O_RDWR)
	if err != nil {
		return false, err
	}
	if t.id != (fileID{s.Device, s.Inode}) || size < p.End-s.Base {
		return false, errors.Join(errors.New("selected WAL tail identity or size mismatch"), t.file.Close())
	}
	o.tail, o.point, o.selected = t, p, s
	return true, nil
}

func rejectRestoreImageForMigration(root *os.Root) error {
	for _, name := range []string{"bog.backup", "bog.wal.tail"} {
		if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return err
			}
			return fmt.Errorf("mixed ordinary WAL and restore entry %s", name)
		}
	}
	return nil
}

func (l *Locked) OpenOrdinary() (int64, error) {
	o := l.owner
	if err := rejectRestoreImageForMigration(o.log.root); err != nil {
		return 0, err
	}
	if _, err := o.log.root.Lstat(walName); errors.Is(err, os.ErrNotExist) {
		if _, err := o.log.root.Lstat("bog.checkpoint"); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return 0, err
			}
			return 0, errors.New("checkpoint without ordinary WAL or writable authority")
		}
	} else if err != nil {
		return 0, err
	}
	f, err := o.log.openWAL()
	if err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, errors.Join(err, f.Close())
	}
	o.tail = &Tail{file: f, id: identity(info)}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return 0, fmt.Errorf("lock ordinary WAL: %w", err)
	}
	end, hasMarker, err := o.log.committedEnd(info.Size())
	if err != nil {
		return 0, err
	}
	var header [len(magic)]byte
	if _, err := io.ReadFull(o.tail.Section(0, end), header[:]); err != nil {
		return 0, err
	}
	if string(header[:]) != magic {
		return 0, errors.New("invalid WAL magic")
	}
	if !hasMarker {
		if err := errors.Join(f.Sync(), o.log.syncDir()); err != nil {
			return 0, err
		}
		if _, err := o.log.commit(end); err != nil {
			return 0, err
		}
	}
	return end, nil
}

// OpenRestore pins the verified-image tail before its bytes are validated.
func (l *Locked) OpenRestore(base, end int64) (io.ReadCloser, error) {
	o := l.owner
	for _, name := range []string{walName, markerName} {
		if err := o.log.checkPredecessor(name, nil, false); err != nil {
			return nil, err
		}
	}
	t, size, err := o.openTail("bog.wal.tail", base, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	if size != end-base {
		return nil, errors.Join(errors.New("restore tail length mismatch"), t.file.Close())
	}
	return &restoreReader{SectionReader: t.Section(base, end), file: t.file}, nil
}

type restoreReader struct {
	*io.SectionReader
	file *os.File
}

func (r *restoreReader) Close() error { return r.file.Close() }

func (o *Owner) openTail(name string, base int64, flags int) (*Tail, int64, error) {
	f, err := openRegular(o.log.dir, name, flags)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, 0, errors.Join(err, f.Close())
	}
	return &Tail{file: f, base: base, id: identity(info)}, info.Size(), nil
}

func (l *Locked) State() (Point, *Tail) { return l.owner.point, l.owner.tail }

// FinishRecovery syncs the validated authority and discards uncommitted bytes.
// The caller has already validated the generation, suffix frames and ordinals.
func (l *Locked) FinishRecovery(p Point) error {
	o := l.owner
	t := o.tail
	digest, err := hashSection(t.Section(p.Covered, p.End), p.End-p.Covered)
	if err != nil {
		return err
	}
	if sumDigest(digest) != p.Suffix || (o.selected.Tail != "" && o.point != p) {
		return errors.New("recovered WAL suffix does not match authority")
	}
	if err := o.log.syncDir(); err != nil {
		return err
	}
	n := p.End - t.base
	info, err := t.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > n {
		if err := t.file.Truncate(n); err != nil {
			return err
		}
	}
	if err := t.file.Sync(); err != nil {
		return err
	}
	if _, err := t.file.Seek(n, io.SeekStart); err != nil {
		return err
	}
	o.point, o.digest = p, digest
	return nil
}

func hashSection(r io.Reader, size int64) (hash.Hash, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if n != size {
		err = errors.Join(err, io.ErrUnexpectedEOF)
	}
	return h, err
}

func sumDigest(h hash.Hash) (sum [32]byte) {
	h.Sum(sum[:0])
	return sum
}

// Select copies the exact successor suffix and installs the single writable
// selector before switching the active tail. A returned old tail transfers to
// the caller for retirement after its references drain.
func (l *Locked) Select(p Point, source io.Reader, want [32]byte) (_ *Tail, err error) {
	o := l.owner
	if o.point.Sequence == math.MaxUint64 || p.Covered < int64(len(magic)) || p.End < p.Covered ||
		p.Next < p.First || (p.Next == p.First) != (p.End == p.Covered) {
		return nil, errors.New("invalid successor WAL bounds or sequence")
	}
	var token [stageIDSize]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	name := tailPrefix + hex.EncodeToString(token[:])
	f, err := openRegular(o.log.dir, name, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			err = errors.Join(err, f.Close(), o.log.removeStage(name), o.log.syncDir())
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return nil, err
	}
	digest, err := hashSection(io.TeeReader(source, f), p.End-p.Covered)
	if err != nil {
		return nil, err
	}
	p.Suffix = sumDigest(digest)
	p.Sequence = o.point.Sequence + 1
	if p.Suffix != want {
		return nil, errors.New("successor WAL suffix digest changed")
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	id := identity(info)
	s := selector{Version: 2, Sequence: p.Sequence, Generation: hexDigest(p.Generation), Covered: p.Covered, First: p.First,
		End: p.End, Next: p.Next, Suffix: hexDigest(p.Suffix), Tail: name, Base: p.Covered, Device: id.Device, Inode: id.Inode,
		LegacyWAL: o.selected.LegacyWAL, LegacyMarker: o.selected.LegacyMarker,
		FromRestore: o.selected.FromRestore || o.tail == nil}
	if o.selected.Tail == "" && o.tail != nil {
		marker, err := openRegular(o.log.dir, markerName, os.O_RDONLY)
		if err != nil {
			return nil, err
		}
		info, err := marker.Stat()
		if err := errors.Join(err, marker.Close()); err != nil {
			return nil, err
		}
		markerID, walID := identity(info), o.tail.id
		s.LegacyMarker, s.LegacyWAL = &markerID, &walID
	}
	b, err := s.encode()
	if err != nil {
		return nil, err
	}
	if err := errors.Join(f.Sync(), o.log.syncDir()); err != nil {
		return nil, err
	}
	if _, err := o.log.installRecord(authorityName, b); err != nil {
		if errors.Is(err, ErrUncertain) {
			keep = true
			return nil, errors.Join(l.Fence(err), f.Close())
		}
		return nil, err
	}
	keep = true
	old := o.tail
	o.tail, o.point, o.digest, o.selected = &Tail{file: f, base: p.Covered, id: id}, p, digest, s
	return old, nil
}

// Append writes one admitted, validated frame. It publishes the new byte and
// ordinal bounds while the caller still holds the lock used for its query view.
func (l *Locked) Append(payload []byte, count int) error {
	o := l.owner
	if count <= 0 || len(payload) == 0 || len(payload) > store.MaxBatch || o.digest == nil {
		return os.ErrInvalid
	}
	p := o.point
	bytes := FrameOverhead + int64(len(payload))
	if p.End > math.MaxInt64-bytes || uint64(count) > math.MaxUint64-p.Next {
		return ErrCapacity
	}
	p.End += bytes
	p.Next += uint64(count)

	log := o.log
	log.f, log.offset = o.tail.file, o.point.End-o.tail.base
	var digest hash.Hash
	var err error
	if h, ok := o.digest.(hash.Cloner); ok {
		digest, err = h.Clone()
	} else {
		digest, err = hashSection(o.tail.Section(o.point.Covered, o.point.End), o.point.End-o.point.Covered)
	}
	if err != nil {
		return err
	}
	operation := "wal_write"
	if err = writeFrame(io.MultiWriter(log.f, digest), payload); err == nil {
		operation = "wal_sync"
		err = log.f.Sync()
	}
	if err == nil {
		p.Suffix = sumDigest(digest)
		operation = "commit"
		if o.selected.Tail == "" {
			operation, err = log.commit(p.End)
		} else {
			s := o.selected
			s.End, s.Next, s.Suffix = p.End, p.Next, hexDigest(p.Suffix)
			var b []byte
			if b, err = s.encode(); err == nil {
				operation, err = log.installRecord(authorityName, b)
			}
			if err == nil {
				o.selected = s
			}
		}
	}
	if err != nil && !errors.Is(err, ErrUncertain) {
		err = log.rollback(err)
	}
	outcome := newCommitOutcome(operation, err)
	log.lastOutcome.Store(&outcome)
	switch outcome.State {
	case CommitDurable:
		o.point, o.digest = p, digest
	case CommitUncertain:
		log.poisoned = err
	case CommitRolledBack:
		log.diagnoseRollback(outcome)
	}
	return err
}

func (l *Log) checkPredecessor(name string, expected *fileID, remove bool) error {
	info, err := l.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if expected == nil || !info.Mode().IsRegular() || identity(info) != *expected {
		return fmt.Errorf("mixed or substituted ordinary WAL entry %s", name)
	}
	if remove {
		return l.root.Remove(name)
	}
	return nil
}

// Reclaim drops an old descriptor and removes only its owned, unselected name.
func (l *Locked) Reclaim(t *Tail) error {
	o := l.owner
	if t == nil || t == o.tail {
		return os.ErrInvalid
	}
	name := t.file.Name()
	if t.base == 0 && name == walName {
		return errors.Join(t.Close(), l.ReclaimOrdinary())
	}
	return errors.Join(t.Close(), o.log.checkPredecessor(name, &t.id, true), o.log.syncDir())
}

func (l *Locked) ReclaimOrdinary() error {
	o := l.owner
	s := o.selected
	if s.LegacyWAL == nil {
		return nil
	}
	if err := errors.Join(o.log.checkPredecessor(walName, s.LegacyWAL, false),
		o.log.checkPredecessor(markerName, s.LegacyMarker, false)); err != nil {
		return err
	}
	if err := errors.Join(o.log.checkPredecessor(walName, s.LegacyWAL, true),
		o.log.checkPredecessor(markerName, s.LegacyMarker, true), o.log.syncDir()); err != nil {
		return err
	}
	// A future unrelated file could reuse a reclaimed inode. Remove the
	// predecessor identities durably once both old names are gone.
	s.LegacyWAL, s.LegacyMarker = nil, nil
	b, err := s.encode()
	if err != nil {
		return err
	}
	if _, err := o.log.installRecord(authorityName, b); err != nil {
		if errors.Is(err, ErrUncertain) {
			return l.Fence(err)
		}
		return err
	}
	o.selected = s
	return nil
}

// CleanStartup drops abandoned tail copies and WAL stages after the selected
// generation and suffix have been validated and synced under the directory lock.
func (l *Locked) CleanStartup() (err error) {
	o := l.owner
	dir, err := o.log.root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	for {
		names, err := dir.Readdirnames(64)
		for _, name := range names {
			if !validTail(name) || name == o.selected.Tail {
				continue
			}
			t, _, err := o.openTail(name, 0, os.O_RDONLY)
			if err != nil {
				return err
			}
			if err := errors.Join(o.log.checkPredecessor(name, &t.id, true), t.file.Close()); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return errors.Join(l.ReclaimOrdinary(), o.log.cleanOrphans(), o.log.syncDir())
}

func (o *Owner) Close() error {
	log := o.log
	log.mu.Lock()
	defer log.mu.Unlock()
	if !log.closed {
		log.closed = true
		var err error
		if o.tail != nil {
			err = o.tail.file.Close()
		}
		log.closeErr = errors.Join(err, log.root.Close(), log.dir.Close())
	}
	return log.closeErr
}
