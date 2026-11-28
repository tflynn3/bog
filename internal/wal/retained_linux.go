package wal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"syscall"

	"bog/internal/store"
)

const (
	authorityMagic = "BOGCMT02"
	authorityMax   = 2048
	tailPrefix     = "bog-tail-"
)

// Authority is the selector stored in bog.commit. Offsets are logical and the
// selected tail starts at Covered. Legacy identifies the original bog.wal
// inode, which may still exist after an interrupted first selection.
type Authority struct {
	Generation [32]byte
	Canonical  bool
	Covered    int64
	First      uint64
	End        int64
	Next       uint64
	Digest     [32]byte
	Tail       string
	Identity   FileIdentity
	Legacy     FileIdentity
}

type retiredFile struct {
	*captureFile
}

// Directory returns handles borrowed until Close. The directory descriptor
// holds the owner lock. Open a fresh "." through root for independent users.
func (l *Log) Directory() (*os.Root, *os.File) { return l.root, l.dir }

// OpenStorageFile opens a regular owned basename relative to the pinned data directory.
func (l *Log) OpenStorageFile(name string) (*os.File, error) {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		return nil, os.ErrInvalid
	}
	return openRegular(l.dir, name, os.O_RDONLY)
}

// Authority returns the latest published retained selector, if present.
func (l *Log) Authority() (Authority, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.authority == nil {
		return Authority{}, false
	}
	return *l.authority, true
}

func tailName(name string) bool {
	id, ok := strings.CutPrefix(name, tailPrefix)
	return ok && len(id) == 2*stageIDSize && strings.Trim(id, "0123456789abcdef") == ""
}

func (l *Log) readAuthority(maxTail int64) (err error) {
	f, err := openRegular(l.dir, markerName, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		// The first rebase always starts with a durable legacy marker.
		// A tail without either marker has no recoverable authority.
		dir, openErr := l.root.Open(".")
		if openErr != nil {
			return openErr
		}
		defer func() { err = errors.Join(err, dir.Close()) }()
		for {
			names, readErr := dir.Readdirnames(64)
			for _, name := range names {
				if tailName(name) {
					return errors.New("retained WAL tail without commit selector")
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
	if err != nil {
		return err
	}
	b, err := io.ReadAll(io.LimitReader(f, authorityMax+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return err
	}
	if len(b) == markerSize && string(b[:8]) == markerMagic {
		return nil // the legacy reader validates its offset and complement
	}
	const header = len(authorityMagic) + sha256.Size
	if len(b) <= header || len(b) > authorityMax || string(b[:8]) != authorityMagic ||
		sha256.Sum256(b[header:]) != [32]byte(b[8:header]) {
		return errors.New("invalid WAL authority checksum or version")
	}
	var a Authority
	if err := json.Unmarshal(b[header:], &a); err != nil {
		return fmt.Errorf("decode retained WAL authority: %w", err)
	}
	// Require precisely the complete record emitted by the owner, including
	// zero-valued fields. This also rejects duplicate or unknown fields.
	canonical, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if string(canonical) != string(b[header:]) || !tailName(a.Tail) || a.Identity.Inode == 0 ||
		a.Generation == ([32]byte{}) || a.Covered < int64(len(magic)) || a.End < a.Covered ||
		a.End-a.Covered > maxTail || a.Next < a.First ||
		(a.Covered == int64(len(magic))) != (a.First == 0) ||
		(a.End == a.Covered) != (a.Next == a.First) {
		return errors.New("invalid retained WAL authority")
	}
	l.authority = &a
	return nil
}

func (l *Log) recoverRetained(maxTail int64) error {
	a := l.authority
	f := l.captures
	if f.identity != a.Identity {
		return errors.New("retained WAL file identity differs from selector")
	}
	if err := l.checkLegacy(a.Legacy); err != nil {
		return err
	}
	f.base = a.Covered
	size, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	length := a.End - a.Covered
	if length > maxTail || size < length {
		return errors.New("retained WAL is shorter than its committed range or exceeds its tail limit")
	}
	h := sha256.New().(hash.Cloner)
	input := io.TeeReader(io.NewSectionReader(f.f, 0, length), h)
	next := a.First
	if err := scanFrames(context.Background(), input, a.Covered, a.End, func(payload []byte, _ int64) error {
		rows, err := store.Decode(payload)
		if uint64(len(rows)) > ^uint64(0)-next {
			return errors.New("WAL ordinal overflow")
		}
		next += uint64(len(rows))
		return err
	}); err != nil {
		return err
	}
	if [32]byte(h.Sum(nil)) != a.Digest || next != a.Next {
		return errors.New("retained WAL checksum or ordinal differs from selector")
	}
	l.suffixHash = h
	l.offset = a.End
	l.committed = captureBoundary{end: a.End, nextOrdinal: a.Next}
	return nil
}

func (l *Log) checkLegacy(expected FileIdentity) error {
	f, err := openRegular(l.dir, walName, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err := errors.Join(err, f.Close()); err != nil {
		return err
	}
	st := info.Sys().(*syscall.Stat_t)
	if (FileIdentity{uint64(st.Dev), st.Ino}) != expected {
		return errors.New("unexpected legacy WAL alongside retained authority")
	}
	return nil
}

// FinishRecovery discards torn bytes and unselected tails after the command
// validates and syncs the selected generation and settles checkpoint staging.
// It runs before serving, with no other operations admitted.
func (l *Log) FinishRecovery() (err error) {
	if l.authority != nil {
		if err := l.rollback(nil); err != nil {
			return err
		}
	}
	dir, err := l.root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	for {
		entries, readErr := dir.ReadDir(64)
		for _, entry := range entries {
			name := entry.Name()
			orphan := tailName(name) && (l.authority == nil || name != l.authority.Tail)
			if orphan || (l.authority != nil && name == walName) {
				var expected *FileIdentity
				if name == walName {
					expected = &l.authority.Legacy
				}
				if err := l.removeTail(name, expected); err != nil {
					return err
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return errors.Join(l.cleanOrphans(), l.syncDir())
}

func (l *Log) removeTail(name string, expected *FileIdentity) error {
	f, err := openRegular(l.dir, name, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err := errors.Join(err, f.Close()); err != nil {
		return err
	}
	st := info.Sys().(*syscall.Stat_t)
	if expected != nil && (FileIdentity{uint64(st.Dev), st.Ino}) != *expected {
		return errors.New("retired WAL file identity changed")
	}
	return l.root.Remove(name)
}

// Select rebases exactly [next.Covered, current.End) into a new tail. The
// caller serializes selection with its query-visible view and appends. The
// staging capture fixes the expected predecessor and captured remainder.
func (l *Log) Select(next Authority, from *Capture, remainder [32]byte) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.admissionError(); err != nil {
		return err
	}
	if l.running || l.captures != from.lease.file || next.Covered < l.captures.base ||
		next.Covered > from.CommittedEnd() || next.First > from.NextOrdinal() ||
		(next.Covered == from.CommittedEnd()) != (next.First == from.NextOrdinal()) {
		return errors.New("checkpoint selection no longer matches its WAL capture")
	}
	if l.authority != nil && (next.Covered <= l.authority.Covered || next.First <= l.authority.First) {
		return errors.New("checkpoint selection does not advance authority")
	}
	var id [stageIDSize]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	next.Tail = tailPrefix + hex.EncodeToString(id[:])
	f, err := openRegular(l.dir, next.Tail, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return err
	}
	var reader *captureFile
	keep := false
	defer func() {
		if keep {
			return
		}
		err = errors.Join(err, f.Close())
		if reader != nil {
			err = errors.Join(err, reader.release())
		}
		if !errors.Is(err, ErrUncertain) {
			err = errors.Join(err, l.removeStage(next.Tail), l.syncDir())
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return err
	}
	h := sha256.New().(hash.Cloner)
	dst := io.MultiWriter(f, h)
	old := l.captures
	cut := next.Covered
	if err := copyRange(dst, old, cut, from.CommittedEnd()); err != nil {
		return err
	}
	if [32]byte(h.Sum(nil)) != remainder {
		return errors.New("checkpoint selection captured suffix checksum changed")
	}
	if err := copyRange(dst, old, from.CommittedEnd(), l.committed.end); err != nil {
		return err
	}
	if err := errors.Join(f.Sync(), l.syncDir()); err != nil {
		return err
	}
	reader, err = openCaptureFile(l.dir, f, next.Tail)
	if err != nil {
		return err
	}
	reader.base = cut
	next.End, next.Next = l.committed.end, l.committed.nextOrdinal
	next.Digest, next.Identity = [32]byte(h.Sum(nil)), reader.identity
	if l.authority != nil {
		next.Legacy = l.authority.Legacy
	} else {
		next.Legacy = old.identity
	}
	op, err := l.commitAuthority(next)
	if err != nil {
		if errors.Is(err, ErrUncertain) {
			l.poisoned = err
			outcome := newCommitOutcome(op, err)
			l.lastOutcome.Store(&outcome)
		}
		return err
	}
	previous := l.f
	l.f, l.captures, l.authority, l.suffixHash = f, reader, &next, h
	l.retired = append(l.retired, retiredFile{old})
	keep = true
	l.retirementErr = errors.Join(l.retirementErr, previous.Close(), old.release())
	return nil
}

// ValidateLegacyBoundary proves that a canonical checkpoint covers an exact
// frame and ordinal prefix of this pinned legacy WAL.
func (c *Capture) ValidateLegacyBoundary(ctx context.Context, cutoff int64, ordinal uint64, digest [32]byte) error {
	if c.lease.file.base != 0 || cutoff < int64(len(magic)) || cutoff > c.CommittedEnd() {
		return errors.New("invalid legacy checkpoint boundary")
	}
	h := sha256.New()
	r := io.TeeReader(io.NewSectionReader(c, 0, cutoff), h)
	var header [len(magic)]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	if string(header[:]) != magic {
		return errors.New("invalid legacy WAL magic")
	}
	var count uint64
	if err := scanFrames(ctx, r, int64(len(magic)), cutoff, func(b []byte, _ int64) error {
		rows, err := store.Decode(b)
		if uint64(len(rows)) > ^uint64(0)-count {
			return errors.New("WAL ordinal overflow")
		}
		count += uint64(len(rows))
		return err
	}); err != nil {
		return err
	}
	if count != ordinal || [32]byte(h.Sum(nil)) != digest {
		return errors.New("checkpoint does not match legacy WAL prefix")
	}
	return nil
}

func copyRange(dst io.Writer, from *captureFile, start, end int64) error {
	size := end - start
	n, err := io.Copy(dst, io.NewSectionReader(from.f, start-from.base, size))
	if err == nil && n != size {
		err = io.ErrUnexpectedEOF
	}
	return err
}

func (l *Log) commitAuthority(a Authority) (string, error) {
	payload, err := json.Marshal(a)
	if err != nil {
		return "marker_preparation", err
	}
	sum := sha256.Sum256(payload)
	b := append(append([]byte(authorityMagic), sum[:]...), payload...)
	return l.installMarker(b)
}

// ReclaimTails retires predecessor names after durable selection and the
// final capture or read releases its descriptor.
func (l *Log) ReclaimTails() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var err error
	kept := l.retired[:0]
	for _, old := range l.retired {
		select {
		case <-old.drained:
			if e := errors.Join(l.removeTail(old.name, &old.identity), l.syncDir()); e == nil {
				continue
			} else {
				err = errors.Join(err, e)
			}
		default:
		}
		kept = append(kept, old)
	}
	clear(l.retired[len(kept):])
	l.retired = kept
	return errors.Join(l.retirementErr, err)
}
