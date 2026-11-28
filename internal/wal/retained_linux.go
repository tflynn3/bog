package wal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"strings"
	"syscall"

	"bog/internal/store"
)

const (
	authorityName = "bog.authority"
	tailPrefix    = "bog.wal.tail-"
	authoritySize = 4 << 10
)

// Authority binds one checkpoint to the durable logical limits of its
// writable suffix. Sequence orders publications, including across restart.
type Authority struct {
	Generation            [32]byte
	Covered, End          int64
	First, Next, Sequence uint64
	SuffixSHA256          [32]byte
	tail                  [16]byte
	identity              FileIdentity
}

func (a Authority) name() string { return tailPrefix + hex.EncodeToString(a.tail[:]) }

type runtimeState struct {
	point                        Authority
	hash                         hash.Cloner
	limit, used, active, pending uint64
	retired                      []retiredTail
}

// Leave room for both the old tail and its carried suffix during selection.
func (r *runtimeState) fits(charge uint64) bool {
	return charge <= r.limit-r.used && r.active+r.pending+charge <= r.limit/2
}

type retiredTail struct {
	name  string
	file  *captureFile
	bytes uint64
}

var ErrTailCapacity = errors.New("WAL tail capacity exhausted")

func (l *Log) logicalBase() int64 {
	if l.runtime != nil && l.runtime.point.Sequence != 0 {
		return l.runtime.point.Covered
	}
	return 0
}

func (l *Log) OpenProbeDirectory() (*os.File, error) {
	d, err := l.root.Open(".")
	if err != nil {
		return nil, err
	}
	want, err := l.dir.Stat()
	got, statErr := d.Stat()
	if err = errors.Join(err, statErr); err == nil && !os.SameFile(want, got) {
		err = errors.New("readiness directory differs from WAL directory")
	}
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return d, nil
}

// OpenLegacyRuntime exposes a pinned legacy WAL for streaming recovery.
// finish may run only after the caller validates all committed frames.
func (l *Log) OpenLegacyRuntime() (input io.ReaderAt, end int64, finish func(*store.Store, uint64, uint64, int64, uint64) error, err error) {
	if err := l.openLegacy(); err != nil {
		return nil, 0, nil, err
	}
	f := l.f.(*os.File)
	info, err := f.Stat()
	if err != nil {
		return nil, 0, nil, err
	}
	end, hasMarker, err := l.committedEnd(info.Size())
	if err != nil {
		return nil, 0, nil, err
	}
	var header [len(magic)]byte
	if _, err := f.ReadAt(header[:], 0); err != nil || string(header[:]) != magic {
		return nil, 0, nil, errors.Join(errors.New("invalid WAL magic"), err)
	}
	return f, end, func(s *store.Store, next, limit uint64, covered int64, first uint64) error {
		if err := l.finishLegacy(info.Size(), end, hasMarker); err != nil {
			return err
		}
		if _, err := f.Seek(end, io.SeekStart); err != nil {
			return err
		}
		l.offset, l.committed = end, captureBoundary{end, next}
		l.runtime.point = Authority{Covered: covered, End: end, First: first, Next: next}
		return l.installTailStore(s, limit)
	}, nil
}

func (l *Log) installTailStore(s *store.Store, limit uint64) error {
	p := l.runtime.point
	charge := tailBytes(p.End-p.Covered, p.Next-p.First)
	if charge > limit {
		return ErrTailCapacity
	}
	l.s, l.runtime.limit, l.runtime.used, l.runtime.active = s, limit, charge, charge
	return nil
}

// tailBytes covers raw bytes plus the decoded row and index backing, including
// spare row capacity and copy overlap during store growth.
func tailBytes(n int64, rows uint64) uint64 {
	if n < 0 || rows > (math.MaxUint64-uint64(n))/192 {
		return math.MaxUint64
	}
	return uint64(n) + 192*rows
}

type authorityRecord struct {
	Version      int    `json:"version"`
	Generation   string `json:"generation"`
	Covered      int64  `json:"covered_offset"`
	End          int64  `json:"committed_end"`
	First        uint64 `json:"next_ordinal"`
	Next         uint64 `json:"committed_ordinal"`
	SuffixSHA256 string `json:"suffix_sha256"`
	Tail         string `json:"tail"`
	Base         int64  `json:"tail_base"`
	Device       uint64 `json:"tail_device"`
	Inode        uint64 `json:"tail_inode"`
	Sequence     uint64 `json:"sequence"`
}

type authorityEnvelope struct {
	Payload jsontext.Value `json:"payload"`
	SHA256  string         `json:"sha256"`
}

func encodeAuthority(a Authority) ([]byte, error) {
	payload, err := json.Marshal(authorityRecord{
		Version: 2, Generation: hex.EncodeToString(a.Generation[:]), Covered: a.Covered, End: a.End,
		First: a.First, Next: a.Next, SuffixSHA256: hex.EncodeToString(a.SuffixSHA256[:]),
		Tail: a.name(), Base: a.Covered, Device: a.identity.device, Inode: a.identity.inode, Sequence: a.Sequence,
	})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	return json.Marshal(authorityEnvelope{Payload: payload, SHA256: hex.EncodeToString(sum[:])})
}

func decodeAuthority(b []byte) (Authority, error) {
	var env authorityEnvelope
	if len(b) > authoritySize {
		return Authority{}, errors.New("retained WAL authority exceeds size limit")
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return Authority{}, fmt.Errorf("decode retained WAL authority: %w", err)
	}
	sum := sha256.Sum256(env.Payload)
	if env.SHA256 != hex.EncodeToString(sum[:]) {
		return Authority{}, errors.New("retained WAL authority checksum mismatch")
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(env.Payload, &fields); err != nil {
		return Authority{}, err
	}
	for _, name := range []string{"version", "generation", "covered_offset", "committed_end",
		"next_ordinal", "committed_ordinal", "suffix_sha256", "tail", "tail_base", "tail_device", "tail_inode", "sequence"} {
		if raw := fields[name]; len(raw) == 0 || raw.Kind() == 'n' {
			return Authority{}, fmt.Errorf("missing retained WAL authority field %q", name)
		}
	}
	var record authorityRecord
	if err := json.Unmarshal(env.Payload, &record); err != nil {
		return Authority{}, err
	}
	a := Authority{Covered: record.Covered, End: record.End, First: record.First, Next: record.Next,
		Sequence: record.Sequence, identity: FileIdentity{record.Device, record.Inode}}
	if record.Version != 2 || record.Base != a.Covered || !decodeHex(a.Generation[:], record.Generation) ||
		!decodeHex(a.SuffixSHA256[:], record.SuffixSHA256) || !strings.HasPrefix(record.Tail, tailPrefix) ||
		!decodeHex(a.tail[:], strings.TrimPrefix(record.Tail, tailPrefix)) ||
		a.Covered < lenMagic || a.End < a.Covered || a.Next < a.First ||
		(a.End == a.Covered) != (a.Next == a.First) || a.Generation == [32]byte{} || a.Sequence == 0 {
		return Authority{}, errors.New("invalid retained WAL authority fields")
	}
	return a, nil
}

func decodeHex(dst []byte, text string) bool {
	if len(text) != len(dst)*2 || strings.Trim(text, "0123456789abcdef") != "" {
		return false
	}
	_, err := hex.Decode(dst, []byte(text))
	return err == nil
}

const lenMagic = int64(len(magic))

func (l *Log) ReadAuthority() (Authority, error) {
	f, err := openRegular(l.dir, authorityName, os.O_RDONLY)
	if err != nil {
		return Authority{}, err
	}
	b, err := io.ReadAll(io.LimitReader(f, authoritySize+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return Authority{}, err
	}
	return decodeAuthority(b)
}

// RecoverAuthority validates identity, digest and the caller's committed
// frames before truncating any uncommitted data or admitting writes.
func (l *Log) RecoverAuthority(a Authority, limit uint64, replay func(io.ReaderAt, Authority) (*store.Store, error)) error {
	if tailBytes(a.End-a.Covered, a.Next-a.First) > limit {
		return ErrTailCapacity
	}
	f, err := openRegular(l.dir, a.name(), os.O_RDWR)
	if err != nil {
		return err
	}
	if err := l.installWriter(f, a.name()); err != nil {
		return err
	}
	if l.captures.identity != a.identity {
		return errors.New("retained WAL identity mismatch")
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	length := a.End - a.Covered
	if size < length {
		return io.ErrUnexpectedEOF
	}
	h := sha256.New().(hash.Cloner)
	if _, err := io.CopyN(h, io.NewSectionReader(f, 0, length), length); err != nil {
		return err
	}
	if [32]byte(h.Sum(nil)) != a.SuffixSHA256 {
		return errors.New("retained WAL suffix checksum mismatch")
	}
	// Persist the selected backing before validation can settle any staged
	// checkpoint publication that lost the selector decision.
	if err := f.Sync(); err != nil {
		return err
	}
	l.runtime.point, l.runtime.hash = a, h
	l.committed = captureBoundary{a.End, a.Next}
	c, err := l.Capture(context.Background())
	if err != nil {
		return err
	}
	s, err := replay(c, a)
	if err := errors.Join(err, c.Release()); err != nil {
		return err
	}
	if size > length {
		if err := f.Truncate(length); err != nil {
			return err
		}
	}
	if _, err := f.Seek(length, io.SeekStart); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := l.syncDir(); err != nil {
		return err
	}
	if err := l.cleanOrphans(); err != nil {
		return err
	}
	if err := l.syncDir(); err != nil {
		return err
	}
	l.offset = a.End
	return l.installTailStore(s, limit)
}

// View captures the published WAL and its matching immutable decoded store.
func (l *Log) View(ctx context.Context) (*Capture, *store.PositionedSnapshot, Authority, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, err := l.captureLocked(ctx)
	if err != nil {
		return nil, nil, Authority{}, err
	}
	point := l.runtime.point
	point.End, point.Next = c.CommittedEnd(), c.NextOrdinal()
	return c, l.s.CapturePositionedSnapshot(), point, nil
}

func (l *Log) writeAuthority(a Authority) (string, error) {
	b, err := encodeAuthority(a)
	if err != nil {
		return "marker_preparation", err
	}
	name, err := l.writeStaged(markerStagePrefix, b)
	if err != nil {
		return "marker_preparation", err
	}
	if err := l.root.Rename(name, authorityName); err != nil {
		return "marker_rename", errors.Join(ErrUncertain, err)
	}
	if err := l.syncDir(); err != nil {
		return "marker_sync", errors.Join(ErrUncertain, err)
	}
	return "", nil
}

// Select copies the carried suffix and installs a single durable selector.
// It serializes with commit groups and includes every commit after the cutoff.
// For a verified restore with no writer, input and next supply the initial tail.
func (l *Log) Select(a Authority, input io.ReaderAt, limit uint64, replay func(io.ReaderAt, Authority) (*store.Store, error)) (err error) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.admissionError(); err != nil {
		return err
	}
	r := l.runtime
	capturedEnd, capturedHash := a.End, a.SuffixSHA256
	if r.point.Sequence == math.MaxUint64 {
		return errors.New("retained generation sequence exhausted")
	}
	a.Sequence = r.point.Sequence + 1
	if a.Generation == [32]byte{} || a.Covered < lenMagic || a.End < 0 || input == nil && l.f == nil {
		return errors.New("invalid retained WAL selection")
	}
	if l.f != nil {
		if a.Covered < r.point.Covered || a.First < r.point.First ||
			a.Covered > l.committed.end || a.First > l.committed.nextOrdinal {
			return errors.New("checkpoint cutoff outside committed WAL")
		}
		a.End, a.Next = l.committed.end, l.committed.nextOrdinal
		input = logicalReader{l.captures.f, l.logicalBase()}
	}
	if a.End < a.Covered || a.Next < a.First || (a.End == a.Covered) != (a.Next == a.First) {
		return errors.New("invalid retained WAL suffix bounds")
	}
	charge := tailBytes(a.End-a.Covered, a.Next-a.First)
	if r.used > limit || charge > limit-r.used {
		return ErrTailCapacity
	}
	if _, err := rand.Read(a.tail[:]); err != nil {
		return err
	}
	name := a.name()
	f, err := openRegular(l.dir, name, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return err
	}
	var capFile *captureFile
	selected := false
	defer func() {
		if selected {
			return
		}
		err = errors.Join(err, f.Close())
		if capFile != nil {
			err = errors.Join(err, capFile.release())
		}
		if !errors.Is(err, ErrUncertain) {
			if cleanupErr := errors.Join(l.removeStage(name), l.syncDir()); cleanupErr != nil {
				err = errors.Join(ErrUncertain, err, cleanupErr)
				l.fenceLocked(err)
			}
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	h := sha256.New().(hash.Cloner)
	firstEnd := a.End
	if capturedEnd != 0 {
		if capturedEnd < a.Covered || capturedEnd > a.End {
			return errors.New("captured remainder outside selected tail")
		}
		firstEnd = capturedEnd
	}
	writer := io.MultiWriter(f, h)
	copyRange := func(lo, hi int64) error {
		_, err := io.CopyN(writer, io.NewSectionReader(input, lo, hi-lo), hi-lo)
		return err
	}
	if err := copyRange(a.Covered, firstEnd); err != nil {
		return err
	}
	if capturedEnd != 0 && capturedHash != [32]byte(h.Sum(nil)) {
		return errors.New("captured WAL suffix changed during activation")
	}
	if err := copyRange(firstEnd, a.End); err != nil {
		return err
	}
	a.SuffixSHA256 = [32]byte(h.Sum(nil))
	s, err := replay(logicalReader{f, a.Covered}, a)
	if err != nil {
		return err
	}
	capFile, err = openCaptureFile(l.dir, f, name)
	if err != nil {
		return err
	}
	a.identity = capFile.identity
	if err := errors.Join(f.Sync(), l.syncDir()); err != nil {
		return err
	}
	if _, err := l.writeAuthority(a); err != nil {
		if errors.Is(err, ErrUncertain) {
			l.fenceLocked(err)
		}
		return err
	}
	selected = true
	if l.f != nil {
		oldName := walName
		if r.hash != nil {
			oldName = r.point.name()
		}
		r.retired = append(r.retired, retiredTail{oldName, l.captures, r.active})
		err = errors.Join(l.f.Close(), l.captures.release())
	}
	l.f, l.captures, l.offset, l.s = f, capFile, a.End, s
	l.committed = captureBoundary{a.End, a.Next}
	r.point, r.hash, r.limit, r.active = a, h, limit, charge
	r.used += charge
	if err != nil {
		err = errors.Join(ErrUncertain, err)
		l.fenceLocked(err)
	}
	return err
}

type logicalReader struct {
	io.ReaderAt
	base int64
}

func (r logicalReader) ReadAt(p []byte, off int64) (int, error) {
	return r.ReaderAt.ReadAt(p, off-r.base)
}

func (l *Log) fenceLocked(err error) {
	l.initCond()
	l.poisoned = err
	for _, req := range l.queue {
		l.releaseRequest(req)
		req.done <- err
	}
	clear(l.queue)
	l.queue = nil
	l.cond.Broadcast()
}

// Fence stops appends and captures after an uncertain publication outside the
// WAL. A group already in progress reaches its own durable decision first.
func (l *Log) Fence(cause error) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	err := errors.Join(ErrUncertain, cause)
	l.fenceLocked(err)
	outcome := newCommitOutcome("publication", err)
	l.lastOutcome.Store(&outcome)
}

func (l *Log) ReclaimTails() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.runtime
	for i := 0; i < len(r.retired); {
		old := r.retired[i]
		select {
		case <-old.file.drained:
		default:
			i++
			continue
		}
		if err := l.removeTail(old.name); err != nil {
			return err
		}
		r.used -= old.bytes
		l.initCond()
		l.cond.Broadcast()
		copy(r.retired[i:], r.retired[i+1:])
		r.retired[len(r.retired)-1] = retiredTail{}
		r.retired = r.retired[:len(r.retired)-1]
	}
	return nil
}

func (l *Log) removeTail(name string) error {
	if err := l.removeStage(name); err != nil {
		return err
	}
	if name == walName {
		if err := l.removeStage(markerName); err != nil {
			return err
		}
	}
	return l.syncDir()
}

// RecoverTails retires backing left by an interrupted selection or shutdown.
// It runs only under the owner lock, after the selected authority is durable.
func (l *Log) RecoverTails() (err error) {
	d, err := l.root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	selected := l.runtime.point.name()
	for {
		names, readErr := d.Readdirnames(64)
		for _, name := range names {
			token, tail := strings.CutPrefix(name, tailPrefix)
			if name == selected || !(tail && len(token) == 32 && strings.Trim(token, "0123456789abcdef") == "" ||
				l.runtime.hash != nil && (name == walName || name == markerName)) {
				continue
			}
			f, err := openRegular(l.dir, name, os.O_RDONLY)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if err := errors.Join(l.removeTail(name), f.Close()); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("scan retired WAL files: %w", readErr)
		}
	}
}

func (l *Log) PublishedAuthority() (Authority, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.runtime.point
	a.End, a.Next = l.committed.end, l.committed.nextOrdinal
	return a, l.runtime.active
}
