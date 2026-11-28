package wal

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"strings"
	"syscall"
)

const (
	authorityName = "bog.authority"
	authoritySize = 4 << 10
	tailPrefix    = "bog.wal.tail-"
)

// Authority binds one immutable manifest to an exact committed WAL suffix.
// The zero epoch refers to the original WAL, whose physical base is zero.
type Authority struct {
	Generation       [32]byte
	Epoch            [16]byte
	Revision         uint64
	Inode, Device    uint64
	Base             int64
	CoveredOffset    int64
	NextOrdinal      uint64
	CommittedEnd     int64
	CommittedOrdinal uint64
	SuffixSHA256     [32]byte
}

func (a Authority) tailName() string {
	if a.Epoch == [16]byte{} {
		return walName
	}
	return tailPrefix + hex.EncodeToString(a.Epoch[:])
}

func (a Authority) valid() bool {
	return a.Generation != [32]byte{} && a.Revision != 0 && a.Inode != 0 &&
		a.CoveredOffset >= int64(len(magic)) && a.CommittedEnd >= a.CoveredOffset &&
		a.CommittedOrdinal >= a.NextOrdinal &&
		(a.CommittedEnd == a.CoveredOffset) == (a.CommittedOrdinal == a.NextOrdinal) &&
		(a.Epoch == [16]byte{} && a.Base == 0 ||
			a.Epoch != [16]byte{} && a.Base == a.CoveredOffset)
}

type authorityPayload struct {
	Version          int    `json:"version"`
	Generation       string `json:"generation"`
	Tail             string `json:"tail"`
	Revision         uint64 `json:"revision"`
	Device           uint64 `json:"tail_device"`
	Inode            uint64 `json:"tail_inode"`
	Base             int64  `json:"tail_base"`
	CoveredOffset    int64  `json:"covered_offset"`
	NextOrdinal      uint64 `json:"next_ordinal"`
	CommittedEnd     int64  `json:"committed_end"`
	CommittedOrdinal uint64 `json:"committed_ordinal"`
	SuffixSHA256     string `json:"suffix_sha256"`
}

type authorityEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func (a Authority) encode() ([]byte, error) {
	p, err := json.Marshal(authorityPayload{
		Version: 2, Generation: hex.EncodeToString(a.Generation[:]), Tail: a.tailName(),
		Revision: a.Revision, Device: a.Device, Inode: a.Inode, Base: a.Base,
		CoveredOffset: a.CoveredOffset, NextOrdinal: a.NextOrdinal,
		CommittedEnd: a.CommittedEnd, CommittedOrdinal: a.CommittedOrdinal,
		SuffixSHA256: hex.EncodeToString(a.SuffixSHA256[:]),
	})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(p)
	return json.Marshal(authorityEnvelope{p, hex.EncodeToString(sum[:])})
}

// ReadAuthority selects the on-disk record under the directory owner lock.
func (l *Log) ReadAuthority() (_ Authority, err error) {
	f, err := openRegular(l.dir, authorityName, os.O_RDONLY)
	if err != nil {
		return Authority{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, authoritySize+1))
	if err != nil {
		return Authority{}, err
	}
	return decodeAuthority(b)
}

func decodeAuthority(b []byte) (Authority, error) {
	var envelope authorityEnvelope
	if len(b) > authoritySize {
		return Authority{}, errors.New("oversized retained authority record")
	}
	if err := jsonv2.Unmarshal(b, &envelope); err != nil {
		return Authority{}, fmt.Errorf("invalid retained authority envelope: %w", err)
	}
	sum := sha256.Sum256(envelope.Payload)
	if envelope.SHA256 != hex.EncodeToString(sum[:]) {
		return Authority{}, errors.New("retained authority checksum mismatch")
	}
	var fields map[string]json.RawMessage
	if err := jsonv2.Unmarshal(envelope.Payload, &fields); err != nil {
		return Authority{}, fmt.Errorf("invalid retained authority payload: %w", err)
	}
	for _, name := range []string{"version", "generation", "tail", "revision", "tail_device", "tail_inode", "tail_base",
		"covered_offset", "next_ordinal", "committed_end", "committed_ordinal", "suffix_sha256"} {
		if field := fields[name]; len(field) == 0 || bytes.Equal(field, []byte("null")) {
			return Authority{}, fmt.Errorf("missing retained authority field %s", name)
		}
	}
	var p authorityPayload
	if err := jsonv2.Unmarshal(envelope.Payload, &p); err != nil {
		return Authority{}, fmt.Errorf("invalid retained authority fields: %w", err)
	}
	a := Authority{
		Revision: p.Revision, Inode: p.Inode, Device: p.Device, Base: p.Base, CoveredOffset: p.CoveredOffset,
		NextOrdinal: p.NextOrdinal, CommittedEnd: p.CommittedEnd, CommittedOrdinal: p.CommittedOrdinal,
	}
	if p.Version != 2 || !decodeHex(a.Generation[:], p.Generation) || !decodeHex(a.SuffixSHA256[:], p.SuffixSHA256) ||
		!isTailName(p.Tail) || !decodeHex(a.Epoch[:], strings.TrimPrefix(p.Tail, tailPrefix)) ||
		a.tailName() != p.Tail || !a.valid() {
		return Authority{}, errors.New("invalid retained authority version, identity or bounds")
	}
	return a, nil
}

func decodeHex(dst []byte, src string) bool {
	if len(src) != len(dst)*2 || strings.Trim(src, "0123456789abcdef") != "" {
		return false
	}
	_, err := hex.Decode(dst, []byte(src))
	return err == nil
}

// InstallDataFile syncs and installs immutable data during startup. A durable
// name left before selection is reusable only when its bytes match exactly.
func (l *Log) InstallDataFile(name string, data []byte) error {
	staged, err := l.writeStaged(walStagePrefix, data)
	if err != nil {
		return err
	}
	err = renameNoReplace(l.dir, staged, name)
	if errors.Is(err, os.ErrExist) {
		f, openErr := l.OpenDataFile(name)
		if openErr != nil {
			return errors.Join(openErr, l.removeStage(staged))
		}
		disk, readErr := io.ReadAll(io.LimitReader(f, int64(len(data))+1))
		err = errors.Join(readErr, f.Close(), l.removeStage(staged))
		if err == nil && !bytes.Equal(disk, data) {
			err = errors.New("immutable recovery file already exists with different data")
		}
	}
	if err != nil {
		return err
	}
	return l.syncDir()
}

// RecoverOrdinary validates and streams the original WAL without installing
// an in-memory store. Call only before the owner starts serving.
func (l *Log) RecoverOrdinary(visit FrameVisitor) error {
	if err := rejectRestoreImage(l.root); err != nil {
		return err
	}
	return l.recoverOrdinary(visit)
}

// RecoverSelected validates the selected inode and all committed frames
// before truncating an uncommitted physical suffix.
func (l *Log) RecoverSelected(a Authority, maxTail int64, visit FrameVisitor) (err error) {
	if !a.valid() || a.CommittedEnd-a.CoveredOffset > maxTail {
		return errors.New("invalid or oversized retained WAL suffix")
	}
	if err := l.syncParent(); err != nil {
		return err
	}
	f, err := openRegular(l.dir, a.tailName(), os.O_RDWR)
	if err != nil {
		return err
	}
	l.f = f
	pin, err := openCaptureFile(l.dir, f, a.tailName(), a.Base)
	if err != nil {
		return err
	}
	l.captures = pin
	info, err := f.Stat()
	if err != nil {
		return err
	}
	end := a.CommittedEnd - a.Base
	if (pin.identity.inode != a.Inode || pin.identity.device != a.Device) || info.Size() < end {
		return errors.New("selected WAL tail identity or size changed")
	}
	if a.Base == 0 {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return fmt.Errorf("lock selected WAL: %w", err)
		}
		if err := checkMagic(io.NewSectionReader(f, 0, int64(len(magic)))); err != nil {
			return err
		}
	}
	h := sha256.New()
	reader := bufio.NewReaderSize(io.NewSectionReader(f, a.CoveredOffset-a.Base, a.CommittedEnd-a.CoveredOffset), 64<<10)
	next, err := scanFrames(io.TeeReader(reader, h), a.CoveredOffset, a.CommittedEnd, a.NextOrdinal, visit)
	if err != nil {
		return err
	}
	if next != a.CommittedOrdinal || [32]byte(h.Sum(nil)) != a.SuffixSHA256 {
		return errors.New("retained WAL suffix checksum or ordinal mismatch")
	}
	if info.Size() > end {
		if err := f.Truncate(end); err != nil {
			return fmt.Errorf("discard uncommitted retained WAL suffix: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		return err
	}
	if err := l.cleanOrphans(); err != nil {
		return err
	}
	if err := l.syncDir(); err != nil {
		return err
	}
	l.authority, l.suffixHash = a, h
	l.offset = a.CommittedEnd
	l.committed = captureBoundary{a.CommittedEnd, a.CommittedOrdinal}
	return nil
}

func (l *Log) commitAuthority(a Authority) (string, error) {
	if !a.valid() {
		return "authority_preparation", errors.New("invalid successor WAL authority")
	}
	b, err := a.encode()
	if err != nil {
		return "authority_preparation", err
	}
	name, err := l.writeStaged(markerStagePrefix, b)
	if err != nil {
		return "authority_preparation", err
	}
	if err := l.root.Rename(name, authorityName); err != nil {
		return "authority_rename", errors.Join(ErrUncertain, err)
	}
	if err := l.syncDir(); err != nil {
		return "authority_sync", errors.Join(ErrUncertain, err)
	}
	return "", nil
}

// Selected is the currently published authority. A zero generation denotes
// the ordinary WAL. Capture bounds and selection are sampled together by
// CaptureSelected.
func (l *Log) Selected() Authority {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.authority
}

// Select installs a validated successor. An input is used for initial restore;
// subsequent selections copy the exact remaining frames from the current WAL
// while append publication is excluded. An ordinary bootstrap can preserve
// the original WAL inode. A rebased selection returns its retirement intent.
func (l *Log) Select(ctx context.Context, a Authority, restore io.ReaderAt, keepOriginal bool) (transition TailSwitch, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.initCond()
	for l.running {
		l.cond.Wait()
	}
	if err := l.admissionError(); err != nil {
		return transition, err
	}
	if err := ctx.Err(); err != nil {
		return transition, err
	}
	old := l.authority
	if old.Revision == math.MaxUint64 {
		return transition, errors.New("WAL generation counter overflow")
	}
	a.Revision = old.Revision + 1
	capturedEnd := a.CommittedEnd
	source := restore
	if source == nil {
		if l.captures == nil || capturedEnd > l.committed.end || a.CommittedOrdinal > l.committed.nextOrdinal ||
			a.CoveredOffset < max(int64(len(magic)), old.CoveredOffset) {
			return transition, errors.New("successor is outside current WAL")
		}
		a.CommittedEnd, a.CommittedOrdinal = l.committed.end, l.committed.nextOrdinal
		source = logicalReader{l.captures.f, l.captures.base}
	} else if l.f != nil {
		return transition, errors.New("restore selection requires an unopened WAL")
	}
	h := sha256.New()
	var f *os.File
	var pin *captureFile
	var stage string
	var journal, installed bool
	if keepOriginal {
		if old.Revision != 0 || restore != nil || a.CoveredOffset != int64(len(magic)) {
			return transition, errors.New("cannot preserve original WAL at this boundary")
		}
		a.Base, a.Inode, a.Device = 0, l.captures.identity.inode, l.captures.identity.device
	} else {
		if _, err := rand.Read(a.Epoch[:]); err != nil {
			return transition, err
		}
		a.Base = a.CoveredOffset
		stage = walStagePrefix + hex.EncodeToString(a.Epoch[:])
		f, err = openRegular(l.dir, stage, os.O_CREATE|os.O_EXCL|os.O_RDWR)
		if err != nil {
			return transition, err
		}
		defer func() {
			if f != nil {
				err = errors.Join(err, f.Close())
				if pin != nil {
					err = errors.Join(err, pin.release())
				}
				if !errors.Is(err, ErrUncertain) {
					cleanup := l.removeStage(stage)
					if installed {
						cleanup = errors.Join(cleanup, l.removeStage(a.tailName()))
					}
					if cleanup == nil && journal {
						cleanup = l.FinishTailSwitch(transition)
					}
					err = errors.Join(err, cleanup, l.syncDir())
				}
			}
		}()
		if err := f.Chmod(0600); err != nil {
			return transition, err
		}
		info, err := f.Stat()
		if err != nil {
			return transition, err
		}
		st := info.Sys().(*syscall.Stat_t)
		a.Inode, a.Device = st.Ino, uint64(st.Dev)
	}
	var output io.Writer = h
	if f != nil {
		output = io.MultiWriter(f, h)
	}
	copyRange := func(start, end int64) error {
		r := io.NewSectionReader(source, start, end-start)
		n, err := io.CopyBuffer(output, selectionReader{ctx, r}, make([]byte, 64<<10))
		if err == nil && n != end-start {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := copyRange(a.CoveredOffset, capturedEnd); err != nil {
		return transition, err
	}
	if [32]byte(h.Sum(nil)) != a.SuffixSHA256 {
		return transition, errors.New("successor WAL suffix changed during selection")
	}
	if err := copyRange(capturedEnd, a.CommittedEnd); err != nil {
		return transition, err
	}
	a.SuffixSHA256 = [32]byte(h.Sum(nil))
	if f != nil {
		if err := f.Sync(); err != nil {
			return transition, err
		}
		if old.Revision == 0 && l.captures != nil {
			old.Inode, old.Device = l.captures.identity.inode, l.captures.identity.device
		}
		transition = TailSwitch{Before: old, After: a}
		if err := l.writeTailSwitch(transition); err != nil {
			return transition, err
		}
		journal = true
		if err := renameNoReplace(l.dir, stage, a.tailName()); err != nil {
			return transition, err
		}
		installed = true
		if err := l.syncDir(); err != nil {
			return transition, err
		}
		pin, err = openCaptureFile(l.dir, f, a.tailName(), a.Base)
		if err != nil {
			return transition, err
		}
	}
	if keepOriginal {
		l.authority, l.suffixHash = a, h
		return transition, nil
	}
	if _, err := l.commitAuthority(a); err != nil {
		if errors.Is(err, ErrUncertain) {
			l.poisoned = err
			outcome := newCommitOutcome("authority_selection", err)
			l.lastOutcome.Store(&outcome)
		}
		return transition, err
	}
	l.authority, l.suffixHash = a, h
	l.offset, l.committed = a.CommittedEnd, captureBoundary{a.CommittedEnd, a.CommittedOrdinal}
	if f == nil {
		return transition, nil
	}
	oldWriter, oldPin := l.f, l.captures
	l.f, l.captures, f, pin = f, pin, nil, nil
	if oldWriter != nil {
		return transition, errors.Join(oldWriter.Close(), oldPin.release())
	}
	return transition, nil
}

type logicalReader struct {
	io.ReaderAt
	base int64
}

func (r logicalReader) ReadAt(p []byte, off int64) (int, error) {
	return r.ReaderAt.ReadAt(p, off-r.base)
}

type selectionReader struct {
	ctx context.Context
	io.Reader
}

func (r selectionReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

// ReclaimTail removes an identity retired by Select after its operation pins
// drain. The caller holds the directory owner lock throughout.
func (l *Log) ReclaimTail(old Authority) (err error) {
	if old.Inode == 0 || old.Epoch == l.Selected().Epoch {
		return nil
	}
	f, err := openRegular(l.dir, old.tailName(), os.O_RDONLY)
	if err == nil {
		info, statErr := f.Stat()
		if statErr != nil {
			return errors.Join(statErr, f.Close())
		}
		st := info.Sys().(*syscall.Stat_t)
		if st.Ino != old.Inode || uint64(st.Dev) != old.Device {
			return errors.Join(errors.New("retired WAL identity changed"), f.Close())
		}
		if err := errors.Join(l.removeStage(old.tailName()), f.Close()); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if old.Epoch == [16]byte{} {
		if err := l.removeStage(markerName); err != nil {
			return err
		}
	}
	return l.syncDir()
}

func (l *Log) appendAuthority(next int64, group []*appendRequest, h hash.Hash) (string, error) {
	a := l.authority
	a.CommittedEnd = next
	for _, req := range group {
		a.CommittedOrdinal += uint64(len(req.rows))
	}
	a.SuffixSHA256 = [32]byte(h.Sum(nil))
	var operation string
	var err error
	if a.Epoch == [16]byte{} {
		operation, err = l.commit(next)
	} else {
		operation, err = l.commitAuthority(a)
	}
	if err == nil {
		// The published authority changes together with the committed capture
		// boundary in run, after the append's disk work is complete.
		l.pendingAuthority, l.suffixHash = a, h
	}
	return operation, err
}

func isTailName(name string) bool {
	token, ok := strings.CutPrefix(name, tailPrefix)
	return ok && len(token) == 32 && strings.Trim(token, "0123456789abcdef") == ""
}

// OpenDataFile opens a checked, ordinary file relative to the pinned owner.
func (l *Log) OpenDataFile(name string) (*os.File, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return nil, os.ErrInvalid
	}
	return openRegular(l.dir, name, os.O_RDONLY)
}
