package wal

import (
	"bytes"
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
	"time"
)

const (
	AuthorityName = "bog.authority"
	tailPrefix    = "bog.wal.tail-"
	authorityMax  = 4 << 10
	// The tail is disk-backed. This cap bounds replay and copy work even when
	// appends outrun checkpoint publication.
	MaxTailBytes = 96 << 20
)

var ErrCapacity = errors.New("WAL tail capacity exhausted")

// Authority is the versioned selector for a checkpoint and its writable tail.
type Authority struct {
	Sequence         uint64 `json:"sequence"`
	Version          int    `json:"version"`
	Generation       string `json:"generation"`
	CoveredOffset    int64  `json:"covered_offset"`
	NextOrdinal      uint64 `json:"next_ordinal"`
	CommittedEnd     int64  `json:"committed_end"`
	CommittedOrdinal uint64 `json:"committed_ordinal"`
	SuffixSHA256     string `json:"suffix_sha256"`
	Tail             string `json:"tail"`
	TailBase         int64  `json:"tail_base"`
	TailDevice       uint64 `json:"tail_device"`
	TailInode        uint64 `json:"tail_inode"`
}

type authorityEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

// ReadArtifact opens a service-owned regular file relative to the pinned
// directory. The caller owns the returned descriptor.
func (l *Log) ReadArtifact(name string) (*os.File, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
		return nil, os.ErrInvalid
	}
	return openRegular(l.dir, name, os.O_RDONLY)
}

func (l *Log) ReadAuthority() (_ Authority, err error) {
	f, err := l.ReadArtifact(AuthorityName)
	if err != nil {
		return Authority{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	data, err := io.ReadAll(io.LimitReader(f, authorityMax+1))
	if err != nil {
		return Authority{}, err
	}
	return decodeAuthority(data)
}

func digestText(text string, size int) bool {
	return len(text) == size*2 && strings.Trim(text, "0123456789abcdef") == ""
}

func ownedTailName(name string) bool {
	suffix, ok := strings.CutPrefix(name, tailPrefix)
	return ok && digestText(suffix, stageIDSize)
}

func (a Authority) validate() error {
	if a.Version != 2 || !digestText(a.Generation, sha256.Size) || !digestText(a.SuffixSHA256, sha256.Size) ||
		!strings.HasPrefix(a.Tail, tailPrefix) || a.Tail == tailPrefix || strings.ContainsAny(a.Tail, "/\\\x00") ||
		a.TailInode == 0 || a.TailBase < 0 || a.TailBase > a.CoveredOffset || a.CoveredOffset < int64(len(magic)) ||
		a.CommittedEnd < a.CoveredOffset || a.CommittedOrdinal < a.NextOrdinal ||
		(a.CommittedEnd == a.CoveredOffset) != (a.CommittedOrdinal == a.NextOrdinal) ||
		(a.CoveredOffset == int64(len(magic))) != (a.NextOrdinal == 0) {
		return errors.New("invalid writable authority fields")
	}
	if a.CommittedEnd-a.CoveredOffset > MaxTailBytes {
		return ErrCapacity
	}
	return nil
}

func decodeAuthority(data []byte) (Authority, error) {
	var e authorityEnvelope
	var a Authority
	if len(data) > authorityMax ||
		!uniqueFields(data, "payload", "sha256") || json.Unmarshal(data, &e) != nil ||
		e.SHA256 != fmt.Sprintf("%x", sha256.Sum256(e.Payload)) ||
		!uniqueFields(e.Payload, "version", "generation", "covered_offset", "next_ordinal", "committed_end",
			"committed_ordinal", "suffix_sha256", "tail", "tail_base", "tail_device", "tail_inode") ||
		json.Unmarshal(e.Payload, &a) != nil {
		return Authority{}, errors.New("invalid writable authority envelope")
	}
	return a, a.validate()
}

func uniqueFields(data []byte, required ...string) bool {
	d := json.NewDecoder(bytes.NewReader(data))
	if token, err := d.Token(); err != nil || token != json.Delim('{') {
		return false
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return false
		}
		key := token.(string) // Object keys are strings.
		if _, duplicate := fields[key]; duplicate {
			return false
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return false
		}
		fields[key] = raw
	}
	if token, err := d.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	if _, err := d.Token(); err != io.EOF {
		return false
	}
	for _, key := range required {
		if raw := fields[key]; len(raw) == 0 || string(raw) == "null" {
			return false
		}
	}
	return true
}

func (l *Log) selectAuthority(a Authority) (string, error) {
	if err := a.validate(); err != nil {
		return "authority_preparation", err
	}
	p, err := json.Marshal(a)
	if err != nil {
		return "authority_preparation", err
	}
	data, err := json.Marshal(authorityEnvelope{p, fmt.Sprintf("%x", sha256.Sum256(p))})
	if err != nil {
		return "authority_preparation", err
	}
	return l.commitRecord(AuthorityName, data)
}

// OpenSelected validates the exact selected tail before any suffix truncation.
// validateGeneration is called after replay and before any recovery mutation.
func (l *Log) OpenSelected(a Authority, validateGeneration func(*Capture) error) (err error) {
	f, err := openRegular(l.dir, a.Tail, os.O_RDWR)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	reader, err := openCaptureFile(l.dir, f, a.Tail)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, reader.release())
		}
	}()
	if reader.identity != (FileIdentity{a.TailDevice, a.TailInode}) {
		return errors.New("selected WAL tail identity changed")
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < a.CommittedEnd-a.TailBase {
		return errors.New("selected WAL tail is truncated")
	}
	reader.base = a.TailBase
	boundary := captureBoundary{a.CommittedEnd, a.CommittedOrdinal}
	c := &Capture{lease: &captureLease{file: reader, boundary: boundary}}
	sum := sha256.New().(hash.Cloner)
	frames := io.NewSectionReader(c, a.CoveredOffset, a.CommittedEnd-a.CoveredOffset)
	if _, err := io.Copy(sum, frames); err != nil {
		return err
	}
	if hex.EncodeToString(sum.Sum(nil)) != a.SuffixSHA256 {
		return errors.New("selected WAL suffix checksum mismatch")
	}
	next, err := l.scanRecovery(c, a.CoveredOffset, a.CommittedEnd, a.NextOrdinal, nil)
	if err != nil {
		return err
	}
	if next != a.CommittedOrdinal || len(l.frames) > MaxLiveFrames {
		return errors.New("selected WAL suffix ordinal or frame count mismatch")
	}
	if err := validateGeneration(c); err != nil {
		return err
	}
	if err := finishReplay(f, info.Size(), a.CommittedEnd-a.TailBase); err != nil {
		return err
	}
	if err := errors.Join(f.Sync(), l.cleanOrphans(), l.dir.Sync()); err != nil {
		return err
	}
	l.f, l.captures, l.authority, l.suffix = f, reader, a, sum
	l.committed, l.offset = boundary, a.CommittedEnd-a.TailBase
	return nil
}

// RetiredTail may be reclaimed after Drained closes. The writer has already
// closed and the last capture and read have released the old reader.
type RetiredTail struct {
	Name     string
	Identity FileIdentity
	Drained  <-chan struct{}
}

// Activate installs the first retained selection for either an ordinary WAL
// or a verified restore. Recovery holds the owner lock and has no live users.
func (l *Log) Activate(ctx context.Context, a Authority, input io.ReaderAt) (RetiredTail, error) {
	return l.replaceTail(ctx, a, input, a.CommittedEnd)
}

// Replacement serializes writable selection and catalog activation with
// append commits. The caller closes it after installing the selected catalog.
type Replacement struct{ l *Log }

func (l *Log) BeginReplacement() *Replacement {
	l.ioMu.Lock()
	return &Replacement{l}
}

func (r *Replacement) Close() { r.l.ioMu.Unlock() }

// Select copies all committed frames beyond cutoff, including appends since
// the checkpoint's capture.
func (r *Replacement) Select(ctx context.Context, a Authority, predecessor string) (RetiredTail, error) {
	l := r.l
	l.mu.Lock()
	if err := l.admissionError(); err != nil {
		l.mu.Unlock()
		return RetiredTail{}, err
	}
	c := l.captureLocked()
	selected := l.authority
	l.mu.Unlock()
	if selected.Generation != predecessor || a.CoveredOffset < selected.CoveredOffset ||
		a.CoveredOffset > c.CommittedEnd() {
		return RetiredTail{}, errors.Join(errors.New("WAL checkpoint predecessor changed"), c.Release())
	}
	verifiedEnd := a.CommittedEnd
	a.CommittedEnd, a.CommittedOrdinal = c.CommittedEnd(), c.NextOrdinal()
	retired, err := l.replaceTail(ctx, a, c, verifiedEnd)
	return retired, errors.Join(err, c.Release())
}

func (l *Log) replaceTail(ctx context.Context, a Authority, input io.ReaderAt, verifiedEnd int64) (_ RetiredTail, err error) {
	if a.CoveredOffset < int64(len(magic)) || a.CommittedEnd < a.CoveredOffset || a.CommittedEnd-a.CoveredOffset > MaxTailBytes {
		return RetiredTail{}, ErrCapacity
	}
	var nonce [stageIDSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return RetiredTail{}, err
	}
	a.Tail = tailPrefix + hex.EncodeToString(nonce[:])
	a.Version, a.TailBase = 2, a.CoveredOffset
	f, err := openRegular(l.dir, a.Tail, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return RetiredTail{}, err
	}
	var reader *captureFile
	installed := false
	defer func() {
		if installed {
			return
		}
		err = errors.Join(err, f.Close())
		if reader != nil {
			err = errors.Join(err, reader.release())
		}
		if !errors.Is(err, ErrUncertain) {
			err = errors.Join(err, l.removeStage(a.Tail), l.dir.Sync())
		} else {
			l.Fence(err)
		}
	}()
	size := a.CommittedEnd - a.TailBase
	if verifiedEnd < a.CoveredOffset || verifiedEnd > a.CommittedEnd {
		return RetiredTail{}, errors.New("invalid verified suffix end")
	}
	sum := sha256.New().(hash.Cloner)
	dst := io.MultiWriter(f, sum)
	copyRange := func(start, end int64) error {
		for start < end {
			if err := ctx.Err(); err != nil {
				return err
			}
			n := min(end-start, 1<<20)
			written, err := io.Copy(dst, io.NewSectionReader(input, start, n))
			if err != nil {
				return err
			}
			if written != n {
				return io.ErrUnexpectedEOF
			}
			start += n
		}
		return nil
	}
	if err := copyRange(a.CoveredOffset, verifiedEnd); err != nil {
		return RetiredTail{}, err
	}
	if a.SuffixSHA256 != "" && hex.EncodeToString(sum.Sum(nil)) != a.SuffixSHA256 {
		return RetiredTail{}, errors.New("copied WAL suffix differs from verified suffix")
	}
	if err := copyRange(verifiedEnd, a.CommittedEnd); err != nil {
		return RetiredTail{}, err
	}
	a.SuffixSHA256 = hex.EncodeToString(sum.Sum(nil))
	var frames []Frame
	var bounds frameBounds
	next := a.NextOrdinal
	err = scanFrames(ctx, logicalReader{f, a.TailBase}, a.CoveredOffset, a.CommittedEnd, next, nil, func(frame Frame) error {
		if len(frames) == MaxLiveFrames {
			return ErrCapacity
		}
		var previous *Frame
		if len(frames) != 0 {
			previous = &frames[len(frames)-1]
		}
		bounds.add(frame, previous)
		frame.last = time.Time{}
		frames = append(frames, frame)
		next = frame.NextOrdinal
		return nil
	})
	if err != nil {
		return RetiredTail{}, fmt.Errorf("validate copied WAL suffix: %w", err)
	}
	if next != a.CommittedOrdinal {
		return RetiredTail{}, errors.New("copied WAL suffix ordinal mismatch")
	}
	if err := errors.Join(f.Chmod(0600), f.Sync()); err != nil {
		return RetiredTail{}, err
	}
	reader, err = openCaptureFile(l.dir, f, a.Tail)
	if err != nil {
		return RetiredTail{}, err
	}
	reader.base = a.TailBase
	a.TailDevice, a.TailInode = reader.identity.device, reader.identity.inode
	if err := l.dir.Sync(); err != nil {
		return RetiredTail{}, err
	}
	if err := ctx.Err(); err != nil {
		return RetiredTail{}, err
	}
	if _, err := l.selectAuthority(a); err != nil {
		return RetiredTail{}, err
	}

	l.mu.Lock()
	old, oldReader, oldName := l.f, l.captures, l.authority.Tail
	l.f, l.captures, l.authority, l.suffix = f, reader, a, sum
	l.offset, l.frames, l.bounds = size, frames, bounds
	l.committed = captureBoundary{a.CommittedEnd, a.CommittedOrdinal}
	l.mu.Unlock()
	installed = true
	if old == nil {
		return RetiredTail{}, nil
	}
	if oldName == "" {
		oldName = walName
	}
	return RetiredTail{oldName, oldReader.identity, oldReader.drained}, errors.Join(old.Close(), oldReader.release())
}

type logicalReader struct {
	io.ReaderAt
	base int64
}

func (r logicalReader) ReadAt(p []byte, off int64) (int, error) {
	return r.ReaderAt.ReadAt(p, off-r.base)
}

// Fence terminates new mutations and captures after uncertain publication.
func (l *Log) Fence(cause error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.poisoned == nil {
		l.poisoned = errors.Join(ErrUncertain, cause)
		outcome := newCommitOutcome("authority", l.poisoned)
		l.lastOutcome.Store(&outcome)
	}
}

// EnsureArtifact installs immutable bytes at a protocol-owned name. An
// interrupted activation can reuse the file only when its bytes agree.
func (l *Log) EnsureArtifact(name string, data []byte) (err error) {
	current, err := l.ReadArtifact(name)
	if err == nil {
		got, readErr := io.ReadAll(io.LimitReader(current, int64(len(data))+1))
		if err := errors.Join(readErr, current.Close()); err != nil {
			return err
		}
		if !bytes.Equal(got, data) {
			return fmt.Errorf("immutable artifact %q differs", name)
		}
		return l.dir.Sync()
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	staged, err := l.writeStaged(walStagePrefix, data)
	if err != nil {
		return err
	}
	if err := renameNoReplace(l.dir, staged, name); err != nil {
		return errors.Join(err, l.removeStage(staged))
	}
	return l.dir.Sync()
}

// RemoveRetired unlinks a drained WAL only if the original inode still owns
// the name. The durable successor must already have been selected.
func (l *Log) RemoveRetired(t RetiredTail) (err error) {
	f, err := l.ReadArtifact(t.Name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	identity := fileIdentity(info)
	if identity != t.Identity {
		return fmt.Errorf("retired WAL %q changed identity", t.Name)
	}
	return errors.Join(l.removeStage(t.Name), l.dir.Sync())
}

// RecoverTails removes interrupted and retired, protocol-named tails under
// the owner lock after the selected generation and tail are validated.
func (l *Log) RecoverTails() (err error) {
	d, err := l.root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	for {
		names, readErr := d.Readdirnames(64)
		for _, name := range names {
			if name == l.authority.Tail || !ownedTailName(name) {
				continue
			}
			f, err := l.ReadArtifact(name)
			if err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			if err := l.removeStage(name); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return l.dir.Sync()
		}
		if readErr != nil {
			return readErr
		}
	}
}
