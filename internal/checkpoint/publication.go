package checkpoint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"syscall"

	"bog/internal/store"
)

func randomName(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
func (p *publication) writeNew(name string, data []byte) (err error) {
	f, err := p.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	// The create mode is masked by umask. Set the exact mode on the new file
	// descriptor before syncing it, without changing the process-wide umask.
	if err = f.Chmod(0600); err != nil {
		return err
	}
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return f.Sync()
}

type publicationAttempt struct {
	pub        *publication
	files      []ownedSegment
	installing bool
}

type publication struct {
	root *os.Root
	dir  *os.File

	segmentDirSync  func() error
	manifestRename  func(string, string) error
	manifestDirSync func() error
}

// PublishCaptured extends a validated checkpoint with whole committed frames.
// The caller owns the WAL directory lock, pins input and previous throughout
// the call, and serializes generation activation. The supplied digest binds
// the exact v1 prefix through end. Previous is nil for a first publication.
// Publication takes its own lock and requires recovery of any pending attempt.
func PublishCaptured(ctx context.Context, root *os.Root, dir *os.File, input io.ReaderAt, end int64, previous *Manifest, next uint64, source string) (published *Manifest, err error) {
	if input == nil || !validPublicationBoundary(end, next, source) {
		return nil, fmt.Errorf("%w: invalid checkpoint prefix", ErrIntegrity)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, lock, err := lockOwnedPublication(root, dir)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := CheckPublication(dir); err != nil {
		return nil, err
	}
	if err := p.checkPrevious(ctx, previous); err != nil {
		return nil, err
	}

	start := int64(len("BOGWAL01"))
	m := newManifest(end, source)
	if previous != nil {
		if previous.CoveredOffset > end || previous.NextOrdinal > next {
			return nil, errors.New("checkpoint cutoff precedes current generation")
		}
		start = previous.CoveredOffset
		m.NextOrdinal = previous.NextOrdinal
		m.Segments = append(m.Segments, previous.Segments...)
	}
	return p.publish(ctx, m, &next, func(visit func(uint64, []store.Row) error) error {
		return scanPublicationFrames(ctx, input, start, end, m.NextOrdinal, visit)
	})
}

func validPublicationBoundary(offset int64, ordinal uint64, sum string) bool {
	const headerSize = int64(len("BOGWAL01"))
	const minFrameSize = 16 + 1
	return validDigest(sum) && strings.Trim(sum, "0123456789abcdef") == "" &&
		(offset == headerSize && ordinal == 0 || offset >= headerSize+minFrameSize && ordinal > 0)
}

func lockOwnedPublication(root *os.Root, dir *os.File) (*publication, *os.File, error) {
	if root == nil || dir == nil {
		return nil, nil, errors.New("missing pinned checkpoint directory")
	}
	info, err := dir.Stat()
	if err != nil {
		return nil, nil, err
	}
	rootInfo, err := root.Stat(".")
	if err != nil {
		return nil, nil, err
	}
	if !os.SameFile(info, rootInfo) || !info.IsDir() || !ownedByService(info) || info.Mode().Perm()&0022 != 0 {
		return nil, nil, fmt.Errorf("%w: unsafe or mismatched data directory", ErrIntegrity)
	}
	lock, err := LockPublication(dir, true)
	if err != nil {
		return nil, nil, err
	}
	return &publication{root: root, dir: dir}, lock, nil
}

// checkPrevious ensures an accepted pin still names the current authority.
// Segment digests confirm that the previously validated immutable bytes are
// still present before publishing a reference to them in a new generation.
func (p *publication) checkPrevious(ctx context.Context, previous *Manifest) error {
	b, err := readPublicationFile(p.dir, manifestName, maxManifestSize)
	if err == syscall.ENOENT && previous == nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: read previous checkpoint: %v", ErrIntegrity, err)
	}
	var env manifestEnvelope
	var m Manifest
	if json.Unmarshal(b, &env) != nil || digest(env.Payload) != env.SHA256 ||
		json.Unmarshal(env.Payload, &m) != nil || m.Version != 1 || len(m.Segments) > MaxSegments ||
		!validPublicationBoundary(m.CoveredOffset, m.NextOrdinal, m.SourceSHA256) {
		return fmt.Errorf("%w: invalid authoritative manifest", ErrIntegrity)
	}
	if previous == nil || m.Version != previous.Version || m.CoveredOffset != previous.CoveredOffset ||
		m.NextOrdinal != previous.NextOrdinal || m.SourceSHA256 != previous.SourceSHA256 ||
		!slices.Equal(m.Segments, previous.Segments) {
		return fmt.Errorf("%w: previous checkpoint is no longer authoritative", ErrIntegrity)
	}
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !validSegmentName(s.Name) || s.Size < blockHeaderSize+blockRowSize || s.Size > MaxBlockSize {
			return fmt.Errorf("%w: invalid segment metadata", ErrIntegrity)
		}
		if err := verifyPreviousSegment(p.dir, s); err != nil {
			return fmt.Errorf("%w: referenced segment %s: %v", ErrIntegrity, s.Name, err)
		}
	}
	return nil
}

func verifyPreviousSegment(dir *os.File, s segmentMeta) (err error) {
	f, err := openPublicationFile(dir, s.Name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, s.Size+1))
	if err != nil {
		return err
	}
	if n != s.Size || hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
		return errors.New("segment size or checksum changed")
	}
	return nil
}

func (l *Legacy) publication() *publication {
	return &publication{
		root: l.root, dir: l.dir,
		segmentDirSync: l.segmentDirSync, manifestRename: l.manifestRename, manifestDirSync: l.manifestDirSync,
	}
}

// Publish builds a checkpoint while the Legacy directory lock is held. A
// rename attempt or final sync error requires reopening before another operation.
func (l *Legacy) Publish() (published *Manifest, err error) {
	if err := l.begin(); err != nil {
		return nil, err
	}
	defer l.active.Done()
	l.publicationMu.Lock()
	defer l.publicationMu.Unlock()
	if l.uncertain != nil {
		return nil, l.uncertain
	}
	lock, err := LockPublication(l.dir, true)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := CheckPublication(l.dir); err != nil {
		if !errors.Is(err, ErrPublicationUncertain) {
			return nil, err
		}
		if _, err := l.resolvePublication(false); err != nil && err != os.ErrNotExist {
			return nil, err
		}
	}
	sum, err := l.prefixDigest(l.end)
	if err != nil {
		return nil, err
	}
	m := newManifest(l.end, sum)
	published, err = l.publication().publish(context.Background(), m, nil, func(visit func(uint64, []store.Row) error) error {
		return l.scanFrom(8, 0, func(_, _ int64, first uint64, rows []store.Row) error {
			return visit(first, rows)
		})
	})
	if errors.Is(err, ErrPublicationUncertain) {
		l.uncertain = err
	}
	return published, err
}

func newManifest(end int64, sum string) *Manifest {
	return &Manifest{Version: 1, CoveredOffset: end, SourceSHA256: sum, Segments: []segmentMeta{}}
}

func (p *publication) publish(ctx context.Context, m *Manifest, next *uint64, scan func(func(uint64, []store.Row) error) error) (published *Manifest, err error) {
	if err := p.root.Mkdir(stageDir, 0700); err != nil {
		return nil, err
	}
	a := publicationAttempt{pub: p}
	manifestPending := false
	defer func() {
		if manifestPending {
			return
		}
		if published == nil && a.installing {
			// Leave the durable intent available if cleanup fails.
			if cleanupErr := p.removeOwned(a.files, nil); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
				return
			}
		}
		err = errors.Join(err, p.clearStaging())
	}()
	if err := p.root.Chmod(stageDir, 0700); err != nil {
		return nil, err
	}
	var run []BlockRow
	size := blockHeaderSize
	flush := func() error {
		if len(run) == 0 {
			return nil
		}
		sort.Slice(run, func(i, j int) bool { return lessRow(run[i], run[j]) })
		if len(m.Segments) >= MaxSegments {
			return errors.New("checkpoint segment limit exceeded")
		}
		b, e := EncodeBlock(run)
		if e != nil {
			return e
		}
		name, e := randomName(segmentPrefix)
		if e != nil {
			return e
		}
		sum := digest(b)
		if e = a.stageSegment(name, b, sum); e != nil {
			return e
		}
		m.Segments = append(m.Segments, segmentMeta{Name: name, Size: int64(len(b)), SHA256: sum})
		run = nil
		size = blockHeaderSize
		return nil
	}
	err = scan(func(first uint64, rows []store.Row) error {
		if first != m.NextOrdinal || len(rows) == 0 || uint64(len(rows)) > ^uint64(0)-first {
			return fmt.Errorf("%w: invalid checkpoint batch ordinals", ErrIntegrity)
		}
		for i, row := range rows {
			if len(row.Line) > MaxBlockSize-blockHeaderSize-blockRowSize {
				return errors.New("row exceeds block limit")
			}
			n := blockRowSize + len(row.Line)
			if size+n > MaxBlockSize {
				if e := flush(); e != nil {
					return e
				}
			}
			run = append(run, BlockRow{Row: row, Ordinal: first + uint64(i)})
			size += n
		}
		m.NextOrdinal = first + uint64(len(rows))
		return nil
	})
	if err == nil && next != nil && m.NextOrdinal != *next {
		err = fmt.Errorf("%w: checkpoint scan did not match prefix ordinal", ErrIntegrity)
	}
	if err == nil {
		err = flush()
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(manifestEnvelope{Payload: payload, SHA256: digest(payload)})
	if err != nil {
		return nil, err
	}
	if len(b) > maxManifestSize {
		return nil, errors.New("manifest too large")
	}
	if err := p.writeNew(stagedManifest, b); err != nil {
		return nil, err
	}
	if err := a.install(); err != nil {
		return nil, err
	}
	rename := p.root.Rename
	if p.manifestRename != nil {
		rename = p.manifestRename
	}
	manifestPending = true
	if err = rename(stagedManifest, manifestName); err != nil {
		return nil, fmt.Errorf("%w: rename manifest: %w", ErrPublicationUncertain, err)
	}
	if err = p.syncStaging(); err != nil {
		return nil, fmt.Errorf("%w: sync manifest staging directory: %w", ErrPublicationUncertain, err)
	}
	syncDir := p.dir.Sync
	if p.manifestDirSync != nil {
		syncDir = p.manifestDirSync
	}
	if err = syncDir(); err != nil {
		return nil, fmt.Errorf("%w: sync manifest directory: %w", ErrPublicationUncertain, err)
	}
	manifestPending = false
	return m, nil
}

// scanPublicationFrames reads an owner-pinned suffix at absolute WAL offsets.
// It never reads the omitted prefix of a restored WAL.
func scanPublicationFrames(ctx context.Context, input io.ReaderAt, pos, end int64, ordinal uint64, visit func(uint64, []store.Row) error) error {
	var header [16]byte
	var payload []byte
	for pos < end {
		if err := ctx.Err(); err != nil {
			return err
		}
		if end-pos < int64(len(header)) {
			return fmt.Errorf("%w: incomplete committed frame at %d", ErrIntegrity, pos)
		}
		frame := io.NewSectionReader(input, pos, end-pos)
		if _, err := io.ReadFull(frame, header[:]); err != nil {
			return fmt.Errorf("read committed frame header at %d: %w", pos, err)
		}
		n := binary.LittleEndian.Uint32(header[:4])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(header[4:8]) != ^n ||
			crc32.Checksum(header[:12], castagnoli) != binary.LittleEndian.Uint32(header[12:]) {
			return fmt.Errorf("%w: invalid committed frame header at %d", ErrIntegrity, pos)
		}
		if int64(n) > end-pos-int64(len(header)) {
			return fmt.Errorf("%w: incomplete committed frame at %d", ErrIntegrity, pos)
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		}
		payload = payload[:n]
		if _, err := io.ReadFull(frame, payload); err != nil {
			return fmt.Errorf("read committed frame at %d: %w", pos, err)
		}
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(header[8:12]) {
			return fmt.Errorf("%w: invalid committed frame checksum at %d", ErrIntegrity, pos)
		}
		rows, err := store.Decode(payload)
		if err != nil {
			return fmt.Errorf("%w: invalid committed frame at %d: %w", ErrIntegrity, pos, err)
		}
		if uint64(len(rows)) > ^uint64(0)-ordinal {
			return fmt.Errorf("%w: append ordinal overflow", ErrIntegrity)
		}
		if err := visit(ordinal, rows); err != nil {
			return err
		}
		ordinal += uint64(len(rows))
		pos += int64(len(header)) + int64(n)
	}
	return ctx.Err()
}
