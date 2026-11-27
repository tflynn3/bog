package checkpoint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
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
	ctx        context.Context
	files      []stagedBlock
	scratch    []byte
	installing bool
	manifest   *ownedSegment
}

type publication struct {
	root  *os.Root
	dir   *os.File
	stage *stagingAccount

	segmentDirSync  func() error
	manifestRename  func(string, string) error
	manifestDirSync func() error
	cleanupContext  context.Context
}

// PublishCaptured extends a validated checkpoint with whole committed frames.
// The caller owns the WAL directory lock, pins input and previous throughout
// the call, and serializes generation activation. The supplied digest binds
// the exact v1 prefix through end. Previous is nil for a first publication.
// Publication takes its own lock and requires recovery of any pending attempt.
func PublishCaptured(ctx context.Context, root *os.Root, dir *os.File, input io.ReaderAt, end int64, previous *Manifest, next uint64, source string) (*Manifest, error) {
	return publishCaptured(ctx, root, dir, input, end, previous, next, source, nil)
}

func publishCaptured(ctx context.Context, root *os.Root, dir *os.File, input io.ReaderAt, end int64, previous *Manifest, next uint64, source string, stage *stagingAccount) (published *Manifest, err error) {
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
		m.Segments = previous.Segments
	}
	p.stage = stage
	return p.publish(ctx, m, &next, func(visit func(uint64, []store.Row) error) error {
		return scanCapturedFrames(ctx, input, start, end, m.NextOrdinal, func(_, _ int64, first uint64, rows []store.Row) error {
			return visit(first, rows)
		})
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
	var scratch [32 << 10]byte
	seen := make(map[string]struct{}, len(m.Segments))
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !validSegmentName(s.Name) || s.Size < blockHeaderSize+blockRowSize || s.Size > MaxBlockSize {
			return fmt.Errorf("%w: invalid segment metadata", ErrIntegrity)
		}
		if _, duplicate := seen[s.Name]; duplicate {
			return fmt.Errorf("%w: repeated segment %s", ErrIntegrity, s.Name)
		}
		seen[s.Name] = struct{}{}
		if err := verifyPreviousSegment(p.dir, s, &scratch); err != nil {
			return fmt.Errorf("%w: referenced segment %s: %v", ErrIntegrity, s.Name, err)
		}
	}
	return nil
}

func verifyPreviousSegment(dir *os.File, s segmentMeta, scratch *[32 << 10]byte) (err error) {
	f, err := openPublicationFile(dir, s.Name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	h := sha256.New()
	n, err := io.CopyBuffer(h, io.LimitReader(f, s.Size+1), scratch[:])
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

func (p *publication) publish(ctx context.Context, m *Manifest, next *uint64, scan func(func(uint64, []store.Row) error) error) (*Manifest, error) {
	return p.publishGeneration(ctx, m, next, scan, nil)
}

func (p *publication) publishGeneration(ctx context.Context, m *Manifest, next *uint64, scan func(func(uint64, []store.Row) error) error, selectGeneration func([32]byte) error) (published *Manifest, err error) {
	p.cleanupContext = ctx
	if err := p.root.Mkdir(stageDir, 0700); err != nil {
		return nil, err
	}
	a := publicationAttempt{pub: p, ctx: ctx}
	manifestPending := false
	if p.stage != nil {
		p.stage.pending = true
	}
	defer func() {
		if manifestPending {
			return
		}
		if published == nil && a.installing {
			// Leave the durable intent available if cleanup fails.
			if cleanupErr := p.removeOwned(a.ownedFiles(), nil); cleanupErr != nil {
				err = errors.Join(err, ErrPublicationUncertain, cleanupErr)
				return
			}
		}
		if cleanupErr := p.clearStaging(); cleanupErr != nil {
			err = errors.Join(err, ErrPublicationUncertain, cleanupErr)
			return
		}
		p.stage.settle()
	}()
	if err := p.root.Chmod(stageDir, 0700); err != nil {
		return nil, err
	}
	if err := a.buildSegments(m, next, scan); err != nil {
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
	id := sha256.Sum256(payload)
	if selectGeneration == nil {
		if err := a.writeNew(stagedManifest, b); err != nil {
			return nil, err
		}
	} else if err := a.stageGeneration(id, b); err != nil {
		return nil, err
	}
	if err := a.install(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if selectGeneration != nil {
		if err := p.validateInstalledGeneration(ctx, id); err != nil {
			return nil, err
		}
		manifestPending = true
		if err := selectGeneration(id); err != nil {
			return nil, fmt.Errorf("%w: select checkpoint generation: %w", ErrPublicationUncertain, err)
		}
		manifestPending = false
		return m, nil
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
