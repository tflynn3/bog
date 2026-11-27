package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

const (
	stageDir       = ".bog-checkpoint-stage"
	stagedManifest = stageDir + "/manifest"
	stagedIntent   = stageDir + "/intent"
	intentBase     = "installed-v1"
	intentName     = stageDir + "/" + intentBase
)

type ownedSegment struct {
	Name   string
	Inode  uint64
	Size   int64
	SHA256 string
}

type publicationIntent struct {
	HadManifest bool
	Segments    []ownedSegment
	Generation  *ownedSegment `json:",omitempty"`
}

func (a *publicationAttempt) recordIntent() error {
	p := a.pub
	previous, err := p.root.Lstat(manifestName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	files := a.ownedFiles()
	intent := publicationIntent{Segments: files[:len(a.files)], Generation: a.manifest}
	if previous != nil {
		intent.HadManifest = previous.Mode().IsRegular()
	}
	payload, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	b, err := json.Marshal(manifestEnvelope{Payload: payload, SHA256: digest(payload)})
	if err != nil {
		return err
	}
	if len(b) > maxManifestSize {
		return errors.New("checkpoint intent too large")
	}
	if err := a.writeNew(stagedIntent, b); err != nil {
		return err
	}
	if err := p.root.Rename(stagedIntent, intentName); err != nil {
		return err
	}
	// Make the ownership record durable before the first final name can exist.
	if err := p.syncStaging(); err != nil {
		return err
	}
	return p.dir.Sync()
}

func (a *publicationAttempt) install() error {
	p := a.pub
	if err := a.recordIntent(); err != nil {
		return err
	}
	a.installing = true
	for _, file := range a.ownedFiles() {
		path := stageDir + "/" + file.Name
		if err := stagingFault(a.ctx, linkFinal); err != nil {
			return err
		}
		if err := p.root.Link(path, file.Name); err != nil {
			return err
		}
		if err := stagingFault(a.ctx, unlinkStageLink); err != nil {
			return err
		}
		if err := p.root.Remove(path); err != nil {
			return err
		}
	}
	// Persist the removal of staging links as well as the final segment names.
	if err := p.syncStaging(); err != nil {
		return err
	}
	syncDir := p.dir.Sync
	if p.segmentDirSync != nil {
		syncDir = p.segmentDirSync
	}
	return syncDir()
}

// RecoverCaptured resolves an interrupted publication while the caller owns
// the WAL directory lock. Authoritative runs under the publication lock. It
// must validate the manifest, every referenced segment, and the owner's WAL
// boundary, or return os.ErrNotExist if the manifest itself is absent.
func RecoverCaptured(root *os.Root, dir *os.File, authoritative func() (*Manifest, error)) error {
	return recoverCapturedWithBudget(root, dir, authoritative, nil)
}

func recoverCapturedWithBudget(root *os.Root, dir *os.File, authoritative func() (*Manifest, error), stage *stagingAccount) (err error) {
	if authoritative == nil {
		return errors.New("missing authoritative checkpoint validator")
	}
	p, lock, err := lockOwnedPublication(root, dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	err = p.resolve(authoritative)
	if err == nil || err == os.ErrNotExist {
		stage.settle()
	}
	return err
}

// resolvePublication retains the offline reader's additional WAL checks.
func (l *Legacy) resolvePublication(keepRows bool) (*Published, error) {
	var published *Published
	err := l.publication().resolve(func() (*Manifest, error) {
		var err error
		published, err = l.readPublished(keepRows)
		if err != nil {
			return nil, err
		}
		return &published.manifest, nil
	})
	if err != nil {
		return nil, err
	}
	return published, nil
}

// resolve validates the authoritative namespace and makes it durable before
// deleting losing outputs. Missing is a valid first-publication outcome.
func (p *publication) resolve(load func() (*Manifest, error)) error {
	info, err := p.root.Lstat(stageDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if info != nil {
		if err := checkStaging(info); err != nil {
			return err
		}
	}
	m, authorityErr := load()
	if authorityErr != nil {
		if !errors.Is(authorityErr, os.ErrNotExist) {
			return authorityErr
		}
		// Only absence of the manifest itself is a first-publication case.
		// A validator can encounter a missing referenced file instead.
		_, err := p.root.Lstat(manifestName)
		if err == nil || !errors.Is(err, os.ErrNotExist) || m != nil {
			return fmt.Errorf("%w: validate authoritative manifest: %v", ErrIntegrity, errors.Join(authorityErr, err))
		}
	} else if m == nil {
		return fmt.Errorf("%w: authoritative validator returned no manifest", ErrIntegrity)
	}
	if err := p.dir.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint directory: %w", err)
	}
	if info != nil {
		if err := p.recoverStaging(info, m, ""); err != nil {
			return err
		}
	}
	if m == nil {
		return os.ErrNotExist
	}
	return nil
}

func (p *publication) recoverStaging(info os.FileInfo, m *Manifest, generationName string) error {
	if info.Mode().Perm() != 0700 {
		return p.removeEmptyStaging()
	}
	names, intent, err := p.inspectStaging()
	if err != nil {
		return err
	}
	if intent.Generation != nil && generationName == "" {
		return fmt.Errorf("%w: recovery view required for pending generation", ErrPublicationUncertain)
	}
	if intent.Generation != nil && intent.Generation.Name == generationName {
		b, err := readPublicationFile(p.dir, generationName, maxManifestSize)
		if err != nil {
			return err
		}
		_, id, err := DecodeManifest(b)
		if err != nil {
			return err
		}
		if GenerationManifestName(id) != generationName || digest(b) != intent.Generation.SHA256 {
			return fmt.Errorf("%w: selected manifest does not match publication intent", ErrIntegrity)
		}
	}
	if m == nil && intent.HadManifest {
		return fmt.Errorf("%w: previous authoritative manifest is missing", ErrIntegrity)
	}
	reachable := make(map[string]bool)
	if m != nil {
		for _, s := range m.Segments {
			reachable[s.Name] = true
		}
	}
	reachable[generationName] = true
	if err := p.removeOwned(intent.files(), reachable); err != nil {
		return err
	}
	return p.removeStaging(names)
}

func openPublicationFile(dir *os.File, name string) (*os.File, error) {
	fd, err := openat(int(dir.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err == nil && (!ownedByService(info) || !info.Mode().IsRegular() ||
		info.Sys().(*syscall.Stat_t).Nlink != 1 || info.Mode().Perm()&0022 != 0) {
		err = fmt.Errorf("%w: unsafe publication file %q", ErrIntegrity, name)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

func readPublicationFile(dir *os.File, name string, limit int64) (_ []byte, err error) {
	f, err := openPublicationFile(dir, name)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("%w: publication file %q exceeds size limit", ErrIntegrity, name)
	}
	return b, nil
}

func readIntent(stage *os.File) (publicationIntent, error) {
	b, err := readPublicationFile(stage, intentBase, maxManifestSize)
	if err == syscall.ENOENT {
		// Final names can only be installed after this record is durable.
		return publicationIntent{}, nil
	}
	if err != nil {
		return publicationIntent{}, err
	}
	var env manifestEnvelope
	var intent publicationIntent
	if json.Unmarshal(b, &env) != nil ||
		digest(env.Payload) != env.SHA256 || json.Unmarshal(env.Payload, &intent) != nil || len(intent.Segments) > MaxSegments {
		return publicationIntent{}, fmt.Errorf("%w: invalid publication intent", ErrIntegrity)
	}
	seen := make(map[string]bool, len(intent.Segments))
	for _, file := range intent.Segments {
		if !validSegmentName(file.Name) || file.Inode == 0 || seen[file.Name] ||
			file.Size < blockHeaderSize+blockRowSize || file.Size > MaxBlockSize || !validDigest(file.SHA256) {
			return publicationIntent{}, fmt.Errorf("%w: invalid publication intent entry", ErrIntegrity)
		}
		seen[file.Name] = true
	}
	if file := intent.Generation; file != nil &&
		(!validGenerationName(file.Name) || file.Inode == 0 || file.Size <= 0 ||
			file.Size > maxManifestSize || !lowercaseDigest(file.SHA256)) {
		return publicationIntent{}, fmt.Errorf("%w: invalid publication generation entry", ErrIntegrity)
	}
	return intent, nil
}

func (i publicationIntent) files() []ownedSegment {
	if i.Generation == nil {
		return i.Segments
	}
	return append(i.Segments, *i.Generation)
}

func (p *publication) removeOwned(files []ownedSegment, reachable map[string]bool) error {
	var abandoned []string
	for _, file := range files {
		if reachable[file.Name] {
			continue
		}
		owned, checkErr := p.matchesOwnedSegment(file)
		if checkErr != nil {
			return checkErr
		}
		if owned {
			abandoned = append(abandoned, file.Name)
		}
	}
	for _, name := range abandoned {
		if err := p.cleanupFault(removeFinal); err != nil {
			return err
		}
		if err := p.root.Remove(name); err != nil {
			return err
		}
	}
	if len(files) > 0 {
		return p.dir.Sync()
	}
	return nil
}

// Match the recorded inode and content in the pinned directory. A crash
// between link and staging unlink can leave two links to this inode.
func (p *publication) matchesOwnedSegment(file ownedSegment) (_ bool, err error) {
	info, err := p.root.Lstat(file.Name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	st := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || st.Ino != file.Inode || st.Uid != uint32(os.Geteuid()) ||
		info.Size() != file.Size || info.Mode().Perm()&0022 != 0 {
		return false, nil
	}
	if st.Nlink != 1 {
		// A two-link inode is attempt-owned only while the other link
		// is its original staged entry.
		if st.Nlink != 2 {
			return false, fmt.Errorf("%w: unexpected links to installed segment %q", ErrIntegrity, file.Name)
		}
		staged, err := p.root.Lstat(stageDir + "/" + file.Name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		if staged == nil || !os.SameFile(info, staged) {
			return false, fmt.Errorf("%w: unowned link to installed segment %q", ErrIntegrity, file.Name)
		}
	}
	fd, err := openat(int(p.dir.Fd()), file.Name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return false, err
	}
	f := os.NewFile(uintptr(fd), file.Name)
	defer func() { err = errors.Join(err, f.Close()) }()
	opened, err := f.Stat()
	if err != nil {
		return false, err
	}
	if !os.SameFile(opened, info) {
		return false, nil
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, file.Size+1))
	return n == file.Size && hex.EncodeToString(h.Sum(nil)) == file.SHA256, err
}

func validDigest(sum string) bool {
	if len(sum) != sha256.Size*2 {
		return false
	}
	var b [sha256.Size]byte
	_, err := hex.Decode(b[:], []byte(sum))
	return err == nil
}
