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
}

func (a *publicationAttempt) stageSegment(name string, b []byte, sum string) error {
	l := a.legacy
	path := stageDir + "/" + name
	if err := l.writeNew(path, b); err != nil {
		return err
	}
	info, err := l.root.Lstat(path)
	if err != nil {
		return err
	}
	st := info.Sys().(*syscall.Stat_t)
	a.files = append(a.files, ownedSegment{Name: name, Inode: st.Ino, Size: info.Size(), SHA256: sum})
	return nil
}

func (a *publicationAttempt) recordIntent() error {
	l := a.legacy
	previous, err := l.root.Lstat(manifestName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	intent := publicationIntent{Segments: a.files}
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
	if err := l.writeNew(stagedIntent, b); err != nil {
		return err
	}
	if err := l.root.Rename(stagedIntent, intentName); err != nil {
		return err
	}
	// Make the ownership record durable before the first final name can exist.
	if err := l.syncStaging(); err != nil {
		return err
	}
	return l.dir.Sync()
}

func (a *publicationAttempt) install() error {
	l := a.legacy
	if err := a.recordIntent(); err != nil {
		return err
	}
	a.installing = true
	for _, file := range a.files {
		path := stageDir + "/" + file.Name
		if err := l.root.Link(path, file.Name); err != nil {
			return err
		}
		if err := l.root.Remove(path); err != nil {
			return err
		}
	}
	// Persist the removal of staging links as well as the final segment names.
	if err := l.syncStaging(); err != nil {
		return err
	}
	syncDir := l.dir.Sync
	if l.segmentDirSync != nil {
		syncDir = l.segmentDirSync
	}
	return syncDir()
}

func (l *Legacy) syncStaging() error {
	dir, err := l.openStaging()
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (l *Legacy) openStaging() (*os.File, error) {
	fd, err := syscall.Openat(int(l.dir.Fd()), stageDir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(fd), stageDir)
	info, err := dir.Stat()
	if err == nil {
		st := info.Sys().(*syscall.Stat_t)
		if st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
			err = fmt.Errorf("%w: unsafe publication staging directory", ErrIntegrity)
		}
	}
	if err != nil {
		return nil, errors.Join(err, dir.Close())
	}
	return dir, nil
}

// resolvePublication validates the authoritative namespace and makes it durable
// before deleting any losing outputs. Missing is a valid first-publication
// outcome. Other errors leave the attempt intact for diagnosis or retry.
func (l *Legacy) resolvePublication(keepRows bool) (*Published, error) {
	pending := CheckPublication(l.dir)
	if pending != nil && !errors.Is(pending, ErrPublicationUncertain) {
		return nil, pending
	}
	p, err := l.readPublished(keepRows)
	if err != nil && err != syscall.ENOENT {
		return nil, err
	}
	if err := l.dir.Sync(); err != nil {
		return nil, fmt.Errorf("sync checkpoint directory: %w", err)
	}
	if pending != nil {
		stage, err := l.openStaging()
		if err != nil {
			return nil, err
		}
		if err := stage.Close(); err != nil {
			return nil, err
		}
		intent, err := l.readIntent()
		if err != nil {
			return nil, err
		}
		if p == nil && intent.HadManifest {
			return nil, fmt.Errorf("%w: previous authoritative manifest is missing", ErrIntegrity)
		}
		reachable := make(map[string]bool)
		if p != nil {
			for _, s := range p.manifest.Segments {
				reachable[s.Name] = true
			}
		}
		if err := l.removeOwned(intent.Segments, reachable); err != nil {
			return nil, err
		}
		if err := l.clearStaging(); err != nil {
			return nil, err
		}
	}
	if p == nil {
		return nil, os.ErrNotExist
	}
	return p, nil
}

func (l *Legacy) readIntent() (_ publicationIntent, err error) {
	f, err := l.openFile(intentName)
	if err == syscall.ENOENT {
		// Final names can only be installed after this record is durable.
		return publicationIntent{}, nil
	}
	if err != nil {
		return publicationIntent{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, maxManifestSize+1))
	if err != nil {
		return publicationIntent{}, err
	}
	var env manifestEnvelope
	var intent publicationIntent
	if len(b) > maxManifestSize || json.Unmarshal(b, &env) != nil ||
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
	return intent, nil
}

func (l *Legacy) removeOwned(files []ownedSegment, reachable map[string]bool) error {
	var err error
	for _, file := range files {
		if reachable[file.Name] {
			continue
		}
		owned, checkErr := l.matchesOwnedSegment(file)
		if checkErr != nil {
			err = errors.Join(err, checkErr)
		} else if owned {
			err = errors.Join(err, l.root.Remove(file.Name))
		}
	}
	if len(files) > 0 {
		err = errors.Join(err, l.dir.Sync())
	}
	return err
}

// Match the recorded inode and content in the pinned directory. A crash
// between link and staging unlink can leave two links to this inode.
func (l *Legacy) matchesOwnedSegment(file ownedSegment) (_ bool, err error) {
	info, err := l.root.Lstat(file.Name)
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
	fd, err := syscall.Openat(int(l.dir.Fd()), file.Name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
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

func (l *Legacy) clearStaging() error {
	dir, err := l.openStaging()
	if err != nil {
		return err
	}
	names, err := dir.Readdirnames(MaxSegments + 4)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if err = errors.Join(err, dir.Close()); err != nil {
		return err
	}
	if len(names) > MaxSegments+3 {
		return fmt.Errorf("%w: too many staging files", ErrIntegrity)
	}
	for _, name := range names {
		switch {
		case name == intentBase, name == "intent", name == "manifest", validSegmentName(name):
		default:
			return fmt.Errorf("%w: unexpected staging file %q", ErrIntegrity, name)
		}
	}
	for _, name := range names {
		path := stageDir + "/" + name
		if path != intentName {
			if err := l.root.Remove(path); err != nil {
				return err
			}
		}
	}
	if err := l.root.Remove(intentName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := l.root.Remove(stageDir); err != nil {
		return err
	}
	return l.dir.Sync()
}
