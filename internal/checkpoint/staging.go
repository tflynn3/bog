package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"syscall"
)

type stagingStep string

const (
	writeSuccessor  stagingStep = "write staged successor"
	syncSuccessor   stagingStep = "sync staged successor"
	closeSuccessor  stagingStep = "close staged successor"
	syncOverlap     stagingStep = "sync staged overlap"
	unlinkPrior     stagingStep = "unlink staged predecessor"
	syncRetirement  stagingStep = "sync staged retirement"
	linkFinal       stagingStep = "link final segment"
	unlinkStageLink stagingStep = "unlink installed staging link"
)

// Fault injection is scoped to a single attempt. Production contexts carry
// no callback. In particular nothing can leak between publishers or tests.
type stagingFaultKey struct{}

func stagingFault(ctx context.Context, step stagingStep) error {
	if inject, ok := ctx.Value(stagingFaultKey{}).(func(stagingStep) error); ok {
		if err := inject(step); err != nil {
			return fmt.Errorf("%s: %w", step, err)
		}
	}
	return nil
}

func (a *publicationAttempt) writeSegment(name string, data []byte, sum string, chargeIndex int, replacing bool) (_ stagedBlock, err error) {
	fault := func(step stagingStep) error {
		if replacing {
			return stagingFault(a.ctx, step)
		}
		return nil
	}
	f, err := a.pub.root.OpenFile(stageDir+"/"+name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return stagedBlock{}, err
	}
	defer func() {
		err = errors.Join(err, f.Close(), fault(closeSuccessor))
	}()

	// Use the created descriptor for both the write and the intent identity.
	if err := f.Chmod(0600); err != nil {
		return stagedBlock{}, err
	}
	if err := fault(writeSuccessor); err != nil {
		return stagedBlock{}, err
	}
	n, err := f.Write(data)
	if err != nil {
		return stagedBlock{}, err
	}
	if n != len(data) {
		return stagedBlock{}, io.ErrShortWrite
	}
	if err := fault(syncSuccessor); err != nil {
		return stagedBlock{}, err
	}
	if err := f.Sync(); err != nil {
		return stagedBlock{}, err
	}
	info, err := f.Stat()
	if err != nil {
		return stagedBlock{}, err
	}
	st := info.Sys().(*syscall.Stat_t)
	file := ownedSegment{Name: name, Inode: st.Ino, Size: info.Size(), SHA256: sum}
	return stagedBlock{ownedSegment: file, chargeIndex: chargeIndex}, nil
}

func (a *publicationAttempt) retireStagedBlock(old stagedBlock) (err error) {
	p := a.pub
	stage, err := openStaging(p.dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, stage.Close()) }()

	if err := stagingFault(a.ctx, syncOverlap); err != nil {
		return err
	}
	if err := stage.Sync(); err != nil {
		return fmt.Errorf("%s: %w", syncOverlap, err)
	}
	// Cancellation here leaves two private names and both reservations.
	if err := a.ctx.Err(); err != nil {
		return err
	}
	if err := stagingFault(a.ctx, unlinkPrior); err != nil {
		return err
	}
	if err := p.root.Remove(stageDir + "/" + old.Name); err != nil {
		return fmt.Errorf("%s: %w", unlinkPrior, err)
	}
	if err := stagingFault(a.ctx, syncRetirement); err != nil {
		return err
	}
	if err := stage.Sync(); err != nil {
		return fmt.Errorf("%s: %w", syncRetirement, err)
	}
	// The slot and accounting still describe the predecessor at this point.
	return a.ctx.Err()
}

func ownedByService(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uint32(os.Geteuid())
}

func checkStaging(info os.FileInfo) error {
	if !info.IsDir() || !ownedByService(info) ||
		info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%w: unsafe publication staging directory", ErrIntegrity)
	}
	return nil
}

func openStaging(parent *os.File) (*os.File, error) {
	fd, err := openat(int(parent.Fd()), stageDir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(fd), stageDir)
	info, err := dir.Stat()
	if err == nil {
		err = checkStaging(info)
	}
	if err != nil {
		return nil, errors.Join(err, dir.Close())
	}
	return dir, nil
}

func (p *publication) syncStaging() error {
	dir, err := openStaging(p.dir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (p *publication) removeEmptyStaging() error {
	// This name was checked with Lstat under the exclusive publication lock
	// in the service-owned parent. Directory removal tests emptiness atomically
	// without needing read or search permission on the directory itself.
	if err := p.root.Remove(stageDir); err != nil {
		return fmt.Errorf("%w: remove empty publication staging directory: %w", ErrIntegrity, err)
	}
	return p.dir.Sync()
}

func (p *publication) stagingNames(dir *os.File, intent publicationIntent) ([]string, error) {
	// There can be one extra segment before the intent, or two metadata
	// files (manifest and temporary or durable intent) after replacement.
	const maxEntries = MaxSegments + 2
	names, err := dir.Readdirnames(maxEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(names) > maxEntries {
		return nil, fmt.Errorf("%w: too many staging files", ErrIntegrity)
	}
	hasIntent := slices.Contains(names, intentBase)
	if hasIntent && slices.Contains(names, "intent") {
		return nil, fmt.Errorf("%w: conflicting staging intent names", ErrIntegrity)
	}
	installed := make(map[string]ownedSegment, len(intent.Segments))
	for _, file := range intent.Segments {
		installed[file.Name] = file
	}
	segments := 0
	for _, name := range names {
		segment := validSegmentName(name)
		if segment {
			segments++
		}
		if !segment && name != intentBase && name != "intent" && name != "manifest" {
			return nil, fmt.Errorf("%w: unexpected staging file %q", ErrIntegrity, name)
		}
		info, err := p.root.Lstat(stageDir + "/" + name)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || !ownedByService(info) || info.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("%w: unsafe staging file %q", ErrIntegrity, name)
		}
		st := info.Sys().(*syscall.Stat_t)
		file, recorded := installed[name]
		if hasIntent && segment && (!recorded || file.Inode != st.Ino || file.Size != info.Size()) {
			return nil, fmt.Errorf("%w: unrecorded staged segment %q", ErrIntegrity, name)
		}
		if st.Nlink == 1 {
			continue
		}
		// A second link belongs to installation only if the durable
		// intent identifies the inode and its final entry still exists.
		if st.Nlink != 2 || !recorded {
			return nil, fmt.Errorf("%w: unowned staging link %q", ErrIntegrity, name)
		}
		final, err := p.root.Lstat(name)
		if err != nil {
			return nil, fmt.Errorf("%w: inspect second link for %q: %v", ErrIntegrity, name, err)
		}
		if !os.SameFile(info, final) {
			return nil, fmt.Errorf("%w: unowned staging link %q", ErrIntegrity, name)
		}
	}
	if segments > MaxSegments && (segments != MaxSegments+1 || segments != len(names)) {
		return nil, fmt.Errorf("%w: staged segment limit exceeded outside replacement", ErrIntegrity)
	}
	return names, nil
}

func (p *publication) inspectStaging() (_ []string, _ publicationIntent, err error) {
	dir, err := openStaging(p.dir)
	if err != nil {
		return nil, publicationIntent{}, err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	intent, err := readIntent(dir)
	if err != nil {
		return nil, publicationIntent{}, err
	}
	names, err := p.stagingNames(dir, intent)
	return names, intent, err
}

func (p *publication) clearStaging() error {
	info, err := p.root.Lstat(stageDir)
	if err != nil {
		return err
	}
	if err := checkStaging(info); err != nil {
		return err
	}
	if info.Mode().Perm() != 0700 {
		return p.removeEmptyStaging()
	}
	names, _, err := p.inspectStaging()
	if err != nil {
		return err
	}
	return p.removeStaging(names)
}

func (p *publication) removeStaging(names []string) error {
	for _, name := range names {
		if name != intentBase {
			if err := p.root.Remove(stageDir + "/" + name); err != nil {
				return err
			}
		}
	}
	// Keep the intent recoverable until removal of its other entries is
	// durable. Recovery already synced any losing names in the parent.
	if err := p.syncStaging(); err != nil {
		return err
	}
	if err := p.root.Remove(intentName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return p.removeEmptyStaging()
}
