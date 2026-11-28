package history

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"bog/internal/checkpoint"
)

// RecoverRetired reclaims generation files left by an interrupted owner
// after publication recovery has validated and synced the selected authority.
// A retired manifest identifies the files that may be reclaimed. Pin records
// and the selected manifest protect files still in use.
func RecoverRetired(ctx context.Context, dir *os.File, selected string, current *checkpoint.Manifest) (err error) {
	stream, _, err := openPinnedDirectory(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, stream.Close()) }()
	lock, err := checkpoint.LockPublication(dir, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := checkpoint.CheckPublication(dir); err != nil {
		return err
	}
	reachable := make(map[string]bool, len(current.Segments))
	for _, s := range current.Segments {
		reachable[s.Name] = true
	}
	for {
		names, readErr := stream.Readdirnames(64)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			if name == selected || name != manifestName && !isGenerationManifest(name) {
				continue
			}
			if err := reclaimRetired(ctx, dir, name, reachable); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return discardStalePins(ctx, dir, nil, nil)
}

func isGenerationManifest(name string) bool {
	sum, ok := strings.CutPrefix(name, "bog-checkpoint-")
	if !ok {
		return false
	}
	_, valid := parseDigest(sum)
	return valid
}

func reclaimRetired(ctx context.Context, dir *os.File, name string, reachable map[string]bool) error {
	m, _, err := readManifest(dir, name)
	if err != nil {
		return err
	}
	candidates := make(map[string]os.FileInfo)
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if reachable[s.Name] {
			continue
		}
		info, err := verifyRetiredSegment(dir, s)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		candidates[s.Name] = info
	}
	protected := make(map[string]bool)
	if err := discardStalePins(ctx, dir, candidates, protected); err != nil {
		return err
	}
	pending := false
	for name, info := range candidates {
		if protected[name] {
			pending = true
			continue
		}
		complete, err := reclaimSegment(dir, name, info)
		if err != nil {
			return err
		}
		pending = pending || !complete
	}
	if err := dir.Sync(); err != nil || pending {
		return err
	}
	return errors.Join(unlinkat(int(dir.Fd()), name), dir.Sync())
}

func verifyRetiredSegment(dir *os.File, s checkpoint.Segment) (_ os.FileInfo, err error) {
	f, info, err := openRegular(dir, s.Name)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, s.Size+1))
	if err != nil {
		return nil, err
	}
	if n != s.Size || info.Size() != s.Size || fmt.Sprintf("%x", h.Sum(nil)) != s.SHA256 {
		return nil, corrupt(s.Name, "retired segment does not match manifest")
	}
	return info, nil
}

func discardStalePins(ctx context.Context, dir *os.File, candidates map[string]os.FileInfo, protected map[string]bool) error {
	stream, _, err := openPinnedDirectory(dir)
	if err != nil {
		return err
	}
	return errors.Join(protectPinned(ctx, stream, candidates, protected), stream.Close())
}
