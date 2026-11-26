package checkpoint

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"bog/internal/store"
)

func randomName(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
func (l *Legacy) writeNew(name string, data []byte) (err error) {
	f, err := l.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
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
	legacy     *Legacy
	files      []ownedSegment
	installing bool
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
	if err := l.root.Mkdir(stageDir, 0700); err != nil {
		return nil, err
	}
	a := publicationAttempt{legacy: l}
	defer func() {
		if l.uncertain != nil {
			return
		}
		if published == nil && a.installing {
			// Compare ownership even when a link or its staging unlink failed.
			// Leave the intent available if this cleanup fails.
			if cleanupErr := l.removeOwned(a.files, nil); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
				return
			}
		}
		err = errors.Join(err, l.clearStaging())
	}()
	if err := l.root.Chmod(stageDir, 0700); err != nil {
		return nil, err
	}
	m := &Manifest{Version: 1, CoveredOffset: l.end, Segments: []segmentMeta{}}
	if m.SourceSHA256, err = l.prefixDigest(l.end); err != nil {
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
	err = l.scanFrom(8, 0, func(_, _ int64, first uint64, rows []store.Row) error {
		for i, row := range rows {
			n := blockRowSize + len(row.Line)
			if n > MaxBlockSize-blockHeaderSize {
				return errors.New("row exceeds block limit")
			}
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
	if err == nil {
		err = flush()
	}
	if err != nil {
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
	if err := l.writeNew(stagedManifest, b); err != nil {
		return nil, err
	}
	if err := a.install(); err != nil {
		return nil, err
	}
	rename := l.root.Rename
	if l.manifestRename != nil {
		rename = l.manifestRename
	}
	if err = rename(stagedManifest, manifestName); err != nil {
		l.uncertain = fmt.Errorf("%w: rename manifest: %w", ErrPublicationUncertain, err)
		return nil, l.uncertain
	}
	if err = l.syncStaging(); err != nil {
		l.uncertain = fmt.Errorf("%w: sync manifest staging directory: %w", ErrPublicationUncertain, err)
		return nil, l.uncertain
	}
	syncDir := l.dir.Sync
	if l.manifestDirSync != nil {
		syncDir = l.manifestDirSync
	}
	if err = syncDir(); err != nil {
		l.uncertain = fmt.Errorf("%w: sync manifest directory: %w", ErrPublicationUncertain, err)
		return nil, l.uncertain
	}
	return m, nil
}
