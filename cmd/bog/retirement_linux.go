package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/wal"
)

const switchPrefix = ".bog-tail-switch-"

type fileRef struct {
	Name          string
	Device, Inode uint64
}

type tailSwitch struct {
	Candidate          fileRef
	PreviousTail       fileRef
	PreviousMarker     fileRef
	PreviousManifest   fileRef
	PreviousGeneration string
}

func refFor(name string, info os.FileInfo) fileRef {
	st := info.Sys().(*syscall.Stat_t)
	return fileRef{name, uint64(st.Dev), st.Ino}
}

func (s *retained) refFor(name string) (_ fileRef, err error) {
	f, err := s.dir.OpenRegular(name, os.O_RDONLY)
	if err != nil {
		return fileRef{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return fileRef{}, err
	}
	return refFor(name, info), nil
}

func tailToken(name string) (string, bool) {
	id, ok := strings.CutPrefix(name, tailPrefix)
	return id, ok && len(id) == 32 && strings.Trim(id, "0123456789abcdef") == ""
}

func (r tailSwitch) recordName() string {
	return switchPrefix + strings.TrimPrefix(r.Candidate.Name, tailPrefix)
}

func (s *retained) prepareSwitch(f *os.File, a authority) (tailSwitch, error) {
	r := tailSwitch{Candidate: fileRef{a.Tail, a.TailDevice, a.TailInode}}
	if e := s.active; e != nil {
		r.PreviousTail = refFor(e.name, e.info)
		name := s.manifestName()
		_, _, id, err := s.readManifest(name)
		if err != nil {
			return r, err
		}
		r.PreviousGeneration = hexDigest(id)
		r.PreviousManifest, err = s.refFor(name)
		if err != nil {
			return r, err
		}
		if e.name == wal.LegacyName {
			r.PreviousMarker, err = s.refFor(wal.LegacyMarkerName)
			if err != nil {
				return r, err
			}
		}
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	b, err := json.Marshal(authorityEnvelope{Payload: payload, SHA256: hexDigest(sha256.Sum256(payload))})
	if err != nil {
		return r, err
	}
	// Record the actual inode before installing its final name. If the
	// process dies earlier, only a recognizable WAL stage can remain.
	if _, err := s.dir.Select(r.recordName(), b); err != nil {
		return r, err
	}
	if err := s.dir.InstallStage(f.Name(), a.Tail); err != nil {
		return r, err
	}
	return r, s.dir.File.Sync()
}

func (s *retained) discardSwitch(f *os.File, a authority, cause error) error {
	err := errors.Join(cause, f.Close())
	if errors.Is(cause, wal.ErrUncertain) {
		return err // recovery follows the actual durable selector
	}
	ref := fileRef{a.Tail, a.TailDevice, a.TailInode}
	staged := ref
	staged.Name = f.Name()
	cleanupErr := errors.Join(s.removeRef(ref), s.removeRef(staged))
	if cleanupErr == nil {
		cleanupErr = s.remove((tailSwitch{Candidate: ref}).recordName())
	}
	if cleanupErr != nil {
		return errors.Join(err, wal.ErrUncertain, cleanupErr)
	}
	return err
}

func (s *retained) removeRef(ref fileRef) error {
	if ref.Name == "" {
		return nil
	}
	info, err := s.dir.Root.Lstat(ref.Name)
	if errors.Is(err, os.ErrNotExist) {
		return s.dir.File.Sync()
	}
	if err != nil {
		return err
	}
	if !fileMatches(info, ref.Device, ref.Inode) {
		return fmt.Errorf("refusing to retire substituted %s", ref.Name)
	}
	return s.remove(ref.Name)
}

func decodeSwitch(raw []byte, name string) (tailSwitch, error) {
	var env authorityEnvelope
	var r tailSwitch
	bad := fmt.Errorf("invalid tail switch record %s", name)
	if !jsontext.Value(raw).IsValid() || json.Unmarshal(raw, &env) != nil ||
		env.SHA256 != hexDigest(sha256.Sum256(env.Payload)) || json.Unmarshal(env.Payload, &r) != nil {
		return r, bad
	}
	if _, ok := tailToken(r.Candidate.Name); !ok || r.recordName() != name || r.Candidate.Inode == 0 {
		return r, bad
	}
	if r.PreviousTail == (fileRef{}) {
		if r.PreviousGeneration != "" || r.PreviousManifest != (fileRef{}) || r.PreviousMarker != (fileRef{}) {
			return r, bad
		}
		return r, nil
	}
	id, err := digestField(r.PreviousGeneration)
	if err != nil || r.PreviousTail.Inode == 0 || r.PreviousManifest.Inode == 0 {
		return r, bad
	}
	if r.PreviousTail.Name == wal.LegacyName {
		if r.PreviousManifest.Name != canonicalManifest || r.PreviousMarker.Name != wal.LegacyMarkerName || r.PreviousMarker.Inode == 0 {
			return r, bad
		}
	} else if _, ok := tailToken(r.PreviousTail.Name); !ok || r.PreviousTail.Name == r.Candidate.Name ||
		r.PreviousManifest.Name != checkpoint.GenerationManifestName(id) || r.PreviousMarker != (fileRef{}) {
		return r, bad
	}
	return r, nil
}

func (s *retained) recoverSwitches(ctx context.Context) (err error) {
	dir, err := s.dir.Root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	for {
		entries, err := dir.ReadDir(64)
		for _, entry := range entries {
			name := entry.Name()
			id, ok := strings.CutPrefix(name, switchPrefix)
			if _, valid := tailToken(tailPrefix + id); !ok || !valid {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			raw, err := s.readFile(name, 4<<10)
			if err != nil {
				return err
			}
			r, err := decodeSwitch(raw, name)
			if err != nil {
				return err
			}
			if s.selector.Version == 0 || s.selector.Tail == r.PreviousTail.Name {
				if err := s.removeRef(r.Candidate); err != nil {
					return err
				}
				if err := s.remove(r.recordName()); err != nil {
					return err
				}
				continue
			}
			_, complete, err := s.finishRetirement(r)
			if err != nil {
				return err
			}
			if !complete {
				s.pendingRetirement = append(s.pendingRetirement, r)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *retained) finishRetirement(r tailSwitch) (uint64, bool, error) {
	var removed uint64
	if r.PreviousManifest.Name != "" {
		_, m, id, err := s.readManifest(r.PreviousManifest.Name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, false, err
		}
		if m != nil {
			if hexDigest(id) != r.PreviousGeneration {
				return 0, false, errors.New("retired manifest digest changed")
			}
			var complete bool
			removed, complete, err = history.ReclaimObsolete(context.Background(), s.dir.File, history.GenerationID(s.point.Generation), m)
			if err != nil || !complete {
				return removed, complete, err
			}
		}
	}
	for _, ref := range []fileRef{r.PreviousTail, r.PreviousMarker, r.PreviousManifest} {
		if err := s.removeRef(ref); err != nil {
			return removed, false, err
		}
	}
	return removed, true, s.remove(r.recordName())
}
