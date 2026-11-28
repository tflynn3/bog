package wal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const switchPrefix = ".bog-wal-switch-"

// TailSwitch records both inode identities before a replacement tail name
// becomes visible. Recovery can reclaim the losing side after the selector
// and checkpoint publication intent have been resolved.
type TailSwitch struct {
	Before, After Authority
}

func (t TailSwitch) name() string {
	return switchPrefix + hex.EncodeToString(t.After.Epoch[:])
}

func (l *Log) writeTailSwitch(t TailSwitch) error {
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	b, err := json.Marshal(authorityEnvelope{data, hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	staged, err := l.writeStaged(markerStagePrefix, b)
	if err != nil {
		return err
	}
	if err := renameNoReplace(l.dir, staged, t.name()); err != nil {
		return errors.Join(err, l.removeStage(staged))
	}
	return l.syncDir()
}

// FinishTailSwitch clears the durable intent after losing artifacts have
// been reclaimed and synced. The same cleanup can safely run again.
func (l *Log) FinishTailSwitch(t TailSwitch) error {
	if err := l.removeStage(t.name()); err != nil {
		return err
	}
	return l.syncDir()
}

// TailSwitches reads bounded, checksummed ownership records at startup.
func (l *Log) TailSwitches(ctx context.Context) (_ []TailSwitch, err error) {
	dir, err := l.OpenDirectory()
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	var pending []TailSwitch
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		names, readErr := dir.Readdirnames(64)
		for _, name := range names {
			token, ok := strings.CutPrefix(name, switchPrefix)
			var epoch [16]byte
			if !ok || !decodeHex(epoch[:], token) {
				continue
			}
			if len(pending) >= 64 {
				return nil, errors.New("too many interrupted WAL tail switches")
			}
			t, err := l.readTailSwitch(name, epoch)
			if err != nil {
				return nil, fmt.Errorf("read WAL tail switch %s: %w", name, err)
			}
			pending = append(pending, t)
		}
		if errors.Is(readErr, io.EOF) {
			return pending, nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}

func (l *Log) readTailSwitch(name string, epoch [16]byte) (_ TailSwitch, err error) {
	f, err := l.OpenDataFile(name)
	if err != nil {
		return TailSwitch{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, authoritySize+1))
	if err != nil {
		return TailSwitch{}, err
	}
	var envelope authorityEnvelope
	if len(b) > authoritySize || json.Unmarshal(b, &envelope) != nil {
		return TailSwitch{}, errors.New("invalid WAL tail switch envelope")
	}
	sum := sha256.Sum256(envelope.Payload)
	var t TailSwitch
	if envelope.SHA256 != hex.EncodeToString(sum[:]) || json.Unmarshal(envelope.Payload, &t) != nil ||
		!t.After.valid() || epoch == [16]byte{} || t.After.Epoch != epoch ||
		t.Before.Epoch == epoch || t.Before.Revision+1 != t.After.Revision ||
		t.Before.Revision != 0 && !t.Before.valid() ||
		t.Before.Revision == 0 && (t.Before.Epoch != [16]byte{} || t.Before.Generation != [32]byte{}) {
		return TailSwitch{}, errors.New("invalid WAL tail switch identity")
	}
	return t, nil
}
