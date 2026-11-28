package wal

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"syscall"
)

// Owner provides the pinned filesystem and WAL operations for the retained
// runtime. Its caller serializes commits, recovery and authority selection.
type Owner struct{ log *Log }

func OpenOwner(path string) (_ *Owner, err error) {
	l, err := lockDirectory(path)
	if err != nil {
		return nil, err
	}
	parent, err := openParent(l.dir)
	if err == nil {
		err = errors.Join(parent.Sync(), parent.Close())
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("sync data directory parent: %w", err), l.dir.Close(), l.root.Close())
	}
	return &Owner{l}, nil
}

func (o *Owner) Directory() (*os.Root, *os.File) { return o.log.root, o.log.dir }
func (o *Owner) Close() error {
	return errors.Join(o.log.root.Close(), syscall.Flock(int(o.log.dir.Fd()), syscall.LOCK_UN), o.log.dir.Close())
}

func (o *Owner) OpenRegular(name string) (*os.File, error) {
	return openRegular(o.log.dir, name, os.O_RDONLY)
}

func (o *Owner) OpenTail(name string) (*os.File, error) {
	f, err := openRegular(o.log.dir, name, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(fmt.Errorf("lock WAL: %w", err), f.Close())
	}
	return f, nil
}

func (o *Owner) OpenLegacy() (_ *os.File, _ int64, _ bool, err error) {
	f, err := o.log.openWAL()
	if err != nil {
		return nil, 0, false, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, 0, false, fmt.Errorf("lock WAL: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, 0, false, err
	}
	end, marker, err := o.log.committedEnd(info.Size())
	return f, end, marker, err
}

func (o *Owner) FinishRecovery(f *os.File, physicalEnd int64) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < physicalEnd {
		return io.ErrUnexpectedEOF
	}
	if info.Size() > physicalEnd {
		if err := f.Truncate(physicalEnd); err != nil {
			return fmt.Errorf("discard uncommitted WAL suffix: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := o.log.cleanOrphans(); err != nil {
		return err
	}
	return o.log.syncDir()
}

// NewTail copies exactly size committed bytes and makes the new inode and
// directory entry durable. It remains private until selected.
func (o *Owner) NewTail(ctx context.Context, reader io.Reader, size int64) (_ *os.File, err error) {
	var id [stageIDSize]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	name := "bog.wal.tail-" + hex.EncodeToString(id[:])
	f, err := openRegular(o.log.dir, name, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			cleanup := errors.Join(f.Close(), o.Remove(name))
			if cleanup != nil {
				err = errors.Join(ErrUncertain, err, cleanup)
			}
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	if size != 0 {
		if _, err := io.CopyN(f, cancelReader{ctx, reader}, size); err != nil {
			return nil, err
		}
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	return f, o.log.syncDir()
}

type cancelReader struct {
	ctx context.Context
	io.Reader
}

func (r cancelReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func (o *Owner) Remove(name string) error {
	if err := o.log.removeStage(name); err != nil {
		return err
	}
	return o.log.syncDir()
}

func (o *Owner) CommitLegacy(end int64) (string, error) { return o.log.commit(end) }

func (o *Owner) Select(name string, data []byte) (string, error) {
	staged, err := o.log.writeStaged(markerStagePrefix, data)
	if err != nil {
		return "marker_preparation", err
	}
	if err := o.log.root.Rename(staged, name); err != nil {
		return "marker_rename", errors.Join(ErrUncertain, err)
	}
	if err := o.log.syncDir(); err != nil {
		return "marker_sync", errors.Join(ErrUncertain, err)
	}
	return "", nil
}

// CommitFrame publishes a whole frame. selectEnd runs only after file sync;
// it must distinguish a definitive refusal from an uncertain selection.
func (o *Owner) CommitFrame(f *os.File, offset int64, payload []byte, selectEnd func() (string, error)) error {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return o.observe("wal_seek", err)
	}
	h := FrameHeader(payload)
	op := "wal_write"
	err := writeAll(f, h[:])
	if err == nil {
		err = writeAll(f, payload)
	}
	if err == nil {
		op, err = "wal_sync", f.Sync()
	}
	if err == nil {
		op, err = selectEnd()
	}
	if err != nil && !errors.Is(err, ErrUncertain) {
		rollback := f.Truncate(offset)
		if rollback == nil {
			rollback = f.Sync()
		}
		if rollback != nil {
			err = errors.Join(ErrUncertain, err, fmt.Errorf("rollback WAL: %w", rollback))
		}
	}
	return o.observe(op, err)
}

func (o *Owner) LastCommitOutcome() CommitOutcome { return o.log.LastCommitOutcome() }

func (o *Owner) observe(op string, err error) error {
	outcome := newCommitOutcome(op, err)
	o.log.lastOutcome.Store(&outcome)
	if outcome.State == CommitRolledBack {
		o.log.diagnoseRollback(outcome)
	}
	return err
}

func FrameHeader(payload []byte) (h [16]byte) {
	n := uint32(len(payload))
	binary.LittleEndian.PutUint32(h[:], n)
	binary.LittleEndian.PutUint32(h[4:], ^n)
	binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(payload, table))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], table))
	return h
}
