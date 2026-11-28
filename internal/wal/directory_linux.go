package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"syscall"
)

const (
	LegacyName       = "bog.wal"
	LegacyMarkerName = "bog.commit"
	Magic            = magic
	FrameHeaderSize  = 16
)

// Directory owns the exclusive WAL directory lease. Root and File are
// borrowed handles. Close only after all operations and pins have drained.
type Directory struct {
	Root *os.Root
	File *os.File
	log  Log // shared legacy file protocol and outcome reporting
}

func LockDirectory(path string) (_ *Directory, err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, root.Close())
		}
	}()
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, dir.Close())
		}
	}()
	info, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		return nil, errors.New("data directory must be owned by the service user and not group- or world-writable")
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock data directory: %w", err)
	}
	d := &Directory{Root: root, File: dir}
	d.log.root, d.log.dir, d.log.syncDir = root, dir, dir.Sync
	return d, nil
}

func syncParent(dir *os.File) error {
	parent, err := openParent(dir)
	if err != nil {
		return err
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return fmt.Errorf("sync data directory parent: %w", err)
	}
	return nil
}

func (d *Directory) SyncParent() error { return syncParent(d.File) }
func (d *Directory) Close() error      { return errors.Join(d.Root.Close(), d.File.Close()) }

func (d *Directory) OpenRegular(name string, flags int) (*os.File, error) {
	return openRegular(d.File, name, flags)
}

// OpenLegacy returns the bounded committed byte view. The caller validates every
// committed frame before calling FinishLegacy or discarding any data.
func (d *Directory) OpenLegacy() (_ *os.File, _ int64, _ bool, err error) {
	f, err := d.log.openWAL()
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
	end, marked, err := d.log.committedEnd(info.Size())
	if err != nil {
		return nil, 0, false, err
	}
	var header [len(magic)]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		return nil, 0, false, fmt.Errorf("read WAL magic: %w", err)
	}
	if string(header[:]) != magic {
		return nil, 0, false, errors.New("invalid WAL magic")
	}
	return f, end, marked, nil
}

func (d *Directory) FinishLegacy(f *os.File, end int64, marked bool) error {
	if err := d.CleanStages(); err != nil {
		return err
	}
	if err := FinishRecovery(f, end); err != nil {
		return err
	}
	if !marked {
		if err := d.File.Sync(); err != nil {
			return err
		}
		_, err := d.CommitLegacy(end)
		return err
	}
	return nil
}

func (d *Directory) CleanStages() error { return d.log.cleanOrphans() }

func (d *Directory) CommitLegacy(end int64) (string, error) { return d.log.commit(end) }

// Select durably replaces one selector. A failure preparing the temporary
// record leaves the old selector authoritative.
func (d *Directory) Select(name string, data []byte) (string, error) {
	return d.log.selectRecord(name, data)
}

// FinishRecovery removes bytes beyond a previously validated committed end.
func FinishRecovery(f *os.File, end int64) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < end {
		return io.ErrUnexpectedEOF
	}
	if info.Size() != end {
		if err := f.Truncate(end); err != nil {
			return err
		}
	}
	return f.Sync()
}

// WriteFrame appends a complete encoded frame at the writer's position and
// extends the caller's private digest when supplied.
func WriteFrame(f io.Writer, digest hash.Hash, payload []byte) (string, error) {
	var h [FrameHeaderSize]byte
	n := uint32(len(payload))
	binary.LittleEndian.PutUint32(h[:], n)
	binary.LittleEndian.PutUint32(h[4:], ^n)
	binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(payload, table))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], table))
	if err := writeAll(f, h[:]); err != nil {
		return "wal_write", err
	}
	if err := writeAll(f, payload); err != nil {
		return "wal_write", err
	}
	if digest != nil {
		if err := errors.Join(writeAll(digest, h[:]), writeAll(digest, payload)); err != nil {
			return "wal_digest", err
		}
	}
	return "", nil
}

// RollbackFrame restores the file and its position to the prior commit.
func RollbackFrame(f interface {
	Truncate(int64) error
	Sync() error
	io.Seeker
}, end int64, cause error) error {
	if err := f.Truncate(end); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("truncate WAL: %w", err))
	}
	if err := f.Sync(); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("sync truncated WAL: %w", err))
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("seek truncated WAL: %w", err))
	}
	return cause
}

// RecordCommit publishes readiness and diagnostic evidence in writer order.
func (d *Directory) RecordCommit(operation string, err error) {
	outcome := newCommitOutcome(operation, err)
	d.log.lastOutcome.Store(&outcome)
	if outcome.State == CommitRolledBack {
		d.log.diagnoseRollback(outcome)
	}
}

func (d *Directory) LastCommitOutcome() CommitOutcome { return d.log.LastCommitOutcome() }

// OpenProbe opens an unlocked directory descriptor relative to the lease.
func (d *Directory) OpenProbe() (_ *os.File, err error) {
	probe, err := d.Root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, probe.Close())
		}
	}()
	a, err := d.File.Stat()
	if err != nil {
		return nil, err
	}
	b, err := probe.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(a, b) {
		return nil, errors.New("readiness directory differs from the locked WAL directory")
	}
	return probe, nil
}

// InstallStage installs an exclusively named file prepared by the owner.
func (d *Directory) InstallStage(stage, target string) error {
	return renameNoReplace(d.File, stage, target)
}
