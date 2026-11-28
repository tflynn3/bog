package wal

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"bog/internal/store"
)

const magic = "BOGWAL01"

var table = crc32.MakeTable(crc32.Castagnoli)
var ErrUncertain = errors.New("WAL rollback uncertain")

type file interface {
	io.Reader
	io.Writer
	io.Seeker
	Sync() error
	Truncate(int64) error
	Close() error
}
type Log struct {
	ioMu      sync.Mutex // group disk commit and retained replacement
	authority Authority  // writes under ioMu and mu
	suffix    hash.Cloner
	frames    []Frame // immutable through each capture
	bounds    frameBounds

	mu        sync.Mutex
	cond      *sync.Cond
	queue     []*appendRequest
	running   bool
	f         file
	s         *store.Store
	offset    int64
	committed captureBoundary // protected by mu after recovery
	captures  *captureFile
	poisoned  error
	closed    bool
	closeDone bool
	closeErr  error

	root            *os.Root
	dir             *os.File
	syncDir         func() error
	lastOutcome     atomic.Pointer[CommitOutcome]
	lastDiagnostic  time.Time
	diagnosticDrops atomic.Uint64
	diagnostics     *diagnosticDispatcher
}

const maxGroup = 16

type appendRequest struct {
	payload []byte
	rows    []store.Row
	done    chan error
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
func (l *Log) initCond() {
	if l.cond == nil {
		l.cond = sync.NewCond(&l.mu)
	}
}

// Append waits for admission and for its entire batch to become durable and visible.
// Callers must keep payload and rows unmodified until Append returns.
func (l *Log) Append(payload []byte, rows []store.Row) error {
	l.mu.Lock()
	err := l.admissionError()
	l.mu.Unlock()
	if err != nil {
		return err
	}
	decoded, err := l.matchBatch(payload, rows)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.initCond()
	for len(l.queue) >= maxGroup && l.admissionError() == nil {
		l.cond.Wait()
	}
	if err := l.admissionError(); err != nil {
		l.mu.Unlock()
		return err
	}
	req := &appendRequest{payload: payload, rows: decoded, done: make(chan error, 1)}
	l.queue = append(l.queue, req)
	if !l.running {
		l.running = true
		go l.run()
	}
	l.mu.Unlock()
	return <-req.done
}

// Use the same canonical rows for live publication and recovery.
func (l *Log) matchBatch(payload []byte, rows []store.Row) ([]store.Row, error) {
	i := 0
	var decoded []store.Row
	err := store.VisitDecoded(payload, func(row store.Row) error {
		if i >= len(rows) {
			return errors.New("WAL batch row count differs from payload")
		}
		if !row.TS.Equal(rows[i].TS) || row.Line != rows[i].Line {
			return fmt.Errorf("WAL batch row %d differs from payload", i)
		}
		i++
		if l.s != nil {
			decoded = append(decoded, row)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid WAL batch: %w", err)
	}
	if i != len(rows) {
		return nil, errors.New("WAL batch row count differs from payload")
	}
	if l.s == nil {
		return rows, nil
	}
	return decoded, nil
}

// admissionError is called under mu. Preserve the uncertainty cause for queued
// and future callers as well as for the group that encountered it.
func (l *Log) admissionError() error {
	if l.poisoned != nil {
		return l.poisoned
	}
	if l.closed {
		return os.ErrClosed
	}
	return nil
}

func (l *Log) run() {
	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.running = false
			l.cond.Broadcast()
			l.mu.Unlock()
			return
		}
		group := l.queue
		l.queue = nil
		l.cond.Broadcast()
		l.mu.Unlock()

		l.ioMu.Lock()
		operation, err := l.appendGroup(group)
		outcome := newCommitOutcome(operation, err)
		l.mu.Lock()
		if !errors.Is(err, ErrCapacity) {
			l.lastOutcome.Store(&outcome)
		}
		if outcome.State == CommitUncertain {
			l.poisoned = err
			for _, req := range l.queue {
				req.done <- err
			}
			l.queue = nil
			l.cond.Broadcast()
		}
		if outcome.State == CommitRolledBack && !errors.Is(err, ErrCapacity) {
			l.diagnoseRollback(outcome)
		}
		for _, req := range group {
			req.done <- err
		}
		l.mu.Unlock()
		l.ioMu.Unlock()
	}
}

func (l *Log) appendGroup(group []*appendRequest) (string, error) {
	l.mu.Lock()
	poisoned := l.poisoned
	l.mu.Unlock()
	if poisoned != nil {
		return "authority", poisoned
	}
	a := l.authority
	next, ordinal := l.offset+a.TailBase, l.committed.nextOrdinal
	var prepared [maxGroup]Frame
	for i, req := range group {
		length := int64(frameHeader + len(req.payload))
		if next > math.MaxInt64-length || uint64(len(req.rows)) > math.MaxUint64-ordinal {
			return "admission", ErrCapacity
		}
		f := Frame{Start: next, End: next + length, FirstOrdinal: ordinal, NextOrdinal: ordinal}
		for _, row := range req.rows {
			if err := f.add(row); err != nil {
				return "admission", err
			}
		}
		f.last = time.Time{}
		prepared[i] = f
		next, ordinal = f.End, f.NextOrdinal
	}
	if l.s == nil && (next-max(int64(len(magic)), a.CoveredOffset) > MaxTailBytes ||
		len(l.frames)+len(group) > MaxLiveFrames) {
		return "admission", ErrCapacity
	}
	var digest hash.Cloner
	if l.suffix != nil {
		var err error
		digest, err = l.suffix.Clone()
		if err != nil {
			return "authority_preparation", err
		}
	}
	for _, req := range group {
		var h [frameHeader]byte
		n := uint32(len(req.payload))
		binary.LittleEndian.PutUint32(h[:], n)
		binary.LittleEndian.PutUint32(h[4:], ^n)
		binary.LittleEndian.PutUint32(h[8:], crc32.Checksum(req.payload, table))
		binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], table))
		if err := writeAll(l.f, h[:]); err != nil {
			return "wal_write", l.rollback(err)
		}
		if err := writeAll(l.f, req.payload); err != nil {
			return "wal_write", l.rollback(err)
		}
		if digest != nil {
			if err := errors.Join(writeAll(digest, h[:]), writeAll(digest, req.payload)); err != nil {
				return "authority_preparation", l.rollback(err)
			}
		}
	}
	if err := l.f.Sync(); err != nil {
		return "wal_sync", l.rollback(err)
	}
	if l.root != nil {
		var operation string
		var err error
		if digest != nil {
			a.CommittedEnd, a.CommittedOrdinal = next, ordinal
			a.SuffixSHA256 = hex.EncodeToString(digest.Sum(nil))
			operation, err = l.selectAuthority(a)
		} else {
			operation, err = l.commit(next)
		}
		if err != nil {
			if !errors.Is(err, ErrUncertain) {
				err = l.rollback(err)
			}
			return operation, err
		}
	}
	if l.s != nil {
		for _, req := range group {
			l.s.Publish(req.rows)
		}
	}
	l.mu.Lock()
	l.offset, l.suffix, l.authority = next-a.TailBase, digest, a
	l.committed = captureBoundary{next, ordinal}
	if l.s == nil {
		for _, frame := range prepared[:len(group)] {
			l.addFrame(frame)
		}
	}
	l.mu.Unlock()
	return "commit", nil
}

func (l *Log) rollback(cause error) error {
	if err := l.f.Truncate(l.offset); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("truncate WAL: %w", err))
	}
	if err := l.f.Sync(); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("sync truncated WAL: %w", err))
	}
	if _, err := l.f.Seek(l.offset, io.SeekStart); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("seek truncated WAL: %w", err))
	}
	return cause
}

func (l *Log) Close() error {
	l.mu.Lock()
	l.initCond()
	if l.closed {
		for !l.closeDone {
			l.cond.Wait()
		}
		err := l.closeErr
		l.mu.Unlock()
		return err
	}
	l.closed = true
	l.cond.Broadcast()
	for l.running {
		l.cond.Wait()
	}
	l.mu.Unlock()

	l.ioMu.Lock()
	err := l.closeFiles()
	if l.dir != nil {
		err = errors.Join(err, syscall.Flock(int(l.dir.Fd()), syscall.LOCK_UN), l.dir.Close())
	}
	if l.root != nil {
		err = errors.Join(err, l.root.Close())
	}
	l.ioMu.Unlock()
	l.mu.Lock()
	l.closeErr = err
	l.closeDone = true
	l.cond.Broadcast()
	l.mu.Unlock()
	return err
}

// closeFiles releases the WAL descriptors before the directory owner lock.
func (l *Log) closeFiles() error {
	var err error
	if l.f != nil {
		if f, ok := l.f.(*os.File); ok {
			err = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		}
		err = errors.Join(err, l.f.Close())
	}
	if l.captures != nil {
		err = errors.Join(err, l.captures.release())
	}
	return err
}
