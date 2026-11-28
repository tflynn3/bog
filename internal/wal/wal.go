package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
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
	mu        sync.Mutex
	writeMu   sync.Mutex
	runtime   *runtimeState
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
	charge  uint64
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
	return l.append(nil, payload, rows)
}

// AppendWaiting allows a bounded wait for capacity reclaimed by maintenance.
// The context limits admission. A submitted group always reports its outcome.
func (l *Log) AppendWaiting(ctx context.Context, payload []byte, rows []store.Row) error {
	return l.append(ctx, payload, rows)
}

func (l *Log) append(ctx context.Context, payload []byte, rows []store.Row) error {
	l.mu.Lock()
	err := l.admissionError()
	l.mu.Unlock()
	if err != nil {
		return err
	}
	decoded, err := matchBatch(payload, rows)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.initCond()
	req := &appendRequest{payload: payload, rows: decoded, done: make(chan error, 1)}
	if l.runtime != nil {
		req.charge = tailBytes(int64(len(payload)+16), uint64(len(decoded)))
	}
	var stop func() bool
	if ctx != nil {
		stop = context.AfterFunc(ctx, func() {
			l.mu.Lock()
			l.cond.Broadcast()
			l.mu.Unlock()
		})
		defer stop()
	}
	for {
		if err := l.admissionError(); err != nil {
			l.mu.Unlock()
			return err
		}
		fits := l.runtime == nil || l.runtime.fits(req.charge)
		if ctx != nil && ctx.Err() != nil || ctx == nil && !fits {
			l.mu.Unlock()
			return ErrTailCapacity
		}
		if len(l.queue) < maxGroup && fits {
			break
		}
		l.cond.Wait()
	}
	if l.runtime != nil {
		l.runtime.used += req.charge
		l.runtime.pending += req.charge
	}
	l.queue = append(l.queue, req)
	if !l.running {
		l.running = true
		go l.run()
	}
	l.mu.Unlock()
	return <-req.done
}

// Use the same canonical rows for live publication and recovery.
func matchBatch(payload []byte, rows []store.Row) ([]store.Row, error) {
	decoded, err := store.Decode(payload)
	if err != nil {
		return nil, fmt.Errorf("invalid WAL batch: %w", err)
	}
	if len(decoded) != len(rows) {
		return nil, errors.New("WAL batch row count differs from payload")
	}
	for i, row := range decoded {
		if !row.TS.Equal(rows[i].TS) || row.Line != rows[i].Line {
			return nil, fmt.Errorf("WAL batch row %d differs from payload", i)
		}
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

		l.writeMu.Lock()
		l.mu.Lock()
		err := l.poisoned
		l.mu.Unlock()
		operation := "commit"
		if err == nil {
			operation, err = l.appendGroup(group)
		}
		outcome := newCommitOutcome(operation, err)
		l.mu.Lock()
		if err == nil {
			l.committed.end = l.offset
			if l.runtime != nil && l.runtime.hash != nil {
				l.runtime.point.SuffixSHA256 = [32]byte(l.runtime.hash.Sum(nil))
			}
			for _, req := range group {
				if l.runtime != nil {
					l.s.Publish(req.rows)
					l.runtime.pending -= req.charge
					l.runtime.active += req.charge
				}
				l.committed.nextOrdinal += uint64(len(req.rows))
			}
		} else {
			for _, req := range group {
				l.releaseRequest(req)
			}
		}
		l.lastOutcome.Store(&outcome)
		if outcome.State == CommitUncertain {
			l.fenceLocked(err)
		}
		if outcome.State == CommitRolledBack {
			l.diagnoseRollback(outcome)
		}
		for _, req := range group {
			req.done <- err
		}
		l.mu.Unlock()
		l.writeMu.Unlock()
	}
}

func (l *Log) releaseRequest(req *appendRequest) {
	if l.runtime != nil {
		l.runtime.used -= req.charge
		l.runtime.pending -= req.charge
		l.cond.Broadcast()
	}
}

func (l *Log) appendGroup(group []*appendRequest) (string, error) {
	next, ordinal := l.offset, l.committed.nextOrdinal
	var successor Authority
	var digest hash.Cloner
	if l.runtime != nil && l.runtime.hash != nil {
		successor = l.runtime.point
		var err error
		digest, err = l.runtime.hash.Clone()
		if err != nil {
			return "marker_preparation", err
		}
	}
	for _, req := range group {
		if int64(len(req.payload)+16) > math.MaxInt64-next || uint64(len(req.rows)) > math.MaxUint64-ordinal {
			return "wal_write", errors.New("WAL position overflow")
		}
		next += int64(len(req.payload) + 16)
		ordinal += uint64(len(req.rows))
	}
	for _, req := range group {
		var h [16]byte
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
				return "wal_write", l.rollback(err)
			}
		}
	}
	if err := l.f.Sync(); err != nil {
		return "wal_sync", l.rollback(err)
	}
	if l.root != nil {
		var operation string
		var err error
		if digest == nil {
			operation, err = l.commit(next)
		} else {
			successor.End, successor.Next = next, ordinal
			successor.SuffixSHA256 = [32]byte(digest.Sum(nil))
			operation, err = l.writeAuthority(successor)
		}
		if err != nil {
			if !errors.Is(err, ErrUncertain) {
				err = l.rollback(err)
			}
			return operation, err
		}
	}
	// The runtime publishes rows, digest and committed bounds together in run.
	if digest != nil {
		l.runtime.hash = digest
	}
	l.offset = next
	if l.runtime == nil {
		for _, req := range group {
			l.s.Publish(req.rows)
		}
	}
	return "commit", nil
}

func (l *Log) rollback(cause error) error {
	if err := l.f.Truncate(l.offset - l.logicalBase()); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("truncate WAL: %w", err))
	}
	if err := l.f.Sync(); err != nil {
		return errors.Join(ErrUncertain, cause, fmt.Errorf("sync truncated WAL: %w", err))
	}
	if _, err := l.f.Seek(l.offset-l.logicalBase(), io.SeekStart); err != nil {
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

	var err error
	if l.f != nil {
		err = l.f.Close()
	}
	if l.captures != nil {
		err = errors.Join(err, l.captures.release())
	}
	if l.dir != nil {
		err = errors.Join(err, l.dir.Close())
	}
	if l.root != nil {
		err = errors.Join(err, l.root.Close())
	}
	l.mu.Lock()
	l.closeErr = err
	l.closeDone = true
	l.cond.Broadcast()
	l.mu.Unlock()
	return err
}
