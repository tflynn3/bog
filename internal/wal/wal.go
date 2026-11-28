package wal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
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
	decoded, err := matchBatch(payload, rows)
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

		operation, err := l.appendGroup(group)
		outcome := newCommitOutcome(operation, err)
		l.mu.Lock()
		if err == nil {
			l.committed.end = l.offset
			for _, req := range group {
				l.committed.nextOrdinal += uint64(len(req.rows))
			}
		}
		l.lastOutcome.Store(&outcome)
		if outcome.State == CommitUncertain {
			l.poisoned = err
			for _, req := range l.queue {
				req.done <- err
			}
			l.queue = nil
			l.cond.Broadcast()
		}
		if outcome.State == CommitRolledBack {
			l.diagnoseRollback(outcome)
		}
		for _, req := range group {
			req.done <- err
		}
		l.mu.Unlock()
	}
}

func (l *Log) appendGroup(group []*appendRequest) (string, error) {
	next := l.offset
	for _, req := range group {
		if _, err := WriteFrame(l.f, nil, req.payload); err != nil {
			return "wal_write", l.rollback(err)
		}
		next += int64(FrameHeaderSize + len(req.payload))
	}
	if err := l.f.Sync(); err != nil {
		return "wal_sync", l.rollback(err)
	}
	if l.root != nil {
		operation, err := l.commit(next)
		if err != nil {
			if !errors.Is(err, ErrUncertain) {
				err = l.rollback(err)
			}
			return operation, err
		}
	}
	l.offset = next
	for _, req := range group {
		l.s.Publish(req.rows)
	}
	return "commit", nil
}

func (l *Log) rollback(cause error) error {
	return RollbackFrame(l.f, l.offset, cause)
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

	err := l.f.Close()
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
