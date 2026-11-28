package wal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
)

func TestOpenDirectoryPinsOwnerDirectory(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	l := openCaptureTestLog(t, dir)
	probe, err := l.OpenDirectory()
	if err != nil {
		t.Fatal(err)
	}
	before, err := probe.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir, filepath.Join(parent, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	current, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, current) {
		t.Fatal("probe followed the replaced path")
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(filepath.Join(parent, "moved"), &store.Store{}); err == nil {
		if err := other.Close(); err != nil {
			t.Error(err)
		}
		t.Fatal("closing probe released WAL lock")
	}
}

func mustCapture(t *testing.T, l *Log) *Capture {
	t.Helper()
	c, err := l.Capture(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Release(); err != nil {
			t.Error(err)
		}
	})
	return c
}

func openCaptureTestLog(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	return l
}

func checkCapture(t *testing.T, c *Capture, want []byte, ordinal uint64) {
	t.Helper()
	if c.CommittedEnd() != int64(len(want)) || c.NextOrdinal() != ordinal {
		t.Fatalf("capture boundary = (%d, %d), want (%d, %d)", c.CommittedEnd(), c.NextOrdinal(), len(want), ordinal)
	}
	got := make([]byte, len(want))
	if n, err := c.ReadAt(got, 0); n != len(got) || err != nil || !bytes.Equal(got, want) {
		t.Fatalf("captured prefix = %q (%d bytes, %v), want %q", got, n, err, want)
	}
}

func waitCaptureTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for WAL")
		var zero T
		return zero
	}
}

func TestCapturePublishesAfterCommitGroup(t *testing.T) {
	dir := t.TempDir()
	l := openCaptureTestLog(t, dir)
	initial := mustCapture(t, l)
	checkCapture(t, initial, []byte(magic), 0)

	a, ar := testBatch(t, "first frame")
	// Include duplicate rows, blank lines, and an escaped newline.
	b := append(append([]byte("\n \t\n"), a...), '\n')
	b = append(b, a...)
	b = append(b, []byte("\n{\"ts\":\"0001-01-01T00:00:00Z\",\"line\":\"backfill\\nline\"}")...)
	br, err := store.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(magic), replayTestFrame(a)...), replayTestFrame(b)...)

	markerSynced, publish := make(chan error, 1), make(chan struct{})
	var unblock sync.Once
	resume := func() { unblock.Do(func() { close(publish) }) }
	defer resume()
	syncDir := l.syncDir
	l.syncDir = func() error {
		err := syncDir()
		markerSynced <- err
		<-publish
		return err
	}
	group := []*appendRequest{
		{payload: a, rows: ar, done: make(chan error, 1)},
		{payload: b, rows: br, done: make(chan error, 1)},
	}
	l.mu.Lock()
	l.initCond()
	l.queue = group
	l.running = true
	l.mu.Unlock()
	go l.run()

	if err := waitCaptureTest(t, markerSynced); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(dir, markerName))
	if err != nil {
		t.Fatal(err)
	}
	if end, err := readCommitMarker(bytes.NewReader(marker), int64(len(want))); err != nil || end != int64(len(want)) {
		t.Fatalf("on-disk marker = %d, %v", end, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type result struct {
		c   *Capture
		err error
	}
	acquired := make(chan result)
	go func() {
		c, err := l.Capture(ctx)
		select {
		case acquired <- result{c, err}:
		case <-ctx.Done():
			if c != nil {
				if err := c.Release(); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	during := waitCaptureTest(t, acquired)
	if during.err != nil {
		t.Fatal(during.err)
	}
	defer func() {
		if err := during.c.Release(); err != nil {
			t.Error(err)
		}
	}()
	checkCapture(t, during.c, []byte(magic), 0)
	if rows := allTestRows(l.s); len(rows) != 0 {
		t.Fatalf("rows became visible before publication: %v", rows)
	}

	resume()
	for _, req := range group {
		if err := waitCaptureTest(t, req.done); err != nil {
			t.Fatal(err)
		}
	}
	current := mustCapture(t, l)
	checkCapture(t, current, want, uint64(len(ar)+len(br)))
	checkCapture(t, initial, []byte(magic), 0)
	checkCapture(t, during.c, []byte(magic), 0)
	if current.Identity() != initial.Identity() {
		t.Fatal("append changed WAL identity")
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Recovery must count every decoded row and discard any physical suffix.
	wal, err := os.OpenFile(filepath.Join(dir, walName), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(writeAll(wal, []byte("uncommitted tail")), wal.Close()); err != nil {
		t.Fatal(err)
	}
	reopened := openCaptureTestLog(t, dir)
	recovered := mustCapture(t, reopened)
	checkCapture(t, recovered, want, uint64(len(ar)+len(br)))
	if recovered.Identity() != current.Identity() {
		t.Fatal("legacy recovery changed WAL identity")
	}
}

func TestCaptureReadBoundsAndPinnedIdentity(t *testing.T) {
	dir := t.TempDir()
	l := openCaptureTestLog(t, dir)
	rows, err := store.Decode(batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	c := mustCapture(t, l)
	want := append([]byte(magic), replayTestFrame(batch)...)
	if err := l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	checkCapture(t, c, want, 1)

	for _, tc := range []struct {
		name string
		off  int64
		size int
		n    int
		err  error
	}{
		{"absolute", 2, 3, 3, nil},
		{"cross", c.CommittedEnd() - 2, 4, 2, io.EOF},
		{"end", c.CommittedEnd(), 1, 0, io.EOF},
		{"empty at end", c.CommittedEnd(), 0, 0, nil},
		{"oversized offset", math.MaxInt64, 2, 0, io.EOF},
		{"negative offset", -1, 3, 0, os.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := bytes.Repeat([]byte{0xff}, tc.size)
			n, err := c.ReadAt(p, tc.off)
			if n != tc.n || !errors.Is(err, tc.err) {
				t.Fatalf("ReadAt = (%d, %v), want (%d, %v)", n, err, tc.n, tc.err)
			}
			if n > 0 && !bytes.Equal(p[:n], want[tc.off:tc.off+int64(n)]) {
				t.Fatalf("ReadAt bytes = %x", p[:n])
			}
			if !bytes.Equal(p[n:], bytes.Repeat([]byte{0xff}, tc.size-n)) {
				t.Fatal("ReadAt wrote outside the captured boundary")
			}
		})
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := l.Capture(t.Context()); other != nil || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("capture after close = %v, %v", other, err)
	}
	for _, name := range []string{walName, markerName} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	newLog := openCaptureTestLog(t, dir)
	if next := mustCapture(t, newLog); next.Identity() == c.Identity() {
		t.Fatal("replacement file has the live pin's identity")
	}
	checkCapture(t, c, want, 1)
	copyOfCapture := *c
	if err := copyOfCapture.Release(); err != nil {
		t.Fatal(err)
	}
	if err := c.Release(); err != nil {
		t.Fatal(err)
	}
	if n, err := c.ReadAt(make([]byte, 1), 0); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("released ReadAt = (%d, %v)", n, err)
	}
	if _, err := l.captures.f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("last release left the capture descriptor open: %v", err)
	}
}

func TestCaptureRollbackAndTerminalUncertainty(t *testing.T) {
	for _, fault := range []string{"confirmed rollback", "failed rollback", "marker sync"} {
		t.Run(fault, func(t *testing.T) {
			dir := t.TempDir()
			l := openCaptureTestLog(t, dir)
			rows, err := store.Decode(batch)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Append(batch, rows); err != nil {
				t.Fatal(err)
			}
			c := mustCapture(t, l)
			prefix := append([]byte(magic), replayTestFrame(batch)...)
			switch fault {
			case "confirmed rollback":
				l.f = &failingFile{File: l.f.(*os.File), syncFail: true}
			case "failed rollback":
				l.f = &failingFile{File: l.f.(*os.File), syncFail: true, truncateFail: true}
			case "marker sync":
				l.syncDir = func() error { return syscall.EIO }
			}
			err = l.Append(batch, rows)
			uncertain := fault != "confirmed rollback"
			if err == nil || errors.Is(err, ErrUncertain) != uncertain {
				t.Fatalf("append returned %v, uncertain = %t", err, uncertain)
			}
			checkCapture(t, c, prefix, 1)
			if uncertain {
				next, acquireErr := l.Capture(t.Context())
				if next != nil || !errors.Is(acquireErr, ErrUncertain) || !errors.Is(acquireErr, err) {
					t.Fatalf("capture after uncertainty = %v, %v, want %v", next, acquireErr, err)
				}
			} else {
				checkCapture(t, mustCapture(t, l), prefix, 1)
				if err := l.Append(batch, rows); err != nil {
					t.Fatal(err)
				}
				checkCapture(t, mustCapture(t, l), append(prefix, replayTestFrame(batch)...), 2)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openCaptureTestLog(t, dir)
			// Marker rename is observable on this running filesystem. Only a
			// fault during rollback leaves the previous marker authoritative.
			ordinal := uint64(1)
			if fault != "failed rollback" {
				prefix = append(prefix, replayTestFrame(batch)...)
				ordinal++
			}
			checkCapture(t, mustCapture(t, reopened), prefix, ordinal)
		})
	}
}

func TestCaptureCancellationAndConcurrentRelease(t *testing.T) {
	l := openCaptureTestLog(t, t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if c, err := l.Capture(ctx); c != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled capture = %v, %v", c, err)
	}

	c := mustCapture(t, l)
	start := make(chan struct{})
	var operations sync.WaitGroup
	for range 16 {
		operations.Go(func() {
			<-start
			p := make([]byte, len(magic))
			for range 16 {
				n, err := c.ReadAt(p, 0)
				if errors.Is(err, os.ErrClosed) && n == 0 {
					return
				}
				if err != nil || n != len(magic) || string(p) != magic {
					t.Errorf("concurrent ReadAt = %q, %d, %v", p, n, err)
					return
				}
			}
		})
	}
	operations.Go(func() {
		<-start
		if err := c.Release(); err != nil {
			t.Error(err)
		}
	})
	operations.Go(func() {
		<-start
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	close(start)
	operations.Wait()
	if _, err := l.captures.f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("capture descriptor leaked: %v", err)
	}
}

type cancelAfterCaptureCheck struct {
	context.Context
	cancel context.CancelFunc
}

func (c cancelAfterCaptureCheck) Err() error {
	err := c.Context.Err()
	c.cancel()
	return err
}

func TestCaptureCancellationDropsAcquiredPin(t *testing.T) {
	l := openCaptureTestLog(t, t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Cancel after acquisition has checked the context but before it returns.
	c, err := l.Capture(cancelAfterCaptureCheck{ctx, cancel})
	if c != nil {
		t.Fatalf("cancelled acquisition returned a capture: %v", errors.Join(err, c.Release()))
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquisition error = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.captures.f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cancelled acquisition leaked a reference: %v", err)
	}
}
