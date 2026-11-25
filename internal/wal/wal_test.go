package wal

import (
	"bog/internal/store"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

var batch = []byte(`{"ts":"2025-01-01T00:00:00Z","line":"a"}`)

func TestOpenParentUsesPinnedDirectory(t *testing.T) {
	base := t.TempDir()
	data := filepath.Join(base, "actual", "data")
	if err := os.MkdirAll(data, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(data, alias); err != nil {
		t.Fatal(err)
	}
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(data); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	for _, path := range []string{".", alias} {
		d, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		parent, err := openParent(d)
		d.Close()
		if err != nil {
			t.Fatal(err)
		}
		got, err := parent.Stat()
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.Stat(filepath.Dir(data))
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(got, want) {
			t.Fatalf("wrong parent for %q", path)
		}
		if err := parent.Sync(); err != nil {
			t.Fatal(err)
		}
		parent.Close()
		log, err := Open(path, &store.Store{})
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFrameFormat(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Decode(batch)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "bog.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 8+16+len(batch) || string(b[:8]) != "BOGWAL01" {
		t.Fatalf("invalid frame size or magic: %d", len(b))
	}
	h := b[8:24]
	if binary.LittleEndian.Uint32(h) != uint32(len(batch)) || binary.LittleEndian.Uint32(h[4:]) != ^uint32(len(batch)) || binary.LittleEndian.Uint32(h[8:]) != crc32.Checksum(batch, table) || binary.LittleEndian.Uint32(h[12:]) != crc32.Checksum(h[:12], table) || string(b[24:]) != string(batch) {
		t.Fatal("invalid frame contents")
	}
}

func TestTornTail(t *testing.T) {
	h := make([]byte, 16)
	binary.LittleEndian.PutUint32(h, 10)
	binary.LittleEndian.PutUint32(h[4:], ^uint32(10))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(h[:12], table))
	for _, tail := range [][]byte{{1, 2, 3}, append(h, 'x')} {
		dir := t.TempDir()
		s := &store.Store{}
		l, e := Open(dir, s)
		if e != nil {
			t.Fatal(e)
		}
		rows, _ := store.Decode(batch)
		if e = l.Append(batch, rows); e != nil {
			t.Fatal(e)
		}
		l.Close()
		p := filepath.Join(dir, "bog.wal")
		f, e := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
		if e != nil {
			t.Fatal(e)
		}
		f.Write(tail)
		f.Close()
		s = &store.Store{}
		l, e = Open(dir, s)
		if e != nil {
			t.Fatal(e)
		}
		if len(s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10)) != 1 {
			t.Fatal("missing row")
		}
		l.Close()
	}
}

type failingFile struct {
	*os.File
	writeFail, syncFail, truncateFail bool
}

func (f *failingFile) Write(b []byte) (int, error) {
	if f.writeFail {
		f.writeFail = false
		n, _ := f.File.Write(b[:1])
		return n, errors.New("write failure")
	}
	return f.File.Write(b)
}
func (f *failingFile) Sync() error {
	if f.syncFail {
		f.syncFail = false
		return errors.New("sync failure")
	}
	return f.File.Sync()
}
func (f *failingFile) Truncate(n int64) error {
	if f.truncateFail {
		return errors.New("truncate failure")
	}
	return f.File.Truncate(n)
}
func TestAppendRollback(t *testing.T) {
	for _, kind := range []string{"write", "sync", "uncertain"} {
		t.Run(kind, func(t *testing.T) {
			f, e := os.CreateTemp(t.TempDir(), "wal")
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			ff := &failingFile{File: f, writeFail: kind != "sync", syncFail: kind == "sync", truncateFail: kind == "uncertain"}
			s := &store.Store{}
			l := &Log{f: ff, s: s}
			rows, _ := store.Decode(batch)
			e = l.Append(batch, rows)
			if e == nil || (kind == "uncertain") != errors.Is(e, ErrUncertain) {
				t.Fatalf("unexpected error %v", e)
			}
			if kind != "uncertain" {
				end, _ := f.Seek(0, io.SeekEnd)
				if end != 0 {
					t.Fatalf("rollback size %d", end)
				}
				if e = l.Append(batch, rows); e != nil {
					t.Fatal(e)
				}
			}
			if len(s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10)) != map[bool]int{true: 0, false: 1}[kind == "uncertain"] {
				t.Fatal("publication")
			}
		})
	}
}

func TestExistingInvalidMagicIsNotInitialized(t *testing.T) {
	for _, contents := range []string{"", "BOGW", "XXXXXXXX"} {
		t.Run(contents, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "bog.wal")
			if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			if l, err := Open(dir, &store.Store{}); err == nil {
				l.Close()
				t.Fatal("accepted invalid magic")
			}
			got, err := os.ReadFile(path)
			if err != nil || string(got) != contents {
				t.Fatalf("WAL changed: %q, %v", got, err)
			}
		})
	}
}

func TestAppendAfterClose(t *testing.T) {
	l, err := Open(t.TempDir(), &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	rows, _ := store.Decode(batch)
	if err := l.Append(batch, rows); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("append after close: %v", err)
	}
}

func TestCorruptLengthDoesNotTruncate(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := store.Decode(batch)
	if err = l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "bog.wal")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(b[8:], uint32(len(batch)+100))
	binary.LittleEndian.PutUint32(b[12:], ^uint32(len(batch)+100))
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(dir, &store.Store{}); err == nil {
		l.Close()
		t.Fatal("accepted corrupt header")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(b) {
		t.Fatal("corrupt WAL changed", err)
	}
}

func TestWALInodeLockAndSymlink(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	l, err := Open(a, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err = os.Link(filepath.Join(a, "bog.wal"), filepath.Join(b, "bog.wal")); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(b, &store.Store{}); err == nil {
		other.Close()
		t.Fatal("locked inode opened twice")
	}
	c := t.TempDir()
	if err = os.Symlink(filepath.Join(a, "bog.wal"), filepath.Join(c, "bog.wal")); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(c, &store.Store{}); err == nil {
		other.Close()
		t.Fatal("accepted symlink")
	}
}

func TestUncommittedSuffixDiscarded(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := store.Decode(batch)
	if err = l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	l.Close()
	path := filepath.Join(dir, "bog.wal")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A complete but corrupt frame can be left behind by an interrupted write.
	if _, err = f.Write(append(make([]byte, 16), batch...)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	s := &store.Store{}
	l, err = Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(before) {
		t.Fatalf("suffix not discarded: %v", err)
	}
	if len(s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10)) != 1 {
		t.Fatal("committed row lost")
	}
}

func TestIncompleteCommittedFrameFails(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := store.Decode(batch)
	if err = l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	l.Close()
	path := filepath.Join(dir, "bog.wal")
	info, _ := os.Stat(path)
	if err = os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(dir, &store.Store{}); err == nil {
		l.Close()
		t.Fatal("accepted truncated committed frame")
	}
}

func TestRejectLinkedAndSymlinkedWAL(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	l, err := Open(first, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	source := filepath.Join(first, "bog.wal")
	if err = os.Link(source, filepath.Join(second, "bog.wal")); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(second, &store.Store{}); err == nil {
		l.Close()
		t.Fatal("accepted hard-linked WAL")
	}
	if l, err = Open(first, &store.Store{}); err == nil {
		l.Close()
		t.Fatal("accepted multiply linked original WAL")
	}
	third := t.TempDir()
	if err = os.WriteFile(filepath.Join(third, "target"), []byte(magic), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("target", filepath.Join(third, "bog.wal")); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(third, &store.Store{}); err == nil {
		l.Close()
		t.Fatal("accepted relative WAL symlink")
	}
}

func TestInvalidMarkerDoesNotTruncate(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := store.Decode(batch)
	if err = l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	l.Close()
	p := filepath.Join(dir, "bog.wal")
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	binary.LittleEndian.PutUint64(marker[8:], 9)
	binary.LittleEndian.PutUint64(marker[16:], ^uint64(9))
	if err = os.WriteFile(filepath.Join(dir, "bog.commit"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(dir, &store.Store{}); err == nil {
		l.Close()
		t.Fatal("accepted marker inside frame")
	}
	after, err := os.ReadFile(p)
	if err != nil || string(after) != string(before) {
		t.Fatal("WAL modified on invalid marker", err)
	}
}

func TestMarkerPreparationFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	// Prevent creation of the temporary marker, before replacement can occur.
	if err := l.dir.Close(); err != nil {
		t.Fatal(err)
	}
	rows, _ := store.Decode(batch)
	err = l.Append(batch, rows)
	if err == nil || errors.Is(err, ErrUncertain) {
		t.Fatalf("expected confirmed rollback: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "bog.wal"))
	if err != nil || info.Size() != int64(len(magic)) {
		t.Fatalf("WAL after rollback: %v, %v", info, err)
	}
	if len(s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10)) != 0 {
		t.Fatal("published failed batch")
	}
	l.Close()
	reopened, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
}

func TestMissingStoreFilesFailClosed(t *testing.T) {
	for _, missing := range []string{"bog.wal", "bog.commit"} {
		t.Run(missing, func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, &store.Store{})
			if err != nil {
				t.Fatal(err)
			}
			rows, _ := store.Decode(batch)
			if err := l.Append(batch, rows); err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dir, missing)); err != nil {
				t.Fatal(err)
			}
			if l, err := Open(dir, &store.Store{}); err == nil {
				l.Close()
				t.Fatal("accepted missing " + missing)
			}
			if _, err := os.Stat(filepath.Join(dir, missing)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing file recreated: %v", err)
			}
		})
	}
}

func TestDirectoryLockSurvivesLockPathReplacement(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	lockPath := filepath.Join(dir, "lock")
	if err := os.WriteFile(lockPath, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, []byte("new inode"), 0600); err != nil {
		t.Fatal(err)
	}
	if second, err := Open(dir, &store.Store{}); err == nil {
		second.Close()
		t.Fatal("second initializer acquired a replaced lock pathname")
	}
}

func TestBootstrapRenameDoesNotReplaceWinner(t *testing.T) {
	dir := t.TempDir()
	d, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := os.WriteFile(filepath.Join(dir, "winner"), []byte("committed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "candidate"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := renameNoReplace(d, "candidate", "winner"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("rename: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "winner"))
	if err != nil || string(got) != "committed" {
		t.Fatalf("winner: %q, %v", got, err)
	}
}

func TestReopenPreservesMarker(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := store.Decode(batch)
	if err := l.Append(batch, rows); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "bog.commit")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !os.SameFile(before, after) || string(contents) != string(got) {
		t.Fatalf("marker replaced or changed on reopen: %v", err)
	}
}

func TestMarkerDirectorySyncFailureIsUncertain(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	l.syncDir = func() error { return errors.New("injected directory sync failure") }
	rows, _ := store.Decode(batch)
	if err := l.Append(batch, rows); !errors.Is(err, ErrUncertain) {
		t.Fatalf("append: %v", err)
	}
	if err := l.Append(batch, rows); !errors.Is(err, ErrUncertain) {
		t.Fatalf("poisoned append: %v", err)
	}
	if got := s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10); len(got) != 0 {
		t.Fatal("published uncertain batch")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := &store.Store{}
	l, err = Open(dir, recovered)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got := recovered.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10); len(got) != 1 {
		t.Fatal("replaced marker did not retain WAL")
	}
}

func TestRejectUnsafePermissions(t *testing.T) {
	for _, target := range []string{"directory", "bog.wal", "bog.commit"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, &store.Store{})
			if err != nil {
				t.Fatal(err)
			}
			l.Close()
			path := dir
			if target != "directory" {
				path = filepath.Join(dir, target)
			}
			if err := os.Chmod(path, 0777); err != nil {
				t.Fatal(err)
			}
			if l, err := Open(dir, &store.Store{}); err == nil {
				l.Close()
				t.Fatal("accepted unsafe permissions")
			}
		})
	}
}

type groupFile struct {
	*os.File
	writes, syncs    int
	failWrite        int
	entered, release chan struct{}
}

func (f *groupFile) Write(p []byte) (int, error) {
	f.writes++
	if f.writes == f.failWrite {
		return 0, errors.New("injected later frame failure")
	}
	return f.File.Write(p)
}
func (f *groupFile) Sync() error {
	f.syncs++
	if f.entered != nil && f.syncs == 1 {
		close(f.entered)
		<-f.release
	}
	return f.File.Sync()
}

func TestGroupFramesAndRollback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			gf := &groupFile{File: f}
			if fail {
				gf.failWrite = 4
			}
			l := &Log{f: gf, s: &store.Store{}}
			a := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"first"}`)
			b := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"second"}`)
			ar, _ := store.Decode(a)
			br, _ := store.Decode(b)
			err = l.appendGroup([]*appendRequest{{payload: a, rows: ar}, {payload: b, rows: br}})
			if fail {
				if err == nil || errors.Is(err, ErrUncertain) {
					t.Fatalf("failure: %v", err)
				}
				if end, _ := f.Seek(0, io.SeekEnd); end != 0 {
					t.Fatalf("rollback size %d", end)
				}
				if err = l.Append(a, ar); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err != nil || gf.syncs != 1 {
				t.Fatalf("err=%v syncs=%d", err, gf.syncs)
			}
			data, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			if string(data[16:16+len(a)]) != string(a) || string(data[32+len(a):]) != string(b) {
				t.Fatal("frame order")
			}
			rows := l.s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10)
			if len(rows) != 2 || rows[0].Line != "first" || rows[1].Line != "second" {
				t.Fatalf("publication order: %+v", rows)
			}
		})
	}
}

func TestQueuedGroupAndClose(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	gf := &groupFile{File: f, entered: make(chan struct{}), release: make(chan struct{})}
	l := &Log{f: gf, s: &store.Store{}}
	rows, _ := store.Decode(batch)
	results := make(chan error, 4)
	go func() { results <- l.Append(batch, rows) }()
	<-gf.entered
	// Admit the remaining requests in a known order while the first sync is blocked.
	for i := 0; i < 3; i++ {
		go func() { results <- l.Append(batch, rows) }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			l.mu.Lock()
			n := len(l.queue)
			l.mu.Unlock()
			if n == i+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("request was not queued")
			}
			runtime.Gosched()
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		c := l.closed
		l.mu.Unlock()
		if c {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close did not start")
		}
		runtime.Gosched()
	}
	if err := l.Append(batch, rows); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("append during close: %v", err)
	}
	close(gf.release)
	for i := 0; i < 4; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if gf.syncs != 2 {
		t.Fatalf("got %d WAL syncs for two groups", gf.syncs)
	}
}

type blockingCloseFile struct {
	*groupFile
	closeEntered chan struct{}
	closeRelease chan struct{}
}

func (f *blockingCloseFile) Close() error {
	close(f.closeEntered)
	<-f.closeRelease
	return f.File.Close()
}

func TestConcurrentCloseWaitsForDescriptors(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	bf := &blockingCloseFile{
		groupFile:    &groupFile{File: f, entered: make(chan struct{}), release: make(chan struct{})},
		closeEntered: make(chan struct{}), closeRelease: make(chan struct{}),
	}
	l := &Log{f: bf, s: &store.Store{}}
	rows, _ := store.Decode(batch)
	appended := make(chan error, 1)
	go func() { appended <- l.Append(batch, rows) }()
	<-bf.entered
	closed := make(chan error, 2)
	go func() { closed <- l.Close() }()
	go func() { closed <- l.Close() }()
	close(bf.release)
	<-bf.closeEntered
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the file was closed: %v", err)
	default:
	}
	close(bf.closeRelease)
	if err := <-appended; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("file still open: %v", err)
	}
}
