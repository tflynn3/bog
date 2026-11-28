package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
)

func await[T any](t *testing.T, ch <-chan T) T {
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

func closeLog(t *testing.T, l *Log) {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Error(err)
	}
}

func testBatch(t *testing.T, line string) ([]byte, []store.Row) {
	t.Helper()
	p := []byte(fmt.Sprintf(`{"ts":"2025-01-01T00:00:00Z","line":%q}`, line))
	rows, err := store.Decode(p)
	if err != nil {
		t.Fatal(err)
	}
	return p, rows
}

func allTestRows(s *store.Store) []store.Row {
	return s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 100)
}

func TestInvalidAppendDoesNotReachWorker(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeLog(t, l) })

	p, rows := testBatch(t, "original")
	_, different := testBatch(t, "different")
	twoRows := append(slices.Clone(rows), different...)
	twoLines := bytes.Join([][]byte{p, []byte(`{"ts":"2025-01-01T00:00:00Z","line":"different"}`)}, []byte("\n"))
	before, err := os.ReadFile(filepath.Join(dir, markerName))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		payload []byte
		rows    []store.Row
	}{
		{"empty", nil, nil},
		{"blank", []byte(" \n"), nil},
		{"bad UTF-8", []byte{0xff}, nil},
		{"oversize", make([]byte, store.MaxBatch+1), rows},
		{"malformed after valid row", bytes.Join([][]byte{p, []byte("{bad}")}, []byte("\n")), rows},
		{"missing row", p, nil},
		{"extra row", p, twoRows},
		{"different line", p, different},
		{"different timestamp", p, []store.Row{{TS: rows[0].TS.Add(time.Nanosecond), Line: rows[0].Line}}},
		{"reordered", twoLines, []store.Row{different[0], rows[0]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := l.Append(tc.payload, tc.rows); err == nil {
				t.Fatal("accepted invalid append")
			}
			marker, err := os.ReadFile(filepath.Join(dir, markerName))
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(dir, walName))
			if err != nil {
				t.Fatal(err)
			}
			if l.LastCommitOutcome() != (CommitOutcome{}) || len(allTestRows(s)) != 0 ||
				!bytes.Equal(marker, before) || info.Size() != int64(len(magic)) {
				t.Fatal("invalid append changed outcome, rows, or durable files")
			}
		})
	}
	equivalent := slices.Clone(rows)
	equivalent[0].TS = equivalent[0].TS.In(time.FixedZone("offset", 3600))
	if err := l.Append(p, equivalent); err != nil {
		t.Fatal(err)
	}
	durable := l.LastCommitOutcome()
	if err := l.Append(p, different); err == nil || l.LastCommitOutcome() != durable {
		t.Fatalf("mismatch after durable group: %v, outcome %+v", err, l.LastCommitOutcome())
	}
	if got := allTestRows(s); !slices.Equal(got, rows) {
		t.Fatalf("rows = %v", got)
	}
	closeLog(t, l)
	if err := l.Append(p, rows); !errors.Is(err, os.ErrClosed) || l.LastCommitOutcome() != durable {
		t.Fatalf("post-close append: %v, outcome %+v", err, l.LastCommitOutcome())
	}
}

type readErrorFile struct {
	file
	err error
}

func (f readErrorFile) Read([]byte) (int, error) { return 0, f.err }

func TestRecoveryReadErrorsKeepCauseAndReleaseOwnership(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	_, rows := testBatch(t, "already visible")
	s.Publish(rows)
	// Use the real WAL descriptor for seeks, but inject a read failure before
	// recovery can validate the magic or expose its private recovery state.
	f := l.f
	l.f = readErrorFile{f, syscall.EIO}
	if err := l.recover(nil); !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "WAL magic") {
		t.Fatalf("WAL recovery error = %v", err)
	}
	if got := allTestRows(s); !slices.Equal(got, rows) {
		t.Fatalf("failed recovery altered store: %v", got)
	}
	marker := filepath.Join(dir, markerName)
	markerBytes, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, markerSize - 1, markerSize} {
		reader := io.MultiReader(bytes.NewReader(markerBytes[:n]), readErrorFile{err: syscall.EIO})
		if _, err := readCommitMarker(reader, int64(len(magic))); !errors.Is(err, syscall.EIO) {
			t.Fatalf("marker read after %d bytes returned %v", n, err)
		}
	}
	closeLog(t, l)
	if err := os.WriteFile(marker, markerBytes[:markerSize-1], 0600); err != nil {
		t.Fatal(err)
	}
	if failed, err := Open(dir, s); err == nil {
		closeLog(t, failed)
		t.Fatal("accepted a short marker")
	}
	if err := os.WriteFile(marker, markerBytes, 0600); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, &store.Store{})
	if err != nil {
		t.Fatalf("failed startup retained directory or WAL ownership: %v", err)
	}
	closeLog(t, l)
}

func TestBootstrapAndMarkerModesUnderRestrictiveUmask(t *testing.T) {
	dir := t.TempDir()
	p, rows := testBatch(t, "healthy")
	old := syscall.Umask(0777)
	defer syscall.Umask(old)
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeLog(t, l)
	checkModes := func() {
		t.Helper()
		for _, name := range []string{walName, markerName} {
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0600 {
				t.Fatalf("%s mode = %o", name, info.Mode().Perm())
			}
		}
	}
	checkModes()
	if err := l.Append(p, rows); err != nil {
		t.Fatal(err)
	}
	checkModes()
}

func TestOrphanCleanupRequiresOwnershipAndValidPrefix(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	p, rows := testBatch(t, "committed")
	if err := l.Append(p, rows); err != nil {
		t.Fatal(err)
	}

	removed := make([]string, 130)
	for i := range removed {
		prefix := walStagePrefix
		if i%2 == 0 {
			prefix = markerStagePrefix
		}
		removed[i] = filepath.Join(dir, fmt.Sprintf("%s%032x", prefix, i))
		if err := os.WriteFile(removed[i], []byte("orphan"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	preserved := []string{
		filepath.Join(dir, "unrelated"),
		filepath.Join(dir, markerStagePrefix+strings.Repeat("A", 32)),
		filepath.Join(dir, markerStagePrefix+strings.Repeat("a", 33)),
		filepath.Join(dir, walStagePrefix+strings.Repeat("z", 32)),
	}
	for _, path := range preserved {
		if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	symlink := filepath.Join(dir, markerStagePrefix+strings.Repeat("b", 32))
	hardlink := filepath.Join(dir, markerStagePrefix+strings.Repeat("c", 32))
	subdir := filepath.Join(dir, markerStagePrefix+strings.Repeat("d", 32))
	fifo := filepath.Join(dir, walStagePrefix+strings.Repeat("e", 32))
	if err := errors.Join(os.Symlink(preserved[0], symlink), os.Link(preserved[0], hardlink),
		os.Mkdir(subdir, 0700), syscall.Mkfifo(fifo, 0600)); err != nil {
		t.Fatal(err)
	}
	preserved = append(preserved, symlink, hardlink, subdir, fifo)
	if other, err := Open(dir, &store.Store{}); err == nil {
		closeLog(t, other)
		t.Fatal("second startup acquired the lock")
	}
	closeLog(t, l)

	marker := filepath.Join(dir, markerName)
	contents, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir, &store.Store{}); err == nil {
		closeLog(t, other)
		t.Fatal("startup accepted corrupt marker")
	}
	for _, path := range removed {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("failed or concurrent startup touched %s: %v", path, err)
		}
	}
	if err := os.WriteFile(marker, contents, 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		recovered := &store.Store{}
		reopened, err := Open(dir, recovered)
		if err != nil {
			t.Fatal(err)
		}
		closeLog(t, reopened)
		if got := allTestRows(recovered); !slices.Equal(got, rows) {
			t.Fatalf("recovery %d rows = %v", i, got)
		}
		for _, path := range removed {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("orphan %s still present: %v", path, err)
			}
		}
		for _, path := range preserved {
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("lost unrelated entry %s: %v", path, err)
			}
		}
	}
}

func waitQueueSize(t *testing.T, l *Log, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		n := len(l.queue)
		l.mu.Unlock()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue size = %d, want %d", n, want)
		}
		runtime.Gosched()
	}
}

func TestUncertainOutcomeRejectsQueuedAppendsWithCause(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() {
		unblock()
		closeLog(t, l)
	})
	l.syncDir = func() error {
		close(entered)
		<-release
		return syscall.EIO
	}
	start := func(line string) <-chan error {
		p, rows := testBatch(t, line)
		done := make(chan error, 1)
		go func() { done <- l.Append(p, rows) }()
		return done
	}
	first := start("maybe durable")
	await(t, entered)
	second := start("still queued")
	waitQueueSize(t, l, 1)
	if l.LastCommitOutcome() != (CommitOutcome{}) || len(allTestRows(s)) != 0 {
		t.Fatal("published in-flight group")
	}
	unblock()
	for _, done := range []<-chan error{first, second, start("after failure")} {
		err := await(t, done)
		if !errors.Is(err, ErrUncertain) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("append error = %v", err)
		}
		outcome := l.LastCommitOutcome()
		if outcome.State != CommitUncertain || outcome.Category != "eio" ||
			outcome.Operation != "marker_sync" || outcome.At.IsZero() {
			t.Fatalf("outcome = %+v", outcome)
		}
	}
	outcome := l.LastCommitOutcome()
	closeLog(t, l)
	if l.LastCommitOutcome() != outcome || len(allTestRows(s)) != 0 {
		t.Fatal("uncertainty changed or published rows")
	}
	recovered := &store.Store{}
	reopened, err := Open(dir, recovered)
	if err != nil {
		t.Fatal(err)
	}
	closeLog(t, reopened)
	if got := allTestRows(recovered); len(got) != 1 || got[0].Line != "maybe durable" {
		t.Fatalf("marker did not recover exactly the uncertain group: %v", got)
	}
}

func TestProductionGroupCommitsInQueueOrder(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	first := &groupFile{File: l.f.(*os.File), entered: make(chan struct{}), release: make(chan struct{})}
	l.f = first
	secondMarker, releaseSecond := make(chan struct{}), make(chan struct{})
	unblockFirst := sync.OnceFunc(func() { close(first.release) })
	unblockSecond := sync.OnceFunc(func() { close(releaseSecond) })
	t.Cleanup(func() {
		unblockFirst()
		unblockSecond()
		closeLog(t, l)
	})
	var markerSyncs int
	syncDir := l.syncDir
	l.syncDir = func() error {
		markerSyncs++
		if markerSyncs == 2 {
			close(secondMarker)
			<-releaseSecond
		}
		return syncDir()
	}
	start := func(line string) <-chan error {
		p, rows := testBatch(t, line)
		done := make(chan error, 1)
		go func() { done <- l.Append(p, rows) }()
		return done
	}
	results := []<-chan error{start("first")}
	await(t, first.entered)
	for i, line := range []string{"second", "third", "third", "last"} {
		results = append(results, start(line))
		waitQueueSize(t, l, i+1)
	}
	unblockFirst()
	await(t, secondMarker)
	if err := await(t, results[0]); err != nil {
		t.Fatal(err)
	}
	firstOutcome := l.LastCommitOutcome()
	if firstOutcome.State != CommitDurable || firstOutcome.At.IsZero() {
		t.Fatalf("first outcome = %+v", firstOutcome)
	}
	if got := allTestRows(s); len(got) != 1 || got[0].Line != "first" {
		t.Fatalf("rows before second group completed = %v", got)
	}
	unblockSecond()
	for _, done := range results[1:] {
		if err := await(t, done); err != nil {
			t.Fatal(err)
		}
	}
	secondOutcome := l.LastCommitOutcome()
	if secondOutcome.State != CommitDurable || secondOutcome.At.Before(firstOutcome.At) || secondOutcome == firstOutcome {
		t.Fatalf("second outcome = %+v, first = %+v", secondOutcome, firstOutcome)
	}
	closeLog(t, l)
	if first.syncs != 2 || markerSyncs != 2 || l.running || len(l.queue) != 0 {
		t.Fatalf("WAL syncs = %d, directory syncs = %d, worker running = %t, queued = %d",
			first.syncs, markerSyncs, l.running, len(l.queue))
	}
	recovered := &store.Store{}
	reopened, err := Open(dir, recovered)
	if err != nil {
		t.Fatal(err)
	}
	closeLog(t, reopened)
	for _, current := range []*store.Store{s, recovered} {
		lines := make([]string, 0, len(results))
		for _, row := range allTestRows(current) {
			lines = append(lines, row.Line)
		}
		if !slices.Equal(lines, []string{"first", "second", "third", "third", "last"}) {
			t.Fatalf("row order = %v", lines)
		}
	}
}

func TestMaxFrameReopensWithLiteralAndTimestampBounds(t *testing.T) {
	dir := t.TempDir()
	earliest := `{"ts":"0001-01-01T00:00:00Z","line":"earliest"}`
	prefix := `{"ts":"9999-12-31T23:59:59.999999998Z","line":"[*] `
	suffix := "\"}\n" + earliest
	p := []byte(prefix + strings.Repeat("x", store.MaxBatch-len(prefix)-len(suffix)) + suffix)
	rows, err := store.Decode(p)
	if err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(p, rows); err != nil {
		closeLog(t, l)
		t.Fatal(err)
	}
	closeLog(t, l)

	s := &store.Store{}
	reopened, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeLog(t, reopened)
	first, last := rows[1], rows[0]
	if got := s.Query(first.TS, last.TS, "", 2); !slices.Equal(got, []store.Row{first}) {
		t.Fatal("replay changed the half-open time window")
	}
	if got := s.Query(first.TS, last.TS.Add(time.Nanosecond), "[*]", 2); !slices.Equal(got, []store.Row{last}) {
		t.Fatal("replay changed the literal match or latest timestamp")
	}
}
