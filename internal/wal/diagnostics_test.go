package wal

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
)

type diagnosticFile struct {
	file
	writeAt, writes        int
	syncFail, truncateFail bool
	failure                error
}

func (f *diagnosticFile) Write(p []byte) (int, error) {
	f.writes++
	if f.writes == f.writeAt {
		return 0, f.failure
	}
	return f.file.Write(p)
}
func (f *diagnosticFile) Sync() error {
	if f.syncFail {
		f.syncFail = false
		return f.failure
	}
	return f.file.Sync()
}
func (f *diagnosticFile) Truncate(n int64) error {
	if f.truncateFail {
		return errors.New("rollback failure")
	}
	return f.file.Truncate(n)
}

type diagnosticSink struct {
	writes  chan string
	release chan struct{}
}

func (s diagnosticSink) Write(p []byte) (int, error) {
	s.writes <- string(p)
	<-s.release
	return len(p), nil
}

func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func testDiagnosticQueue(t *testing.T, blocked bool) (diagnosticQueue, <-chan string, func()) {
	t.Helper()
	sink := diagnosticSink{
		writes:  make(chan string, diagnosticQueueCapacity+2),
		release: make(chan struct{}),
	}
	release := sync.OnceFunc(func() { close(sink.release) })
	if !blocked {
		release()
	}
	queue := newDiagnosticQueue(log.New(sink, "", 0))
	t.Cleanup(func() {
		release()
		close(queue)
	})
	return queue, sink.writes, release
}

func TestRecoverableDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, operation, category string
		failure                   error
		sync, uncertain           bool
	}{
		{"wrapped no space", "wal_write", "enospc", fmt.Errorf("private error: %w", syscall.ENOSPC), false, false},
		{"wrapped io", "wal_sync", "eio", fmt.Errorf("private error: %w", syscall.EIO), true, false},
		{"other", "wal_write", "other", errors.New("private error"), false, false},
		{"uncertain", "", "", errors.New("private error"), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue := make(diagnosticQueue, 1)
			f, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			ff := &diagnosticFile{file: f, failure: tc.failure, syncFail: tc.sync, truncateFail: tc.uncertain}
			if !tc.sync {
				ff.writeAt = 1
			}
			l := &Log{f: ff, s: &store.Store{}}
			l.diagnostics.queue = queue
			t.Cleanup(func() {
				if err := l.Close(); err != nil {
					t.Error(err)
				}
			})
			payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"secret payload"}`)
			rows, err := store.Decode(payload)
			if err != nil {
				t.Fatal(err)
			}
			err = l.Append(payload, rows)
			if tc.uncertain {
				if !errors.Is(err, ErrUncertain) || !errors.Is(err, tc.failure) || len(queue) != 0 {
					t.Fatalf("error=%v diagnostics=%d", err, len(queue))
				}
				return
			}
			if err != tc.failure {
				t.Fatalf("original error lost: %v", err)
			}
			want := rollbackDiagnostic{operation: tc.operation, category: tc.category}
			if got := waitFor(t, queue, "diagnostic"); got != want {
				t.Fatalf("diagnostic = %+v, want %+v", got, want)
			}
			outcome := l.LastCommitOutcome()
			if outcome.State != "rolled_back" || outcome.Category != tc.category || outcome.At.IsZero() {
				t.Fatalf("outcome = %+v", outcome)
			}
		})
	}
}

func TestDiagnosticRateLimit(t *testing.T) {
	queue := make(diagnosticQueue, 2)
	l := &Log{}
	l.diagnostics.queue = queue
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	l.diagnostics.report("wal_write", syscall.ENOSPC, now)
	l.diagnostics.report("wal_sync", syscall.EIO, now.Add(time.Second))
	l.diagnostics.report("marker_preparation", errors.New("private"), now.Add(time.Minute))
	for _, want := range []rollbackDiagnostic{{"wal_write", "enospc"}, {"marker_preparation", "other"}} {
		if got := waitFor(t, queue, "rate-limited diagnostic"); got != want {
			t.Fatalf("diagnostic = %+v, want %+v", got, want)
		}
	}
	if len(queue) != 0 || l.DroppedDiagnostics() != 0 {
		t.Fatalf("queue length = %d, drops = %d", len(queue), l.DroppedDiagnostics())
	}
}

func TestDiagnosticRecoveryOfFailedGroup(t *testing.T) {
	queue, output, _ := testDiagnosticQueue(t, false)
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	l.diagnostics.queue = queue
	appendLine := func(line string) error {
		payload := []byte(fmt.Sprintf(`{"ts":"2025-01-01T00:00:00Z","line":%q}`, line))
		rows, e := store.Decode(payload)
		if e != nil {
			t.Fatal(e)
		}
		return l.Append(payload, rows)
	}
	if err = appendLine("acknowledged"); err != nil {
		t.Fatal(err)
	}
	failure := fmt.Errorf("private error: %w", syscall.ENOSPC)
	ff := &diagnosticFile{file: l.f, writeAt: 4, failure: failure}
	l.f = ff
	var group []*appendRequest
	for _, line := range []string{"secret payload one", "secret payload two"} {
		payload := []byte(fmt.Sprintf(`{"ts":"2025-01-01T00:00:00Z","line":%q}`, line))
		rows, e := store.Decode(payload)
		if e != nil {
			t.Fatal(e)
		}
		group = append(group, &appendRequest{payload: payload, rows: rows})
	}
	if err = l.appendGroup(group); err != failure {
		t.Fatalf("error = %v", err)
	}
	if got := waitFor(t, output, "rollback diagnostic"); got != "WAL rollback confirmed operation=wal_write category=enospc\n" || strings.Contains(got, "secret") || strings.Contains(got, "private") {
		t.Fatalf("diagnostic = %q", got)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	check := func(s *store.Store, want ...string) {
		t.Helper()
		rows := s.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10)
		if len(rows) != len(want) {
			t.Fatalf("rows = %+v", rows)
		}
		for i := range want {
			if rows[i].Line != want[i] {
				t.Fatalf("rows = %+v", rows)
			}
		}
	}
	s = &store.Store{}
	l, err = Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	check(s, "acknowledged")
	if err = appendLine("healthy"); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	s = &store.Store{}
	l, err = Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	check(s, "acknowledged", "healthy")
}

func TestMarkerPreparationDiagnostic(t *testing.T) {
	queue, output, _ := testDiagnosticQueue(t, false)
	l, err := Open(t.TempDir(), &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	l.diagnostics.queue = queue
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	// An invalid directory descriptor makes marker creation fail before rename.
	badDir, err := os.Open(l.dir.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err = badDir.Close(); err != nil {
		t.Fatal(err)
	}
	goodDir := l.dir
	l.dir = badDir
	rows, err := store.Decode(batch)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Append(batch, rows)
	l.dir = goodDir
	if err == nil || errors.Is(err, ErrUncertain) {
		t.Fatalf("error = %v", err)
	}
	if got := waitFor(t, output, "rollback diagnostic"); got != "WAL rollback confirmed operation=marker_preparation category=other\n" {
		t.Fatalf("diagnostic = %q", got)
	}
}

func TestBlockedDiagnosticSink(t *testing.T) {
	queue, output, release := testDiagnosticQueue(t, true)
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release()
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	l.diagnostics.queue = queue
	failure := fmt.Errorf("private path: %w", syscall.ENOSPC)
	l.f = &diagnosticFile{file: l.f, writeAt: 2, failure: failure}

	appendLine := func(line string) <-chan error {
		t.Helper()
		payload := []byte(fmt.Sprintf(`{"ts":"2025-01-01T00:00:00Z","line":%q}`, line))
		rows, err := store.Decode(payload)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { result <- l.Append(payload, rows) }()
		return result
	}
	if got := l.LastCommitOutcome(); got != (CommitOutcome{}) {
		t.Fatalf("outcome before ingest = %+v", got)
	}
	rejected := appendLine("secret payload")
	const want = "WAL rollback confirmed operation=wal_write category=enospc\n"
	if got := waitFor(t, output, "sink entry"); got != want {
		t.Fatalf("diagnostic = %q, want %q", got, want)
	}
	if err := waitFor(t, rejected, "rejected append while sink is blocked"); err != failure {
		t.Fatalf("append error = %v", err)
	}
	rolledBack := l.LastCommitOutcome()
	if rolledBack.State != "rolled_back" || rolledBack.Category != "enospc" || rolledBack.At.IsZero() {
		t.Fatalf("outcome = %+v", rolledBack)
	}

	// Advance only the limiter clock while the real sink remains blocked.
	now := l.diagnostics.last
	for i := 1; i <= diagnosticQueueCapacity+3; i++ {
		l.diagnostics.report("wal_sync", syscall.EIO, now.Add(time.Duration(i)*time.Minute))
	}
	if got := l.DroppedDiagnostics(); got != 3 {
		t.Fatalf("dropped diagnostics = %d, want 3", got)
	}
	if got := len(queue); got != diagnosticQueueCapacity {
		t.Fatalf("queued diagnostics = %d, want %d", got, diagnosticQueueCapacity)
	}
	if got := len(output); got != 0 {
		t.Fatalf("additional concurrent sink writes = %d", got)
	}
	if err := waitFor(t, appendLine("healthy"), "successful append while sink is blocked"); err != nil {
		t.Fatal(err)
	}
	durable := l.LastCommitOutcome()
	if durable.State != "durable" || durable.Category != "" || durable.At.Before(rolledBack.At) {
		t.Fatalf("outcome = %+v", durable)
	}
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	if err := waitFor(t, closed, "close while sink is blocked"); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(t, appendLine("closed"), "closed append"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed append error = %v", err)
	}
	if got := l.LastCommitOutcome(); got != durable {
		t.Fatalf("outcome after close = %+v, want %+v", got, durable)
	}
	recovered := &store.Store{}
	reopened, err := Open(dir, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	rows := recovered.Query(time.Time{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "", 10)
	if len(rows) != 1 || rows[0].Line != "healthy" {
		t.Fatalf("recovered rows = %+v", rows)
	}

	release()
	for range diagnosticQueueCapacity {
		if got := waitFor(t, output, "queued diagnostic"); got != "WAL rollback confirmed operation=wal_sync category=eio\n" {
			t.Fatalf("queued diagnostic = %q", got)
		}
	}
}
