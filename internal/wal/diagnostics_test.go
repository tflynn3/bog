package wal

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
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

func captureDiagnostics(l *Log) func(*testing.T) string {
	var out bytes.Buffer
	d := &diagnosticDispatcher{
		queue:  make(chan diagnostic, diagnosticQueueSize),
		logger: log.New(&out, "", 0),
	}
	l.diagnostics = d
	return func(t *testing.T) string {
		t.Helper()
		awaitDiagnosticIdle(t, d)
		return out.String()
	}
}

func awaitDiagnosticIdle(t *testing.T, d *diagnosticDispatcher) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d.mu.Lock()
		active := d.active
		d.mu.Unlock()
		if !active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("diagnostic worker did not exit")
		}
		runtime.Gosched()
	}
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
			f, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			ff := &diagnosticFile{file: f, failure: tc.failure, syncFail: tc.sync, truncateFail: tc.uncertain}
			if !tc.sync {
				ff.writeAt = 1
			}
			l := &Log{f: ff, s: &store.Store{}}
			out := captureDiagnostics(l)
			payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"secret payload"}`)
			rows, err := store.Decode(payload)
			if err != nil {
				t.Fatal(err)
			}
			err = l.Append(payload, rows)
			if tc.uncertain {
				if !errors.Is(err, ErrUncertain) || out(t) != "" {
					t.Fatalf("error=%v log=%q", err, out(t))
				}
				return
			}
			if err != tc.failure {
				t.Fatalf("original error lost: %v", err)
			}
			want := "WAL rollback confirmed operation=" + tc.operation + " category=" + tc.category + "\n"
			if out(t) != want {
				t.Fatalf("diagnostic = %q, want %q", out(t), want)
			}
		})
	}
}

func TestDiagnosticRateLimit(t *testing.T) {
	l := &Log{}
	out := captureDiagnostics(l)
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	l.diagnoseRollback(CommitOutcome{At: now, Operation: "wal_write", Category: "enospc"})
	l.diagnoseRollback(CommitOutcome{At: now.Add(time.Second), Operation: "wal_sync", Category: "eio"})
	l.diagnoseRollback(CommitOutcome{At: now.Add(time.Minute), Operation: "marker_preparation", Category: "other"})
	if l.DroppedDiagnostics() != 0 {
		t.Fatal("rate limiting should not count as queue saturation")
	}
	want := "WAL rollback confirmed operation=wal_write category=enospc\nWAL rollback confirmed operation=marker_preparation category=other\n"
	if out(t) != want {
		t.Fatalf("diagnostics = %q", out(t))
	}
}

func TestDiagnosticRecoveryOfFailedAppend(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	out := captureDiagnostics(l)
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
	l.f = &diagnosticFile{file: l.f, writeAt: 2, failure: failure}
	if err = appendLine("secret payload"); err != failure {
		t.Fatalf("error = %v", err)
	}
	if got := out(t); got != "WAL rollback confirmed operation=wal_write category=enospc\n" || strings.Contains(got, "secret") || strings.Contains(got, "private") {
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
	l, err := Open(t.TempDir(), &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	out := captureDiagnostics(l)
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
	if got := out(t); got != "WAL rollback confirmed operation=marker_preparation category=other\n" {
		t.Fatalf("diagnostic = %q", got)
	}
}

type blockedDiagnosticSink struct {
	entered chan struct{}
	release chan struct{}
	writes  int
}

func (s *blockedDiagnosticSink) Write(p []byte) (int, error) {
	s.writes++
	if s.writes == 1 {
		close(s.entered)
		<-s.release
	}
	return len(p), nil
}

func TestBlockedDiagnosticDoesNotBlockAppendCloseOrRecovery(t *testing.T) {
	sink := &blockedDiagnosticSink{entered: make(chan struct{}), release: make(chan struct{})}
	d := &diagnosticDispatcher{
		queue:  make(chan diagnostic, diagnosticQueueSize),
		logger: log.New(sink, "", 0),
	}

	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(sink.release)
		closeLog(t, l)
		awaitDiagnosticIdle(t, d)
	})

	l.diagnostics = d
	appendAsync := func(line string) <-chan error {
		t.Helper()
		p, rows := testBatch(t, line)
		done := make(chan error, 1)
		go func() { done <- l.Append(p, rows) }()
		return done
	}
	failure := fmt.Errorf("private detail: %w", syscall.ENOSPC)
	f := l.f
	l.f = &diagnosticFile{file: f, writeAt: 2, failure: failure}
	failed := appendAsync("discard this payload")
	await(t, sink.entered)
	if err := await(t, failed); err != failure {
		t.Fatalf("append error = %v, want original failure", err)
	}
	rolledBack := l.LastCommitOutcome()
	if rolledBack.State != CommitRolledBack || rolledBack.Operation != "wal_write" ||
		rolledBack.Category != "enospc" || rolledBack.At.IsZero() {
		t.Fatalf("rollback outcome = %+v", rolledBack)
	}
	if err := await(t, appendAsync("durable with blocked sink")); err != nil {
		t.Fatal(err)
	}
	durable := l.LastCommitOutcome()
	if durable.State != CommitDurable || durable.At.Before(rolledBack.At) {
		t.Fatalf("durable outcome = %+v", durable)
	}

	// Simulate independent rate-eligible producers while the shared sink is
	// blocked. A real rollback on l then observes saturation.
	for i := 0; i < diagnosticQueueSize; i++ {
		producer := &Log{diagnostics: d}
		producer.diagnoseRollback(rolledBack)
		if n := producer.DroppedDiagnostics(); n != 0 {
			t.Fatalf("producer %d dropped %d events", i, n)
		}
	}
	l.mu.Lock()
	l.lastDiagnostic = time.Time{}
	l.f = &diagnosticFile{file: f, writeAt: 1, failure: syscall.EIO}
	l.mu.Unlock()
	if err := await(t, appendAsync("discard when queue is full")); err != syscall.EIO {
		t.Fatalf("append error with full diagnostic queue = %v", err)
	}
	if n := l.DroppedDiagnostics(); n != 1 {
		t.Fatalf("diagnostics dropped = %d", n)
	}

	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	if err := await(t, closed); err != nil {
		t.Fatal(err)
	}
	recovered := &store.Store{}
	reopened, err := Open(dir, recovered)
	if err != nil {
		t.Fatal(err)
	}
	closeLog(t, reopened)
	for _, current := range []*store.Store{s, recovered} {
		got := allTestRows(current)
		if len(got) != 1 || got[0].Line != "durable with blocked sink" {
			t.Fatalf("rows = %v", got)
		}
	}
}
