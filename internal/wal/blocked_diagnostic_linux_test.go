package wal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
)

type blockedDiagnosticSink struct {
	once             sync.Once
	entered, release chan struct{}
	messages         chan string
}

func (s *blockedDiagnosticSink) Write(p []byte) (int, error) {
	s.messages <- string(p)
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return len(p), nil
}

func TestBlockedDiagnosticDoesNotStallWAL(t *testing.T) {
	// Isolate the process-wide logger so a blocked sink cannot affect other tests.
	const child = "BOG_TEST_BLOCKED_WAL_DIAGNOSTIC"
	if os.Getenv(child) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 4*testWait)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBlockedDiagnosticDoesNotStallWAL$")
		cmd.Env = append(os.Environ(), child+"=1", "GORACE=atexit_sleep_ms=0")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child test: %v\n%s", err, output)
		}
		return
	}
	sink := &blockedDiagnosticSink{
		entered: make(chan struct{}), release: make(chan struct{}),
		messages: make(chan string, diagnosticQueueCapacity+2),
	}
	log.SetOutput(sink)
	log.SetFlags(0)
	release := sync.OnceFunc(func() { close(sink.release) })
	defer release()
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release()
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	failure := fmt.Errorf("private message: %w", syscall.ENOSPC)
	l.f = &diagnosticFile{file: l.f, writeAt: 2, failure: failure}
	rows, err := store.Decode(batch)
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan error, 1)
	go func() { failed <- l.Append(batch, rows) }()
	awaitTestValue(t, sink.entered, "sink entry")
	if err := awaitTestValue(t, failed, "rejected append while sink is blocked"); err != failure {
		t.Fatalf("append error = %v, want original error %v", err, failure)
	}

	checkCommitOutcome(t, l, CommitRolledBack, "wal_write", "enospc")
	if got := awaitTestValue(t, sink.messages, "blocked diagnostic"); got != "WAL rollback confirmed operation=wal_write category=enospc\n" {
		t.Fatalf("diagnostic = %q", got)
	}

	// Fill the shared queue while the sole worker is inside the sink.
	for range diagnosticQueueCapacity {
		other := &Log{}
		other.diagnoseRollback("wal_sync", syscall.EIO, time.Now())
		if dropped := other.DroppedDiagnostics(); dropped != 0 {
			t.Fatalf("queue dropped an event before reaching capacity: %d", dropped)
		}
	}

	// Reset the limiter to exercise a new eligible rollback without a minute's wait.
	l.lastDiagnostic = time.Time{}
	l.f = &diagnosticFile{file: l.f, writeAt: 1, failure: failure}
	go func() { failed <- l.Append(batch, rows) }()
	if err := awaitTestValue(t, failed, "rejected append while queue is full"); err != failure {
		t.Fatalf("append error = %v, want %v", err, failure)
	}
	if dropped := l.DroppedDiagnostics(); dropped != 1 {
		t.Fatalf("queue drops = %d, want 1", dropped)
	}
	checkCommitOutcome(t, l, CommitRolledBack, "wal_write", "enospc")

	healthy := make(chan error, 1)
	go func() { healthy <- l.Append(batch, rows) }()
	if err := awaitTestValue(t, healthy, "successful append while sink is blocked"); err != nil {
		t.Fatal(err)
	}
	checkCommitOutcome(t, l, CommitDurable, "", "")
	closed := make(chan error, 2)
	for range cap(closed) {
		go func() { closed <- l.Close() }()
	}
	for range cap(closed) {
		if err := awaitTestValue(t, closed, "Close while sink is blocked"); err != nil {
			t.Fatal(err)
		}
	}
	checkCommitOutcome(t, l, CommitDurable, "", "")
	restored := &store.Store{}
	reopened, err := Open(dir, restored)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if got := restored.Query(time.Time{}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "", 10); len(got) != 1 || got[0] != rows[0] {
		t.Fatalf("recovered rows = %+v, want %+v", got, rows)
	}
	checkCommitOutcome(t, reopened, CommitNone, "", "")
	release()
	for range diagnosticQueueCapacity {
		if got := awaitTestValue(t, sink.messages, "queued diagnostic"); got != "WAL rollback confirmed operation=wal_sync category=eio\n" {
			t.Fatalf("diagnostic = %q", got)
		}
	}
}
