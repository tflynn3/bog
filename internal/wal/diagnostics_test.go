package wal

import (
	"bytes"
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

type gatedDiagnosticSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedDiagnosticSink) Emit(DiagnosticEvent) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
}

func TestAsyncSinkDropsWithoutBlocking(t *testing.T) {
	consumer := &gatedDiagnosticSink{entered: make(chan struct{}), release: make(chan struct{})}
	sink := NewAsyncSink(1, consumer)
	defer close(consumer.release)
	event := DiagnosticEvent{OpCode: OpWALWrite, ErrCategory: ErrCategoryENOSPC}
	sink.Emit(event)
	select {
	case <-consumer.entered:
	case <-time.After(time.Second):
		t.Fatal("consumer did not start")
	}
	sink.Emit(event) // fill the queue while the consumer is blocked
	before := DiagnosticsDropped.Load()
	done := make(chan struct{})
	go func() { sink.Emit(event); sink.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emit or stop blocked on consumer")
	}
	if got := DiagnosticsDropped.Load() - before; got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	select {
	case <-sink.Done():
		t.Fatal("worker exited while consumer was blocked")
	default:
	}
}

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

func captureDiagnostics(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(&out)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() { log.SetOutput(oldWriter); log.SetFlags(oldFlags); log.SetPrefix(oldPrefix) })
	return &out
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
			out := captureDiagnostics(t)
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
			payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"secret payload"}`)
			rows, err := store.Decode(payload)
			if err != nil {
				t.Fatal(err)
			}
			err = l.Append(payload, rows)
			if tc.uncertain {
				if !errors.Is(err, ErrUncertain) || out.Len() != 0 {
					t.Fatalf("error=%v log=%q", err, out.String())
				}
				return
			}
			if err != tc.failure {
				t.Fatalf("original error lost: %v", err)
			}
			want := "WAL rollback confirmed operation=" + tc.operation + " category=" + tc.category + "\n"
			if out.String() != want {
				t.Fatalf("diagnostic = %q, want %q", out.String(), want)
			}
		})
	}
}

func TestDiagnosticRateLimit(t *testing.T) {
	out := captureDiagnostics(t)
	l := &Log{}
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	l.diagnoseRollback("wal_write", syscall.ENOSPC, now)
	l.diagnoseRollback("wal_sync", syscall.EIO, now.Add(time.Second))
	l.diagnoseRollback("marker_preparation", errors.New("private"), now.Add(time.Minute))
	want := "WAL rollback confirmed operation=wal_write category=enospc\nWAL rollback confirmed operation=marker_preparation category=other\n"
	if out.String() != want {
		t.Fatalf("diagnostics = %q", out.String())
	}
}

func TestDiagnosticRecoveryOfFailedGroup(t *testing.T) {
	out := captureDiagnostics(t)
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
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
	if got := out.String(); got != "WAL rollback confirmed operation=wal_write category=enospc\n" || strings.Contains(got, "secret") || strings.Contains(got, "private") {
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
	out := captureDiagnostics(t)
	l, err := Open(t.TempDir(), &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
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
	if got := out.String(); got != "WAL rollback confirmed operation=marker_preparation category=other\n" {
		t.Fatalf("diagnostic = %q", got)
	}
}
