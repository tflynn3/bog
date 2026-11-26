package wal

import (
	"errors"
	"fmt"
	"os"
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

func captureDiagnostics(l *Log) <-chan rollbackDiagnostic {
	out := make(chan rollbackDiagnostic, 2)
	l.diagnosticQueue = out
	return out
}

func checkDiagnostic(t *testing.T, out <-chan rollbackDiagnostic, want rollbackDiagnostic) {
	t.Helper()
	select {
	case event := <-out:
		if event != want {
			t.Fatalf("diagnostic = %+v, want %+v", event, want)
		}
	default:
		t.Fatalf("missing diagnostic, want %+v", want)
	}
}

func checkNoDiagnostic(t *testing.T, out <-chan rollbackDiagnostic) {
	t.Helper()
	select {
	case event := <-out:
		t.Fatalf("unexpected diagnostic: %+v", event)
	default:
	}
}

func closeLog(t *testing.T, l *Log) {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Error(err)
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
			ff := &diagnosticFile{file: f, failure: tc.failure, syncFail: tc.sync, truncateFail: tc.uncertain}
			if !tc.sync {
				ff.writeAt = 1
			}
			l := &Log{f: ff, s: &store.Store{}}
			t.Cleanup(func() { closeLog(t, l) })
			out := captureDiagnostics(l)
			payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"secret payload"}`)
			rows, err := store.Decode(payload)
			if err != nil {
				t.Fatal(err)
			}
			err = l.Append(payload, rows)
			if tc.uncertain {
				if !errors.Is(err, ErrUncertain) || !errors.Is(err, tc.failure) {
					t.Fatalf("append error = %v", err)
				}
				checkNoDiagnostic(t, out)
				return
			}
			if err != tc.failure {
				t.Fatalf("original error lost: %v", err)
			}
			checkDiagnostic(t, out, rollbackDiagnostic{tc.operation, tc.category})
			checkNoDiagnostic(t, out)
		})
	}
}

func TestDiagnosticRateLimit(t *testing.T) {
	l := &Log{}
	out := captureDiagnostics(l)
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	l.diagnoseRollback("wal_write", syscall.ENOSPC, now)
	l.diagnoseRollback("wal_sync", syscall.EIO, now.Add(time.Second))
	l.diagnoseRollback("marker_preparation", errors.New("private"), now.Add(time.Minute))
	checkDiagnostic(t, out, rollbackDiagnostic{"wal_write", "enospc"})
	checkDiagnostic(t, out, rollbackDiagnostic{"marker_preparation", "other"})
	checkNoDiagnostic(t, out)
	if got := l.DroppedDiagnostics(); got != 0 {
		t.Fatalf("rate-limited event counted as queue drop: %d", got)
	}
}

func TestDiagnosticRecoveryOfFailedGroup(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	l, err := Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeLog(t, l)
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
	checkDiagnostic(t, out, rollbackDiagnostic{"wal_write", "enospc"})
	checkNoDiagnostic(t, out)
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
	defer closeLog(t, l)
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
	defer closeLog(t, l)
	check(s, "acknowledged", "healthy")
}

func TestMarkerPreparationDiagnostic(t *testing.T) {
	l, err := Open(t.TempDir(), &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeLog(t, l)
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
	checkDiagnostic(t, out, rollbackDiagnostic{"marker_preparation", "other"})
}
