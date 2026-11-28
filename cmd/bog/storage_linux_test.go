package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

type testBackupView struct{ *recoveryView }

func (v testBackupView) Point() backup.RecoveryPoint {
	return backup.RecoveryPoint(v.recoveryView.Point())
}

func TestWritableRestoreActivation(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	source, err := openRetained(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := source.close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"restored"}`)
	rows, err := store.DecodeIngest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Append(payload, rows); err != nil {
		t.Fatal(err)
	}
	view, err := source.pinLocked(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.close(); err != nil {
			t.Error(err)
		}
	}()
	view.point.SuffixSHA256, err = rangeDigest(view.tail, view.point.CoveredOffset, view.point.CommittedEnd)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(dir, "bundle")
	if err := backup.ExportRecoveryView(ctx, testBackupView{&view.recoveryView}, bundle, wal.MaxTailBytes); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"fresh", "interrupted preparation"} {
		t.Run(name, func(t *testing.T) {
			restored := filepath.Join(t.TempDir(), "restored")
			if err := backup.Restore(t.Context(), bundle, restored); err != nil {
				t.Fatal(err)
			}
			const orphanTail = "bog.wal.tail-00000000000000000000000000000000"
			if name == "interrupted preparation" {
				data, err := os.ReadFile(filepath.Join(restored, ordinaryManifest))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(restored, checkpoint.GenerationManifestName(view.point.Generation)), data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(restored, orphanTail), []byte("partial"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			active, err := openRetained(restored)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := active.close(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			if _, err := os.Stat(filepath.Join(restored, orphanTail)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("interrupted tail remains: %v", err)
			}
			a, err := active.wal.ReadAuthority()
			if err != nil || a.CommittedOrdinal != 1 || a.CommittedEnd != view.point.CommittedEnd {
				t.Fatalf("restored authority = %+v, %v", a, err)
			}
			if err := active.Append(payload, rows); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAutomaticCompactionDiskFull(t *testing.T) {
	mount := os.Getenv("SLIPWAY_PARAM_ENOSPC_MOUNT")
	if mount == "" {
		t.Skip("requires a private tmpfs via SLIPWAY_PARAM_ENOSPC_MOUNT")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(mount, &fs); err != nil {
		t.Fatal(err)
	}
	const tmpfsMagic = 0x01021994
	if fs.Type != tmpfsMagic || fs.Bsize <= 0 || fs.Blocks > (64<<20)/uint64(fs.Bsize) || fs.Bavail != fs.Blocks {
		t.Fatalf("want an empty private tmpfs of at most 64 MiB: %+v", fs)
	}
	dir, err := os.MkdirTemp(mount, "compaction-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
	s, err := openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.stopMaintenance()
		if err := s.close(context.Background()); err != nil {
			t.Error(err)
		}
	}()

	const before = `{"ts":"2025-01-01T00:00:00Z","line":"witness before"}`
	const after = `{"ts":"2025-01-01T00:00:01Z","line":"witness after"}`
	for i := range checkpointFrames {
		payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"seed"}`)
		if i == 0 {
			payload = []byte(before)
		}
		rows, err := store.DecodeIngest(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.wal.Append(payload, rows); err != nil {
			t.Fatal(err)
		}
	}

	// Let the automatic worker stage a complete checkpoint, then hold its
	// selector behind the WAL mutation lock while filling the filesystem.
	gate := s.wal.BeginReplacement()
	defer func() {
		if gate != nil {
			gate.Close()
		}
	}()
	s.startMaintenance(false)
	s.signalMaintenance()
	wait := func(what string, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !ready() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	wait("the staged checkpoint", func() bool {
		names, err := filepath.Glob(filepath.Join(dir, "bog-checkpoint-*"))
		return err == nil && len(names) == 1
	})
	if err := syscall.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Bavail < 2 {
		t.Fatal("need two free blocks before filling the fixture")
	}
	filler := filepath.Join(dir, "fill")
	f, err := os.OpenFile(filler, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Leave one block. The first selection needs both a guard and a selector.
	err = syscall.Fallocate(int(f.Fd()), 0, 0, int64(fs.Bavail-1)*int64(fs.Bsize))
	if err := errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	gate.Close()
	gate = nil
	var failure error
	wait("the selector refusal", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		failure = s.failed
		return failure != nil
	})
	var pathErr *os.PathError
	if !errors.Is(failure, syscall.ENOSPC) || !errors.As(failure, &pathErr) ||
		!strings.HasPrefix(filepath.Base(pathErr.Path), ".bog-commit-") || errors.Is(failure, checkpoint.ErrPublicationUncertain) {
		t.Fatalf("expected a definite selector preparation failure: %v", failure)
	}
	if _, err := s.wal.ReadAuthority(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused compaction changed authority: %v", err)
	}

	c, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := s.root.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	handler, api := newReadyHandler(ctx, c, s, probe, false)
	defer func() {
		cancel()
		if err := errors.Join(api.Close(context.Background()), probe.Close()); err != nil {
			t.Error(err)
		}
	}()
	request := func(method, path, body string, status int, want string) {
		t.Helper()
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, httptest.NewRequest(method, path, strings.NewReader(body)))
		if out.Code != status || out.Body.String() != want {
			t.Fatalf("%s %s = %d %q, want %d %q", method, path, out.Code, out.Body, status, want)
		}
	}
	const query = "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=witness"
	request(http.MethodGet, query, "", http.StatusOK, before+"\n")
	large := `{"ts":"2025-01-01T00:00:00Z","line":"witness ` + strings.Repeat("x", 1<<20) + `"}`
	request(http.MethodPost, "/ingest", large, http.StatusServiceUnavailable, "disk full\n")
	request(http.MethodGet, query, "", http.StatusOK, before+"\n")
	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	request(http.MethodPost, "/ingest", after, http.StatusNoContent, "")
	wait("checkpoint advancement", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.current.Boundary.NextOrdinal >= checkpointFrames
	})
	request(http.MethodGet, query, "", http.StatusOK, before+"\n"+after+"\n")
}

func TestRecoverInterruptedFirstRetainedSelection(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			if err := s.close(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})

	payload := []byte("{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"before selection\"}\n")
	rows, err := store.DecodeIngest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(payload, rows); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	pin, err := s.pinLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	pin.point.SuffixSHA256, err = rangeDigest(pin.tail, pin.point.CoveredOffset, pin.point.CommittedEnd)
	if err != nil {
		t.Fatal(errors.Join(err, pin.close()))
	}
	interrupt := errors.New("interrupted before selector")
	var staged string
	_, err = s.publisher.PublishRecoveryView(ctx, &pin.recoveryView, pin.point.CommittedEnd, pin.point.CommittedOrdinal,
		func(_ context.Context, next checkpoint.RecoveryPoint) error {
			staged = checkpoint.GenerationManifestName(next.Generation)
			return interrupt
		})
	if err := errors.Join(pin.close(), s.close(ctx)); err != nil {
		t.Fatal(err)
	}
	s = nil
	if !errors.Is(err, checkpoint.ErrPublicationUncertain) || !errors.Is(err, interrupt) {
		t.Fatalf("publication error = %v", err)
	}

	s, err = openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.root.Stat(staged); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted successor manifest remains: %v", err)
	}
	if s.current.Boundary.NextOrdinal != 0 {
		t.Fatalf("wrong checkpoint selected: %+v", s.current.Boundary)
	}
	c, err := s.wal.Capture(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Release(); err != nil {
			t.Error(err)
		}
	}()
	if c.NextOrdinal() != 1 {
		t.Fatalf("acknowledged row lost: committed ordinal %d", c.NextOrdinal())
	}
}

func TestRecoverFirstSelectionBeforeOrdinaryRetirement(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendLine := func(line string) {
		t.Helper()
		payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"` + line + `"}`)
		rows, err := store.DecodeIngest(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Append(payload, rows); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if s != nil {
			if err := s.close(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})
	appendLine("covered")
	appendLine("carried")
	if err := s.compact(ctx, compactionJob{cutoff: 1}); err != nil {
		t.Fatal(err)
	}
	a, err := s.wal.ReadAuthority()
	if err != nil {
		t.Fatal(err)
	}
	// Stop immediately after durable publication, before runtime reclamation.
	closeErr := errors.Join(s.catalog.Close(), s.wal.Close())
	s = nil
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	names := []string{"bog.wal", "bog.commit", ordinaryManifest}
	for _, name := range names {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	s, err = openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("superseded %s remains: %v", name, err)
		}
	}
	reopened, err := s.wal.ReadAuthority()
	if err != nil || reopened != a || a.NextOrdinal != 1 || a.CommittedOrdinal != 2 {
		t.Fatalf("selected authority = %+v, %v, want %+v at cutoff 1 with two commits", reopened, err, a)
	}

	appendLine("new")
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	page, err := s.Page(ctx, httpapi.PageRequest{
		Filter: httpapi.PageFilter{Start: from, End: from.Add(time.Hour)},
		Limit:  10,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Release(page.Capture); err != nil {
		t.Fatal(err)
	}
	if page.More || len(page.Rows) != 3 ||
		page.Rows[0].Row.Line != "covered" || page.Rows[1].Row.Line != "carried" || page.Rows[2].Row.Line != "new" {
		t.Fatalf("query after restart and append: %+v", page)
	}
}

func TestMissingRetainedSelectorFailsClosed(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendRow := func(line string) error {
		p := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"` + line + `"}`)
		rows, err := store.DecodeIngest(p)
		if err != nil {
			return err
		}
		return s.Append(p, rows)
	}
	if err := appendRow("before"); err != nil {
		t.Fatal(errors.Join(err, s.close(ctx)))
	}
	if err := s.compact(ctx, compactionJob{cutoff: 1}); err != nil {
		t.Fatal(errors.Join(err, s.close(ctx)))
	}
	if err := appendRow("after"); err != nil {
		t.Fatal(errors.Join(err, s.close(ctx)))
	}
	a, err := s.wal.ReadAuthority()
	if err := errors.Join(err, s.close(ctx)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, wal.AuthorityName)); err != nil {
		t.Fatal(err)
	}
	if reopened, err := openRetained(dir); err == nil {
		t.Fatal(errors.Join(errors.New("opened without the retained selector"), reopened.close(ctx)))
	} else if !strings.Contains(err.Error(), "missing writable authority") {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(dir, a.Tail)); err != nil || info.Size() == 0 {
		t.Fatalf("committed tail changed after failed startup: %v, %v", info, err)
	}
}
