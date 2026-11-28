package wal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"bog/internal/store"
)

func openEmptyRuntimeWAL(t *testing.T, dir string) *Log {
	t.Helper()
	w, err := OpenOwner(dir, func(w *Log, _ *os.Root, _ *os.File) error {
		_, end, finish, err := w.OpenLegacyRuntime()
		if err != nil {
			return err
		}
		if end != lenMagic {
			t.Fatalf("unexpected legacy end: %d", end)
		}
		return finish(&store.Store{}, 0, 1<<20, lenMagic, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func emptyRetainedReplay(_ io.ReaderAt, a Authority) (*store.Store, error) {
	if a.Covered != a.End || a.First != a.Next {
		return nil, errors.New("expected an empty suffix")
	}
	return &store.Store{}, nil
}

func TestRetainedSelectionFencesBeforeFirstAppend(t *testing.T) {
	dir := t.TempDir()
	w := openEmptyRuntimeWAL(t, dir)
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	syncs := 0
	w.syncDir = func() error {
		syncs++
		if syncs == 2 {
			return syscall.EIO
		}
		return w.dir.Sync()
	}
	point := Authority{Generation: sha256.Sum256([]byte("empty generation")), Covered: lenMagic}
	if err := w.Select(point, nil, 1<<20, emptyRetainedReplay); !errors.Is(err, ErrUncertain) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("selection = %v, want uncertain sync failure", err)
	}
	if _, err := w.Capture(t.Context()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("capture = %v, want fenced WAL", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if legacy, err := Open(dir, &store.Store{}); err == nil {
		t.Fatal(errors.Join(errors.New("legacy open accepted retained authority"), legacy.Close()))
	}
	if _, err := os.Stat(filepath.Join(dir, authorityName)); err != nil {
		t.Fatalf("retained authority lost: %v", err)
	}
}

func TestRetainedSelectorFailureAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failSync  int
		uncertain bool
	}{
		{"tail name sync before selector", 1, false},
		{"selector sync after rename", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			w := openEmptyRuntimeWAL(t, dir)
			payload := []byte("{\"ts\":\"2025-01-02T00:00:00Z\",\"line\":\"row\"}\n")
			rows, err := store.Decode(payload)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Append(payload, rows); err != nil {
				t.Fatal(err)
			}
			old, err := w.Capture(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := old.Release(); err != nil {
					t.Error(err)
				}
			}()
			point := Authority{Generation: sha256.Sum256([]byte("test generation")), Covered: old.CommittedEnd(), First: 1}
			calls := 0
			w.syncDir = func() error {
				calls++
				if calls == tc.failSync {
					return syscall.ENOSPC
				}
				return w.dir.Sync()
			}
			err = w.Select(point, nil, 1<<20, emptyRetainedReplay)
			if !errors.Is(err, syscall.ENOSPC) || errors.Is(err, ErrUncertain) != tc.uncertain {
				t.Fatalf("selection error: %v", err)
			}
			if tc.uncertain {
				if _, err := w.Capture(t.Context()); !errors.Is(err, ErrUncertain) {
					t.Fatalf("capture after uncertain authority: %v", err)
				}
				if err := w.Append(payload, rows); !errors.Is(err, ErrUncertain) {
					t.Fatalf("append after uncertain authority: %v", err)
				}
			} else {
				if _, err := w.ReadAuthority(); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed selection changed the selector: %v", err)
				}
				if err := w.Append(payload, rows); err != nil {
					t.Fatal(err)
				}
			}
			frozen, err := io.ReadAll(io.NewSectionReader(old, lenMagic+16, int64(len(payload))))
			if err != nil || !bytes.Equal(frozen, payload) {
				t.Fatalf("pinned predecessor lost committed bytes: %q, %v", frozen, err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if !tc.uncertain {
				return
			}
			reopened, err := OpenOwner(dir, func(w *Log, _ *os.Root, _ *os.File) error {
				a, err := w.ReadAuthority()
				if err != nil {
					return err
				}
				if a.Generation != point.Generation || a.Covered != point.Covered || a.First != 1 || a.Next != 1 {
					t.Fatalf("wrong recovered authority: %+v", a)
				}
				return w.RecoverAuthority(a, 1<<20, emptyRetainedReplay)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetainedTailReclamationWaitsForCapturedInode(t *testing.T) {
	dir := t.TempDir()
	w := openEmptyRuntimeWAL(t, dir)
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	payload := []byte("{\"ts\":\"2025-01-02T00:00:00Z\",\"line\":\"row\"}\n")
	rows, err := store.Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(payload, rows); err != nil {
		t.Fatal(err)
	}
	before, _, point, err := w.View(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := before.Release(); err != nil {
			t.Error(err)
		}
	}()
	if err := w.Select(Authority{Generation: sha256.Sum256([]byte("generation")), Covered: point.End, First: point.Next},
		nil, 1<<20, emptyRetainedReplay); err != nil {
		t.Fatal(err)
	}
	if err := w.ReclaimTails(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, walName)); err != nil {
		t.Fatalf("predecessor reclaimed while pinned: %v", err)
	}
	first, err := w.ReadAuthority()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(payload, rows); err != nil {
		t.Fatal(err)
	}
	second, _ := w.PublishedAuthority()
	if err := w.Select(Authority{Generation: sha256.Sum256([]byte("successor")), Covered: second.End, First: second.Next},
		nil, 1<<20, emptyRetainedReplay); err != nil {
		t.Fatal(err)
	}
	if err := w.ReclaimTails(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, first.name())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unpinned tail retained behind an older pin: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, walName)); err != nil {
		t.Fatalf("oldest tail disappeared while pinned: %v", err)
	}
	if err := before.Release(); err != nil {
		t.Fatal(err)
	}
	if err := w.ReclaimTails(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{walName, markerName} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected retired file %s to be removed: %v", name, err)
		}
	}
	if err := w.AppendWaiting(context.Background(), payload, rows); err != nil {
		t.Fatal(err)
	}
	a, err := w.ReadAuthority()
	if err != nil {
		t.Fatal(err)
	}
	if a.Covered != second.End || a.End <= a.Covered || a.First != 2 || a.Next != 3 {
		t.Fatalf("append did not update retained authority: %+v", a)
	}
}
