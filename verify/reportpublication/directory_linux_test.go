package reportpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateOnlyPreservesDanglingSymlink(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	path := filepath.Join(parent, "report")
	if err := os.Symlink("missing-target", path); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDirectory(parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	state, err := Publish(t.Context(), dir, "report", []byte("new"), CreateOnly)
	if state != Unpublished || !errors.Is(err, os.ErrExist) {
		t.Fatalf("create-only symlink conflict = %q, %v", state, err)
	}
	if target, err := os.Readlink(path); err != nil || target != "missing-target" {
		t.Fatalf("destination symlink target = %q, %v", target, err)
	}
	if entries, err := os.ReadDir(parent); err != nil || len(entries) != 1 || entries[0].Name() != "report" {
		t.Fatalf("destination entries = %v, %v", entries, err)
	}
}
