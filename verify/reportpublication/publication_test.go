package reportpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishRejectsInvalidRequestsBeforeStaging(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mode Mode
		want error
	}{
		{"", Replace, nil},
		{".", Replace, nil},
		{"..", Replace, nil},
		{"a/b", Replace, nil},
		{"escaped\x00name", Replace, nil},
		{"valid", 42, nil},
		{"valid", CreateOnly, errors.ErrUnsupported},
	} {
		path := t.TempDir()
		dir, err := OpenDirectory(path)
		if err != nil {
			t.Fatal(err)
		}
		state, err := Publish(t.Context(), struct{ Directory }{dir}, tc.name, []byte("unused"), tc.mode)
		if state != Unpublished || err == nil || tc.want != nil && !errors.Is(err, tc.want) {
			t.Errorf("Publish(%q, %d) = %q, %v", tc.name, tc.mode, state, err)
		}
		if err := dir.Close(); err != nil {
			t.Fatal(err)
		}
		if entries, err := os.ReadDir(path); err != nil || len(entries) != 0 {
			t.Fatalf("staging entries = %v, %v", entries, err)
		}
	}
}

type collidingDirectory struct {
	Directory
	collisions, attempts int
}

func (d *collidingDirectory) OpenExclusive(name string) (StagingFile, error) {
	d.attempts++
	if d.attempts <= d.collisions {
		return nil, os.ErrExist
	}
	return d.Directory.OpenExclusive(name)
}

func TestStagingCollisions(t *testing.T) {
	t.Parallel()
	for _, collisions := range []int{15, 16} {
		path := t.TempDir()
		dir, err := OpenDirectory(path)
		if err != nil {
			t.Fatal(err)
		}
		d := &collidingDirectory{Directory: dir, collisions: collisions}
		state, err := Publish(t.Context(), d, "report", []byte("new"), Replace)
		if d.attempts != 16 {
			t.Errorf("attempts = %d, want 16", d.attempts)
		}
		got, readErr := os.ReadFile(filepath.Join(path, "report"))
		if collisions == 15 {
			if state != DurablyPublished || err != nil || readErr != nil || string(got) != "new" {
				t.Errorf("publication = %q, %v; bytes = %q, %v", state, err, got, readErr)
			}
		} else if state != Unpublished || err == nil || !errors.Is(readErr, os.ErrNotExist) {
			t.Errorf("exhausted publication = %q, %v; bytes = %q, %v", state, err, got, readErr)
		}
		if err := dir.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
