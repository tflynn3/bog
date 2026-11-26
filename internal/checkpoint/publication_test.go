package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

func checkpointNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func requirePublicationOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefinitivePublicationFailureCleansOnlyAttempt(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	old, err := l.Publish()
	requirePublicationOK(t, err)
	unknown := segmentPrefix + "11111111111111111111111111111111"
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, unknown), []byte("unknown ownership"), 0600))
	want := checkpointNames(t, dir)
	l.segmentDirSync = func() error { return syscall.EIO }
	for range 3 {
		if m, err := l.Publish(); m != nil || !errors.Is(err, syscall.EIO) || errors.Is(err, ErrPublicationUncertain) {
			t.Fatalf("definitive failure returned manifest %v and error %v", m, err)
		}
		if got := checkpointNames(t, dir); !slices.Equal(got, want) {
			t.Fatalf("directory after failed publication: got %v, want %v", got, want)
		}
		p, err := l.OpenPublished()
		requirePublicationOK(t, err)
		if !slices.Equal(p.Manifest().Segments, old.Segments) {
			t.Fatal("failed publication changed authoritative segments")
		}
	}
	l.segmentDirSync = nil
	next, err := l.Publish()
	requirePublicationOK(t, err)
	if slices.Equal(old.Segments, next.Segments) {
		t.Fatal("retry did not install a successor")
	}
	if got := readFixture(t, dir, unknown); string(got) != "unknown ownership" {
		t.Fatal("unowned file changed")
	}
}

func TestRecoveryPreservesCollisionAndRemovesInstalledName(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	_, err = l.Publish()
	requirePublicationOK(t, err)
	requirePublicationOK(t, l.root.Mkdir(stageDir, 0700))
	a := publicationAttempt{legacy: l}
	first := segmentPrefix + "22222222222222222222222222222222"
	collision := segmentPrefix + "33333333333333333333333333333333"
	_, blocks := blockSeeds(t)
	requirePublicationOK(t, a.stageSegment(first, blocks[0], digest(blocks[0])))
	requirePublicationOK(t, a.stageSegment(collision, blocks[1], digest(blocks[1])))
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, collision), []byte("preexisting file"), 0600))
	if err := a.install(); !errors.Is(err, os.ErrExist) {
		t.Fatalf("expected final name collision: %v", err)
	}
	// Bypass the normal failure defer to model interruption after a partial install.
	requirePublicationOK(t, l.Close())
	l, err = OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	_, err = l.OpenPublished()
	requirePublicationOK(t, err)
	if got := readFixture(t, dir, collision); string(got) != "preexisting file" {
		t.Fatal("recovery removed or changed a name collision")
	}
	for _, name := range []string{first, stageDir} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned path %q remains after recovery: %v", name, err)
		}
	}
}

func TestUncertainPublicationWithUnreadableAuthorityLeavesAllFiles(t *testing.T) {
	dir, _, _ := fixture(t)
	l, err := OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	_, err = l.Publish()
	requirePublicationOK(t, err)
	good := readFixture(t, dir, manifestName)
	l.manifestRename = func(string, string) error { return syscall.EIO }
	if _, err := l.Publish(); !errors.Is(err, ErrPublicationUncertain) {
		t.Fatalf("expected uncertainty: %v", err)
	}
	requirePublicationOK(t, l.Close())
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, manifestName), []byte("damaged"), 0600))
	before := checkpointNames(t, dir)
	l, err = OpenLegacy(dir)
	requirePublicationOK(t, err)
	closeOnCleanup(t, l)
	if _, err := l.OpenPublished(); err == nil {
		t.Fatal("corrupt authority accepted")
	}
	requirePublicationOK(t, os.Remove(filepath.Join(dir, manifestName)))
	if _, err := l.OpenPublished(); !errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing previous manifest: %v", err)
	}
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, manifestName), []byte("damaged"), 0600))
	if got := checkpointNames(t, dir); !slices.Equal(got, before) {
		t.Fatalf("recovery mutated ambiguous files: %v, want %v", got, before)
	}
	requirePublicationOK(t, os.WriteFile(filepath.Join(dir, manifestName), good, 0600))
	recovered, err := l.Publish()
	requirePublicationOK(t, err)
	if recovered == nil || len(recovered.Segments) != 1 {
		t.Fatalf("retry did not publish: %+v", recovered)
	}
	if len(checkpointNames(t, dir)) != len(before)-1 {
		t.Fatalf("retry retained losing attempt files: %v", checkpointNames(t, dir))
	}
}

func TestRecoverBeforeManifestAttempt(t *testing.T) {
	for _, phase := range []string{"staged only", "intent durable", "final and staging links", "final link only"} {
		t.Run(phase, func(t *testing.T) {
			dir, _, _ := fixture(t)
			l, err := OpenLegacy(dir)
			requirePublicationOK(t, err)
			closeOnCleanup(t, l)

			name := segmentPrefix + "44444444444444444444444444444444"
			staged := stageDir + "/" + name
			requirePublicationOK(t, l.root.Mkdir(stageDir, 0700))
			a := publicationAttempt{legacy: l}
			_, blocks := blockSeeds(t)
			requirePublicationOK(t, a.stageSegment(name, blocks[0], digest(blocks[0])))
			if phase != "staged only" {
				requirePublicationOK(t, a.recordIntent())
			}
			if phase == "final and staging links" || phase == "final link only" {
				requirePublicationOK(t, l.root.Link(staged, name))
			}
			if phase == "final link only" {
				requirePublicationOK(t, l.root.Remove(staged))
			}
			requirePublicationOK(t, l.Close())

			l, err = OpenLegacy(dir)
			requirePublicationOK(t, err)
			closeOnCleanup(t, l)
			if _, err := l.OpenPublished(); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("first publication had no authority: %v", err)
			}
			for _, path := range []string{stageDir, name} {
				if _, err := os.Lstat(filepath.Join(dir, path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("orphan %q survived recovery: %v", path, err)
				}
			}
			if _, err := l.Publish(); err != nil {
				t.Fatalf("retry after recovery: %v", err)
			}
		})
	}
}
