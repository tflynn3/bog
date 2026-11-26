package checkpoint

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestRecoverModeZeroStaging(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires a non-root Linux user")
	}
	for _, existing := range []bool{false, true} {
		name := "first publication"
		if existing {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			dir, _, _ := fixture(t)
			l, err := OpenLegacy(dir)
			requirePublicationOK(t, err)
			closeOnCleanup(t, l)
			var old *Manifest
			if existing {
				old, err = l.Publish()
				requirePublicationOK(t, err)
			}
			lock, err := LockPublication(l.dir, true)
			requirePublicationOK(t, err)
			t.Cleanup(func() {
				if err := lock.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error(err)
				}
			})
			// Model exit between mkdir and chmod under a hostile umask.
			err = func() error {
				mask := syscall.Umask(0777)
				defer syscall.Umask(mask)
				return l.root.Mkdir(stageDir, 0700)
			}()
			requirePublicationOK(t, err)
			requirePublicationOK(t, l.dir.Sync())
			staging, err := openStaging(l.dir)
			if staging != nil {
				requirePublicationOK(t, staging.Close())
			}
			if !errors.Is(err, os.ErrPermission) {
				t.Fatalf("staging should be inaccessible: %v", err)
			}
			if _, err := l.OpenPublished(); !errors.Is(err, syscall.EWOULDBLOCK) {
				t.Fatalf("recovery bypassed competing publisher's lock: %v", err)
			}
			requirePublicationOK(t, lock.Close())
			requirePublicationOK(t, l.Close())

			l, err = OpenLegacy(dir)
			requirePublicationOK(t, err)
			closeOnCleanup(t, l)
			if existing {
				recovered, err := l.OpenPublished()
				requirePublicationOK(t, err)
				if recovered.Manifest().Segments[0] != old.Segments[0] {
					t.Fatal("recovery replaced the authoritative manifest")
				}
			} else if err := recoverCaptured(l); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("no authority expected: %v", err)
			}
			requirePublicationOK(t, CheckPublication(l.dir))
			_, err = l.Publish()
			requirePublicationOK(t, err)
		})
	}
}

func TestRecoverRejectsUnsafeStaging(t *testing.T) {
	for _, name := range []string{
		"symlink", "regular file", "foreign mode zero directory", "foreign private directory",
		"nonempty mode zero directory", "unrecognized private entry", "staged symlink",
		"ambiguous stage with durable intent",
	} {
		t.Run(name, func(t *testing.T) {
			foreign := name == "foreign mode zero directory" || name == "foreign private directory"
			if foreign && os.Geteuid() != 0 {
				t.Skip("chown fixture requires root")
			}
			dir, l, _ := capturedPublicationFixture(t)
			m, err := l.Publish()
			requirePublicationOK(t, err)
			old := readFixture(t, dir, manifestName)
			segment := m.Segments[0].Name
			originalSegment := readFixture(t, dir, segment)
			stage := filepath.Join(dir, stageDir)
			evidence := filepath.Join(stage, "manifest")
			var installed string
			switch name {
			case "symlink":
				requirePublicationOK(t, os.Symlink(dir, stage))
			case "regular file":
				requirePublicationOK(t, os.WriteFile(stage, []byte("do not remove"), 0600))
			default:
				requirePublicationOK(t, os.Mkdir(stage, 0700))
				switch name {
				case "foreign mode zero directory", "foreign private directory":
					requirePublicationOK(t, os.Chown(stage, 65534, -1))
				case "nonempty mode zero directory":
					requirePublicationOK(t, os.WriteFile(evidence, []byte("ambiguous"), 0600))
				case "unrecognized private entry", "ambiguous stage with durable intent":
					evidence = filepath.Join(stage, "unexpected")
					requirePublicationOK(t, os.WriteFile(evidence, []byte("ambiguous"), 0600))
				case "staged symlink":
					requirePublicationOK(t, os.Symlink(filepath.Join(dir, segment), evidence))
				}
				if name == "foreign mode zero directory" || name == "nonempty mode zero directory" {
					requirePublicationOK(t, os.Chmod(stage, 0000))
					t.Cleanup(func() { requirePublicationOK(t, os.Chmod(stage, 0700)) })
				}
				if name == "ambiguous stage with durable intent" {
					requirePublicationOK(t, withTestPublication(l, func(p *publication) error {
						a := publicationAttempt{pub: p, ctx: t.Context()}
						installed = segmentPrefix + "55555555555555555555555555555555"
						if err := a.stageSegment(installed, originalSegment, digest(originalSegment)); err != nil {
							return err
						}
						return a.install()
					}))
				}
			}

			before, err := os.Lstat(stage)
			requirePublicationOK(t, err)
			if err := recoverCaptured(l); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("recovery accepted unsafe staging: %v", err)
			}
			if _, err := l.Publish(); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("publication accepted unsafe staging: %v", err)
			}
			after, err := os.Lstat(stage)
			requirePublicationOK(t, err)
			if !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatal("recovery changed unsafe staging")
			}
			if name == "nonempty mode zero directory" {
				requirePublicationOK(t, os.Chmod(stage, 0700))
			}
			switch name {
			case "nonempty mode zero directory", "unrecognized private entry", "staged symlink", "ambiguous stage with durable intent":
				_, err := os.Lstat(evidence)
				requirePublicationOK(t, err)
			}
			if installed != "" {
				if got := readFixture(t, dir, installed); !bytes.Equal(got, originalSegment) {
					t.Fatal("failed recovery changed pending segment")
				}
			}
			if got := readFixture(t, dir, manifestName); !bytes.Equal(got, old) {
				t.Fatal("failed recovery changed authoritative manifest")
			}
			if got := readFixture(t, dir, segment); !bytes.Equal(got, originalSegment) {
				t.Fatal("failed recovery changed reachable segment")
			}
		})
	}
}

func withTestPublication(l *Legacy, run func(*publication) error) (err error) {
	lock, err := LockPublication(l.dir, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	return run(l.publication())
}
