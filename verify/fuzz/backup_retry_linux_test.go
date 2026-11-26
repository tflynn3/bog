package fuzz

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"bog/internal/backup"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	backupGuard       = ".bog-backup-v1-lock"
	backupClaimPrefix = ".bog-backup-v1-claim-"
	backupStagePrefix = ".bog-backup-v1-stage-"
	legacyStage       = ".bog-backup-legacy"
)

func snapshotFromWAL(t *testing.T, path string) capture {
	t.Helper()
	var s store.Store
	log, err := wal.Open(path, &s)
	must(t, err)
	defer func() { must(t, log.Close()) }()
	c, err := log.Capture(t.Context())
	must(t, err)
	defer func() { must(t, c.Release()) }()
	var catalog history.Catalog
	defer func() { must(t, catalog.Close()) }()
	g, err := catalog.Replace(t.Context(), path)
	must(t, err)
	if c.CommittedEnd() < g.Boundary.CoveredOffset || c.CommittedEnd() > maxArtifactSize || c.NextOrdinal() < g.Boundary.NextOrdinal {
		t.Fatalf("unexpected captured boundary: %+v, WAL end %d ordinal %d", g.Boundary, c.CommittedEnd(), c.NextOrdinal())
	}
	data := make([]byte, c.CommittedEnd())
	_, err = io.ReadFull(io.NewSectionReader(c, 0, c.CommittedEnd()), data)
	must(t, err)
	return capture{
		point: backup.Point{
			Generation: [32]byte(g.ID), CoveredOffset: g.Boundary.CoveredOffset,
			NextOrdinal: g.Boundary.NextOrdinal, CommittedEnd: c.CommittedEnd(),
		},
		files: historyFiles(readFiles(t, path)),
		wal:   data,
	}
}

func backupFixture(t *testing.T) (string, capture, []backup.Row) {
	t.Helper()
	path := t.TempDir()
	f := newRecoveryFixture(t, path)
	publishFixtureCheckpoint(t, path, f)
	return path, snapshotFromWAL(t, path), checkpointRows(f.rows)
}

func stagePaths(t *testing.T, dest string) (stage, claim string) {
	t.Helper()
	key := digest([]byte(filepath.Base(dest)))
	parent := filepath.Dir(dest)
	claim = filepath.Join(parent, backupClaimPrefix+key)
	token, err := os.Readlink(claim)
	if errors.Is(err, os.ErrNotExist) {
		return "", claim
	}
	must(t, err)
	if len(token) != 32 || strings.Trim(token, "0123456789abcdef") != "" {
		t.Fatalf("read stage claim %s: invalid nonce %q", claim, token)
	}
	return filepath.Join(parent, backupStagePrefix+key+"-"+token), claim
}

func assertStagedCopy(t *testing.T, dest string, source files) (string, string) {
	t.Helper()
	stage, claim := stagePaths(t, dest)
	if stage == "" {
		t.Fatal("copy barrier reached without a destination claim")
	}
	owner, err := os.Readlink(claim + ".owner")
	must(t, err)
	if len(owner) != 40 {
		t.Fatalf("invalid owner record length %d", len(owner))
	}
	copied := readFiles(t, stage)
	if len(copied) >= len(source) || !slices.Equal(copied[manifestFile], source[manifestFile]) {
		t.Fatalf("copy barrier: got staged files %v, want a complete manifest in a partial image", fileNames(copied))
	}
	for name, data := range copied {
		if !slices.Equal(data, source[name]) {
			t.Fatalf("staged copy differs from source for %s", name)
		}
	}
	t.Logf("phase=copy-before-publication destination=%s staged_files=%v", filepath.Base(dest), fileNames(copied))
	return stage, claim
}

func assertAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("check absent entry %s: %v", path, err)
	}
}

func interruptBackup(t *testing.T, mode, source, dest string, park func()) {
	var snap capture
	var manifestSize int
	if mode != "restore" {
		snap = snapshotFromWAL(t, source)
		manifestSize = len(snap.files[manifestFile])
	} else {
		manifestSize = len(readArtifact(t, filepath.Join(source, manifestFile)))
	}
	if mode == "export-umask" {
		old := syscall.Umask(0777)
		defer syscall.Umask(old)
	}
	abandoned, _ := stagePaths(t, dest)
	ctx := phaseContext{t.Context(), func() {
		stage, _ := stagePaths(t, dest)
		if stage == "" || stage == abandoned {
			return
		}
		info, err := os.Lstat(filepath.Join(stage, manifestFile))
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		must(t, err)
		if info.Mode().IsRegular() && info.Size() == int64(manifestSize) {
			park()
		}
	}}
	var err error
	if mode == "restore" {
		err = backup.Restore(ctx, source, dest)
	} else {
		err = backup.Export(ctx, snap, dest)
	}
	t.Fatalf("%s returned before the copy barrier: %v", mode, err)
}

func runBackupRetries(t *testing.T) {
	source, snap, want := backupFixture(t)
	bundle := filepath.Join(t.TempDir(), "bundle")
	must(t, backup.Export(t.Context(), snap, bundle))
	bundleFiles := readFiles(t, bundle)
	for _, mode := range []string{"export", "restore"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dest := filepath.Join(parent, "retry")
			published := filepath.Join(parent, "published")
			unrelated := filepath.Join(parent, "unrelated")
			unrelatedSource := filepath.Join(parent, legacyStage)
			sentinel := files{"keep": []byte("outside the staging protocol")}
			writeFiles(t, unrelatedSource, sentinel)
			must(t, backup.Restore(t.Context(), bundle, published))

			killOther := startInterrupted(t, "export", source, unrelated)
			otherStage, otherClaim := assertStagedCopy(t, unrelated, bundleFiles)
			killOther()
			otherImage := readFiles(t, otherStage)
			otherOwner, err := os.Readlink(otherClaim + ".owner")
			must(t, err)
			preserveSiblings := func() {
				t.Helper()
				assertFiles(t, published, bundleFiles)
				assertFiles(t, unrelatedSource, sentinel)
				assertAbsent(t, unrelated)
				stage, _ := stagePaths(t, unrelated)
				owner, err := os.Readlink(otherClaim + ".owner")
				must(t, err)
				if stage != otherStage || owner != otherOwner {
					t.Fatal("retry modified the unrelated abandoned attempt")
				}
				assertFiles(t, otherStage, otherImage)
				assertFiles(t, bundle, bundleFiles)
			}

			interruptSource := source
			if mode == "restore" {
				interruptSource = bundle
				must(t, os.Mkdir(dest, 0700))
			}
			publish := func(ctx context.Context) error {
				if mode == "restore" {
					return backup.Restore(ctx, bundle, dest)
				}
				return backup.Export(ctx, snap, dest)
			}
			var previousStage string
			for range 3 {
				kill := startInterrupted(t, mode, interruptSource, dest)
				stage, claim := assertStagedCopy(t, dest, bundleFiles)
				names := []string{backupGuard, legacyStage, "published"}
				for _, name := range []string{stage, claim, claim + ".owner", otherStage, otherClaim, otherClaim + ".owner"} {
					names = append(names, filepath.Base(name))
				}
				if mode == "restore" {
					assertChildren(t, dest)
					names = append(names, "retry")
				} else {
					assertAbsent(t, dest)
				}
				assertChildren(t, parent, names...)
				if previousStage != "" {
					if stage == previousStage {
						t.Fatal("retry reused the abandoned stage")
					}
					assertAbsent(t, previousStage)
				}
				preserveSiblings()
				kill()
				previousStage = stage
			}

			results := make(chan error, 2)
			for range cap(results) {
				go func() { results <- publish(t.Context()) }()
			}
			successes := 0
			for range cap(results) {
				err := <-results
				if err == nil {
					successes++
				} else if !populatedDestination(err) {
					t.Errorf("race to retry: %v", err)
				}
			}
			if successes != 1 {
				t.Fatalf("race to retry: %d successful publications, want one", successes)
			}
			assertAbsent(t, previousStage)
			_, claim := stagePaths(t, dest)
			assertAbsent(t, claim)
			assertAbsent(t, claim+".owner")
			assertRestored(t, dest, snap.point, want)
			assertFiles(t, dest, bundleFiles)
			if err := publish(t.Context()); !populatedDestination(err) {
				t.Fatalf("retry over published image: %v", err)
			}
			assertFiles(t, dest, bundleFiles)
			preserveSiblings()
			must(t, backup.Export(t.Context(), snap, unrelated))
			assertRestored(t, unrelated, snap.point, want)
			assertChildren(t, parent, backupGuard, legacyStage, "published", "retry", "unrelated")
			assertRestored(t, published, snap.point, want)
		})
	}
}

func populatedDestination(err error) bool {
	return err != nil && !errors.Is(err, backup.ErrPublicationUncertain) &&
		strings.Contains(err.Error(), "backup destination is not empty")
}

func runRestrictiveUmask(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("restrictive umask regression must execute without root privileges")
	}
	source, snap, want := backupFixture(t)
	parent := t.TempDir()
	reference := filepath.Join(t.TempDir(), "reference")
	must(t, backup.Export(t.Context(), snap, reference))
	original := readFiles(t, reference)
	dest := filepath.Join(parent, "retry")
	kill := startInterrupted(t, "export-umask", source, dest)
	stage, claim := assertStagedCopy(t, dest, original)
	kill()
	partial := readFiles(t, stage)

	oldMask := syscall.Umask(0777)
	defer syscall.Umask(oldMask)
	err := backup.Export(t.Context(), snap, dest)
	if err != nil {
		// #69 tracks the mode-zero persistent guard left by the first
		// publisher. Characterize only this failure and verify recovery
		// after repairing the guard so staging cleanup is still asserted.
		var openErr *os.PathError
		if !errors.As(err, &openErr) || !errors.Is(err, syscall.EACCES) || filepath.Base(openErr.Path) != backupGuard ||
			errors.Is(err, backup.ErrPublicationUncertain) {
			t.Fatalf("restrictive umask retry: %v", err)
		}
		guard := filepath.Join(parent, backupGuard)
		mode, statErr := os.Stat(guard)
		must(t, statErr)
		if mode.Mode().Perm() != 0 {
			t.Fatalf("restrictive umask guard has mode %04o, want 0000", mode.Mode().Perm())
		}
		t.Logf("issue=#69 phase=reopen-guard uid=%d umask=0777 errno=%v", os.Geteuid(), err)
		assertAbsent(t, dest)
		got, _ := stagePaths(t, dest)
		if got != stage {
			t.Fatalf("failed guard open changed the abandoned attempt to %q", got)
		}
		assertFiles(t, stage, partial)
		must(t, os.Chmod(guard, 0600))
		must(t, backup.Export(t.Context(), snap, dest))
	} else {
		t.Logf("issue=#69 phase=retry-succeeded uid=%d umask=0777", os.Geteuid())
	}
	assertAbsent(t, stage)
	assertAbsent(t, claim)
	assertAbsent(t, claim+".owner")
	assertChildren(t, parent, backupGuard, "retry")
	assertFiles(t, dest, original)
	assertRestored(t, dest, snap.point, want)
	if err := backup.Export(t.Context(), snap, dest); !populatedDestination(err) {
		t.Fatalf("reopen guard under umask 0777 after recovery: %v", err)
	}
	assertFiles(t, dest, original)
}
