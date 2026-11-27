package main

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

func TestCheckpointRestoredIssuerQualification(t *testing.T) {
	binary, revision := os.Getenv("BOG_RELEASE_BINARY"), os.Getenv("BOG_RELEASE_REVISION")
	if binary == "" && revision == "" {
		t.Skip("set BOG_RELEASE_BINARY and BOG_RELEASE_REVISION to qualify the native retained candidate")
	}
	rawRevision, err := hex.DecodeString(revision)
	if binary == "" || err != nil || len(rawRevision) != 20 || revision != strings.ToLower(revision) {
		t.Fatal("set BOG_RELEASE_BINARY and a full lowercase BOG_RELEASE_REVISION together")
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := recoveryBinaryDigest(binary)
	if err != nil {
		t.Fatal(err)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	settings := make(map[string]string, len(info.Settings))
	for _, entry := range info.Settings {
		settings[entry.Key] = entry.Value
	}
	if info.Path != "bog/cmd/bog" || settings["GOOS"] != "linux" || settings["GOARCH"] != runtime.GOARCH ||
		settings["vcs.revision"] != revision || settings["vcs.modified"] != "false" {
		t.Fatalf("expected clean bog/cmd/bog linux/%s at %s, got path=%s goos=%s goarch=%s revision=%s modified=%s",
			runtime.GOARCH, revision, info.Path, settings["GOOS"], settings["GOARCH"], settings["vcs.revision"], settings["vcs.modified"])
	}
	t.Logf("candidate revision=%s os=linux arch=%s binary_sha256=%x", revision, runtime.GOARCH, digest)
	t.Cleanup(func() {
		after, err := recoveryBinaryDigest(binary)
		if err != nil || after != digest {
			t.Errorf("candidate digest after qualification = %x, %v; want %x", after, err, digest)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	for _, scenario := range exportRestoreCases {
		t.Run(scenario.name, func(t *testing.T) {
			dir, point := makeExportVerifiedRestore(t, ctx)
			t.Logf("verified restore generation=%x covered_offset=%d next_ordinal=%d committed_end=%d",
				point.Generation, point.CoveredOffset, point.NextOrdinal, point.CommittedEnd)
			addr := qualificationAddress(t)
			scenario.run(t, ctx, func() *qualificationIssuer {
				issuer := startQualificationIssuer(t, ctx, binary, dir, addr)
				got, err := recoveryBinaryDigest(fmt.Sprintf("/proc/%d/exe", issuer.cmd.Process.Pid))
				if err != nil || got != digest {
					t.Fatalf("running issuer digest = %x, %v; want %x", got, err, digest)
				}
				return issuer
			})
		})
	}
}

func recoveryBinaryDigest(path string) (digest [sha256.Size]byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return digest, err
	}
	h := sha256.New()
	_, readErr := io.Copy(h, f)
	return [sha256.Size]byte(h.Sum(nil)), errors.Join(readErr, f.Close())
}

type exportRestoreSnapshot struct {
	root  *os.Root
	point backup.Point
}

func (s exportRestoreSnapshot) Point() backup.Point { return s.point }
func (s exportRestoreSnapshot) OpenManifest() (io.ReadCloser, error) {
	return s.root.Open("bog.checkpoint")
}
func (s exportRestoreSnapshot) OpenSegment(name string) (io.ReadCloser, error) {
	return s.root.Open(name)
}
func (s exportRestoreSnapshot) OpenWAL() (backup.WALReader, error) { return s.root.Open("bog.wal") }

func appendRestoreFixture(t *testing.T, dir, batch string) {
	t.Helper()
	rows, err := decodeRecoveryRows(strings.NewReader(batch))
	if err != nil {
		t.Fatal(err)
	}
	log, err := wal.Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(log.Append([]byte(batch), rows), log.Close()); err != nil {
		t.Fatal(err)
	}
}

func makeExportVerifiedRestore(t *testing.T, ctx context.Context) (string, backup.Point) {
	t.Helper()
	dir := qualificationRestoreRoot(t)
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}

	const prefix = rowC + rowA + rowB
	tail, found := strings.CutPrefix(exportSeed, prefix)
	if !found || tail == "" {
		t.Fatal("restore seed must begin with the checkpoint rows and have a committed suffix")
	}
	appendRestoreFixture(t, source, prefix)
	legacy, err := checkpoint.OpenLegacy(source)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := legacy.Publish()
	if err := errors.Join(err, legacy.Close()); err != nil {
		t.Fatal(err)
	}
	appendRestoreFixture(t, source, tail)
	legacy, err = checkpoint.OpenLegacy(source)
	if err != nil {
		t.Fatal(err)
	}
	committed := legacy.CommittedOffset()
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	}()
	raw, err := root.ReadFile("bog.checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct{ Payload json.RawMessage }
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	point := backup.Point{Generation: sha256.Sum256(envelope.Payload), CoveredOffset: manifest.CoveredOffset,
		NextOrdinal: manifest.NextOrdinal, CommittedEnd: committed}
	if len(envelope.Payload) == 0 || point.NextOrdinal != 3 || point.CoveredOffset <= 0 || point.CommittedEnd <= point.CoveredOffset {
		t.Fatalf("expected checkpoint plus committed tail: %+v", point)
	}
	// The source is private and quiescent with all WAL handles closed.
	bundle, restore := filepath.Join(dir, "bundle"), filepath.Join(dir, "restored")
	if err := backup.Export(ctx, exportRestoreSnapshot{root, point}, bundle); err != nil {
		t.Fatal(err)
	}
	if err := backup.Restore(ctx, bundle, restore); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{bundle, restore} {
		verified, err := backup.Verify(ctx, path)
		if err != nil || verified != point {
			t.Fatalf("verify %s = %+v, %v; want %+v", filepath.Base(path), verified, err, point)
		}
	}
	return restore, point
}

func TestExportVerifiedRestoreFixture(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	makeExportVerifiedRestore(t, ctx)
}
