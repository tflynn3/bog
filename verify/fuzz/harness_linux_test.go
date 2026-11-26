package fuzz

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
)

func requireIntegrity(t testing.TB, op string, err, integrity error) {
	t.Helper()
	if !errors.Is(err, integrity) || errors.Is(err, backup.ErrPublicationUncertain) {
		t.Fatalf("%s returned %v, want %v", op, err, integrity)
	}
}

func rejectBackup(t testing.TB, source, parent string) {
	t.Helper()
	dest := filepath.Join(parent, "absent")
	placeholder := filepath.Join(parent, "placeholder")
	must(t, os.Mkdir(placeholder, 0700))
	if point, err := backup.Verify(t.Context(), source); point != (backup.Point{}) || !errors.Is(err, backup.ErrIntegrity) {
		t.Fatalf("verify returned point %+v and error %v, want zero point and integrity error", point, err)
	}
	for _, path := range []string{dest, placeholder} {
		requireIntegrity(t, "restore", backup.Restore(t.Context(), source, path), backup.ErrIntegrity)
	}
	// Check the absent path, the empty placeholder, and the staging namespace.
	assertChildren(t, parent, "placeholder")
	assertChildren(t, placeholder)
}

func runBackupCorpus(t *testing.T, f fixture) {
	t.Helper()
	all := append(backupMutations(f), historyMutations(t, f)...)
	for _, m := range all {
		t.Run(m.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "malformed")
			image := mutateImage(t, f, m.target, m.data)
			writeFiles(t, source, image)
			// Keep a verified image in the publication parent. A failed
			// attempt must leave its bytes and recovery point unchanged.
			existing := filepath.Join(root, "existing")
			writeFiles(t, existing, f.image)
			parent := filepath.Join(root, "destinations")
			must(t, os.Mkdir(parent, 0700))
			rejectBackup(t, source, parent)
			if err := backup.Restore(t.Context(), source, existing); err == nil || errors.Is(err, backup.ErrPublicationUncertain) {
				t.Fatalf("restore over an existing image returned %v", err)
			}
			assertChildren(t, root, "destinations", "existing", "malformed")
			assertFiles(t, source, image)
			assertFiles(t, existing, f.image)
			assertRestored(t, existing, f.point, f.rows)
		})
	}
}

func runHistoryCorpus(t *testing.T, f fixture) {
	t.Helper()
	for _, m := range historyMutations(t, f) {
		t.Run(m.name, func(t *testing.T) {
			root := t.TempDir()
			valid := filepath.Join(root, "current")
			writeFiles(t, valid, historyFiles(f.image))
			candidate := filepath.Join(root, "candidate")
			candidateFiles := historyFiles(mutateImage(t, f, m.target, m.data))
			writeFiles(t, candidate, candidateFiles)

			var catalog history.Catalog
			defer func() { must(t, catalog.Close()) }()
			current, err := catalog.Replace(t.Context(), valid)
			must(t, err)
			pinned, err := catalog.AcquireCurrent()
			must(t, err)
			defer func() { must(t, pinned.Close()) }()
			before := readFiles(t, valid)

			replacement, err := catalog.Replace(t.Context(), candidate)
			assertHistoryRejection(t, &catalog, pinned, current, replacement, err, f)
			assertFiles(t, valid, before)
			assertUnpinnedCheckpoint(t, candidate, candidateFiles)

			// Owners and readers release independently. A rejected
			// replacement must not leak a reference to the old generation.
			must(t, catalog.Close())
			select {
			case <-current.Drained:
				t.Fatal("current generation drained with a retained reader")
			default:
			}
			checkHistoryRows(t, pinned, current, f.covered)
			must(t, pinned.Close())
			select {
			case <-current.Drained:
			default:
				t.Fatal("current generation did not drain after closing its last reader")
			}
			assertUnpinnedCheckpoint(t, valid, historyFiles(f.image))
		})
	}
}

func assertHistoryRejection(t testing.TB, catalog *history.Catalog, pinned *history.Reader, current, replacement history.Generation, err error, f fixture) {
	t.Helper()
	requireIntegrity(t, "replace history", err, history.ErrIntegrity)
	if replacement != (history.Generation{}) {
		t.Fatalf("malformed checkpoint returned generation %+v", replacement)
	}
	checkHistoryRows(t, pinned, current, f.covered)
	assertHistoryRows(t, catalog.AcquireCurrent, current, f.covered)
}

func assertUnpinnedCheckpoint(t testing.TB, directory string, contents files) {
	t.Helper()
	// Validation can create a persistent publication lock file. All other
	// bytes should match the supplied image, including absence of pin files.
	want := maps.Clone(contents)
	want["bog.checkpoint.lock"] = nil
	assertFiles(t, directory, want)
}

func FuzzBackup(f *testing.F) {
	baseline := newFixture(f)
	seed(f, baseline, append(backupMutations(baseline), historyMutations(f, baseline)...), rawMetadata)
	f.Fuzz(func(t *testing.T, selector uint8, data []byte) {
		which := target(selector % uint8(targetCount))
		if !fitsArtifact(which, data) {
			t.Skip("artifact exceeds harness input limit")
		}
		root := t.TempDir()
		source, dest := filepath.Join(root, "bundle"), filepath.Join(root, "restore")
		writeFiles(t, source, mutateImage(t, baseline, which, data))
		point, err := backup.Verify(t.Context(), source)
		if err != nil {
			requireIntegrity(t, "verify", err, backup.ErrIntegrity)
			if point != (backup.Point{}) {
				t.Fatalf("failed verification returned point %+v", point)
			}
			requireIntegrity(t, "restore", backup.Restore(t.Context(), source, dest), backup.ErrIntegrity)
			assertChildren(t, root, "bundle")
			return
		}
		must(t, backup.Restore(t.Context(), source, dest))
		assertRestored(t, dest, point, nil)
		assertChildren(t, root, "bundle", "restore")
	})
}

func FuzzHistory(f *testing.F) {
	baseline := newFixture(f)
	seed(f, baseline, historyMutations(f, baseline), rawManifest)
	f.Fuzz(func(t *testing.T, selector uint8, data []byte) {
		// Remap the full selector range to the four history variants.
		which := rawManifest + target(selector%uint8(targetCount-rawManifest))
		if !fitsArtifact(which, data) {
			t.Skip("artifact exceeds harness input limit")
		}
		root := t.TempDir()
		valid, candidate := filepath.Join(root, "current"), filepath.Join(root, "candidate")
		writeFiles(t, valid, historyFiles(baseline.image))
		candidateFiles := historyFiles(mutateImage(t, baseline, which, data))
		writeFiles(t, candidate, candidateFiles)
		var catalog history.Catalog
		defer func() { must(t, catalog.Close()) }()
		current, err := catalog.Replace(t.Context(), valid)
		must(t, err)
		pinned, err := catalog.AcquireCurrent()
		must(t, err)
		defer func() { must(t, pinned.Close()) }()
		replacement, err := catalog.Replace(t.Context(), candidate)
		if err != nil {
			assertHistoryRejection(t, &catalog, pinned, current, replacement, err, baseline)
			assertUnpinnedCheckpoint(t, candidate, candidateFiles)
			return
		}
		reader, err := catalog.Acquire(replacement.ID)
		must(t, err)
		defer func() { must(t, reader.Close()) }()
		if got := reader.Generation(); got != replacement {
			t.Fatalf("acquired generation %+v, want %+v", got, replacement)
		}
		checkHistoryRows(t, pinned, current, baseline.covered)
		assertCompleteHistory(t, reader, replacement.Boundary.NextOrdinal)
	})
}

func assertCompleteHistory(t *testing.T, reader *history.Reader, want uint64) {
	t.Helper()
	var count uint64
	var previous checkpoint.BlockRow
	query := history.Query{
		Start: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
	}
	err := reader.Scan(t.Context(), query, func(row checkpoint.BlockRow) error {
		if count >= want || row.Ordinal >= want {
			return errors.New("scan exceeded validated row coverage")
		}
		if count > 0 && (row.Row.TS.Before(previous.Row.TS) ||
			(row.Row.TS.Equal(previous.Row.TS) && row.Ordinal <= previous.Ordinal)) {
			return errors.New("scan rows are out of order")
		}
		previous = row
		count++
		return nil
	})
	must(t, err)
	if count != want {
		t.Fatalf("scan returned %d rows, want %d", count, want)
	}
}

func fitsArtifact(which target, b []byte) bool {
	size := len(b)
	if which == boundManifestPayload {
		size += len(`{"sha256":"","payload":}`) + 64 // Hex-encoded SHA-256 and envelope.
	}
	return size <= maxArtifactSize
}

func seed(f *testing.F, baseline fixture, corpus []mutation, first target) {
	f.Helper()
	for which := first; which < targetCount; which++ {
		var b []byte
		switch which {
		case rawMetadata, signedMetadata:
			b = baseline.image[metadataFile]
		case rawTail, boundTail:
			b = baseline.image[tailFile]
		case rawManifest:
			b = baseline.image[manifestFile]
		case boundManifestPayload:
			b = decodeEnvelope(f, baseline.image[manifestFile]).Payload
		case rawSegment, boundSegment:
			b = baseline.image[baseline.segment]
		}
		f.Add(uint8(which-first), b)
	}
	for _, m := range corpus {
		if !fitsArtifact(m.target, m.data) {
			f.Fatalf("corpus entry %s exceeds harness input limit", m.name)
		}
		f.Add(uint8(m.target-first), m.data)
	}
}
