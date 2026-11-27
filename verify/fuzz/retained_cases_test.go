//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
)

const retainedSeed uint8 = 92

type retainedGeneration struct {
	image files
	view  replayView
	id    [sha256.Size]byte
}

func (g retainedGeneration) expectation(image files) replayExpectation {
	return g.view.expect(g.id, image)
}

type retainedFixture struct {
	previous, successor retainedGeneration
	cuts                []replayCut
	successorSegment    string
}

// Build two distinct, reproducible v1 generations over the same committed
// end. Each block is encoded from the fixture's append input. The ledger
// independently supplies the query order and physical frame boundaries.
func seedRetainedFixture() (retainedFixture, error) {
	var fixture retainedFixture
	var ledger committedBatchLedger
	segments := make(files)
	var entries []checkpoint.Segment
	batches := replayHistory(retainedSeed)
	for i, rows := range batches {
		batch, err := prepareReplayBatch(rows)
		if err != nil {
			return fixture, err
		}
		first := 0
		if i > 0 {
			first = ledger.cuts[i-1].rows
		}
		if err := ledger.record(batch, replayCommitted); err != nil {
			return fixture, err
		}
		if i == len(batches)-1 {
			break
		}
		block := make([]checkpoint.BlockRow, len(rows))
		for j, row := range rows {
			block[j] = checkpoint.BlockRow{Row: row, Ordinal: uint64(first + j)}
		}
		slices.SortStableFunc(block, func(a, b checkpoint.BlockRow) int { return a.Row.TS.Compare(b.Row.TS) })
		data, err := checkpoint.EncodeBlock(block)
		if err != nil {
			return fixture, err
		}
		name := fmt.Sprintf("bog-segment-%032x", i+1)
		segments[name] = data
		entries = append(entries, checkpoint.Segment{Name: name, Size: int64(len(data)), SHA256: retainedDigest(data)})
	}
	var err error
	fixture.previous, err = makeRetainedGeneration(&ledger, 1, entries[:1], segments)
	if err != nil {
		return fixture, err
	}
	fixture.successor, err = makeRetainedGeneration(&ledger, 2, entries, segments)
	fixture.cuts = slices.Clone(ledger.cuts)
	fixture.successorSegment = entries[1].Name
	return fixture, err
}

func retainedDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func retainedEnvelope(value any) (envelope []byte, id [sha256.Size]byte, err error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, id, err
	}
	id = sha256.Sum256(payload)
	envelope, err = json.Marshal(struct {
		Payload json.RawMessage `json:"payload"`
		SHA256  string          `json:"sha256"`
	}{payload, fmt.Sprintf("%x", id)})
	return envelope, id, err
}

func makeRetainedGeneration(ledger *committedBatchLedger, covered int, entries []checkpoint.Segment, segments files) (retainedGeneration, error) {
	view, err := ledger.view(covered, len(ledger.cuts))
	if err != nil {
		return retainedGeneration{}, err
	}
	manifest, id, err := retainedEnvelope(checkpoint.Manifest{
		Version: 1, CoveredOffset: int64(view.covered.end), NextOrdinal: uint64(view.covered.rows),
		SourceSHA256: retainedDigest(view.prefix()), Segments: entries,
	})
	if err != nil {
		return retainedGeneration{}, err
	}
	image := files{manifestFile: manifest, metadataFile: replayBackupMetadata(view, id), tailFile: slices.Clone(view.tail())}
	for _, entry := range entries {
		image[entry.Name] = slices.Clone(segments[entry.Name])
	}
	return retainedGeneration{image, view, id}, validateReplayFiles(image)
}

type retainedOutcome string

const (
	retainedReject   retainedOutcome = "reject-committed-authority"
	retainedRefuse   retainedOutcome = "reject-replay-admission"
	retainedFallback retainedOutcome = "recover-previous-generation"
	retainedAccept   retainedOutcome = "recover-selected-generation"
)

type retainedImages struct {
	main, stage    files
	truncateIntent bool
}

type retainedCase struct {
	name           string
	outcome        retainedOutcome
	reject         error
	admissionCalls int
	malformedFrame bool
	// A nil stage selects the pinned restore path. A nonnil stage selects
	// interrupted checkpoint recovery with the previous committed generation.
	prepare func(retainedFixture) retainedImages
}

func retainedCases() []retainedCase {
	return []retainedCase{
		backupRecoveryCase("valid-successor", retainedAccept, nil, 1, func(_ retainedFixture, _ files) {}),
		backupRecoveryCase("truncated-backup-metadata", retainedReject, backup.ErrIntegrity, 0, func(_ retainedFixture, image files) {
			meta := image[metadataFile]
			image[metadataFile] = meta[:len(meta)-1]
		}),
		backupRecoveryCase("committed-end-before-checkpoint", retainedReject, backup.ErrIntegrity, 0, func(f retainedFixture, image files) {
			setRetainedEnd(image, uint64(f.successor.view.covered.end-1))
		}),
		backupRecoveryCase("huge-declared-tail", retainedRefuse, checkpoint.ErrExhausted, 1, func(_ retainedFixture, image files) {
			setRetainedEnd(image, math.MaxInt64)
		}),
		backupRecoveryCase("old-metadata-with-new-generation", retainedReject, backup.ErrIntegrity, 0, func(f retainedFixture, image files) {
			// This is intact metadata from a real, different fixture generation.
			image[metadataFile] = slices.Clone(f.previous.image[metadataFile])
		}),
		backupRecoveryCase("old-manifest-with-new-metadata", retainedReject, backup.ErrIntegrity, 0, func(f retainedFixture, image files) {
			image[manifestFile] = slices.Clone(f.previous.image[manifestFile])
		}),
		backupRecoveryCase("committed-successor-segment-missing", retainedReject, backup.ErrIntegrity, 1, func(f retainedFixture, image files) {
			// Preserve the entry count so validation reaches the reference.
			image[retainedUnrelatedSegment] = image[f.successorSegment]
			delete(image, f.successorSegment)
		}),
		{name: "invalid-second-committed-frame-length", outcome: retainedReject, reject: backup.ErrIntegrity, admissionCalls: 1, malformedFrame: true,
			prepare: func(f retainedFixture) retainedImages {
				image := cloneRetainedFiles(f.previous.image)
				tail := image[tailFile]
				second := f.cuts[1].end - f.cuts[0].end
				copy(tail[second:], frameLength(tail[second:], math.MaxUint32))
				rebindRetainedTail(image)
				return retainedImages{main: image}
			}},
		checkpointRecoveryCase("truncated-unpublished-manifest", retainedFallback, nil, func(image *retainedImages) {
			manifest := image.stage["manifest"]
			image.stage["manifest"] = manifest[:len(manifest)-1]
		}),
		checkpointRecoveryCase("staged-successor-cannot-replace-damaged-authority", retainedReject, history.ErrIntegrity, func(image *retainedImages) {
			manifest := image.main[manifestFile]
			image.main[manifestFile] = manifest[:len(manifest)-1]
		}),
		checkpointRecoveryCase("truncated-publication-intent", retainedReject, checkpoint.ErrIntegrity, func(image *retainedImages) {
			image.truncateIntent = true
		}),
		checkpointRecoveryCase("missing-previous-authority-with-installed-intent", retainedReject, checkpoint.ErrIntegrity, func(image *retainedImages) {
			delete(image.main, manifestFile)
		}),
	}
}

func cloneRetainedFiles(source files) files {
	out := make(files, len(source))
	for name, data := range source {
		out[name] = slices.Clone(data)
	}
	return out
}

func setRetainedEnd(image files, end uint64) {
	meta := image[metadataFile]
	le.PutUint64(meta[56:64], end)
	signMetadata(meta)
}

func rebindRetainedTail(image files) {
	meta := image[metadataFile]
	sum := sha256.Sum256(image[tailFile])
	copy(meta[64:96], sum[:])
	signMetadata(meta)
}

func backupRecoveryCase(name string, outcome retainedOutcome, reject error, calls int, edit func(retainedFixture, files)) retainedCase {
	return retainedCase{name: name, outcome: outcome, reject: reject, admissionCalls: calls, prepare: func(f retainedFixture) retainedImages {
		image := cloneRetainedFiles(f.successor.image)
		edit(f, image)
		return retainedImages{main: image}
	}}
}

const (
	retainedStage            = ".bog-checkpoint-stage"
	retainedIntent           = "installed-v1"
	retainedLock             = "bog.checkpoint.lock"
	retainedWALFile          = "bog.wal"
	retainedCommitFile       = "bog.commit"
	retainedUnrelatedSegment = "bog-segment-00000000000000000000000000000003"
)

func retainedCommitMarker(end uint64) []byte {
	marker := make([]byte, 24)
	copy(marker, "BOGCMT01")
	le.PutUint64(marker[8:16], end)
	le.PutUint64(marker[16:24], ^end)
	return marker
}

func checkpointRecoveryCase(name string, outcome retainedOutcome, reject error, edit func(*retainedImages)) retainedCase {
	return retainedCase{name: name, outcome: outcome, reject: reject, prepare: func(f retainedFixture) retainedImages {
		main := retainedCheckpointFiles(f.previous.image)
		wal := f.previous.view.wal
		main[retainedWALFile] = slices.Clone(wal)
		main[retainedCommitFile] = retainedCommitMarker(uint64(len(wal)))
		// The successor block has reached its final name. The authority
		// rename has yet to occur. Also keep an unclaimed segment so
		// recovery must distinguish ownership from mere reachability.
		main[f.successorSegment] = slices.Clone(f.successor.image[f.successorSegment])
		main[retainedUnrelatedSegment] = slices.Clone(main[f.successorSegment])
		image := retainedImages{main: main, stage: files{"manifest": slices.Clone(f.successor.image[manifestFile])}}
		edit(&image)
		return image
	}}
}

func retainedCheckpointFiles(image files) files {
	out := cloneRetainedFiles(image)
	delete(out, metadataFile)
	delete(out, tailFile)
	out[retainedLock] = nil
	return out
}

func assertRetainedImage(ctx context.Context, path string, want files) error {
	got, err := readReplayFiles(ctx, path)
	if err != nil {
		return err
	}
	return compareReplayFiles(got, want)
}

func requireRetainedRejection(err, want error) error {
	if !errors.Is(err, want) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, backup.ErrPublicationUncertain) || errors.Is(err, checkpoint.ErrPublicationUncertain) {
		return fmt.Errorf("want clean rejection matching %v, got %w", want, err)
	}
	return nil
}

func findRetainedCase(t testing.TB, name string) retainedCase {
	t.Helper()
	for _, tc := range retainedCases() {
		if tc.name == name {
			return tc
		}
	}
	t.Fatalf("unknown retained recovery case %q", name)
	return retainedCase{}
}

func retainedGenerationMatches(g retainedGeneration, p backup.Point) bool {
	b := g.view.boundary(g.id)
	return p == (backup.Point{Generation: b.generation, CoveredOffset: b.covered, CommittedEnd: b.end, NextOrdinal: b.next})
}

func sameRetainedPrefix(m *checkpoint.Manifest, v replayView) bool {
	return m.CoveredOffset == int64(v.covered.end) && m.NextOrdinal == uint64(v.covered.rows) &&
		m.SourceSHA256 == retainedDigest(v.prefix())
}
