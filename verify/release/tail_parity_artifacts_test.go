package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/store"
)

const (
	tailParityFileLimit = 4096
	tailManifestLimit   = 256 << 10
	tailParityByteLimit = 4 << 10
)

func requireTailOnlyDirectory(dir string) error {
	_, err := os.Lstat(filepath.Join(dir, "bog.wal"))
	switch {
	case err == nil:
		return errors.New("tail-only fixture unexpectedly contains a full WAL")
	case !errors.Is(err, os.ErrNotExist):
		return err
	default:
		return nil
	}
}

func readTailParityImage(ctx context.Context, dir, magic string) (_ tailParityPoint, err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return tailParityPoint{}, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	meta, err := root.Open("bog.backup")
	if err != nil {
		return tailParityPoint{}, err
	}
	var header [8]byte
	_, readErr := io.ReadFull(meta, header[:])
	if err := errors.Join(readErr, meta.Close()); err != nil {
		return tailParityPoint{}, err
	}
	if string(header[:]) != magic {
		return tailParityPoint{}, fmt.Errorf("backup magic %q, want %q", header, magic)
	}
	p, tail, err := backup.VerifyPinnedRecovery(ctx, root, func(start, end int64) error {
		if end-start > tailParityByteLimit {
			return fmt.Errorf("fixture suffix exceeds %d bytes", tailParityByteLimit)
		}
		return nil
	})
	if err != nil {
		return tailParityPoint{}, err
	}
	return reportTailParityPoint(p), tail.Close()
}

func reportTailParityPoint(p backup.RecoveryPoint) tailParityPoint {
	return tailParityPoint{
		Generation: hex.EncodeToString(p.Generation[:]), CoveredOffset: p.CoveredOffset, NextOrdinal: int(p.NextOrdinal),
		CommittedEnd: p.CommittedEnd, CommittedOrdinal: int(p.CommittedOrdinal), SuffixSHA256: hex.EncodeToString(p.SuffixSHA256[:]),
	}
}

// The candidate has exited before export. Hold the private directory and
// use its installed manifest, segment bytes, and physical rebased tail.
// The restarted candidate must subsequently extend that same tail.
type tailParityView struct {
	root     *os.Root
	manifest string
	tail     string
	point    backup.RecoveryPoint
}

func (v tailParityView) Point() backup.RecoveryPoint                    { return v.point }
func (v tailParityView) OpenManifest() (io.ReadCloser, error)           { return v.root.Open(v.manifest) }
func (v tailParityView) OpenSegment(name string) (io.ReadCloser, error) { return v.root.Open(name) }
func (v tailParityView) OpenSuffix() (io.ReadCloser, error)             { return v.root.Open(v.tail) }

func exportTailParityCycle(ctx context.Context, dir, exported string, fixture tailCycleFixture, cycle *tailParityCycle) (_ string, err error) {
	if err := requireTailOnlyDirectory(dir); err != nil {
		return "", err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	view, consumed, err := openTailParityView(ctx, root, fixture, cycle.Input)
	if err != nil {
		return "", err
	}
	if err := backup.ExportRecoveryView(ctx, view, exported, tailParityByteLimit); err != nil {
		return "", fmt.Errorf("export advanced tail-only view: %w", err)
	}
	cycle.Export, err = readTailParityImage(ctx, exported, "BOGBAK02")
	if err != nil {
		return "", fmt.Errorf("verify v2 export: %w", err)
	}
	if err := checkTailParityPoint(cycle.Export, reportTailParityPoint(view.point)); err != nil {
		return "", err
	}
	cycle.ConsumedSHA256 = consumed
	cycle.TailBase = cycle.Export.CoveredOffset
	cycle.TailBytes = cycle.Export.CommittedEnd - cycle.TailBase
	return view.tail, nil
}

func openTailParityView(ctx context.Context, root *os.Root, fixture tailCycleFixture, input tailParityPoint) (view tailParityView, consumed string, err error) {
	dir, err := root.Open(".")
	if err != nil {
		return view, "", err
	}
	entries, readErr := dir.ReadDir(tailParityFileLimit + 1)
	if readErr == io.EOF {
		readErr = nil
	}
	if err := errors.Join(readErr, dir.Close()); err != nil {
		return view, "", err
	}
	if len(entries) > tailParityFileLimit {
		return view, "", errors.New("tail-only fixture contains too many files")
	}
	view.root = root
	wantAdvance := checkpoint.Advance{
		Generation: input.Generation, CoveredOffset: input.CoveredOffset, NextOrdinal: uint64(input.NextOrdinal),
		ConsumedSHA256: parityDigest(fixture.consumed),
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return view, "", err
		}
		if !entry.Type().IsRegular() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "bog-checkpoint-") {
			if len(name) != len("bog-checkpoint-")+64 {
				continue
			}
			data, err := readTailArtifact(root, name, tailManifestLimit)
			if err != nil {
				return view, "", err
			}
			m, id, err := checkpoint.DecodeManifest(data)
			if err != nil {
				return view, "", fmt.Errorf("decode installed checkpoint %s: %w", name, err)
			}
			if m.NextOrdinal != uint64(fixture.export.NextOrdinal) {
				continue
			}
			if view.manifest != "" || name != checkpoint.GenerationManifestName(id) ||
				m.CoveredOffset != fixture.export.CoveredOffset || m.Advance == nil || *m.Advance != wantAdvance {
				return view, "", fmt.Errorf("unexpected installed advance at cutoff %d in %s", fixture.export.NextOrdinal, name)
			}
			view.manifest, consumed = name, m.Advance.ConsumedSHA256
			view.point.Generation = id
			view.point.CoveredOffset, view.point.NextOrdinal = m.CoveredOffset, m.NextOrdinal
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return view, "", err
		}
		if info.Size() != int64(len(fixture.suffix)) {
			continue
		}
		data, err := readTailArtifact(root, name, info.Size())
		if err != nil {
			return view, "", err
		}
		if !bytes.Equal(data, fixture.suffix) {
			continue
		}
		if view.tail != "" {
			return view, "", errors.New("ambiguous physical rebased tail")
		}
		view.tail = name
		view.point.CommittedEnd = int64(len(data))
		view.point.CommittedOrdinal = uint64(fixture.export.CommittedOrdinal)
		view.point.SuffixSHA256 = sha256.Sum256(data)
	}
	if view.manifest == "" || view.tail == "" {
		return view, "", fmt.Errorf("missing installed checkpoint or exact rebased tail at cutoff %d", fixture.export.NextOrdinal)
	}
	view.point.CommittedEnd += view.point.CoveredOffset
	return view, consumed, nil
}

func readTailArtifactPath(dir, name string, limit int64) (_ []byte, err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	return readTailArtifact(root, name, limit)
}

func readTailArtifact(root *os.Root, name string, limit int64) ([]byte, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("fixture artifact %s must be a regular file at most %d bytes", name, limit)
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, limit+1))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("fixture artifact %s exceeds %d bytes", name, limit)
	}
	return data, nil
}

func TestExportTailParityCycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		file    string
		content func(tailCycleFixture) []byte
		want    string
	}{
		{name: "complete"},
		{name: "full WAL", file: "bog.wal", content: func(tailCycleFixture) []byte { return []byte("BOGWAL01") }, want: "full WAL"},
		{name: "ambiguous tail", file: "other-tail", content: func(f tailCycleFixture) []byte { return f.suffix }, want: "ambiguous physical rebased tail"},
		{name: "extra byte on tail", file: "rebased-tail", content: func(f tailCycleFixture) []byte { return append(bytes.Clone(f.suffix), 0) }, want: "missing installed checkpoint or exact rebased tail"},
		{name: "corrupt tail", file: "rebased-tail", content: func(f tailCycleFixture) []byte {
			suffix := bytes.Clone(f.suffix)
			suffix[len(suffix)-1] ^= 1
			return suffix
		}, want: "missing installed checkpoint or exact rebased tail"},
		{name: "corrupt segment", file: tailTestSegment, content: func(tailCycleFixture) []byte { return []byte("corrupt") }, want: "export advanced tail-only view"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixtures, err := tailParityFixtures()
			if err != nil {
				t.Fatal(err)
			}
			f := fixtures[0]
			dir := t.TempDir()
			cycle := tailParityCycle{Input: f.input}
			cycle.Input.Generation = strings.Repeat("1", 64)
			f.export.Generation = writeTailArtifactFixture(t, dir, f, cycle.Input)
			if tc.content != nil {
				writeFixture(t, filepath.Join(dir, tc.file), tc.content(f))
			}
			bundle := filepath.Join(t.TempDir(), "bundle")
			tail, err := exportTailParityCycle(t.Context(), dir, bundle, f, &cycle)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("export = %v, want %q", err, tc.want)
				}
				if _, err := os.Stat(bundle); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed export left a published bundle: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tail != "rebased-tail" || cycle.ConsumedSHA256 != parityDigest(f.consumed) ||
				cycle.TailBytes != int64(len(f.suffix)) || cycle.TailBase != f.export.CoveredOffset {
				t.Fatalf("unexpected export evidence: %q, %+v", tail, cycle)
			}
			restored := filepath.Join(t.TempDir(), "restored")
			if err := backup.Restore(t.Context(), bundle, restored); err != nil {
				t.Fatal(err)
			}
			got, err := readTailParityImage(t.Context(), restored, "BOGBAK02")
			if err != nil {
				t.Fatal(err)
			}
			if err := checkTailParityPoint(got, f.export); err != nil {
				t.Fatal(err)
			}
		})
	}
}

const tailTestSegment = "bog-segment-00000000000000000000000000000001"

// The adapter test constructs already-published artifacts. In the pinned
// producer only the candidate creates the writable files and manifests.
func writeTailArtifactFixture(t *testing.T, dir string, f tailCycleFixture, input tailParityPoint) string {
	t.Helper()
	rows := make([]checkpoint.BlockRow, f.export.NextOrdinal)
	for i := range rows {
		rows[i] = checkpoint.BlockRow{Row: store.Row{TS: fixtureTime(1), Line: "ordinal fixture"}, Ordinal: uint64(i)}
	}
	block, err := checkpoint.EncodeBlock(rows)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(dir, tailTestSegment), block)
	m := checkpoint.Manifest{
		Version: 2, CoveredOffset: f.export.CoveredOffset, NextOrdinal: uint64(f.export.NextOrdinal),
		Segments: []checkpoint.Segment{{Name: tailTestSegment, Size: int64(len(block)), SHA256: parityDigest(block)}},
		Advance: &checkpoint.Advance{
			Generation: input.Generation, CoveredOffset: input.CoveredOffset, NextOrdinal: uint64(input.NextOrdinal),
			ConsumedSHA256: parityDigest(f.consumed),
		},
	}
	payload, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	id := sha256.Sum256(payload)
	sum := hex.EncodeToString(id[:])
	raw, err := json.Marshal(struct {
		Payload json.RawMessage `json:"payload"`
		SHA256  string          `json:"sha256"`
	}{payload, sum})
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(dir, checkpoint.GenerationManifestName(id)), raw)
	writeFixture(t, filepath.Join(dir, "rebased-tail"), f.suffix)
	return sum
}
