//go:build unix

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
	"syscall"
	"testing"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/store"
)

const (
	tailManifestLimit   = 256 << 10
	tailParityByteLimit = 4 << 10
)

func requireTailOnlyDirectory(root *os.Root) error {
	_, err := root.Lstat("bog.wal")
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

// The reaped candidate's selected manifest and suffix have already been
// checked byte for byte where independently known. Readers use those bounded
// snapshots and the manifest's named segments in the pinned directory.
type tailParityView struct {
	root     *os.Root
	manifest []byte
	suffix   []byte
	point    backup.RecoveryPoint
}

func (v tailParityView) Point() backup.RecoveryPoint { return v.point }
func (v tailParityView) OpenManifest() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(v.manifest)), nil
}
func (v tailParityView) OpenSegment(name string) (io.ReadCloser, error) {
	f, _, err := openTailArtifact(v.root, name, checkpoint.MaxBlockSize)
	if err != nil {
		return nil, err
	}
	return f, nil
}
func (v tailParityView) OpenSuffix() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(v.suffix)), nil
}

func exportTailParityCycle(ctx context.Context, root *os.Root, exported string, fixture tailCycleFixture, cycle *tailParityCycle) (selectedParityTail, error) {
	if err := requireTailOnlyDirectory(root); err != nil {
		return selectedParityTail{}, err
	}
	tail, err := openSelectedParityTail(ctx, root, fixture.export, fixture.suffix)
	if err != nil {
		return selectedParityTail{}, err
	}
	if err := exportSelectedTailParity(ctx, root, exported, fixture, cycle, tail); err != nil {
		return selectedParityTail{}, errors.Join(err, tail.file.Close())
	}
	return tail, nil
}

func exportSelectedTailParity(ctx context.Context, root *os.Root, exported string, fixture tailCycleFixture, cycle *tailParityCycle, tail selectedParityTail) error {
	a := tail.authority
	manifestName := "bog-checkpoint-" + a.Generation
	raw, err := readTailArtifact(root, manifestName, tailManifestLimit)
	if err != nil {
		return fmt.Errorf("read selected checkpoint: %w", err)
	}
	m, id, err := checkpoint.DecodeManifest(raw)
	if err != nil {
		return fmt.Errorf("decode selected checkpoint %s: %w", manifestName, err)
	}
	wantAdvance := checkpoint.Advance{
		Generation: cycle.Input.Generation, CoveredOffset: cycle.Input.CoveredOffset,
		NextOrdinal: uint64(cycle.Input.NextOrdinal), ConsumedSHA256: parityDigest(fixture.consumed),
	}
	if m.Version != 2 || checkpoint.GenerationManifestName(id) != manifestName ||
		m.CoveredOffset != a.CoveredOffset || m.NextOrdinal != uint64(a.NextOrdinal) ||
		m.Advance == nil || *m.Advance != wantAdvance {
		return fmt.Errorf("selected checkpoint %s differs from expected advance at cutoff %d", manifestName, a.NextOrdinal)
	}
	view := tailParityView{
		root: root, manifest: raw, suffix: tail.suffix,
		point: backup.RecoveryPoint{
			Generation: id, CoveredOffset: a.CoveredOffset, NextOrdinal: uint64(a.NextOrdinal),
			CommittedEnd: a.CommittedEnd, CommittedOrdinal: uint64(a.CommittedOrdinal), SuffixSHA256: sha256.Sum256(tail.suffix),
		},
	}
	if err := backup.ExportRecoveryView(ctx, view, exported, tailParityByteLimit); err != nil {
		return fmt.Errorf("export advanced tail-only view: %w", err)
	}
	cycle.Export, err = readTailParityImage(ctx, exported, "BOGBAK02")
	if err != nil {
		return fmt.Errorf("verify v2 export: %w", err)
	}
	if err := checkTailParityPoint(cycle.Export, a.tailParityPoint); err != nil {
		return err
	}
	cycle.ConsumedSHA256 = m.Advance.ConsumedSHA256
	cycle.TailBase = a.TailBase
	cycle.TailBytes = a.CommittedEnd - a.TailBase
	return nil
}

func verifyParityRestartAppend(ctx context.Context, root *os.Root, previous selectedParityTail, frame []byte, rows int) (err error) {
	if err := requireTailOnlyDirectory(root); err != nil {
		return err
	}
	want := previous.authority.tailParityPoint
	want.CommittedEnd += int64(len(frame))
	want.CommittedOrdinal += rows
	suffix := append(bytes.Clone(previous.suffix), frame...)
	want.SuffixSHA256 = parityDigest(suffix)
	current, err := openSelectedParityTail(ctx, root, want, suffix)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, current.file.Close()) }()
	if !os.SameFile(previous.info, current.info) {
		return errors.New("restart append selected a different physical tail")
	}
	return nil
}

func openTailArtifact(root *os.Root, name string, limit int64) (*os.File, os.FileInfo, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, nil, fmt.Errorf("artifact %s must be a regular file at most %d bytes", name, limit)
	}
	// Nonblocking prevents a substituted FIFO from hanging the probe.
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	opened, err := f.Stat()
	if err == nil && (!opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() > limit) {
		err = fmt.Errorf("artifact %s changed while opening", name)
	}
	if err != nil {
		return nil, nil, errors.Join(err, f.Close())
	}
	return f, opened, nil
}

func readBoundedTailArtifact(f *os.File, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(data)) > limit {
		err = fmt.Errorf("artifact %s exceeds %d bytes", f.Name(), limit)
	}
	return data, err
}

func readTailArtifact(root *os.Root, name string, limit int64) ([]byte, error) {
	f, _, err := openTailArtifact(root, name, limit)
	if err != nil {
		return nil, err
	}
	data, err := readBoundedTailArtifact(f, limit)
	return data, errors.Join(err, f.Close())
}

func TestExportTailParityCycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, string, tailParityAuthority, tailCycleFixture)
		want   string
	}{
		{name: "complete"},
		{name: "unrelated identical tail and invalid manifest", mutate: func(t *testing.T, dir string, _ tailParityAuthority, f tailCycleFixture) {
			writeFixture(t, filepath.Join(dir, "bog.wal.tail-orphan"), f.suffix)
			writeFixture(t, filepath.Join(dir, "bog-checkpoint-"+strings.Repeat("2", 64)), []byte("orphan"))
		}},
		{name: "full WAL", want: "full WAL", mutate: func(t *testing.T, dir string, _ tailParityAuthority, _ tailCycleFixture) {
			writeFixture(t, filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"))
		}},
		{name: "missing selector", want: "read writable selector", mutate: func(t *testing.T, dir string, _ tailParityAuthority, _ tailCycleFixture) {
			removeTailFixture(t, dir, tailAuthorityName)
		}},
		{name: "bad selector checksum", want: "checksum mismatch", mutate: func(t *testing.T, dir string, _ tailParityAuthority, _ tailCycleFixture) {
			writeFixture(t, filepath.Join(dir, tailAuthorityName), []byte(`{"payload":{},"sha256":"corrupt"}`))
		}},
		{name: "oversize selector", want: "regular file at most", mutate: func(t *testing.T, dir string, _ tailParityAuthority, _ tailCycleFixture) {
			writeFixture(t, filepath.Join(dir, tailAuthorityName), make([]byte, tailAuthorityLimit+1))
		}},
		{name: "missing selected checkpoint with identical orphan", want: "read selected checkpoint", mutate: func(t *testing.T, dir string, a tailParityAuthority, _ tailCycleFixture) {
			selected := filepath.Join(dir, "bog-checkpoint-"+a.Generation)
			orphan := filepath.Join(dir, "bog-checkpoint-"+strings.Repeat("2", 64))
			if err := os.Rename(selected, orphan); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing selected tail with identical orphan", want: "open selected tail", mutate: func(t *testing.T, dir string, a tailParityAuthority, f tailCycleFixture) {
			removeTailFixture(t, dir, a.Tail)
			writeFixture(t, filepath.Join(dir, "bog.wal.tail-orphan"), f.suffix)
		}},
		{name: "substituted selected tail with identical bytes", want: "file identity", mutate: func(t *testing.T, dir string, a tailParityAuthority, f tailCycleFixture) {
			// Keep the old inode alive so the replacement always has a different identity.
			if err := os.Rename(filepath.Join(dir, a.Tail), filepath.Join(dir, "original")); err != nil {
				t.Fatal(err)
			}
			writeFixture(t, filepath.Join(dir, a.Tail), f.suffix)
		}},
		{name: "selected tail symlink", want: "regular file", mutate: func(t *testing.T, dir string, a tailParityAuthority, _ tailCycleFixture) {
			if err := os.Rename(filepath.Join(dir, a.Tail), filepath.Join(dir, "original")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("original", filepath.Join(dir, a.Tail)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "extra byte on selected tail", want: "regular file at most", mutate: func(t *testing.T, dir string, a tailParityAuthority, f tailCycleFixture) {
			writeFixture(t, filepath.Join(dir, a.Tail), append(bytes.Clone(f.suffix), 0))
		}},
		{name: "corrupt selected tail with identical orphan", want: "bytes or digest", mutate: func(t *testing.T, dir string, a tailParityAuthority, f tailCycleFixture) {
			suffix := bytes.Clone(f.suffix)
			suffix[len(suffix)-1] ^= 1
			writeFixture(t, filepath.Join(dir, a.Tail), suffix)
			writeFixture(t, filepath.Join(dir, "bog.wal.tail-orphan"), f.suffix)
		}},
		{name: "corrupt segment", want: "export advanced tail-only view", mutate: func(t *testing.T, dir string, _ tailParityAuthority, _ tailCycleFixture) {
			writeFixture(t, filepath.Join(dir, tailTestSegment), []byte("corrupt"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, root, f, a, cycle := newTailArtifactFixture(t)
			if tc.mutate != nil {
				tc.mutate(t, dir, a, f)
			}
			bundle := filepath.Join(t.TempDir(), "bundle")
			tail, err := exportTailParityCycle(t.Context(), root, bundle, f, &cycle)
			if err == nil {
				t.Cleanup(func() { closeTailFixture(t, tail.file) })
			}
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
			if tail.authority.Tail != a.Tail || cycle.ConsumedSHA256 != parityDigest(f.consumed) ||
				cycle.TailBytes != int64(len(f.suffix)) || cycle.TailBase != f.export.CoveredOffset {
				t.Fatalf("unexpected export evidence: %q, %+v", tail.authority.Tail, cycle)
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

func newTailArtifactFixture(t *testing.T) (string, *os.Root, tailCycleFixture, tailParityAuthority, tailParityCycle) {
	t.Helper()
	dir, root, f, a := newTailFileFixture(t)
	cycle := tailParityCycle{Input: f.input}
	cycle.Input.Generation = strings.Repeat("1", 64)
	f.export.Generation = writeTailArtifactFixture(t, dir, f, cycle.Input)
	a.Generation = f.export.Generation
	writeTailSelector(t, dir, a)
	return dir, root, f, a, cycle
}

func closeTailFixture(t *testing.T, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func removeTailFixture(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.Remove(filepath.Join(dir, name)); err != nil {
		t.Fatal(err)
	}
}

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
	return sum
}
