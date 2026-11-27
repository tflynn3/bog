package fuzz

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
	"os/exec"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	querymodel "bog/verify/query-model"
)

// readReplayFiles caps both the bytes passed to artifact readers and the byte
// evidence retained by the oracle. The caller owns a private, stable fixture.
func readReplayFiles(ctx context.Context, path string) (files, error) {
	return readReplayDirectory(ctx, path, "")
}

// child, when set, must be the sole subdirectory. The caller inventories it
// separately with the same file and byte limits.
func readReplayDirectory(ctx context.Context, path, child string) (_ files, err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	entryLimit := replayMaxFiles
	if child != "" {
		entryLimit++
	}
	entries, err := dir.ReadDir(entryLimit + 1)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if err := errors.Join(err, dir.Close()); err != nil {
		return nil, err
	}
	if len(entries) > entryLimit {
		return nil, fmt.Errorf("%w: %s has too many files", errReplayLimit, path)
	}
	image := make(files, len(entries))
	remaining := replayMaxImageBytes
	foundChild := child == ""
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := entry.Name()
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if name == child && info.IsDir() {
			foundChild = true
			continue
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("non-regular replay artifact %q", name)
		}
		limit := min(replayMaxFileBytes, remaining)
		if info.Size() > int64(limit) {
			return nil, fmt.Errorf("%w: artifact bytes at %s", errReplayLimit, name)
		}
		f, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		payload, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
		if err := errors.Join(err, f.Close()); err != nil {
			return nil, err
		}
		if len(payload) > limit {
			return nil, fmt.Errorf("%w: artifact bytes at %s", errReplayLimit, name)
		}
		image[name] = payload
		remaining -= len(payload)
	}
	if !foundChild {
		return nil, fmt.Errorf("missing replay subdirectory %q", child)
	}
	return image, nil
}

func emitReplayRow(emit func(querymodel.Entry) error, row store.Row, ordinal uint64) error {
	if ordinal >= replayMaxRows {
		return fmt.Errorf("%w: observed ordinal %d", errReplayLimit, ordinal)
	}
	return emit(querymodel.Entry{Row: querymodel.Row{Time: row.TS, Line: row.Line}, Ordinal: int(ordinal)})
}

func scanReplayCheckpoint(ctx context.Context, reader *history.Reader, emit func(querymodel.Entry) error) error {
	q := history.Query{Start: replayFrom, End: replayUntil}
	return reader.Scan(ctx, q, func(row checkpoint.BlockRow) error {
		return emitReplayRow(emit, row.Row, row.Ordinal)
	})
}

func observeCheckpoint(path string) artifactObservation {
	return func(ctx context.Context, emit func(querymodel.Entry) error) (_ replayEvidence, err error) {
		image, err := readReplayFiles(ctx, path)
		if err != nil {
			return replayEvidence{}, err
		}
		return loadReplayCheckpoint(ctx, path, image, emit)
	}
}

func loadReplayCheckpoint(ctx context.Context, path string, image files, emit func(querymodel.Entry) error) (_ replayEvidence, err error) {
	var catalog history.Catalog
	defer func() { err = errors.Join(err, catalog.Close()) }()
	gen, err := catalog.Replace(ctx, path)
	if err != nil {
		if gen != (history.Generation{}) {
			return replayEvidence{}, fmt.Errorf("checkpoint admitted a generation alongside error: %v", err)
		}
		return replayEvidence{}, err
	}
	reader, err := catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return replayEvidence{}, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	if err := scanReplayCheckpoint(ctx, reader, emit); err != nil {
		return replayEvidence{}, err
	}
	if reader.Generation() != gen {
		return replayEvidence{}, errors.New("checkpoint reader pinned a different generation")
	}
	b := gen.Boundary
	return replayEvidence{
		boundary: replayBoundary{[sha256.Size]byte(gen.ID), b.SourceSHA256, b.CoveredOffset, b.CoveredOffset, b.NextOrdinal},
		files:    image,
	}, nil
}
func scanReplayBackup(ctx context.Context, reader *backup.Reader, emit func(querymodel.Entry) error) error {
	q := backup.Query{Start: replayFrom, End: replayUntil}
	// Each continuing page must be nonempty. The absolute page cap also
	// stops a reader that emits endless continuations.
	for range replayMaxRows + 1 {
		page, next, err := reader.QueryPage(ctx, q, replayPageSize)
		if err != nil {
			return err
		}
		if len(page) > replayPageSize {
			return fmt.Errorf("%w: backup page returned %d rows", errReplayLimit, len(page))
		}
		for _, row := range page {
			if err := emitReplayRow(emit, row.Row, row.Ordinal); err != nil {
				return err
			}
		}
		if next == nil {
			return nil
		}
		if len(page) == 0 || *next != page[len(page)-1].Position() ||
			(q.After != nil && *next == *q.After) {
			return errors.New("invalid backup replay continuation")
		}
		after := *next
		q.After = &after
	}
	return fmt.Errorf("%w: backup page count", errReplayLimit)
}

// observeBackup uses Verify for bundles and OpenRestored for both layouts. A
// verified v1 bundle is an offline-readable restore image as published.
func observeBackup(path string, bundle bool) artifactObservation {
	return func(ctx context.Context, emit func(querymodel.Entry) error) (_ replayEvidence, err error) {
		image, err := readReplayFiles(ctx, path)
		if err != nil {
			return replayEvidence{}, err
		}
		var verified backup.Point
		if bundle {
			verified, err = backup.Verify(ctx, path)
			if err != nil {
				if verified != (backup.Point{}) {
					return replayEvidence{}, fmt.Errorf("backup verification returned a point alongside error: %v", err)
				}
				return replayEvidence{}, err
			}
		}
		reader, err := backup.OpenRestored(ctx, path)
		if err != nil {
			if reader != nil {
				return replayEvidence{}, fmt.Errorf("backup opened a reader alongside error: %v", errors.Join(err, reader.Close()))
			}
			// Verification already accepted the bundle. Rejecting it only
			// when opening the reader cannot satisfy bundle rejection.
			if bundle {
				return replayEvidence{}, fmt.Errorf("verified bundle could not be opened: %v", err)
			}
			return replayEvidence{}, err
		}
		defer func() { err = errors.Join(err, reader.Close()) }()
		point := reader.Point()
		if bundle && verified != point {
			return replayEvidence{}, fmt.Errorf("verified point %+v differs from reader point %+v", verified, point)
		}
		var envelope struct{ Payload json.RawMessage }
		var manifest struct {
			SourceSHA256 string `json:"source_sha256"`
		}
		if err := json.Unmarshal(image[manifestFile], &envelope); err != nil {
			return replayEvidence{}, fmt.Errorf("observe accepted manifest envelope: %w", err)
		}
		if err := json.Unmarshal(envelope.Payload, &manifest); err != nil {
			return replayEvidence{}, fmt.Errorf("observe accepted manifest payload: %w", err)
		}
		b := replayBoundary{generation: point.Generation, covered: point.CoveredOffset, end: point.CommittedEnd, next: point.NextOrdinal}
		n, err := hex.Decode(b.prefixHash[:], []byte(manifest.SourceSHA256))
		if err != nil || n != sha256.Size {
			return replayEvidence{}, fmt.Errorf("invalid prefix digest in accepted manifest %q: %v", manifest.SourceSHA256, err)
		}
		if err := scanReplayBackup(ctx, reader, emit); err != nil {
			return replayEvidence{}, err
		}
		return replayEvidence{b, image}, nil
	}
}

func replayBackupMetadata(v replayView, generation [sha256.Size]byte) []byte {
	b := make([]byte, 128)
	copy(b, "BOGBAK01")
	copy(b[8:40], generation[:])
	le.PutUint64(b[40:48], uint64(v.covered.end))
	le.PutUint64(b[48:56], uint64(v.covered.rows))
	le.PutUint64(b[56:64], uint64(len(v.wal)))
	tailHash := sha256.Sum256(v.tail())
	copy(b[64:96], tailHash[:])
	sum := sha256.Sum256(b[:96])
	copy(b[96:], sum[:])
	return b
}

type capture struct {
	point backup.Point
	files files
	wal   []byte
}

func (c capture) Point() backup.Point { return c.point }

func (c capture) OpenManifest() (io.ReadCloser, error) {
	return c.OpenSegment(manifestFile)
}

func (c capture) OpenSegment(name string) (io.ReadCloser, error) {
	data, ok := c.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c capture) OpenWAL() (backup.WALReader, error) {
	return memoryWAL{bytes.NewReader(c.wal)}, nil
}

type memoryWAL struct{ *bytes.Reader }

func (memoryWAL) Close() error { return nil }

// Keep the first bounded prefix of subprocess output for diagnostics. Drain
// the rest so a noisy worker cannot grow parent memory or block on its pipe.
const replayWorkerLogBytes = 256 << 10

type replayWorkerLog struct {
	buffer   bytes.Buffer
	overflow bool
}

func (w *replayWorkerLog) Write(p []byte) (int, error) {
	keep := min(len(p), replayWorkerLogBytes-w.buffer.Len())
	if _, err := w.buffer.Write(p[:keep]); err != nil {
		return 0, err
	}
	w.overflow = w.overflow || keep < len(p)
	return len(p), nil
}

func (w *replayWorkerLog) limitError() error {
	if w.overflow {
		return fmt.Errorf("%w: worker output exceeds %d bytes", errReplayLimit, replayWorkerLogBytes)
	}
	return nil
}

func runBoundedReplayWorker(cmd *exec.Cmd) ([]byte, error) {
	output := new(replayWorkerLog)
	cmd.Stdout, cmd.Stderr = output, output
	err := cmd.Run()
	return output.buffer.Bytes(), errors.Join(err, output.limitError())
}
