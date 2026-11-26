package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// WALReader provides absolute offsets in a pinned legacy WAL.
type WALReader interface {
	io.ReaderAt
	io.Closer
}

// Snapshot is one already-pinned recovery point. The caller holds the pin until
// Export returns. Each open yields a fresh reader owned by Export. The manifest
// and named segments are immutable, and the WAL prefix through Point's
// CommittedEnd stays readable even if appends or generation replacement occur.
// A snapshot shared by concurrent exports must support concurrent opens.
type Snapshot interface {
	Point() Point
	OpenManifest() (io.ReadCloser, error)
	OpenSegment(name string) (io.ReadCloser, error)
	OpenWAL() (WALReader, error)
}

// Export creates a verified bundle in an absent or empty destination directory.
// Concurrent publishers coordinate within the destination parent. The caller
// must control the pathname against other filesystem writers.
func Export(ctx context.Context, snapshot Snapshot, destination string) error {
	return exportBackup(ctx, snapshot, destination, publish)
}

func exportBackup(ctx context.Context, snapshot Snapshot, destination string, publish publisher) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot == nil {
		return errors.New("backup snapshot is required")
	}
	return publish(ctx, destination, func(stage *os.Root) error {
		p := snapshot.Point()
		r, err := snapshot.OpenManifest()
		if err != nil {
			return fmt.Errorf("open captured manifest: %w", err)
		}
		b, err := readLimited(r, manifestLimit)
		if err != nil {
			return fmt.Errorf("read captured manifest: %w", err)
		}
		m, err := readManifest(b, p)
		if err != nil {
			return err
		}
		if err := writeBytes(stage, manifestName, b); err != nil {
			return err
		}
		for _, s := range m.Segments {
			if err := ctx.Err(); err != nil {
				return err
			}
			r, err := snapshot.OpenSegment(s.Name)
			if err != nil {
				return fmt.Errorf("open captured segment %s: %w", s.Name, err)
			}
			if err := copyFile(ctx, stage, s.Name, r, s.Size); err != nil {
				return err
			}
		}
		if _, err := validateSegments(ctx, stage, m); err != nil {
			return err
		}
		tailSum, err := exportWAL(ctx, snapshot, stage, p, m.SourceSHA256)
		if err != nil {
			return err
		}
		return writeBytes(stage, metadataName, (metadata{p, tailSum}).encode())
	})
}

func exportWAL(ctx context.Context, snapshot Snapshot, stage *os.Root, p Point, sourceSum string) (_ [32]byte, err error) {
	wal, err := snapshot.OpenWAL()
	if err != nil {
		return [32]byte{}, fmt.Errorf("open captured WAL: %w", err)
	}
	defer func() { err = errors.Join(err, wal.Close()) }()
	hash := sha256.New()
	prefix := io.TeeReader(io.NewSectionReader(wal, 0, p.CoveredOffset), hash)
	var magic [len(walMagic)]byte
	if _, err := io.ReadFull(prefix, magic[:]); err != nil {
		return [32]byte{}, fmt.Errorf("read captured WAL magic: %w", err)
	}
	if string(magic[:]) != walMagic {
		return [32]byte{}, corrupt("invalid WAL magic")
	}
	next, err := walkWAL(ctx, prefix, int64(len(magic)), p.CoveredOffset, 0, nil)
	if err != nil {
		return [32]byte{}, err
	}
	if next != p.NextOrdinal || hex.EncodeToString(hash.Sum(nil)) != sourceSum {
		return [32]byte{}, corrupt("captured WAL does not match checkpoint prefix or next ordinal")
	}
	hash.Reset()
	err = writeFile(stage, tailName, func(w io.Writer) error {
		tail := io.NewSectionReader(wal, p.CoveredOffset, p.CommittedEnd-p.CoveredOffset)
		_, err := walkWAL(ctx, io.TeeReader(tail, io.MultiWriter(w, hash)), p.CoveredOffset, p.CommittedEnd, next, nil)
		return err
	})
	return [32]byte(hash.Sum(nil)), err
}

type image struct {
	metadata
	manifest
}

func inspect(root *os.Root) (image, error) {
	var img image
	f, _, err := openRegular(root, metadataName)
	if err != nil {
		return img, err
	}
	b, err := readLimited(f, metadataSize)
	if err != nil {
		return img, err
	}
	img.metadata, err = decodeMetadata(b)
	if err != nil {
		return img, err
	}
	f, _, err = openRegular(root, manifestName)
	if err != nil {
		return img, err
	}
	b, err = readLimited(f, manifestLimit)
	if err != nil {
		return img, err
	}
	img.manifest, err = readManifest(b, img.Point)
	if err != nil {
		return img, err
	}
	d, err := root.Open(".")
	if err != nil {
		return img, err
	}
	want := len(img.Segments) + 3
	names, readErr := d.Readdirnames(want + 1)
	if err := errors.Join(unexpectedDirRead(readErr), d.Close()); err != nil {
		return img, err
	}
	if len(names) != want {
		return img, corrupt("unexpected file count")
	}
	// Opening every required file during validation accounts for the whole
	// directory. The count check rejects extra files, including orphans.
	return img, nil
}

func unexpectedDirRead(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// Verify validates the complete bundle and reports the exact recovery point.
func Verify(ctx context.Context, bundle string) (_ Point, err error) {
	root, err := os.OpenRoot(bundle)
	if err != nil {
		return Point{}, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	img, _, err := verifyImage(ctx, root)
	return img.Point, err
}

// Restore verifies a private staged copy before installing it. A destination
// that was absent or empty remains usable after any pre-publication failure.
// The bundle must remain immutable throughout the call. The caller must
// control the destination pathname against other filesystem writers.
func Restore(ctx context.Context, bundle, destination string) error {
	return restoreBackup(ctx, bundle, destination, publish)
}

func restoreBackup(ctx context.Context, bundle, destination string, publish publisher) error {
	return publish(ctx, destination, func(stage *os.Root) (err error) {
		src, err := os.OpenRoot(bundle)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, src.Close()) }()
		img, err := inspect(src)
		if err != nil {
			return err
		}
		files := []struct {
			name     string
			min, max int64
		}{
			{metadataName, metadataSize, metadataSize},
			{manifestName, 1, manifestLimit},
			{tailName, img.tailSize(), img.tailSize()},
		}
		for _, f := range files {
			if err := copyRegular(ctx, src, stage, f.name, f.min, f.max); err != nil {
				return err
			}
		}
		for _, s := range img.Segments {
			if err := copyRegular(ctx, src, stage, s.Name, s.Size, s.Size); err != nil {
				return err
			}
		}
		verified, _, err := verifyImage(ctx, stage)
		if err == nil && verified.metadata != img.metadata {
			return corrupt("bundle recovery point changed during restore")
		}
		return err
	})
}

func verifyImage(ctx context.Context, root *os.Root) (image, []bounds, error) {
	img, bounds, tail, err := verifyPinnedImage(ctx, root, nil)
	if err != nil {
		return image{}, nil, err
	}
	if err := tail.Close(); err != nil {
		return image{}, nil, err
	}
	return img, bounds, nil
}

func verifyPinnedImage(ctx context.Context, root *os.Root, checkReplay func(start, end int64) error) (image, []bounds, *os.File, error) {
	if err := ctx.Err(); err != nil {
		return image{}, nil, nil, err
	}
	img, err := inspect(root)
	if err != nil {
		return image{}, nil, nil, err
	}
	if checkReplay != nil {
		if err := checkReplay(img.CoveredOffset, img.CommittedEnd); err != nil {
			return image{}, nil, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return image{}, nil, nil, err
	}
	bounds, err := validateSegments(ctx, root, img.manifest)
	if err != nil {
		return image{}, nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return image{}, nil, nil, err
	}
	tail, err := openTail(root, img.metadata)
	if err != nil {
		return image{}, nil, nil, err
	}
	if err := scanTail(ctx, tail, img.metadata, nil); err != nil {
		return image{}, nil, nil, errors.Join(err, tail.Close())
	}
	return img, bounds, tail, nil
}

func validateSegments(ctx context.Context, root *os.Root, m manifest) ([]bounds, error) {
	var capacity int64
	for _, s := range m.Segments {
		capacity = max(capacity, s.Size)
	}
	buf := make([]byte, capacity)
	ordinals := make([]byte, (m.NextOrdinal+7)/8)
	result := make([]bounds, 0, len(m.Segments))
	var total uint64
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, size, err := openRegular(root, s.Name)
		if err != nil {
			return nil, err
		}
		if size != s.Size {
			return nil, errors.Join(corrupt("wrong size for %s", s.Name), f.Close())
		}
		b := buf[:size]
		_, err = io.ReadFull(f, b)
		if err := errors.Join(err, f.Close()); err != nil {
			return nil, fmt.Errorf("read %s: %w", s.Name, err)
		}
		bounds, n, err := validateBlock(b, s, ordinals, m.NextOrdinal)
		if err != nil {
			return nil, err
		}
		total += n
		result = append(result, bounds)
	}
	if total != m.NextOrdinal {
		return nil, corrupt("incomplete checkpoint ordinal coverage")
	}
	return result, nil
}

func openTail(root *os.Root, m metadata) (*os.File, error) {
	f, size, err := openRegular(root, tailName)
	if err != nil {
		return nil, err
	}
	if size != m.tailSize() {
		return nil, errors.Join(corrupt("wrong WAL tail size"), f.Close())
	}
	return f, nil
}

func readTail(ctx context.Context, root *os.Root, m metadata, visit walFrameVisitor) (err error) {
	f, err := openTail(root, m)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	return scanTail(ctx, f, m, visit)
}

func scanTail(ctx context.Context, f *os.File, m metadata, visit walFrameVisitor) error {
	hash := sha256.New()
	_, err := walkWAL(ctx, io.TeeReader(f, hash), m.Point.CoveredOffset, m.CommittedEnd, m.Point.NextOrdinal, visit)
	if err == nil && [32]byte(hash.Sum(nil)) != m.tailSum {
		return corrupt("WAL tail digest mismatch")
	}
	return err
}

func openRegular(root *os.Root, name string) (*os.File, int64, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: open %s: %w", ErrIntegrity, name, err)
	}
	if !info.Mode().IsRegular() {
		return nil, 0, corrupt("%s is not a regular file", name)
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, 0, err
	}
	current, err := f.Stat()
	if err == nil && (!current.Mode().IsRegular() || !os.SameFile(info, current)) {
		err = corrupt("%s changed during open", name)
	}
	if err != nil {
		return nil, 0, errors.Join(err, f.Close())
	}
	return f, current.Size(), nil
}

func readLimited(r io.ReadCloser, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	err = errors.Join(err, r.Close())
	if int64(len(b)) > limit {
		err = errors.Join(err, corrupt("file exceeds %d bytes", limit))
	}
	return b, err
}

func copyRegular(ctx context.Context, src, stage *os.Root, name string, minSize, maxSize int64) error {
	f, size, err := openRegular(src, name)
	if err != nil {
		return err
	}
	if size < minSize || size > maxSize {
		return errors.Join(corrupt("wrong size for %s", name), f.Close())
	}
	return copyFile(ctx, stage, name, f, size)
}

func copyFile(ctx context.Context, stage *os.Root, name string, r io.ReadCloser, size int64) (err error) {
	defer func() { err = errors.Join(err, r.Close()) }()
	return writeFile(stage, name, func(w io.Writer) error {
		source := contextReader{ctx, r}
		if _, err := io.CopyN(w, source, size); err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
		}
		var extra [1]byte
		n, err := io.ReadFull(source, extra[:])
		if n != 0 {
			return corrupt("extra bytes in %s", name)
		}
		if err == io.EOF {
			return nil
		}
		return err
	})
}

type contextReader struct {
	ctx context.Context
	io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(b)
}

func writeBytes(root *os.Root, name string, b []byte) error {
	return writeFile(root, name, func(w io.Writer) error {
		_, err := io.Copy(w, bytes.NewReader(b))
		return err
	})
}

func writeFile(root *os.Root, name string, write func(io.Writer) error) (err error) {
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := f.Chmod(0600); err != nil {
		return err
	}
	if err := write(f); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return f.Sync()
}
