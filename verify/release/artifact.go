package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

var (
	errArtifactNonregular = errors.New("artifact is not a regular file")
	errArtifactOversized  = errors.New("artifact exceeds byte limit")
	errArtifactMalformed  = errors.New("malformed artifact")
)

// Require a regular file and compare the path's identity with the opened
// descriptor. Rooted artifacts reject symlinks. Standalone inputs may use
// symlinks to regular targets. O_NONBLOCK prevents a substituted FIFO from
// holding the open while it waits for a writer.
func openRegular(ctx context.Context, root *os.Root, path string, limit int64) (*os.File, os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if path == "" {
		return nil, nil, fmt.Errorf("missing path: %w", os.ErrNotExist)
	}
	stat, open := os.Stat, os.OpenFile
	flags := os.O_RDONLY | syscall.O_NONBLOCK
	if root != nil {
		stat, open = root.Lstat, root.OpenFile
		flags |= syscall.O_NOFOLLOW
	}
	before, err := stat(path)
	if err != nil {
		return nil, nil, artifactOpenError(err)
	}
	if !before.Mode().IsRegular() {
		return nil, nil, errArtifactNonregular
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	file, err := open(path, flags, 0)
	if err != nil {
		return nil, nil, artifactOpenError(err)
	}
	info, err := checkArtifactDescriptor(ctx, file, before, limit)
	if err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	return file, info, nil
}

func artifactOpenError(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("missing artifact: %w", err)
	}
	return err
}

func checkArtifactDescriptor(ctx context.Context, file *os.File, before os.FileInfo, limit int64) (os.FileInfo, error) {
	info, err := file.Stat()
	switch {
	case err != nil:
		return nil, err
	case !info.Mode().IsRegular():
		return nil, errArtifactNonregular
	case before != nil && !os.SameFile(before, info):
		return nil, errors.New("artifact changed while opening")
	case limit >= 0 && info.Size() > limit:
		return nil, artifactSizeError(limit)
	default:
		return info, ctx.Err()
	}
}

func artifactSizeError(limit int64) error {
	return fmt.Errorf("%w (%d bytes)", errArtifactOversized, limit)
}

// artifactReader enforces the inclusive limit on bytes actually read,
// including when a file grows after Stat. A one-byte probe at the boundary
// distinguishes an exact-size EOF from an oversized artifact.
type artifactReader struct {
	ctx       context.Context
	source    io.Reader
	limit     int64
	remaining int64
	overflow  bool
}

func newArtifactReader(ctx context.Context, source io.Reader, limit int64) *artifactReader {
	return &artifactReader{ctx: ctx, source: source, limit: limit, remaining: limit}
}

func (r *artifactReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		if r.overflow {
			return 0, artifactSizeError(r.limit)
		}
		var extra [1]byte
		n, err := r.source.Read(extra[:])
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if n > 0 {
			r.overflow = true
			return 0, artifactSizeError(r.limit)
		}
		return 0, err
	}
	n, err := r.source.Read(p[:min(int64(len(p)), r.remaining)])
	r.remaining -= int64(n)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	return n, err
}

func readArtifact(ctx context.Context, root *os.Root, path string, limit int64) ([]byte, error) {
	file, _, err := openRegular(ctx, root, path, limit)
	if err != nil {
		return nil, fmt.Errorf("open artifact %q: %w", path, err)
	}
	data, err := io.ReadAll(newArtifactReader(ctx, file, limit))
	if err := errors.Join(err, file.Close()); err != nil {
		return nil, fmt.Errorf("read artifact %q: %w", path, err)
	}
	return data, nil
}

// The caller receiving this reader still owns the descriptor's close.
type artifactReadCloser struct {
	io.Reader
	io.Closer
}

func boundedArtifactFile(ctx context.Context, file *os.File, limit int64) io.ReadCloser {
	return artifactReadCloser{Reader: newArtifactReader(ctx, file, limit), Closer: file}
}
