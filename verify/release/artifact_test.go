package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestArtifactReaderByteBudget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		input string
		limit int64
		want  string
		err   error
	}{
		{"empty at zero", "", 0, "", nil},
		{"nonempty at zero", "ab", 0, "", errArtifactOversized},
		{"short", "a", 2, "a", nil},
		{"exact", "ab", 2, "ab", nil},
		{"one over", "abc", 2, "ab", errArtifactOversized},
		{"many over", "abcdef", 2, "ab", errArtifactOversized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := strings.NewReader(tc.input)
			reader := newArtifactReader(t.Context(), source, tc.limit)
			data, err := io.ReadAll(reader)
			if string(data) != tc.want || !errors.Is(err, tc.err) {
				t.Fatalf("read = %q, %v; want %q, %v", data, err, tc.want, tc.err)
			}
			if tc.err == errArtifactOversized {
				var probe [1]byte
				if n, err := reader.Read(probe[:]); n != 0 || !errors.Is(err, errArtifactOversized) {
					t.Fatalf("read after overflow = %d, %v", n, err)
				}
			}
			if got := len(tc.input) - source.Len(); got != min(len(tc.input), int(tc.limit)+1) {
				t.Fatalf("read %d source bytes from %d with limit %d", got, len(tc.input), tc.limit)
			}
		})
	}
}

type artifactTestReader func([]byte) (int, error)

func (r artifactTestReader) Read(p []byte) (int, error) { return r(p) }

func TestArtifactReaderCancellationAndIOFailure(t *testing.T) {
	t.Parallel()
	t.Run("before any read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		source := artifactTestReader(func([]byte) (int, error) {
			t.Fatal("read after cancellation")
			return 0, nil
		})
		if data, err := io.ReadAll(newArtifactReader(ctx, source, 4)); len(data) != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("read = %q, %v; want canceled", data, err)
		}
	})
	t.Run("during final read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		source := artifactTestReader(func(p []byte) (int, error) {
			p[0] = 'x'
			cancel()
			return 1, io.EOF
		})
		if data, err := io.ReadAll(newArtifactReader(ctx, source, 4)); len(data) != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("read = %q, %v; want canceled", data, err)
		}
	})
	t.Run("I/O error at exact limit", func(t *testing.T) {
		failure := errors.New("injected final read failure")
		source := io.MultiReader(strings.NewReader("ok"), artifactTestReader(func([]byte) (int, error) {
			return 0, failure
		}))
		if data, err := io.ReadAll(newArtifactReader(t.Context(), source, 2)); string(data) != "ok" || !errors.Is(err, failure) {
			t.Fatalf("read = %q, %v; want partial data and I/O failure", data, err)
		}
	})
}

func TestArtifactFileChecks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, root) })
	writeFixture(t, filepath.Join(dir, "regular"), []byte("exact"))
	if err := os.Symlink("regular", filepath.Join(dir, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		entry  string
		rooted bool
		limit  int64
		want   error
	}{
		{"missing", "missing", false, 5, os.ErrNotExist},
		{"missing rooted", "missing", true, 5, os.ErrNotExist},
		{"directory", "directory", false, 5, errArtifactNonregular},
		{"rooted symlink", "symlink", true, 5, errArtifactNonregular},
		{"standalone symlink", "symlink", false, 5, nil},
		{"rooted exact regular", "regular", true, 5, nil},
		{"oversized regular", "regular", false, 4, errArtifactOversized},
		{"oversized rooted", "regular", true, 4, errArtifactOversized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var anchor *os.Root
			path := filepath.Join(dir, tc.entry)
			if tc.rooted {
				anchor, path = root, tc.entry
			}
			data, err := readArtifact(t.Context(), anchor, path, tc.limit)
			if tc.want == nil {
				if string(data) != "exact" || err != nil {
					t.Fatalf("read = %q, %v; want exact data", data, err)
				}
				return
			}
			if data != nil || !errors.Is(err, tc.want) || !strings.Contains(err.Error(), "open artifact "+strconv.Quote(path)) {
				t.Fatalf("read = %q, %v; want open failure for %q wrapping %v", data, err, path, tc.want)
			}
		})
	}
}

// Wait for a nonblocking operation on a FIFO that has no writer or reader
// configured by the test. A regression fails promptly even on the host.
func checkFIFO(t *testing.T, path string, check func() error) error {
	t.Helper()
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- check() }()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("artifact check blocked on FIFO %q", path)
		return nil
	}
}

func TestArtifactOpenedDescriptor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact")
	writeFixture(t, path, []byte("exact"))
	f, before, err := openRegular(t.Context(), nil, path, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, f) })
	if err := os.Rename(path, filepath.Join(dir, "original")); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, path, []byte("other"))
	replacement, _, err := openRegular(t.Context(), nil, path, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, replacement) })
	if info, err := checkArtifactDescriptor(t.Context(), replacement, before, 5); info != nil || err == nil ||
		err.Error() != "artifact changed while opening" {
		t.Fatalf("descriptor check = %v, %v", info, err)
	}
	// The original descriptor is still valid. Growing its inode after the
	// checked open must trip the streaming bound even after a path replacement.
	writer, err := os.OpenFile(filepath.Join(dir, "original"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Write(bytes.Repeat([]byte("x"), 100))
	if err := errors.Join(err, writer.Close()); err != nil {
		t.Fatal(err)
	}
	if data, err := io.ReadAll(newArtifactReader(t.Context(), f, 5)); string(data) != "exact" || !errors.Is(err, errArtifactOversized) {
		t.Fatalf("read grown descriptor = %q, %v", data, err)
	}
}
