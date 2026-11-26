package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestVerifyPinnedRestoreUsesPinnedDirectoryAndTail(t *testing.T) {
	ctx := t.Context()
	f := newFixture(t)
	base := t.TempDir()
	path, moved := filepath.Join(base, "restored"), filepath.Join(base, "pinned")
	must(t, Export(ctx, f, path))
	root, err := os.OpenRoot(path)
	must(t, err)
	rootClosed := false
	t.Cleanup(func() {
		if !rootClosed {
			must(t, root.Close())
		}
	})
	must(t, os.Rename(path, moved))
	must(t, os.Mkdir(path, 0700))

	start, end := f.point.CoveredOffset, f.point.CommittedEnd
	calls := 0
	point, tail, err := VerifyPinnedRestore(ctx, root, func(gotStart, gotEnd int64) error {
		calls++
		if gotStart != start || gotEnd != end {
			t.Fatalf("replay range = [%d, %d), want [%d, %d)", gotStart, gotEnd, start, end)
		}
		return nil
	})
	must(t, err)
	tailClosed := false
	t.Cleanup(func() {
		if !tailClosed {
			must(t, tail.Close())
		}
	})
	if calls != 1 || point != f.point {
		t.Fatalf("got point %+v and %d checks, want %+v and one check", point, calls, f.point)
	}
	_, err = root.Stat(metadataName)
	must(t, err)
	must(t, root.Rename(tailName, "verified-inode"))
	must(t, root.WriteFile(tailName, make([]byte, end-start), 0600))
	must(t, root.Close())
	rootClosed = true

	// Additional physical bytes must stay outside the verified logical range.
	writer, err := os.OpenFile(filepath.Join(moved, "verified-inode"), os.O_WRONLY|os.O_APPEND, 0)
	must(t, err)
	_, writeErr := writer.WriteString("outside")
	must(t, errors.Join(writeErr, writer.Close()))
	for _, tc := range []struct {
		name string
		off  int64
		size int
		n    int
		err  error
	}{
		{"full suffix", start, int(end - start), int(end - start), nil},
		{"interior", start + 2, 8, 8, nil},
		{"crosses end", end - 2, 9, 2, io.EOF},
		{"starts before suffix", start - 1, 8, 0, io.EOF},
		{"negative offset", math.MinInt64, 1, 0, io.EOF},
		{"at end", end, 1, 0, io.EOF},
		{"large offset", math.MaxInt64, 1, 0, io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.Repeat([]byte{0xff}, tc.size)
			n, err := tail.ReadAt(buf, tc.off)
			if n != tc.n || !errors.Is(err, tc.err) {
				t.Fatalf("ReadAt = %d, %v, want %d, %v", n, err, tc.n, tc.err)
			}
			if n > 0 && !bytes.Equal(buf[:n], f.wal[tc.off:tc.off+int64(n)]) {
				t.Fatal("read bytes differ from verified suffix")
			}
			if !bytes.Equal(buf[n:], bytes.Repeat([]byte{0xff}, tc.size-n)) {
				t.Fatal("read modified bytes outside its reported length")
			}
		})
	}
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.ReplayTailBytes: uint64(end - start)})
	replay, err := budget.ReplayCapturedTail(ctx, tail, start, end, point.NextOrdinal)
	must(t, err)
	var rows []store.Row
	var firsts []uint64
	for _, batch := range replay.Batches {
		rows = append(rows, batch.Rows...)
		firsts = append(firsts, batch.FirstOrdinal)
		batch.Reservation.Release()
	}
	if !slices.Equal(rows, f.rows[point.NextOrdinal:]) ||
		!slices.Equal(firsts, []uint64{6, 8}) || replay.NextOrdinal != uint64(len(f.rows)) {
		t.Fatalf("unexpected replay ordinals %v through %d", firsts, replay.NextOrdinal)
	}
	if got := budget.Usage().InUse[checkpoint.ReplayTailBytes]; got != 0 {
		t.Fatalf("replay retained %d bytes after releasing the batches", got)
	}
	must(t, tail.Close())
	tailClosed = true
	if _, err := tail.ReadAt(make([]byte, 1), start); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read after tail close = %v", err)
	}
}

func TestVerifyPinnedRestoreAdmissionAndIntegrity(t *testing.T) {
	denied := errors.New("replay range exceeds budget")
	for _, tc := range []struct {
		name  string
		edit  func(*testing.T, string, *fixture)
		check error
		want  error
		calls int
	}{
		{"reject before opening missing tail", func(t *testing.T, dir string, _ *fixture) {
			must(t, os.Rename(filepath.Join(dir, tailName), filepath.Join(dir, "orphan")))
		}, denied, denied, 1},
		{"reject before decoding damaged tail", func(t *testing.T, dir string, _ *fixture) {
			rewrite(t, filepath.Join(dir, tailName), func(b []byte) []byte { b[0] ^= 1; return b })
		}, denied, denied, 1},
		{"missing referenced segment", func(t *testing.T, dir string, f *fixture) {
			must(t, os.Rename(filepath.Join(dir, f.order[0]), filepath.Join(dir, "orphan")))
		}, nil, ErrIntegrity, 1},
		{"invalid logical range skips admission", func(t *testing.T, dir string, f *fixture) {
			rewrite(t, filepath.Join(dir, metadataName), func(b []byte) []byte {
				m, err := decodeMetadata(b)
				must(t, err)
				m.CommittedEnd = f.point.CoveredOffset - 1
				return m.encode()
			})
		}, denied, ErrIntegrity, 0},
		{"bad committed frame with valid digest", func(t *testing.T, dir string, f *fixture) {
			suffix := bytes.Clone(f.wal[f.point.CoveredOffset:f.point.CommittedEnd])
			suffix[len(suffix)-1] ^= 1
			rehashTail(t, dir, suffix)
		}, nil, ErrIntegrity, 1},
		{"bad tail digest with valid frames", func(t *testing.T, dir string, _ *fixture) {
			rewrite(t, filepath.Join(dir, metadataName), func(b []byte) []byte {
				m, err := decodeMetadata(b)
				must(t, err)
				m.tailSum[0] ^= 1
				return m.encode()
			})
		}, nil, ErrIntegrity, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			dir := filepath.Join(t.TempDir(), "restored")
			must(t, Export(t.Context(), f, dir))
			tc.edit(t, dir, f)
			root, err := os.OpenRoot(dir)
			must(t, err)
			t.Cleanup(func() { must(t, root.Close()) })
			calls := 0
			point, tail, err := VerifyPinnedRestore(t.Context(), root, func(start, end int64) error {
				calls++
				if start != f.point.CoveredOffset || end != f.point.CommittedEnd {
					t.Fatalf("unexpected admitted range [%d, %d)", start, end)
				}
				return tc.check
			})
			if tail != nil {
				must(t, tail.Close())
				t.Fatal("failed verification returned a tail")
			}
			if !errors.Is(err, tc.want) || point != (Point{}) || calls != tc.calls {
				t.Fatalf("verification got point %+v, %d checks, and error %v", point, calls, err)
			}
			_, err = root.Stat(metadataName)
			must(t, err)
		})
	}
}

func TestVerifyPinnedRestoreEmptyTailAndCancellation(t *testing.T) {
	f := newFixture(t)
	f.point.CommittedEnd = f.point.CoveredOffset
	dir := filepath.Join(t.TempDir(), "restored")
	must(t, Export(t.Context(), f, dir))
	root, err := os.OpenRoot(dir)
	must(t, err)
	t.Cleanup(func() { must(t, root.Close()) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p, tail, err := VerifyPinnedRestore(ctx, root, func(start, end int64) error {
		if start != f.point.CoveredOffset || end != start {
			t.Fatalf("empty tail range = [%d, %d)", start, end)
		}
		cancel()
		return nil
	})
	if tail != nil {
		must(t, tail.Close())
		t.Fatal("cancelled verification returned a tail")
	}
	if !errors.Is(err, context.Canceled) || p != (Point{}) {
		t.Fatalf("cancelled verification returned %+v, %v", p, err)
	}

	p, tail, err = VerifyPinnedRestore(t.Context(), root, nil)
	must(t, err)
	t.Cleanup(func() { must(t, tail.Close()) })
	if p != f.point {
		t.Fatalf("verified empty tail point = %+v, want %+v", p, f.point)
	}
	if n, err := tail.ReadAt(make([]byte, 1), p.CoveredOffset); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("empty tail read = %d, %v", n, err)
	}
}
