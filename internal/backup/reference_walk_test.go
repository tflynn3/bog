package backup

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/metrics"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/store"
)

func TestReferenceWalkOrderAndPinnedDirectory(t *testing.T) {
	f := newFixture(t)
	dir := filepath.Join(t.TempDir(), "original")
	must(t, Export(t.Context(), f, dir))

	// Open independent walks before retargeting their original path.
	queries := []Query{
		{Start: minTime, End: maxTime},
		{Start: minTime, End: maxTime, Contains: "[x]"},
		{Start: minTime, End: maxTime, After: &Position{Time: f.rows[1].TS, Ordinal: 1}},
		{Start: f.rows[1].TS, End: f.rows[1].TS},
	}
	walks := make([]*ReferenceWalk, len(queries))
	for i, q := range queries {
		w, err := OpenReferenceWalk(t.Context(), dir, q, i+1)
		must(t, err)
		t.Cleanup(func() { must(t, w.Close()) })
		walks[i] = w
	}
	must(t, os.Rename(dir, dir+"-pinned"))
	must(t, os.Mkdir(dir, 0700))
	names := []string{"all rows", "literal filter", "after tied row", "equal bounds"}
	for i, q := range queries {
		t.Run(names[i], func(t *testing.T) {
			t.Parallel()
			w := walks[i]
			want := oracle(f.rows, q)
			if q.After != nil {
				want = slices.DeleteFunc(want, func(row Row) bool { return !q.After.less(row.Position()) })
			}
			if w.Point() != f.point {
				t.Fatalf("point = %+v, want %+v", w.Point(), f.point)
			}
			var got []Row
			for {
				page, terminal, err := w.NextPage(t.Context())
				must(t, err)
				if len(page) > i+1 || (!terminal && len(page) == 0) {
					t.Fatalf("invalid page of %d rows, terminal=%v", len(page), terminal)
				}
				got = append(got, page...)
				if terminal {
					break
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ordinals = %v, want %v", ordinalList(got), ordinalList(want))
			}
			if _, _, err := w.NextPage(t.Context()); !errors.Is(err, io.EOF) {
				t.Fatalf("after terminal page: %v", err)
			}
		})
	}
}

// Make a tail much larger than the walk's memory budgets using a single frame
// buffer, with two timestamps per frame and ties spanning every sort run.
func writeLargeReferenceImage(t *testing.T, frames int) (string, metadata, [2]string) {
	t.Helper()
	dir := t.TempDir()
	prefix := []byte(walMagic)
	b, id := manifestEnvelope(t, manifest{Version: 1, SourceSHA256: digest(prefix), CoveredOffset: int64(len(prefix))})
	must(t, os.WriteFile(filepath.Join(dir, manifestName), b, 0600))
	ts := timeAt(t, "2026-01-01T00:00:00Z")
	lines := [2]string{
		"keep first\n" + strings.Repeat("a", 3<<19),
		"keep second " + strings.Repeat("b", 3<<19),
	}
	frame := frameRows(t, []store.Row{{TS: ts.Add(time.Hour), Line: lines[0]}, {TS: ts, Line: lines[1]}})
	f, err := os.Create(filepath.Join(dir, tailName))
	must(t, err)
	hash := sha256.New()
	out := io.MultiWriter(f, hash)
	var writeErr error
	for range frames {
		if _, writeErr = out.Write(frame); writeErr != nil {
			break
		}
	}
	must(t, errors.Join(writeErr, f.Close()))
	m := metadata{
		Point:   Point{Generation: id, CoveredOffset: int64(len(prefix)), CommittedEnd: int64(len(prefix) + len(frame)*frames)},
		tailSum: [32]byte(hash.Sum(nil)),
	}
	must(t, os.WriteFile(filepath.Join(dir, metadataName), m.encode(), 0600))
	return dir, m, lines
}

func TestReferenceWalkBoundsWholeTailWorkAndMemory(t *testing.T) {
	const frames = 40
	dir, m, lines := writeLargeReferenceImage(t, frames)
	scratch := t.TempDir()
	t.Setenv("TMPDIR", scratch)

	var scans, decodedFrames int
	var scanBytes int64
	measuredScan := func(ctx context.Context, f *os.File, m metadata, visit walFrameVisitor) error {
		scans++
		err := scanTail(ctx, f, m, func(off int64, first uint64, rows []store.Row) error {
			decodedFrames++
			return visit(off, first, rows)
		})
		n, seekErr := f.Seek(0, io.SeekCurrent)
		scanBytes += n
		return errors.Join(err, seekErr)
	}

	runtime.GC()
	base := referenceHeapBytes()
	var peak atomic.Uint64
	sample := func() {
		current := referenceHeapBytes()
		for last := peak.Load(); current > last; last = peak.Load() {
			if peak.CompareAndSwap(last, current) {
				return
			}
		}
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sample()
			case <-stop:
				return
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()

	w, err := openReferenceWalk(t.Context(), dir, Query{Start: minTime, End: maxTime, Contains: "keep"}, 3, measuredScan)
	must(t, err)
	t.Cleanup(func() { must(t, w.Close()) })
	// The original WAL is now unavailable. Every subsequent page must come
	// from the sorted tail and the still-pinned checkpoint.
	must(t, os.Rename(filepath.Join(dir, tailName), filepath.Join(dir, "unavailable")))
	var rows, pages int
	for {
		page, terminal, err := w.NextPage(t.Context())
		must(t, err)
		sample()
		pages++
		for _, row := range page {
			group := rows / frames
			if group >= len(lines) {
				t.Fatalf("unexpected extra row %d", rows+1)
			}
			want := uint64(2*(rows%frames) + 1 - group)
			if row.Ordinal != want || row.Line != lines[1-group] {
				t.Fatalf("row %d has ordinal %d and %d bytes, want ordinal %d", rows+1, row.Ordinal, len(row.Line), want)
			}
			rows++
		}
		if terminal {
			break
		}
	}
	must(t, w.Close())
	assertReferenceScratchEmpty(t, scratch)
	if scans != 1 || decodedFrames != frames || scanBytes != m.tailSize() {
		t.Fatalf("tail verification: %d scans, %d frames, %d bytes, want 1 scan, %d frames, %d bytes",
			scans, decodedFrames, scanBytes, frames, m.tailSize())
	}
	if rows != 2*frames || pages != (2*frames+2)/3 {
		t.Fatalf("walk returned %d rows in %d pages", rows, pages)
	}
	// scanTail feeds each read byte to SHA-256 exactly once. No subsequent
	// page can read the original tail, and the fixture is larger than the
	// allowed peak heap increase.
	heapGrowth := peak.Load() - min(peak.Load(), base)
	t.Logf("%d pages, %d committed-tail bytes read and hashed, %d bytes peak sampled heap growth", pages, scanBytes, heapGrowth)
	if heapGrowth > 80<<20 {
		t.Fatalf("walk grew heap by %d bytes", heapGrowth)
	}
}

func referenceHeapBytes() uint64 {
	sample := [1]metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample[:])
	return sample[0].Value.Uint64()
}

func TestReferenceWalkCleanupOnOpenFailure(t *testing.T) {
	for _, name := range []string{"cancellation after spill", "digest mismatch after spill", "scratch creation failure"} {
		t.Run(name, func(t *testing.T) {
			dir, m, _ := writeLargeReferenceImage(t, 5)
			scratch := t.TempDir()
			t.Setenv("TMPDIR", scratch)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			scan := scanTail
			var want error
			switch name {
			case "cancellation after spill":
				want = context.Canceled
				scan = func(ctx context.Context, f *os.File, m metadata, visit walFrameVisitor) error {
					return scanTail(ctx, f, m, func(off int64, first uint64, rows []store.Row) error {
						if err := visit(off, first, rows); err != nil {
							return err
						}
						entries, err := os.ReadDir(scratch)
						if err != nil {
							return err
						}
						if len(entries) != 0 {
							cancel()
						}
						return ctx.Err()
					})
				}
			case "digest mismatch after spill":
				m.tailSum[0] ^= 1
				must(t, os.WriteFile(filepath.Join(dir, metadataName), m.encode(), 0600))
				want = ErrIntegrity
			case "scratch creation failure":
				t.Setenv("TMPDIR", filepath.Join(scratch, "missing"))
				want = ErrReferenceWalk
			}
			w, err := openReferenceWalk(ctx, dir, Query{Start: minTime, End: maxTime}, 2, scan)
			if w != nil {
				must(t, w.Close())
				t.Fatal("returned a walk after open failed")
			}
			if !errors.Is(err, want) {
				t.Fatalf("open error = %v, want %v", err, want)
			}
			assertReferenceScratchEmpty(t, scratch)
		})
	}
}

func assertReferenceScratchEmpty(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	must(t, err)
	if len(entries) != 0 {
		t.Fatalf("scratch still contains %v", entries)
	}
}
