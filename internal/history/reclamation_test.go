package history

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

func assertSegment(t *testing.T, dir, name string, present bool) {
	t.Helper()
	_, err := os.Lstat(filepath.Join(dir, name))
	if present {
		requireOK(t, err)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s to be reclaimed, got %v", name, err)
	}
}

func installHistoryRun(t *testing.T, dir string, i int, rows ...checkpoint.BlockRow) segmentMeta {
	t.Helper()
	b, err := checkpoint.EncodeBlock(rows)
	requireOK(t, err)
	name := fmt.Sprintf("bog-segment-%032x", i)
	writeFile(t, filepath.Join(dir, name), b)
	return segmentMeta{Name: name, Size: int64(len(b)), SHA256: hashText(b)}
}

func TestReclaimWaitsForSharedSegmentsAndEveryCatalog(t *testing.T) {
	ctx := context.Background()
	firstRow := row(epoch, 0, "first")
	sharedRow := row(epoch.Add(time.Nanosecond), 1, "shared")
	dir, first := segmentFixture(t, [][]checkpoint.BlockRow{{firstRow}, {sharedRow}})
	firstOnly, shared := first.Segments[0], first.Segments[1]
	unknown := installHistoryRun(t, dir, 90, row(epoch, 0, "unknown"))
	nextRow := row(epoch, 0, "next")
	nextOnly := installHistoryRun(t, dir, 2, nextRow)

	var cat, other Catalog
	cleanup(t, cat.Close)
	cleanup(t, other.Close)
	_, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	oldOther, err := other.Replace(ctx, dir)
	requireOK(t, err)
	r, err := other.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, r.Close)

	seen, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		i := 0
		want := []checkpoint.BlockRow{firstRow, sharedRow}
		err := r.Scan(ctx, all, func(item checkpoint.BlockRow) error {
			if i >= len(want) || item != want[i] {
				return fmt.Errorf("old row %d changed: %+v", i, item)
			}
			i++
			if i == 1 {
				close(seen)
				<-resume
			}
			return nil
		})
		if err == nil && i != len(want) {
			err = fmt.Errorf("old scan returned %d rows", i)
		}
		done <- err
	}()
	resumed := false
	defer func() {
		if !resumed {
			close(resume)
		}
	}()
	select {
	case <-seen:
	case err := <-done:
		t.Fatalf("old scan finished early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("old scan did not begin")
	}

	next := first
	next.Segments = []segmentMeta{nextOnly, shared}
	writeManifest(t, dir, next)
	middle, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	middleReader, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, middleReader.Close)
	requireOK(t, other.Close())
	requireOK(t, r.Close())
	requireOK(t, cat.Reclaim(ctx, dir))
	assertSegment(t, dir, firstOnly.Name, true)
	assertSegment(t, dir, shared.Name, true)

	last := next
	last.Segments = []segmentMeta{nextOnly}
	last.NextOrdinal = 1
	// Publish independently of the catalog. Reclamation must consult the new
	// authority while retaining anything still pinned by the middle generation.
	writeManifest(t, dir, last)
	requireOK(t, cat.Reclaim(ctx, dir))
	assertSegment(t, dir, shared.Name, true)
	_, err = cat.Replace(ctx, dir)
	requireOK(t, err)

	close(resume)
	resumed = true
	select {
	case err := <-done:
		requireOK(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("old scan did not drain")
	}
	select {
	case <-oldOther.Drained:
	default:
		t.Fatal("retired scan retained a generation")
	}
	requireOK(t, cat.Reclaim(ctx, dir))
	assertSegment(t, dir, firstOnly.Name, false)
	assertSegment(t, dir, shared.Name, true)
	if got := scanRows(t, middleReader, all); !slices.Equal(got, []checkpoint.BlockRow{nextRow, sharedRow}) {
		t.Fatalf("middle snapshot changed: %v", got)
	}
	requireOK(t, middleReader.Close())
	select {
	case <-middle.Drained:
	default:
		t.Fatal("middle generation did not drain")
	}
	requireOK(t, cat.Reclaim(ctx, dir))
	requireOK(t, cat.Reclaim(ctx, dir))
	assertSegment(t, dir, shared.Name, false)
	assertSegment(t, dir, unknown.Name, true)
	assertSegment(t, dir, nextOnly.Name, true)
}

func TestReclaimChecksAuthorityAndSerializesWithPublication(t *testing.T) {
	ctx := context.Background()
	dir, old := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "old")}})
	next := old
	next.Segments = []segmentMeta{installHistoryRun(t, dir, 1, row(epoch, 0, "next"))}
	var cat Catalog
	cleanup(t, cat.Close)
	_, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	writeManifest(t, dir, next)
	_, err = cat.Replace(ctx, dir)
	requireOK(t, err)

	writeManifest(t, dir, old)
	requireOK(t, cat.Reclaim(ctx, dir))
	assertSegment(t, dir, old.Segments[0].Name, true)
	writeFile(t, filepath.Join(dir, manifestName), []byte("invalid manifest"))
	if err := cat.Reclaim(ctx, dir); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("reclaim with corrupt authority: %v", err)
	}
	assertSegment(t, dir, old.Segments[0].Name, true)

	writeManifest(t, dir, next)
	pinned, _, err := openDirectory(dir)
	requireOK(t, err)
	cleanup(t, pinned.Close)
	func() {
		lock, err := checkpoint.LockPublication(pinned, true)
		requireOK(t, err)
		defer func() { requireOK(t, lock.Close()) }()
		if _, err := cat.Replace(ctx, dir); !errors.Is(err, syscall.EWOULDBLOCK) {
			t.Fatalf("replace during publication: %v", err)
		}
		if err := cat.Reclaim(ctx, dir); !errors.Is(err, syscall.EWOULDBLOCK) {
			t.Fatalf("reclaim during publication: %v", err)
		}
	}()
	assertSegment(t, dir, old.Segments[0].Name, true)
	requireOK(t, cat.Reclaim(ctx, dir))
	assertSegment(t, dir, old.Segments[0].Name, false)
}
