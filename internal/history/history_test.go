package history

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
var all = Query{Start: minTime, End: maxTime}

func row(ts time.Time, ord uint64, line string) checkpoint.BlockRow {
	return checkpoint.BlockRow{Row: store.Row{TS: ts, Line: line}, Ordinal: ord}
}

func requireOK(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func privateDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	requireOK(t, os.Chmod(dir, 0700))
	return dir
}

func cleanup(t testing.TB, close func() error) {
	t.Helper()
	t.Cleanup(func() { requireOK(t, close()) })
}

func publishFixture(t *testing.T, dir string) (m *checkpoint.Manifest, committed int64) {
	t.Helper()
	legacy, err := checkpoint.OpenLegacy(dir)
	requireOK(t, err)
	defer func() { requireOK(t, legacy.Close()) }()
	m, err = legacy.Publish()
	requireOK(t, err)
	return m, legacy.CommittedOffset()
}

func hashText(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func writeFile(t testing.TB, path string, b []byte) {
	t.Helper()
	requireOK(t, os.WriteFile(path, b, 0600))
	requireOK(t, os.Chmod(path, 0600))
}

func writeManifest(t testing.TB, dir string, m manifest) {
	t.Helper()
	p, err := json.Marshal(m)
	requireOK(t, err)
	b, err := json.Marshal(envelope{Payload: p, SHA256: hashText(p)})
	requireOK(t, err)
	writeFile(t, filepath.Join(dir, manifestName), b)
}

// The offline writer partitions append ordinals into contiguous runs. The v1
// reader also accepts sparse runs, so construct those with the shared codec.
func segmentFixture(t testing.TB, runs [][]checkpoint.BlockRow) (string, manifest) {
	t.Helper()
	dir := privateDir(t)
	m := manifest{Version: 1, CoveredOffset: 8, SourceSHA256: hashText([]byte("BOGWAL01"))}
	for i, run := range runs {
		b, err := checkpoint.EncodeBlock(run)
		requireOK(t, err)
		name := fmt.Sprintf("bog-segment-%032x", i)
		writeFile(t, filepath.Join(dir, name), b)
		m.Segments = append(m.Segments, segmentMeta{Name: name, Size: int64(len(b)), SHA256: hashText(b)})
		m.NextOrdinal += uint64(len(run))
	}
	if m.NextOrdinal > 0 {
		m.CoveredOffset = 100 // Fixture models an already verified WAL boundary.
	}
	writeManifest(t, dir, m)
	return dir, m
}

func scanRows(t *testing.T, r *Reader, q Query) []checkpoint.BlockRow {
	t.Helper()
	var rows []checkpoint.BlockRow
	requireOK(t, r.Scan(context.Background(), q, func(row checkpoint.BlockRow) error {
		rows = append(rows, row)
		return nil
	}))
	return rows
}

func TestSeekAcrossSparseRunsAndBackfilledTies(t *testing.T) {
	const runsCount, rowsPerRun = 9, 1100
	runs := make([][]checkpoint.BlockRow, runsCount)
	want := make([]checkpoint.BlockRow, 0, runsCount*rowsPerRun)
	for i := range runsCount * rowsPerRun {
		// A backfill pattern with ties, including ties across sparse seek points.
		ts := epoch.Add(time.Duration(i*19%13) * time.Nanosecond)
		r := row(ts, uint64(i), "same [.*]")
		j := i % runsCount
		runs[j] = append(runs[j], r)
		want = append(want, r)
	}
	less := func(a, b checkpoint.BlockRow) bool {
		return Position{a.Row.TS, a.Ordinal}.less(Position{b.Row.TS, b.Ordinal})
	}
	for _, run := range runs {
		sort.Slice(run, func(i, j int) bool { return less(run[i], run[j]) })
	}
	sort.Slice(want, func(i, j int) bool { return less(want[i], want[j]) })

	ctx := context.Background()
	dir, _ := segmentFixture(t, runs)
	var cat Catalog
	cleanup(t, cat.Close)
	_, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	r, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, r.Close)
	if got := scanRows(t, r, all); !slices.Equal(got, want) {
		t.Fatalf("full scan differs from reference: got %d rows, want %d", len(got), len(want))
	}
	q := Query{Start: epoch.Add(5), End: epoch.Add(10), Contains: "[.*]"}
	lo := sort.Search(len(want), func(i int) bool { return !want[i].Row.TS.Before(q.Start) })
	hi := sort.Search(len(want), func(i int) bool { return !want[i].Row.TS.Before(q.End) })
	want = want[lo:hi]
	stop := errors.New("page full")
	var got []checkpoint.BlockRow
	for {
		count := 0
		err := r.Scan(ctx, q, func(item checkpoint.BlockRow) error {
			got = append(got, item)
			count++
			if count == 97 {
				q.After = &Position{item.Row.TS, item.Ordinal}
				return stop
			}
			return nil
		})
		if err == nil {
			break
		}
		if !errors.Is(err, stop) {
			t.Fatal(err)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("page walk differs from reference: got %d rows, want %d", len(got), len(want))
	}
}

func TestGenerationReplacementDuringScan(t *testing.T) {
	ctx := context.Background()
	oldLine := strings.Repeat("old", readPageSize)
	dir, _ := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "old"), row(epoch, 1, oldLine)}})
	next, _ := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch.Add(-1), 0, "backfill")}})
	var cat Catalog
	cleanup(t, cat.Close)
	_, err := cat.AcquireCurrent()
	assertExpired(t, err, GenerationID{})
	old, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	r, err := cat.Acquire(old.ID)
	requireOK(t, err)
	cleanup(t, r.Close)

	seenFirst, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		n := 0
		err := r.Scan(ctx, all, func(item checkpoint.BlockRow) error {
			if item.Ordinal != uint64(n) || (n == 0 && item.Row.Line != "old") || (n == 1 && item.Row.Line != oldLine) {
				return fmt.Errorf("unexpected old generation row at ordinal %d", item.Ordinal)
			}
			n++
			if n == 1 {
				close(seenFirst)
				<-resume
			}
			return nil
		})
		if err == nil && n != 2 {
			err = fmt.Errorf("old scan finished with %d rows", n)
		}
		done <- err
	}()
	select {
	case <-seenFirst:
	case err := <-done:
		t.Fatalf("scan finished early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not start")
	}

	renamed := dir + "-moved"
	requireOK(t, os.Rename(dir, renamed))
	t.Cleanup(func() { requireOK(t, os.Rename(renamed, dir)) })
	newGen, err := cat.Replace(ctx, next)
	requireOK(t, err)
	if old.ID == newGen.ID {
		t.Fatal("different manifests have the same generation ID")
	}
	_, err = cat.Acquire(old.ID)
	assertExpired(t, err, old.ID)
	newReader, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, newReader.Close)
	if got := scanRows(t, newReader, all); len(got) != 1 || got[0].Row.Line != "backfill" {
		t.Fatalf("unexpected new snapshot: %v", got)
	}
	if got := scanRows(t, r, all); len(got) != 2 || got[1].Row.Line != oldLine {
		t.Fatalf("old reader after swap returned %d rows", len(got))
	}
	requireOK(t, r.Close())
	select {
	case <-old.Drained:
		t.Fatal("generation drained with a scan still active")
	default:
	}
	unblock()
	select {
	case err := <-done:
		requireOK(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("old scan did not finish")
	}
	select {
	case <-old.Drained:
	default:
		t.Fatal("old generation did not drain")
	}
	requireOK(t, cat.Close())
	_, err = cat.Replace(ctx, next)
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("replacement after Close: %v", err)
	}
	requireOK(t, newReader.Close())
	select {
	case <-newGen.Drained:
	default:
		t.Fatal("new generation did not drain")
	}
}

func assertExpired(t *testing.T, err error, id GenerationID) {
	t.Helper()
	var expired *ExpiredError
	if !errors.Is(err, ErrExpired) || !errors.As(err, &expired) || expired.ID != id {
		t.Fatalf("expected typed expiration for %x, got %v", id, err)
	}
}

func TestCheckpointCorruption(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*testing.T, string, *manifest)
	}{
		{"truncated segment", func(t *testing.T, dir string, m *manifest) {
			requireOK(t, os.Truncate(filepath.Join(dir, m.Segments[0].Name), m.Segments[0].Size-1))
		}},
		{"changed payload", editBlock(func(b []byte) { b[len(b)-1] ^= 1 }, false, false)},
		{"invalid header checksum", editBlock(func(b []byte) { b[40] ^= 1 }, true, false)},
		{"invalid payload checksum", editBlock(func(b []byte) { b[len(b)-1] ^= 1 }, true, false)},
		{"oversized row", editBlock(func(b []byte) { binary.LittleEndian.PutUint32(b[headerSize+20:], ^uint32(0)) }, true, true)},
		{"oversized row count", editBlock(func(b []byte) { binary.LittleEndian.PutUint32(b[4:], ^uint32(0)) }, true, true)},
		{"false time bounds", editBlock(func(b []byte) { b[32]++ }, true, true)},
		{"invalid UTF-8", editBlock(func(b []byte) { b[len(b)-1] = 0xff }, true, true)},
		{"missing referenced file", func(t *testing.T, dir string, m *manifest) {
			requireOK(t, os.Remove(filepath.Join(dir, m.Segments[0].Name)))
		}},
		{"unsafe symlink", func(t *testing.T, dir string, m *manifest) {
			path := filepath.Join(dir, m.Segments[0].Name)
			requireOK(t, os.Rename(path, path+".old"))
			requireOK(t, os.Symlink(path+".old", path))
		}},
		{"unsafe fifo", func(t *testing.T, dir string, m *manifest) {
			makeFIFO(t, filepath.Join(dir, m.Segments[0].Name))
		}},
		{"manifest checksum", func(t *testing.T, dir string, _ *manifest) {
			path := filepath.Join(dir, manifestName)
			b, err := os.ReadFile(path)
			requireOK(t, err)
			b[len(b)-3] ^= 1
			writeFile(t, path, b)
		}},
		{"oversized manifest", func(t *testing.T, dir string, _ *manifest) {
			requireOK(t, os.Truncate(filepath.Join(dir, manifestName), manifestMax+1))
		}},
		{"duplicate descriptor", func(t *testing.T, dir string, m *manifest) {
			m.Segments[1] = m.Segments[0]
			writeManifest(t, dir, *m)
		}},
		{"impossible row count", func(t *testing.T, dir string, m *manifest) {
			m.NextOrdinal = ^uint64(0)
			writeManifest(t, dir, *m)
		}},
		{"invalid boundary", func(t *testing.T, dir string, m *manifest) {
			m.CoveredOffset = 8
			writeManifest(t, dir, *m)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			runs := [][]checkpoint.BlockRow{{row(epoch, 0, "one")}, {row(epoch, 1, "two")}}
			good, _ := segmentFixture(t, runs)
			bad, m := segmentFixture(t, runs)
			tt.edit(t, bad, &m)
			var cat Catalog
			cleanup(t, cat.Close)
			gen, err := cat.Replace(ctx, good)
			requireOK(t, err)
			if _, err := cat.Replace(ctx, bad); !errors.Is(err, ErrIntegrity) || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("replacement accepted corruption or returned the wrong error: %v", err)
			}
			r, err := cat.Acquire(gen.ID)
			requireOK(t, err)
			cleanup(t, r.Close)
			if got := scanRows(t, r, all); len(got) != 2 {
				t.Fatalf("failed replacement altered current generation: %v", got)
			}
		})
	}
}

func editBlock(edit func([]byte), updateSHA, updateCRC bool) func(*testing.T, string, *manifest) {
	return func(t *testing.T, dir string, m *manifest) {
		path := filepath.Join(dir, m.Segments[0].Name)
		b, err := os.ReadFile(path)
		requireOK(t, err)
		edit(b)
		if updateCRC {
			binary.LittleEndian.PutUint32(b[36:], crc32.Checksum(b[headerSize:], crcTable))
			binary.LittleEndian.PutUint32(b[40:], crc32.Checksum(b[:40], crcTable))
		}
		writeFile(t, path, b)
		if updateSHA {
			m.Segments[0].SHA256 = hashText(b)
			writeManifest(t, dir, *m)
		}
	}
}

func makeFIFO(t *testing.T, path string) {
	t.Helper()
	requireOK(t, os.Remove(path))
	requireOK(t, syscall.Mkfifo(path, 0600))
}

func TestInvalidAppendOrderAndCoverage(t *testing.T) {
	for _, tt := range []struct {
		name string
		runs [][]checkpoint.BlockRow
	}{
		{"reverse tie", [][]checkpoint.BlockRow{{row(epoch, 1, "a"), row(epoch, 0, "b")}}},
		{"reverse time", [][]checkpoint.BlockRow{{row(epoch, 0, "a"), row(epoch.Add(-1), 1, "b")}}},
		{"ordinal outside coverage", [][]checkpoint.BlockRow{{row(epoch, 1, "a")}}},
		{"overlapping dense ordinals", [][]checkpoint.BlockRow{{row(epoch, 0, "a")}, {row(epoch.Add(1), 0, "b")}}},
		{"duplicate in a sparse run", [][]checkpoint.BlockRow{
			{row(epoch, 0, "a"), row(epoch.Add(1), 0, "b"), row(epoch.Add(2), 3, "c")},
			{row(epoch, 1, "d")},
		}},
		{"duplicate between sparse runs", [][]checkpoint.BlockRow{
			{row(epoch, 0, "a"), row(epoch.Add(1), 2, "b")},
			{row(epoch, 1, "c"), row(epoch.Add(1), 2, "d")},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := segmentFixture(t, tt.runs)
			var c Catalog
			cleanup(t, c.Close)
			_, err := c.Replace(context.Background(), dir)
			if !errors.Is(err, ErrIntegrity) {
				t.Fatalf("expected integrity error, got %v", err)
			}
			_, err = c.AcquireCurrent()
			assertExpired(t, err, GenerationID{})
		})
	}
}

func TestEmptyCheckpointAndOrphanFiles(t *testing.T) {
	dir := privateDir(t)
	ctx := context.Background()
	var c Catalog
	cleanup(t, c.Close)
	_, err := c.Replace(ctx, dir)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing manifest: %v", err)
	}
	writeFile(t, filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"))
	writeFile(t, filepath.Join(dir, "bog-manifest-abandoned"), []byte("torn manifest"))
	writeFile(t, filepath.Join(dir, "bog-segment-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("torn block"))
	_, committed := publishFixture(t, dir)
	a, err := c.Replace(ctx, dir)
	requireOK(t, err)
	if a.Boundary.CoveredOffset != committed || committed != 8 || a.Boundary.NextOrdinal != 0 {
		t.Fatalf("unexpected empty boundary: %+v", a.Boundary)
	}
	b, err := c.Replace(ctx, dir)
	requireOK(t, err)
	if a.ID != b.ID {
		t.Fatal("reopening exact manifest changed its identity")
	}
	select {
	case <-a.Drained:
	default:
		t.Fatal("replaced generation has no outstanding readers")
	}
	r, err := c.Acquire(a.ID)
	requireOK(t, err)
	cleanup(t, r.Close)
	if got := scanRows(t, r, all); len(got) != 0 {
		t.Fatalf("orphan rows became visible: %v", got)
	}
}

func TestScanSkipsDisjointSegmentsAndFailsOnChangedInput(t *testing.T) {
	ctx := context.Background()
	dir, m := segmentFixture(t, [][]checkpoint.BlockRow{
		{row(epoch, 0, "one")},
		{row(epoch.Add(time.Second), 1, "two")},
	})
	var c Catalog
	cleanup(t, c.Close)
	_, err := c.Replace(ctx, dir)
	requireOK(t, err)
	r, err := c.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, r.Close)

	path := filepath.Join(dir, m.Segments[1].Name)
	requireOK(t, os.Truncate(path, m.Segments[1].Size-1))
	narrow := Query{Start: epoch, End: epoch.Add(time.Second)}
	if got := scanRows(t, r, narrow); len(got) != 1 || got[0].Ordinal != 0 {
		t.Fatalf("disjoint range: %v", got)
	}
	err = r.Scan(ctx, all, func(item checkpoint.BlockRow) error {
		t.Fatalf("a scan with an unreadable starting run delivered ordinal %d", item.Ordinal)
		return nil
	})
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("changed segment: %v", err)
	}
}

func TestCancellationAndVisitorError(t *testing.T) {
	dir, _ := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "one"), row(epoch, 1, "two")}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var c Catalog
	cleanup(t, c.Close)
	_, err := c.Replace(ctx, dir)
	requireOK(t, err)
	r, err := c.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, r.Close)

	stop := errors.New("visitor stopped")
	if err := r.Scan(ctx, all, func(checkpoint.BlockRow) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("visitor error: %v", err)
	}
	var count int
	err = r.Scan(ctx, all, func(checkpoint.BlockRow) error {
		count++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || count != 1 {
		t.Fatalf("canceled scan: visited %d rows, returned %v", count, err)
	}
	if _, err := c.Replace(ctx, dir); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replacement: %v", err)
	}
	requireOK(t, r.Close())
	if err := r.Scan(context.Background(), all, func(checkpoint.BlockRow) error { return nil }); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed reader scan: %v", err)
	}
}

func TestMaxSegmentFanIn(t *testing.T) {
	runs := make([][]checkpoint.BlockRow, checkpoint.MaxSegments)
	for i := range runs {
		// Full overlap, in reverse append order in the manifest.
		runs[i] = []checkpoint.BlockRow{row(epoch, uint64(len(runs)-1-i), "same")}
	}
	ctx := context.Background()
	dir, m := segmentFixture(t, runs)
	var c Catalog
	cleanup(t, c.Close)
	g, err := c.Replace(ctx, dir)
	requireOK(t, err)
	r, err := c.Acquire(g.ID)
	requireOK(t, err)
	cleanup(t, r.Close)
	got := scanRows(t, r, all)
	if len(got) != len(runs) {
		t.Fatalf("max fan-in returned %d of %d rows", len(got), len(runs))
	}
	for i, item := range got {
		if item.Ordinal != uint64(i) {
			t.Fatalf("row %d has ordinal %d", i, item.Ordinal)
		}
	}
	m.Segments = append(m.Segments, m.Segments[0])
	writeManifest(t, dir, m)
	if _, err := c.Replace(ctx, dir); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("replacement exceeded max fan-in: %v", err)
	}
}

func BenchmarkHistory(b *testing.B) {
	const runCount, rowsPerRun = 32, 2048
	runs := make([][]checkpoint.BlockRow, runCount)
	line := strings.Repeat("retained history [a.b] ", 24)
	for j := range runs {
		run := make([]checkpoint.BlockRow, rowsPerRun)
		for i := range run {
			ts := epoch.Add(time.Duration(i) * time.Nanosecond)
			ordinal := uint64(j*rowsPerRun + (i*773)%rowsPerRun)
			run[i] = row(ts, ordinal, line)
		}
		runs[j] = run
	}
	dir, _ := segmentFixture(b, runs)
	ctx := context.Background()
	b.Run("validate", func(b *testing.B) {
		var c Catalog
		cleanup(b, c.Close)
		b.ReportAllocs()
		for b.Loop() {
			_, err := c.Replace(ctx, dir)
			requireOK(b, err)
		}
	})
	var c Catalog
	cleanup(b, c.Close)
	_, err := c.Replace(ctx, dir)
	requireOK(b, err)
	r, err := c.AcquireCurrent()
	requireOK(b, err)
	cleanup(b, r.Close)
	for _, q := range []struct {
		name  string
		query Query
	}{
		{"seek", Query{Start: epoch.Add(1100), End: epoch.Add(1101), Contains: "[a.b]"}},
		{"wide", Query{Start: epoch, End: epoch.Add(rowsPerRun), Contains: "[a.b]"}},
	} {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				requireOK(b, r.Scan(ctx, q.query, func(checkpoint.BlockRow) error { return nil }))
			}
		})
	}
}
