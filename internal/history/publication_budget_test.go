package history

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

func budgetTestFrame(payload string) []byte {
	var header [16]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], ^uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[8:12], crc32.Checksum([]byte(payload), crcTable))
	binary.LittleEndian.PutUint32(header[12:], crc32.Checksum(header[:12], crcTable))
	return append(header[:], payload...)
}

type budgetTestReader struct {
	io.ReaderAt
	before func()
}

func (r budgetTestReader) ReadAt(p []byte, offset int64) (int, error) {
	r.before()
	return r.ReaderAt.ReadAt(p, offset)
}

func TestLivePublicationKeepsCapturedTailCharged(t *testing.T) {
	ctx := t.Context()
	firstFrame := budgetTestFrame("{\"ts\":\"2026-01-01T00:00:00Z\",\"line\":\"covered\"}\n")
	secondFrame := budgetTestFrame("{\"ts\":\"2026-01-01T00:00:00Z\",\"line\":\"later\"}\n")
	magic := []byte("BOGWAL01")
	firstPrefix := append(bytes.Clone(magic), firstFrame...)
	boundary, cutoff := int64(len(magic)), int64(len(firstPrefix))
	end := cutoff + int64(len(secondFrame))
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      4096,
		checkpoint.ReplayTailBytes:   uint64(end - boundary),
		checkpoint.PinnedGenerations: 2,
		checkpoint.LiveCaptures:      1,
	})
	root, err := os.OpenRoot(privateDir(t))
	requireOK(t, err)
	cleanup(t, root.Close)
	dir, err := root.Open(".")
	requireOK(t, err)
	cleanup(t, dir.Close)
	requireOK(t, syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	publisher := checkpoint.NewPublisher(root, dir, budget)
	err = publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) { return ValidatePinnedAuthority(ctx, dir) })
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty authority recovery: %v", err)
	}
	first, err := publisher.PublishCaptured(ctx, bytes.NewReader(magic), boundary, nil, 0, hashText(magic))
	requireOK(t, err)
	cat := NewCatalog(budget)
	cleanup(t, cat.Close)
	old, err := cat.ReplacePinned(ctx, dir)
	requireOK(t, err)
	tail, err := budget.ReplayCapturedTail(ctx, bytes.NewReader(firstPrefix), boundary, cutoff, 0)
	requireOK(t, err)
	live := tail.Batches[0].Reservation
	defer live.Release()

	reader, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, reader.Close)
	held := live.Retain()
	defer held.Release()

	ticket, err := cat.ReserveReplacement(ctx)
	requireOK(t, err)
	defer ticket.Close()
	began, resume := make(chan struct{}), make(chan struct{})
	var readOnce, resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	defer unblock()
	input := budgetTestReader{
		ReaderAt: bytes.NewReader(firstPrefix),
		before:   func() { readOnce.Do(func() { close(began); <-resume }) },
	}
	done := make(chan error, 1)
	go func() {
		_, err := publisher.PublishCaptured(ctx, input, cutoff, first, tail.NextOrdinal, hashText(firstPrefix))
		done <- err
	}()
	select {
	case <-began:
	case err := <-done:
		t.Fatalf("publication returned before reading the prefix: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("publication did not reach the WAL")
	}

	// This is the WAL owner's admission and transfer for a concurrent append.
	appended, appendErr := budget.ReserveReplayTail(ctx, cutoff, end)
	unblock()
	requireOK(t, <-done)
	requireOK(t, appendErr)
	defer appended.Release()
	next, err := ticket.ReplacePinned(ctx, dir)
	requireOK(t, err)
	if next.Boundary.CoveredOffset != cutoff || next.Boundary.NextOrdinal != 1 {
		t.Fatalf("active boundary = %+v", next.Boundary)
	}
	live.Release()
	_, err = budget.ReserveReplayTail(ctx, end, end+1)
	assertExhausted(t, err, checkpoint.ReplayTailBytes, uint64(end-boundary))
	if got := scanRows(t, reader, all); len(got) != 0 || reader.Generation().ID != old.ID {
		t.Fatalf("captured generation changed after publication: %v", got)
	}
	requireOK(t, reader.Close())
	held.Release()
	more, err := budget.ReserveReplayTail(ctx, end, end+1)
	requireOK(t, err)
	more.Release()
	current, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, current.Close)
	if got := scanRows(t, current, all); len(got) != 1 || got[0] != row(epoch, 0, "covered") {
		t.Fatalf("active generation = %v", got)
	}
	usage := budget.Usage()
	if usage.InUse[checkpoint.StagingBytes] != 0 || usage.Peak[checkpoint.StagingBytes] == 0 ||
		usage.InUse[checkpoint.ReplayTailBytes] != uint64(len(secondFrame)) ||
		usage.Peak[checkpoint.ReplayTailBytes] != uint64(end-boundary) {
		t.Fatalf("publication evidence = %+v", usage)
	}
}

func TestPublicationKeepsAdvancingAtSegmentCeiling(t *testing.T) {
	ctx := t.Context()
	path := privateDir(t)
	initial, wal, baseline := ceilingFixture(t, path)
	root, err := os.OpenRoot(path)
	requireOK(t, err)
	cleanup(t, root.Close)
	dir, err := root.Open(".")
	requireOK(t, err)
	cleanup(t, dir.Close)
	requireOK(t, syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      256 << 10,
		checkpoint.PinnedGenerations: 4,
		checkpoint.LiveCaptures:      3,
	})
	publisher := checkpoint.NewPublisher(root, dir, budget)
	cat := NewCatalog(budget)
	cleanup(t, cat.Close)
	gen, err := cat.ReplacePinned(ctx, dir)
	requireOK(t, err)
	held, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, held.Close)

	nextWAL, added := ceilingBackfill(wal, initial.NextOrdinal)
	assertInitial := func() {
		t.Helper()
		got, err := ValidatePinnedAuthority(ctx, dir)
		requireOK(t, err)
		if !reflect.DeepEqual(initial, got) {
			t.Fatal("old authority changed after failed publication")
		}
		assertPublicationRows(t, held, baseline)
	}

	// Leave enough bytes to stage the merged segment, but not the manifest.
	blocker, err := budget.Reserve(ctx, checkpoint.StagingBytes, (256<<10)-1024)
	requireOK(t, err)
	defer blocker.Release()
	ticket, err := cat.ReserveReplacement(ctx)
	requireOK(t, err)
	defer ticket.Close()
	got, err := publisher.PublishCaptured(ctx, bytes.NewReader(nextWAL), int64(len(nextWAL)),
		initial, initial.NextOrdinal+uint64(len(added)), hashText(nextWAL))
	if got != nil || !errors.Is(err, checkpoint.ErrExhausted) || errors.Is(err, checkpoint.ErrPublicationUncertain) {
		t.Fatalf("budget failure returned manifest %v, error %v", got != nil, err)
	}
	if usage := budget.Usage(); usage.InUse[checkpoint.StagingBytes] != (256<<10)-1024 ||
		usage.Peak[checkpoint.StagingBytes] <= usage.InUse[checkpoint.StagingBytes] {
		t.Fatalf("failed attempt did not stage data and settle its charge: %+v", usage)
	}
	requireOK(t, publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
		return ValidatePinnedAuthority(ctx, dir)
	}))
	assertInitial()
	blocker.Release()

	// Keep an admitted scan alive after its Reader closes, alongside a
	// separate Reader that can start a new scan after replacement.
	scanning, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, scanning.Close)
	started, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var release sync.Once
	continueScan := func() { release.Do(func() { close(resume) }) }
	defer func() {
		continueScan()
		select {
		case err := <-done:
			requireOK(t, err)
		case <-time.After(10 * time.Second):
			t.Error("old scan did not exit")
		}
	}()
	go func() {
		want := orderedPublicationRows(baseline, all)
		i := 0
		err := scanning.Scan(ctx, all, func(item checkpoint.BlockRow) error {
			if i == 0 {
				close(started)
				<-resume
			}
			if i >= len(want) || item != want[i] {
				return fmt.Errorf("old scan row %d changed: %v", i, item)
			}
			i++
			return nil
		})
		if err == nil && i != len(want) {
			err = fmt.Errorf("old scan returned %d rows, want %d", i, len(want))
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("scan did not enter visitor")
	}

	previous := initial
	want := slices.Clone(baseline)
	var retired []string
	var rewritten int64
	merges := 0
	for i := 0; i < 9; i++ {
		nextWAL, added := ceilingBackfill(wal, previous.NextOrdinal)
		next := previous.NextOrdinal + uint64(len(added))
		if i != 0 {
			ticket, err = cat.ReserveReplacement(ctx)
			requireOK(t, err)
			defer ticket.Close()
		}
		after, err := publisher.PublishCaptured(ctx, bytes.NewReader(nextWAL), int64(len(nextWAL)),
			previous, next, hashText(nextWAL))
		requireOK(t, err)
		if after == nil || len(after.Segments) > checkpoint.MaxSegments || after.NextOrdinal != next {
			t.Fatalf("invalid successor on backfill %d: %v", i, after != nil)
		}
		retiredNow, bytes := publicationChanges(previous, after)
		rewritten += bytes
		if len(retiredNow) > 0 {
			merges++
		}
		if i == 0 {
			retired = retiredNow
		}
		active, err := ticket.ReplacePinned(ctx, dir)
		requireOK(t, err)
		if active.Boundary.NextOrdinal != next || active.Boundary.CoveredOffset != int64(len(nextWAL)) {
			t.Fatalf("wrong active cutoff after backfill %d: %+v", i, active.Boundary)
		}
		want = append(want, added...)
		current, err := cat.AcquireCurrent()
		requireOK(t, err)
		cleanup(t, current.Close)
		assertPublicationRows(t, current, want)
		requireOK(t, current.Close())
		requireOK(t, cat.ReclaimPinned(ctx, dir))
		for _, name := range retired {
			assertSegment(t, path, name, true)
		}
		previous, wal = after, nextWAL
	}
	if merges < 2 || len(retired) == 0 || rewritten > 8<<10 {
		t.Fatalf("merge count %d, first retirement %d files, wrote %d segment bytes", merges, len(retired), rewritten)
	}

	assertPublicationRows(t, held, baseline)
	requireOK(t, held.Close())
	requireOK(t, scanning.Close())
	requireOK(t, cat.ReclaimPinned(ctx, dir))
	for _, name := range retired {
		assertSegment(t, path, name, true)
	}
	continueScan()
	select {
	case <-gen.Drained:
	case <-time.After(10 * time.Second):
		t.Fatal("old generation remained pinned")
	}
	requireOK(t, cat.ReclaimPinned(ctx, dir))
	for _, name := range retired {
		assertSegment(t, path, name, false)
	}
	if len(cat.retired) != 0 {
		t.Fatal("reclamation retained candidates after all superseded pins drained")
	}
	for _, s := range previous.Segments {
		assertSegment(t, path, s.Name, true)
	}

	// Open a new catalog from the settled disk authority.
	requireOK(t, cat.Close())
	reopened := NewCatalog(budget)
	cleanup(t, reopened.Close)
	_, err = reopened.ReplacePinned(ctx, dir)
	requireOK(t, err)
	reader, err := reopened.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, reader.Close)
	assertPublicationRows(t, reader, want)
	t.Logf("repeated backfill wrote %d segment bytes across %d merges", rewritten, merges)
}

func ceilingFixture(t testing.TB, path string) (*checkpoint.Manifest, []byte, []checkpoint.BlockRow) {
	t.Helper()
	wal := []byte("BOGWAL01")
	m := &checkpoint.Manifest{Version: 1, NextOrdinal: checkpoint.MaxSegments}
	var baseline []checkpoint.BlockRow
	var payload []byte
	for i := 0; i < checkpoint.MaxSegments; i++ {
		stamp := epoch.Add(time.Duration((i*11)%5) * time.Second)
		line := []string{"keep", "keep", "Keep", "x keep y"}[i%4]
		payload = fmt.Appendf(payload, "{\"ts\":%q,\"line\":%q}\n", stamp.Format(time.RFC3339Nano), line)
		item := row(stamp, uint64(i), line)
		baseline = append(baseline, item)
		itemBlock := installHistoryRunFixture(t, path, i, item)
		m.Segments = append(m.Segments, itemBlock)
	}
	wal = append(wal, budgetTestFrame(string(payload))...)
	m.CoveredOffset, m.SourceSHA256 = int64(len(wal)), hashText(wal)
	writeManifest(t, path, *m)
	return m, wal, baseline
}

func installHistoryRunFixture(t testing.TB, path string, index int, item checkpoint.BlockRow) checkpoint.Segment {
	t.Helper()
	block, err := checkpoint.EncodeBlock([]checkpoint.BlockRow{item})
	requireOK(t, err)
	name := fmt.Sprintf("bog-segment-%032x", index)
	writeFile(t, filepath.Join(path, name), block)
	return checkpoint.Segment{Name: name, Size: int64(len(block)), SHA256: hashText(block)}
}

func ceilingBackfill(wal []byte, ordinal uint64) ([]byte, []checkpoint.BlockRow) {
	data := budgetTestFrame("{\"ts\":\"2026-01-01T00:00:02Z\",\"line\":\"keep\"}\n" +
		"{\"ts\":\"2026-01-01T00:00:01Z\",\"line\":\"x keep y\"}\n" +
		"{\"ts\":\"2026-01-01T00:00:02Z\",\"line\":\"keep\"}\n")
	return append(slices.Clone(wal), data...), []checkpoint.BlockRow{
		row(epoch.Add(2*time.Second), ordinal, "keep"),
		row(epoch.Add(time.Second), ordinal+1, "x keep y"),
		row(epoch.Add(2*time.Second), ordinal+2, "keep"),
	}
}

func publicationChanges(before, after *checkpoint.Manifest) (removed []string, written int64) {
	names := make(map[string]bool, len(before.Segments))
	for _, old := range before.Segments {
		names[old.Name] = true
	}
	for _, current := range after.Segments {
		if names[current.Name] {
			delete(names, current.Name)
		} else {
			written += current.Size
		}
	}
	for name := range names {
		removed = append(removed, name)
	}
	return removed, written
}

func orderedPublicationRows(source []checkpoint.BlockRow, q Query) []checkpoint.BlockRow {
	var rows []checkpoint.BlockRow
	for _, item := range source {
		if !item.Row.TS.Before(q.Start) && item.Row.TS.Before(q.End) && strings.Contains(item.Row.Line, q.Contains) {
			rows = append(rows, item)
		}
	}
	slices.SortFunc(rows, func(a, b checkpoint.BlockRow) int {
		if order := a.Row.TS.Compare(b.Row.TS); order != 0 {
			return order
		}
		return cmp.Compare(a.Ordinal, b.Ordinal)
	})
	return rows
}

func assertPublicationRows(t *testing.T, reader *Reader, want []checkpoint.BlockRow) {
	t.Helper()
	filtered := Query{Start: epoch.Add(time.Second), End: epoch.Add(4 * time.Second), Contains: "keep"}
	for _, q := range []Query{all, filtered} {
		got := scanRows(t, reader, q)
		if expected := orderedPublicationRows(want, q); !slices.Equal(got, expected) {
			t.Fatalf("different ordered rows for %v: got %d rows, want %d", q, len(got), len(expected))
		}
	}
}

// Run with -benchtime=1x to record one nine-publication sequence. The peak
// samples the process heap during publication and includes the live catalog.
func BenchmarkPublicationAtSegmentCeiling(b *testing.B) {
	var written, peakHeap uint64
	for b.Loop() {
		size, peak := benchmarkCeilingBackfill(b)
		written += size
		peakHeap = max(peakHeap, peak)
	}
	b.ReportMetric(float64(written)/float64(b.N), "segment_B/sequence")
	b.ReportMetric(float64(peakHeap), "peak_heap_B")
}

func benchmarkCeilingBackfill(b *testing.B) (written, peakHeap uint64) {
	b.Helper()
	b.StopTimer()
	defer b.StartTimer()
	path := privateDir(b)
	previous, wal, _ := ceilingFixture(b, path)
	root, err := os.OpenRoot(path)
	requireOK(b, err)
	defer func() { requireOK(b, root.Close()) }()
	dir, err := root.Open(".")
	requireOK(b, err)
	defer func() { requireOK(b, dir.Close()) }()
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      256 << 10,
		checkpoint.PinnedGenerations: 3,
		checkpoint.LiveCaptures:      1,
	})
	requireOK(b, syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	pub, cat := checkpoint.NewPublisher(root, dir, budget), NewCatalog(budget)
	defer func() { requireOK(b, cat.Close()) }()
	_, err = cat.ReplacePinned(b.Context(), dir)
	requireOK(b, err)
	held, err := cat.AcquireCurrent()
	requireOK(b, err)
	defer func() { requireOK(b, held.Close()) }()

	runtime.GC()
	stop := make(chan struct{})
	done := make(chan uint64, 1)
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		var peak uint64
		for {
			metrics.Read(sample)
			peak = max(peak, sample[0].Value.Uint64())
			select {
			case <-stop:
				done <- peak
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() {
		close(stop)
		peakHeap = <-done
	}()
	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < 9; i++ {
		nextWAL, added := ceilingBackfill(wal, previous.NextOrdinal)
		next := previous.NextOrdinal + uint64(len(added))
		ticket, err := cat.ReserveReplacement(b.Context())
		requireOK(b, err)
		after, err := pub.PublishCaptured(b.Context(), bytes.NewReader(nextWAL), int64(len(nextWAL)),
			previous, next, hashText(nextWAL))
		if err == nil {
			_, err = ticket.ReplacePinned(b.Context(), dir)
		}
		ticket.Close()
		requireOK(b, err)
		requireOK(b, cat.ReclaimPinned(b.Context(), dir))
		_, size := publicationChanges(previous, after)
		written += uint64(size)
		previous, wal = after, nextWAL
	}
	return written, peakHeap
}
