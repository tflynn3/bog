package fuzz

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/store"
)

const (
	scanMemoryWorkerEnv = "BOG_FUZZ_RETAINED_SCAN_MEMORY"
	scanMemorySeed      = "retained-scan-memory-v1"
	scanMemorySegments  = 1024
	scanMemoryPageBytes = 4 << 10
	scanMemoryDiskCap   = 6 << 20
)

// Retain the -v output on native Linux amd64 and arm64 for #53 admission sizing
// and the Core architecture companion. Heap observations cover history scan
// state with the loaded catalog as the baseline.
func TestRetainedScanMemory(t *testing.T) {
	if os.Getenv(scanMemoryWorkerEnv) == scanMemorySeed {
		setWorkerLimits(t)
		runRetainedScanMemory(t)
		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestRetainedScanMemory$", "-test.count=1", "-test.v")
	cmd.WaitDelay = time.Second
	// The parent can remove the fixture even if the worker is killed.
	cmd.Env = append(cmd.Environ(), scanMemoryWorkerEnv+"="+scanMemorySeed, "TMPDIR="+t.TempDir())
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil || err != nil {
		t.Fatalf("%s worker: context=%v process=%v\n%s", scanMemorySeed, ctx.Err(), err, output)
	}
	t.Logf("%s", output)
}

func runRetainedScanMemory(t *testing.T) {
	path := t.TempDir()
	fixture := seedScanMemory(t, path)
	concurrency := httpapi.DefaultQueryConcurrency
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.PinnedGenerations: 1,
		checkpoint.LiveCaptures:      uint64(concurrency),
	})
	catalog := history.NewCatalog(budget)
	defer func() { must(t, catalog.Close()) }()
	generation, err := catalog.Replace(t.Context(), path)
	must(t, err)
	if generation.ID != fixture.id || generation.Boundary != fixture.boundary {
		t.Fatalf("loaded generation %x at %+v, want %x at %+v", generation.ID, generation.Boundary, fixture.id, fixture.boundary)
	}

	query := history.Query{Start: start, End: start.Add(3 * time.Second)}
	t.Logf("seed=%s generation=%x arch=%s go=%s fixture_bytes=%d fixture_files=%d rows=%d segments=%d overlapping_segments=%d page_bytes=%d covered_wal_bytes=%d configured_query_concurrency=%d",
		scanMemorySeed, fixture.id, runtime.GOARCH, runtime.Version(), fixture.bytes, len(fixture.names), len(fixture.rows),
		scanMemorySegments, scanMemorySegments, scanMemoryPageBytes, fixture.boundary.CoveredOffset, concurrency)
	for _, tc := range []struct {
		name  string
		scans int
	}{{"single", 1}, {"server-concurrency", concurrency}} {
		if !t.Run(tc.name, func(t *testing.T) {
			measureRetainedScans(t, catalog, budget, query, fixture.rows[0], tc.scans)
		}) {
			return
		}
	}

	// Reacquire the same generation and walk all rows after cancellation.
	reader, err := catalog.Acquire(fixture.id)
	must(t, err)
	defer func() { must(t, reader.Close()) }()
	ctx, cancel := context.WithTimeout(t.Context(), interruptionDeadline)
	defer cancel()
	must(t, scanCeilingRows(ctx, reader, query, fixture.rows, nil))
	must(t, reader.Close())
	must(t, catalog.Close())
	select {
	case <-generation.Drained:
	default:
		t.Fatal("generation still pinned after cancellation, retry and catalog close")
	}
	if got := budget.Usage().InUse; got != (checkpoint.Limits{}) {
		t.Fatalf("resource reservations after catalog close: %v", got)
	}
	assertChildren(t, path, append(fixture.names, "bog.checkpoint.lock")...)
}

type scanMemoryFixture struct {
	rows     []checkpoint.BlockRow
	boundary history.Boundary
	id       history.GenerationID
	bytes    int
	names    []string
}

func seedScanMemory(t *testing.T, path string) scanMemoryFixture {
	t.Helper()
	if checkpoint.MaxSegments != scanMemorySegments {
		t.Fatalf("v1 segment ceiling = %d, fixture targets %d", checkpoint.MaxSegments, scanMemorySegments)
	}
	const blockBytes = blockHeader + scanMemoryPageBytes
	const rowsPerBlock = 3
	const tiedPrefix = "tied-"
	const tiedLineBytes = len(tiedPrefix) + 4
	lateLine := strings.Repeat("x", scanMemoryPageBytes-rowsPerBlock*rowHeader-2*tiedLineBytes)
	early, late := start, start.Add(2*time.Second)

	// Stream a valid synthetic WAL prefix into the digest. The checkpoint's
	// append order and boundary have a source without keeping a second copy
	// of the encoded rows on disk or in the heap sample.
	hash := sha256.New()
	_, err := hash.Write([]byte(walMagic))
	must(t, err)
	offset := int64(len(walMagic))
	want := make([]checkpoint.BlockRow, 0, scanMemorySegments*rowsPerBlock)
	m := checkpoint.Manifest{Version: 1, Segments: make([]checkpoint.Segment, 0, scanMemorySegments)}
	names := make([]string, 0, scanMemorySegments+1)
	for i := range scanMemorySegments {
		tied := store.Row{TS: early, Line: fmt.Sprintf("%s%04d", tiedPrefix, i)}
		first := uint64(len(want))
		// Append the late row first, then backfill the identical pair.
		appended := []backup.Row{
			{Row: store.Row{TS: late, Line: lateLine}, Ordinal: first},
			{Row: tied, Ordinal: first + 1},
			{Row: tied, Ordinal: first + 2},
		}
		framed := frame(t, appended)
		_, err := hash.Write(framed)
		must(t, err)
		offset += int64(len(framed))

		var rows [rowsPerBlock]checkpoint.BlockRow
		for j, row := range appended {
			rows[j] = checkpoint.BlockRow{Row: row.Row, Ordinal: row.Ordinal}
		}
		want = append(want, rows[:]...)
		// Every segment spans the same interval and starts with a row at
		// the lower bound. The payload fills the page from that row header.
		block, err := checkpoint.EncodeBlock([]checkpoint.BlockRow{rows[1], rows[2], rows[0]})
		must(t, err)
		if len(block) != blockBytes {
			t.Fatalf("segment %d is %d bytes, want %d", i, len(block), blockBytes)
		}
		name := fmt.Sprintf("bog-segment-%032x", i)
		must(t, os.WriteFile(filepath.Join(path, name), block, 0600))
		m.Segments = append(m.Segments, checkpoint.Segment{Name: name, SHA256: digest(block), Size: int64(len(block))})
		names = append(names, name)
	}
	// Stable timestamp sorting of the append log leaves ties in append order.
	slices.SortStableFunc(want, func(a, b checkpoint.BlockRow) int { return a.Row.TS.Compare(b.Row.TS) })
	boundary := history.Boundary{SourceSHA256: [sha256.Size]byte(hash.Sum(nil)), CoveredOffset: offset, NextOrdinal: uint64(len(want))}
	m.CoveredOffset, m.NextOrdinal, m.SourceSHA256 = offset, boundary.NextOrdinal, fmt.Sprintf("%x", boundary.SourceSHA256)
	payload := manifestPayload(t, m)
	envelope, err := json.Marshal(manifestEnvelope{Payload: payload, SHA256: digest(payload)})
	must(t, err)
	fixtureBytes := scanMemorySegments*blockBytes + len(envelope)
	if len(envelope) >= maxArtifactSize || fixtureBytes > scanMemoryDiskCap-maxOutputFile {
		// Leave room for the transient pin record, itself capped by the
		// worker's per-file limit. The publication lock is empty.
		t.Fatalf("fixture is %d bytes with a %d-byte manifest", fixtureBytes, len(envelope))
	}
	must(t, os.WriteFile(filepath.Join(path, manifestFile), envelope, 0600))
	names = append(names, manifestFile)
	return scanMemoryFixture{rows: want, boundary: boundary, id: sha256.Sum256(payload), bytes: fixtureBytes, names: names}
}

type retainedScanWorker struct {
	reader *history.Reader
	first  chan struct{}
	done   chan struct{}
	err    error // Published by closing done.
}

func measureRetainedScans(t *testing.T, catalog *history.Catalog, budget *checkpoint.Budget, query history.Query, first checkpoint.BlockRow, count int) {
	t.Helper()
	baseline := scanLiveHeap()
	ctx, cancel := context.WithTimeout(t.Context(), interruptionDeadline)
	defer cancel()
	workers := make([]retainedScanWorker, count)
	for i := range workers {
		r, err := catalog.AcquireCurrentUntil(ctx, time.Time{})
		must(t, err)
		defer func() { must(t, r.Close()) }()
		workers[i] = retainedScanWorker{reader: r, first: make(chan struct{}), done: make(chan struct{})}
	}
	wait := func() {
		t.Helper()
		drain, stop := context.WithTimeout(context.Background(), interruptionDeadline)
		defer stop()
		for i := range workers {
			select {
			case <-workers[i].done:
			case <-drain.Done():
				t.Fatalf("scan %d did not stop after cancellation: %v", i, drain.Err())
			}
		}
	}
	defer func() {
		cancel()
		wait()
	}()
	for i := range workers {
		go workers[i].scan(ctx, query, first)
	}
	for i := range workers {
		w := &workers[i]
		select {
		case <-w.first:
		case <-w.done:
			t.Fatalf("scan %d exited before its first visitor: %v", i, w.err)
		case <-ctx.Done():
			t.Fatalf("scan %d did not reach the first visitor: %v", i, ctx.Err())
		}
	}

	// All scans have initialized their merge state and remain in their
	// first visitor across the forced GC.
	live := scanLiveHeap()
	must(t, ctx.Err())
	for i := range workers {
		must(t, workers[i].reader.Close())
	}
	inFlight := checkpoint.Limits{checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: uint64(count)}
	if got := budget.Usage().InUse; got != inFlight {
		t.Fatalf("reservations with active scans and closed readers: %v, want %v", got, inFlight)
	}
	cancel()
	wait()
	for i := range workers {
		if err := workers[i].err; err != context.Canceled {
			t.Fatalf("scan %d returned %v, want context.Canceled", i, err)
		}
	}
	if got, want := budget.Usage().InUse, (checkpoint.Limits{checkpoint.PinnedGenerations: 1}); got != want {
		t.Fatalf("reservations after cancellation: %v, want %v", got, want)
	}
	delta := int64(live) - int64(baseline)
	t.Logf("seed=%s concurrency=%d catalog_heap_bytes=%d paused_heap_bytes=%d incremental_live_heap_bytes=%d incremental_bytes_per_scan=%d post_cancel_heap_bytes=%d",
		scanMemorySeed, count, baseline, live, delta, delta/int64(count), scanLiveHeap())
}

func (w *retainedScanWorker) scan(ctx context.Context, query history.Query, first checkpoint.BlockRow) {
	defer close(w.done)
	visited := false
	w.err = w.reader.ScanWithWork(ctx, query, func(row checkpoint.BlockRow) error {
		if visited {
			return fmt.Errorf("visited ordinal %d after cancellation", row.Ordinal)
		}
		if row != first {
			return fmt.Errorf("first row = %+v, want %+v", row, first)
		}
		visited = true
		close(w.first)
		<-ctx.Done()
		// Let the storage scan observe cancellation and release its pin.
		return nil
	}, store.NewQueryWork(ctx, httpapi.DefaultQueryWorkBudget))
}

func scanLiveHeap() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}
