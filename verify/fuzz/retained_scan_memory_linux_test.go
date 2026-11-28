package fuzz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
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
	scanMemoryCgroupEnv = "BOG_FUZZ_RETAINED_SCAN_CGROUP"
	scanMemorySegments  = 1024
	scanMemoryPageBytes = 4 << 10
	scanMemoryDiskCap   = 10 << 20
	scanMemoryLock      = "bog.checkpoint.lock"
)

func TestRetainedScanMemory(t *testing.T) {
	path, err := filepath.EvalSymlinks(t.TempDir())
	must(t, err)
	fixture := seedScanMemory(t, path)
	configured := httpapi.DefaultQueryConcurrency
	for _, scenario := range []struct {
		name  string
		scans int
	}{{"single", 1}, {"server-concurrency", configured}} {
		t.Run(scenario.name, func(t *testing.T) {
			report := runScanMemoryProcess(t, path, scenario.scans)
			if report.Fixture != fixture || report.Concurrency != scenario.scans || report.ConfiguredConcurrency != configured {
				t.Fatalf("unexpected worker evidence: %+v", report)
			}
			logScanMemoryReport(t, report)
			// Check the shared input namespace before reusing it for the next worker.
			assertChildren(t, path, scanMemoryNames()...)
		})
	}
}

func runScanMemoryProcess(t *testing.T, path string, count int) scanMemoryReport {
	t.Helper()
	group, unavailable, err := makeScanMemoryCgroup(os.Getenv("BOG_TEST_CGROUP_PARENT"))
	must(t, err)
	if group != "" {
		t.Cleanup(func() { must(t, os.Remove(group)) })
	}
	ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestRetainedScanMemoryWorker$", "-test.count=1")
	cmd.WaitDelay = time.Second
	cmd.Env = append(cmd.Environ(), scanMemoryWorkerEnv+"="+strconv.Itoa(count), scanMemoryCgroupEnv+"="+group)
	cmd.Dir = path
	output, err := cmd.CombinedOutput()
	if err != nil || ctx.Err() != nil {
		t.Fatalf("%s worker at concurrency %d: context=%v process=%v\n%s", scanMemorySeed, count, ctx.Err(), err, output)
	}
	report, err := decodeScanMemoryReport(output)
	must(t, err)
	if group == "" {
		report.CgroupUnavailable = unavailable
	}
	must(t, report.validate(group != ""))
	if report.Arch != runtime.GOARCH || report.GoVersion != runtime.Version() {
		t.Fatalf("unexpected worker runtime: %s %s", report.Arch, report.GoVersion)
	}
	return report
}

func TestRetainedScanMemoryWorker(t *testing.T) {
	raw := os.Getenv(scanMemoryWorkerEnv)
	if raw == "" {
		t.Skip("started by TestRetainedScanMemory")
	}
	count, err := strconv.Atoi(raw)
	must(t, err)
	if count != 1 && count != httpapi.DefaultQueryConcurrency {
		t.Fatalf("invalid measurement concurrency %d", count)
	}
	path, err := os.Getwd()
	must(t, err)
	group := os.Getenv(scanMemoryCgroupEnv)
	if group != "" {
		must(t, enterScanMemoryCgroup(group))
	}
	// Concurrent scanners can grow more than one Go arena. The race shadow
	// mappings consume virtual address space outside the measured live heap.
	// Keep the independent heap, RSS and cgroup ceilings unchanged.
	setWorkerAddressLimit(t, 1<<30)
	report := runRetainedScanMemory(t, path, group, count)
	data, err := json.Marshal(report)
	must(t, err)
	_, err = fmt.Printf("%s%s\n", scanMemoryReportPrefix, data)
	must(t, err)
}

func runRetainedScanMemory(t *testing.T, path, group string, count int) scanMemoryReport {
	t.Helper()
	rows := scanMemoryRows(t)
	info := scanMemoryFixture{
		Bytes: scanMemoryFixtureBytes(t, path), Files: scanMemorySegments + 1,
		Rows: len(rows), Segments: scanMemorySegments, PageBytes: scanMemoryPageBytes,
		MaxLineBytes: len(rows[0].Row.Line),
	}
	report := scanMemoryReport{
		Seed: scanMemorySeed, Arch: runtime.GOARCH, GoVersion: runtime.Version(),
		Concurrency: count, ConfiguredConcurrency: httpapi.DefaultQueryConcurrency,
	}
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.PinnedGenerations: 1,
		checkpoint.LiveCaptures:      uint64(count),
	})
	idleFDs, err := scanMemoryFixtureFDs("/proc/self/fd", path)
	must(t, err)
	if len(idleFDs) != 0 {
		t.Fatalf("fixture descriptors before opening catalog: %v", idleFDs)
	}
	catalog := history.NewCatalog(budget)
	defer func() { must(t, catalog.Close()) }()
	generation, err := catalog.Replace(t.Context(), path)
	must(t, err)
	info.Generation = fmt.Sprintf("%x", generation.ID)
	info.CoveredWALBytes = generation.Boundary.CoveredOffset
	report.Fixture = info
	if generation.Boundary.NextOrdinal != uint64(len(rows)) {
		t.Fatalf("generation row count = %d, want %d", generation.Boundary.NextOrdinal, len(rows))
	}
	catalogFDs, err := scanMemoryFixtureFDs("/proc/self/fd", path)
	must(t, err)
	const pinFDs = 2
	if len(catalogFDs) != pinFDs {
		t.Fatalf("catalog fixture descriptors = %v, want %d (directory and pin)", catalogFDs, pinFDs)
	}
	report.CatalogFDs = len(catalogFDs)

	query := history.Query{Start: start, End: start.Add(3 * time.Second)}
	measureRetainedScans(t, catalog, budget, query, rows[0], path, catalogFDs, &report)
	usageRSS, err := scanMemoryPeakRSS()
	must(t, err)
	report.PeakRSS = usageRSS
	if group != "" {
		peak, err := readScanMemoryCgroupPeak(group)
		must(t, err)
		report.CgroupPeak = &peak
	}
	// Retry is outside the memory sample and uses the same metered query.
	ctx, cancel := context.WithTimeout(t.Context(), interruptionDeadline)
	defer cancel()
	reader, err := catalog.Acquire(generation.ID)
	must(t, err)
	defer func() { must(t, reader.Close()) }()
	retried, scanErr := retryScanMemoryRows(ctx, reader, query, rows)
	must(t, errors.Join(scanErr, reader.Close()))
	report.RetryRows = retried
	afterRetry, err := scanMemoryFixtureFDs("/proc/self/fd", path)
	must(t, err)
	if !slices.Equal(afterRetry, catalogFDs) {
		t.Fatalf("fixture descriptors after retry = %v, want %v", afterRetry, catalogFDs)
	}

	must(t, catalog.Close())
	select {
	case <-generation.Drained:
	default:
		t.Fatal("generation still pinned after cancellation, retry and catalog close")
	}
	finalUsage := budget.Usage().InUse
	if finalUsage != (checkpoint.Limits{}) {
		t.Fatalf("resource reservations after catalog close: %v", finalUsage)
	}
	report.FinalPins = finalUsage[checkpoint.PinnedGenerations]
	finalFDs, err := scanMemoryFixtureFDs("/proc/self/fd", path)
	must(t, err)
	if len(finalFDs) != 0 {
		t.Fatalf("fixture descriptors after catalog close: %v", finalFDs)
	}
	report.FinalFDs = len(finalFDs)
	report.GenerationDrained = true
	runtime.KeepAlive(rows) // Keep the exact retry oracle in all heap samples.
	return report
}

func scanMemoryAppendRuns(t *testing.T, yield func([]checkpoint.BlockRow)) {
	t.Helper()
	if checkpoint.MaxSegments != scanMemorySegments {
		t.Fatalf("v1 segment ceiling = %d, fixture targets %d", checkpoint.MaxSegments, scanMemorySegments)
	}
	// An unescaped line in a single-record batch without a final newline
	// uses every byte of the largest supported WAL payload.
	empty, err := json.Marshal(store.Row{TS: start})
	must(t, err)
	large := strings.Repeat("X", store.MaxBatch-len(empty))
	yield([]checkpoint.BlockRow{{Row: store.Row{TS: start, Line: large}}})
	const tiedPrefix = "tied-"
	const tiedLineBytes = len(tiedPrefix) + 4
	lateLine := strings.Repeat("x", scanMemoryPageBytes-3*rowHeader-2*tiedLineBytes)
	for i := 1; i < scanMemorySegments; i++ {
		tied := store.Row{TS: start, Line: fmt.Sprintf("%s%04d", tiedPrefix, i)}
		ordinal := 1 + 3*uint64(i-1)
		// The run contains a late append and two identical backfills.
		yield([]checkpoint.BlockRow{
			{Row: store.Row{TS: start.Add(2 * time.Second), Line: lateLine}, Ordinal: ordinal},
			{Row: tied, Ordinal: ordinal + 1},
			{Row: tied, Ordinal: ordinal + 2},
		})
	}
}

func scanMemoryRows(t *testing.T) []checkpoint.BlockRow {
	t.Helper()
	var rows []checkpoint.BlockRow
	scanMemoryAppendRuns(t, func(run []checkpoint.BlockRow) { rows = append(rows, run...) })
	slices.SortStableFunc(rows, func(a, b checkpoint.BlockRow) int { return a.Row.TS.Compare(b.Row.TS) })
	return rows
}

func scanMemoryNames() []string {
	names := make([]string, 0, scanMemorySegments+2)
	for i := range scanMemorySegments {
		names = append(names, fmt.Sprintf("bog-segment-%032x", i))
	}
	return append(names, manifestFile, scanMemoryLock)
}

func seedScanMemory(t *testing.T, path string) scanMemoryFixture {
	t.Helper()
	names := scanMemoryNames()
	hash := sha256.New()
	_, err := hash.Write([]byte(walMagic))
	must(t, err)
	offset := int64(len(walMagic))
	m := checkpoint.Manifest{Version: 1, Segments: make([]checkpoint.Segment, 0, scanMemorySegments)}
	fixture := scanMemoryFixture{Files: len(names) - 1, Segments: scanMemorySegments, PageBytes: scanMemoryPageBytes}
	scanMemoryAppendRuns(t, func(run []checkpoint.BlockRow) {
		appended := make([]backup.Row, len(run))
		for i, row := range run {
			appended[i] = backup.Row{Row: row.Row, Ordinal: row.Ordinal}
		}
		framed := frame(t, appended)
		blockSize := blockHeader + scanMemoryPageBytes
		if len(m.Segments) == 0 {
			framed = maximumScanMemoryFrame(t, framed)
			fixture.MaxLineBytes = len(run[0].Row.Line)
			blockSize = blockHeader + rowHeader + fixture.MaxLineBytes
		}
		_, err := hash.Write(framed)
		must(t, err)
		offset += int64(len(framed))
		fixture.Rows += len(run)

		ordered := slices.Clone(run)
		slices.SortStableFunc(ordered, func(a, b checkpoint.BlockRow) int { return a.Row.TS.Compare(b.Row.TS) })
		block, err := checkpoint.EncodeBlock(ordered)
		must(t, err)
		if len(block) != blockSize || fixture.Bytes+int64(len(block)) > scanMemoryDiskCap-maxOutputFile {
			t.Fatalf("segment %d: got %d bytes, want %d within fixture cap", len(m.Segments), len(block), blockSize)
		}
		name := names[len(m.Segments)]
		must(t, os.WriteFile(filepath.Join(path, name), block, 0600))
		fixture.Bytes += int64(len(block))
		m.Segments = append(m.Segments, checkpoint.Segment{Name: name, SHA256: digest(block), Size: int64(len(block))})
	})
	m.CoveredOffset, m.NextOrdinal, m.SourceSHA256 = offset, uint64(fixture.Rows), fmt.Sprintf("%x", hash.Sum(nil))
	payload := manifestPayload(t, m)
	envelope, err := json.Marshal(manifestEnvelope{Payload: payload, SHA256: digest(payload)})
	must(t, err)
	fixture.Bytes += int64(len(envelope))
	if len(envelope) >= maxArtifactSize || fixture.Bytes > scanMemoryDiskCap-maxOutputFile {
		t.Fatalf("fixture is %d bytes with a %d-byte manifest", fixture.Bytes, len(envelope))
	}
	must(t, os.WriteFile(filepath.Join(path, manifestFile), envelope, 0600))
	fixture.Generation = digest(payload)
	fixture.CoveredWALBytes = offset
	return fixture
}

func maximumScanMemoryFrame(t *testing.T, framed []byte) []byte {
	t.Helper()
	if len(framed) != frameHeader+store.MaxBatch+1 || !bytes.HasSuffix(framed, []byte{'\n'}) {
		t.Fatal("maximum line does not fill the expected single-row WAL payload")
	}
	framed = framed[:len(framed)-1]
	le.PutUint32(framed[:4], store.MaxBatch)
	le.PutUint32(framed[4:8], ^uint32(store.MaxBatch))
	le.PutUint32(framed[8:12], crc32.Checksum(framed[frameHeader:], crcTable))
	fixFrameHeader(framed)
	return framed
}

func scanMemoryFixtureBytes(t *testing.T, path string) int64 {
	t.Helper()
	names := scanMemoryNames()
	var total int64
	for _, name := range names[:len(names)-1] {
		info, err := os.Stat(filepath.Join(path, name))
		must(t, err)
		if !info.Mode().IsRegular() || info.Size() > checkpoint.MaxBlockSize {
			t.Fatalf("invalid fixture file %s: %v", name, info)
		}
		total += info.Size()
	}
	if total > scanMemoryDiskCap-maxOutputFile {
		t.Fatalf("fixture bytes %d exceed cap", total)
	}
	return total
}

func logScanMemoryReport(t *testing.T, report scanMemoryReport) {
	t.Helper()
	data, err := json.Marshal(report)
	must(t, err)
	t.Logf("%s%s", scanMemoryReportPrefix, data)
}

func TestScanMemoryOverflowFixture(t *testing.T) {
	rows := scanMemoryRows(t)
	first := rows[0]
	if first.Ordinal != 0 || len(first.Row.Line) <= scanMemoryPageBytes {
		t.Fatalf("first row ordinal=%d line_bytes=%d does not force overflow", first.Ordinal, len(first.Row.Line))
	}
	framed := maximumScanMemoryFrame(t, frame(t, []backup.Row{{Row: first.Row}}))
	payload := framed[frameHeader:]
	if len(payload) != store.MaxBatch {
		t.Fatalf("maximum WAL payload is %d bytes, want %d", len(payload), store.MaxBatch)
	}
	decoded, err := store.Decode(payload)
	must(t, err)
	if len(decoded) != 1 || decoded[0] != first.Row {
		t.Fatal("maximum WAL payload failed exact row round trip")
	}
	if _, err := store.Decode(append(payload, ' ')); err == nil {
		t.Fatal("accepted a payload larger than the WAL batch limit")
	}
	firstLate := rows[1+2*(scanMemorySegments-1)]
	if rows[1].Row != rows[2].Row || rows[1].Ordinal+1 != rows[2].Ordinal ||
		rows[1].Ordinal <= firstLate.Ordinal || !rows[1].Row.TS.Before(firstLate.Row.TS) {
		t.Fatal("fixture does not exercise duplicate, tied and backfilled rows")
	}
}
