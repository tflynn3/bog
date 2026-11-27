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
	scanMemoryDiskCap   = 10 << 20
	scanMemoryLock      = "bog.checkpoint.lock"
)

func TestRetainedScanMemory(t *testing.T) {
	path, err := filepath.EvalSymlinks(t.TempDir())
	must(t, err)
	fixture := seedScanMemory(t, path)
	configured := httpapi.DefaultQueryConcurrency
	if configured != scanMemoryReaders {
		t.Fatalf("scan qualification requires %d server readers, got %d", scanMemoryReaders, configured)
	}
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
	cmd.Env = append(cmd.Environ(), scanMemoryWorkerEnv+"="+strconv.Itoa(count), scanMemoryCgroupEnv+"="+group,
		"GOMAXPROCS="+strconv.Itoa(scanMemoryReaders))
	cmd.Dir = path
	output, runErr := runBoundedReplayWorker(cmd)
	var startup scanMemoryStartup
	if err := errors.Join(decodeScanMemoryRecord(output, scanMemoryStartupPrefix, &startup), startup.validate()); err != nil {
		t.Fatalf("%s harness startup at concurrency %d: %v\n%s",
			scanMemorySeed, count, errors.Join(err, ctx.Err(), runErr), output)
	}
	logScanMemoryRecord(t, scanMemoryStartupPrefix, startup)
	if err := errors.Join(ctx.Err(), runErr); err != nil {
		t.Fatalf("%s reader workload at concurrency %d: %v\n%s", scanMemorySeed, count, err, output)
	}
	report, err := decodeScanMemoryReport(output)
	if err != nil {
		t.Fatalf("reader workload evidence: %v\n%s", err, output)
	}
	if group == "" {
		report.CgroupUnavailable = unavailable
	}
	if err := report.validate(group != ""); err != nil {
		t.Fatalf("reader workload evidence: %v", err)
	}
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
	count, err := parseScanMemoryReaders(raw)
	if err != nil {
		t.Fatalf("harness startup: %v", err)
	}
	path, err := os.Getwd()
	if err != nil {
		t.Fatalf("harness startup: %v", err)
	}
	group := os.Getenv(scanMemoryCgroupEnv)
	if group != "" {
		if err := enterScanMemoryCgroup(group); err != nil {
			t.Fatalf("harness startup: enter scan memory cgroup: %v", err)
		}
	}
	startup, err := prepareScanMemoryWorker()
	if err != nil {
		t.Fatalf("harness startup: %v", err)
	}
	if err := writeScanMemoryRecord(scanMemoryStartupPrefix, startup); err != nil {
		t.Fatalf("harness startup: write startup evidence: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), scanMemoryDeadline)
	defer cancel()
	started := time.Now()
	before := scanMemoryTotalAlloc()
	report, workErr := runRetainedScanMemory(ctx, path, group, count, scanMemoryRows(t))
	after := scanMemoryTotalAlloc()
	report.ElapsedNS = int64(time.Since(started))
	workErr = errors.Join(workErr, ctx.Err(), checkScanMemoryBytes("workload allocation", before, after, report.Limits.Allocated))
	if report.ElapsedNS > report.Limits.DeadlineNS {
		workErr = errors.Join(workErr, fmt.Errorf("workload elapsed %s: %w", time.Duration(report.ElapsedNS), context.DeadlineExceeded))
	}
	if workErr != nil {
		t.Fatalf("reader workload: %v", workErr)
	}
	report.AllocatedBytes = after - before
	if err := writeScanMemoryRecord(scanMemoryReportPrefix, report); err != nil {
		t.Fatalf("reader workload: write evidence: %v", err)
	}
}

func parseScanMemoryReaders(raw string) (int, error) {
	// The worker protocol accepts exactly the single-reader control and
	// the eight-reader qualification case.
	switch raw {
	case "1":
		return 1, nil
	case "8":
		return scanMemoryReaders, nil
	default:
		return 0, errors.New("invalid measurement concurrency; expected 1 or 8")
	}
}

func writeScanMemoryRecord(prefix string, record any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = fmt.Printf("%s%s\n", prefix, data)
	return err
}

func scanMemoryTotalAlloc() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.TotalAlloc
}

func runRetainedScanMemory(ctx context.Context, path, group string, count int, rows []checkpoint.BlockRow) (report scanMemoryReport, err error) {
	report = scanMemoryReport{
		Seed: scanMemorySeed, Arch: runtime.GOARCH, GoVersion: runtime.Version(),
		Concurrency: count, ConfiguredConcurrency: httpapi.DefaultQueryConcurrency, Limits: scanMemoryBudget(count),
	}
	fixtureBytes, err := scanMemoryFixtureBytes(ctx, path)
	if err != nil {
		return report, err
	}
	info := scanMemoryFixture{
		Bytes: fixtureBytes, Files: scanMemorySegments + 1,
		Rows: len(rows), Segments: scanMemorySegments, PageBytes: scanMemoryPageBytes,
		MaxLineBytes: len(rows[0].Row.Line),
	}
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.PinnedGenerations: 1,
		checkpoint.LiveCaptures:      uint64(count),
	})
	if err := checkRetainedReaderFDs(path, nil); err != nil {
		return report, err
	}
	catalog := history.NewCatalog(budget)
	var generation history.Generation
	defer func() {
		err = errors.Join(err, catalog.Close(), checkRetainedReaderBudget(budget))
		finalFDs, fdErr := retainedReaderFDs(path)
		if fdErr != nil || len(finalFDs) != 0 {
			err = errors.Join(err, fdErr, fmt.Errorf("fixture descriptors after catalog close: %v", finalFDs))
		}
		report.FinalFDs = len(finalFDs)
		report.FinalPins = budget.Usage().InUse[checkpoint.PinnedGenerations]
		if generation.Drained != nil {
			select {
			case <-generation.Drained:
				report.GenerationDrained = true
			default:
				err = errors.Join(err, errors.New("generation still pinned after catalog close"))
			}
		}
	}()
	generation, err = catalog.Replace(ctx, path)
	if err != nil {
		return report, fmt.Errorf("load catalog: %w", err)
	}
	info.Generation = fmt.Sprintf("%x", generation.ID)
	info.CoveredWALBytes = generation.Boundary.CoveredOffset
	report.Fixture = info
	if generation.Boundary.NextOrdinal != uint64(len(rows)) {
		return report, fmt.Errorf("generation row count = %d, want %d", generation.Boundary.NextOrdinal, len(rows))
	}
	catalogFDs, err := retainedReaderFDs(path)
	if err != nil {
		return report, err
	}
	const pinFDs = 2
	if len(catalogFDs) != pinFDs {
		return report, fmt.Errorf("catalog fixture descriptors = %v, want %d (directory and pin)", catalogFDs, pinFDs)
	}
	report.CatalogFDs = len(catalogFDs)

	query := history.Query{Start: start, End: start.Add(3 * time.Second)}
	if err := measureRetainedScans(ctx, catalog, budget, query, rows[0], path, catalogFDs, &report); err != nil {
		return report, err
	}
	report.PeakRSS, err = readScanMemoryStatus("VmHWM")
	if err != nil {
		return report, err
	}
	if group != "" {
		peak, err := readScanMemoryCgroupPeak(group)
		if err != nil {
			return report, err
		}
		report.CgroupPeak = &peak
	}
	// Exercise the work limit and verify exact retry outside the heap
	// sample. Both paths still count toward the worker allocation limit.
	err = rejectScanMemoryWork(ctx, catalog, query, uint64(info.MaxLineBytes-1))
	if err := errors.Join(err, checkScanMemoryIdle(path, catalogFDs, budget)); err != nil {
		return report, err
	}
	report.WorkRejections = 1
	report.RetryRows, err = retryScanMemory(ctx, catalog, query, rows, report.Limits.WorkPerReader)
	return report, errors.Join(err, checkScanMemoryIdle(path, catalogFDs, budget))
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

func scanMemoryFixtureBytes(ctx context.Context, path string) (int64, error) {
	names := scanMemoryNames()
	var total int64
	for _, name := range names[:len(names)-1] {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		info, err := os.Lstat(filepath.Join(path, name))
		if err != nil {
			return 0, err
		}
		limit := int64(checkpoint.MaxBlockSize)
		if name == manifestFile {
			limit = maxArtifactSize - 1
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
			return 0, fmt.Errorf("%w: invalid fixture file %s: %v", errReplayLimit, name, info)
		}
		if info.Size() > scanMemoryDiskCap-maxOutputFile-total {
			return 0, fmt.Errorf("%w: fixture exceeds disk cap at %s", errReplayLimit, name)
		}
		total += info.Size()
	}
	return total, nil
}

func logScanMemoryReport(t *testing.T, report scanMemoryReport) {
	t.Helper()
	logScanMemoryRecord(t, scanMemoryReportPrefix, report)
}

func logScanMemoryRecord(t *testing.T, prefix string, record any) {
	t.Helper()
	data, err := json.Marshal(record)
	must(t, err)
	t.Logf("%s%s", prefix, data)
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
