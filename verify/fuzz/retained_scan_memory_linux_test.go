package fuzz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
)

const (
	scanMemoryWorkerEnv = "BOG_FUZZ_RETAINED_SCAN_MEMORY"
	scanMemoryCgroupEnv = "BOG_FUZZ_RETAINED_SCAN_CGROUP"
)

func TestRetainedScanMemory(t *testing.T) {
	path, err := filepath.EvalSymlinks(t.TempDir())
	must(t, err)
	fixture := seedScanMemory(t, path)
	configured := httpapi.DefaultQueryConcurrency
	for _, scenario := range []struct {
		name  string
		scans int
	}{{"single", 1}, {"server-concurrency", scanMemoryQualificationReaders}} {
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
	if err != nil {
		t.Fatal(errors.Join(errScanWorkerStartup, err))
	}
	if group != "" {
		t.Cleanup(func() { must(t, os.Remove(group)) })
	}
	ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestRetainedScanMemoryWorker$", "-test.count=1")
	cmd.WaitDelay = time.Second
	cmd.Env = append(cmd.Environ(), scanMemoryWorkerEnv+"="+strconv.Itoa(count), scanMemoryCgroupEnv+"="+group)
	cmd.Dir = path
	output, processErr := runBoundedReplayWorker(cmd)
	report, err := scanMemoryVerdict(output, errors.Join(ctx.Err(), processErr))
	if err != nil {
		t.Fatalf("%s worker at concurrency %d: %v\n%s", scanMemorySeed, count, err, output)
	}
	if group == "" {
		report.CgroupUnavailable = unavailable
	}
	must(t, report.validate(group != ""))
	if report.Arch != runtime.GOARCH || report.GoVersion != runtime.Version() || report.Race != scanMemoryRace {
		t.Fatalf("unexpected worker runtime: %s %s race=%t", report.Arch, report.GoVersion, report.Race)
	}
	return report
}

func TestRetainedScanMemoryWorker(t *testing.T) {
	raw := os.Getenv(scanMemoryWorkerEnv)
	if raw == "" {
		t.Skip("started by TestRetainedScanMemory")
	}
	entry := readScanMemoryStats(false)
	count, err := strconv.Atoi(raw)
	must(t, err)
	if count != 1 && count != scanMemoryQualificationReaders {
		t.Fatalf("invalid measurement concurrency %d", count)
	}
	path, err := os.Getwd()
	must(t, err)
	group := os.Getenv(scanMemoryCgroupEnv)
	if group != "" {
		must(t, enterScanMemoryCgroup(group))
	}
	// Applying RLIMIT_AS after runtime initialization can make later
	// race shadow mappings fail in the allocator. Keep the independent
	// file/process limits and account for reader work in the workload.
	setHardLimit(t, syscall.RLIMIT_FSIZE, scanMemoryOutputLimit)
	report, err := runRetainedScanMemory(t, path, group, count, entry)
	if errors.Is(err, errScanWorkerBudget) {
		err = errors.Join(err, writeScanMemoryRecord(scanMemoryBudgetPrefix, err.Error()))
	}
	must(t, err)
	must(t, writeScanMemoryRecord(scanMemoryReportPrefix, report))
}

func writeScanMemoryRecord(prefix string, record any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = fmt.Printf("%s%s\n", prefix, data)
	return err
}

func runRetainedScanMemory(t *testing.T, path, group string, count int, entry scanMemoryStats) (report scanMemoryReport, err error) {
	t.Helper()
	rows := scanMemoryRows(t)
	info := scanMemoryFixture{
		Bytes: scanMemoryFixtureBytes(t, path), Files: scanMemorySegments + 1,
		Rows: len(rows), Segments: scanMemorySegments, PageBytes: scanMemoryPageBytes,
		MaxLineBytes: len(rows[0].Row.Line),
	}
	report = scanMemoryReport{
		Seed: scanMemorySeed, Arch: runtime.GOARCH, GoVersion: runtime.Version(), Race: scanMemoryRace,
		Concurrency: count, ConfiguredConcurrency: httpapi.DefaultQueryConcurrency,
	}
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.PinnedGenerations: 1,
		checkpoint.LiveCaptures:      uint64(count),
	})
	idleFDs, err := retainedReaderFDs(path)
	if err != nil {
		return report, err
	}
	if len(idleFDs) != 0 {
		return report, fmt.Errorf("fixture descriptors before opening catalog: %v", idleFDs)
	}
	catalog := history.NewCatalog(budget)
	defer func() { err = errors.Join(err, catalog.Close()) }()
	generation, err := catalog.Replace(t.Context(), path)
	if err != nil {
		return report, err
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
	workload := scanMemoryWorkload{catalog: catalog, budget: budget, path: path, catalogFD: catalogFDs, rows: rows, readers: count}
	report.CatalogFDs = len(catalogFDs)
	if report.CatalogFDs != 2 {
		return report, fmt.Errorf("catalog fixture descriptors = %v, want directory and pin", catalogFDs)
	}
	if err := workload.checkUsage(0); err != nil {
		return report, err
	}
	report.Startup, err = measureScanMemoryStartup(entry)
	if err != nil {
		return report, err
	}
	if err := writeScanMemoryRecord(scanMemoryReadyPrefix, report.Startup); err != nil {
		return report, err
	}

	for _, phase := range scanMemoryPhases {
		measured, err := workload.measure(t.Context(), phase, scanMemoryLimits(count))
		if err != nil {
			return report, fmt.Errorf("%s reader phase: %w", phase, err)
		}
		report.Phases = append(report.Phases, measured)
	}

	if err := catalog.Close(); err != nil {
		return report, err
	}
	select {
	case <-generation.Drained:
		report.GenerationDrained = true
	default:
		return report, errors.New("generation still pinned after catalog close")
	}
	finalUsage := budget.Usage().InUse
	if finalUsage != (checkpoint.Limits{}) {
		return report, fmt.Errorf("resource reservations after catalog close: %v", finalUsage)
	}
	report.FinalPins = finalUsage[checkpoint.PinnedGenerations]
	finalFDs, err := retainedReaderFDs(path)
	if err != nil {
		return report, err
	}
	if !slices.Equal(finalFDs, idleFDs) {
		return report, fmt.Errorf("fixture descriptors after catalog close: %v, want %v", finalFDs, idleFDs)
	}
	report.FinalFDs = len(finalFDs)
	report.PeakRSS, err = scanMemoryPeakRSS()
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
	runtime.KeepAlive(rows) // Keep the exact row oracle in all heap samples.
	return report, nil
}

func measureScanMemoryStartup(entry scanMemoryStats) (scanMemoryStartup, error) {
	ready := readScanMemoryStats(true)
	allocated, err := scanAllocationDelta(entry, ready)
	if err != nil {
		return scanMemoryStartup{}, err
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return scanMemoryStartup{}, err
	}
	peak, peakErr := parseScanMemoryPeakRSS(string(status))
	vm, vmErr := parseScanMemoryStatusBytes(string(status), "VmSize")
	report := scanMemoryStartup{
		GoAllocated: allocated, GoAllocLimit: scanMemoryStartupAllocLimit,
		CatalogHeap: ready.heap, PeakRSS: peak, VirtualMemory: vm,
	}
	return report, errors.Join(peakErr, vmErr, report.validate())
}

func logScanMemoryReport(t *testing.T, report scanMemoryReport) {
	t.Helper()
	data, err := json.Marshal(report)
	must(t, err)
	t.Logf("%s%s", scanMemoryReportPrefix, data)
}
