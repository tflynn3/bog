package fuzz

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	scanMemorySeed                 = "retained-scan-memory-v3"
	scanMemoryQualificationReaders = 8

	scanMemoryReadyPrefix  = "retained_scan_ready "
	scanMemoryReportPrefix = "retained_scan_memory "
	scanMemoryBudgetPrefix = "retained_scan_budget_failure "
)

type scanMemoryFixture struct {
	Generation      string `json:"generation"`
	Bytes           int64  `json:"fixture_bytes"`
	Files           int    `json:"fixture_files"`
	Rows            int    `json:"rows"`
	Segments        int    `json:"segments"`
	PageBytes       int    `json:"page_bytes"`
	MaxLineBytes    int    `json:"max_line_bytes"`
	CoveredWALBytes int64  `json:"covered_wal_bytes"`
}

type scanMemoryStartup struct {
	GoAllocated   uint64 `json:"go_allocated_bytes_since_worker_entry"`
	GoAllocLimit  uint64 `json:"go_allocation_limit_bytes"`
	CatalogHeap   uint64 `json:"catalog_live_go_heap_bytes"`
	PeakRSS       uint64 `json:"startup_peak_rss_bytes"`
	VirtualMemory uint64 `json:"startup_virtual_memory_bytes"`
}

func (s scanMemoryStartup) validate() error {
	if s.GoAllocLimit != scanMemoryStartupAllocLimit || s.GoAllocated == 0 || s.GoAllocated > s.GoAllocLimit ||
		s.CatalogHeap == 0 || s.PeakRSS == 0 || s.VirtualMemory == 0 {
		return fmt.Errorf("invalid scan worker startup accounting: %+v", s)
	}
	return nil
}

type scanMemoryReport struct {
	Seed                  string                  `json:"seed"`
	Race                  bool                    `json:"race"`
	Arch                  string                  `json:"arch"`
	GoVersion             string                  `json:"go_version"`
	Fixture               scanMemoryFixture       `json:"fixture"`
	Concurrency           int                     `json:"concurrency"`
	ConfiguredConcurrency int                     `json:"configured_query_concurrency"`
	Startup               scanMemoryStartup       `json:"startup"`
	Phases                []scanMemoryPhaseReport `json:"phases"`
	PeakRSS               uint64                  `json:"peak_rss_bytes"`
	CgroupPeak            *uint64                 `json:"cgroup_peak_memory_bytes"`
	CgroupUnavailable     string                  `json:"cgroup_peak_unavailable,omitempty"`
	CatalogFDs            int                     `json:"catalog_fixture_fds"`
	FinalPins             uint64                  `json:"final_generation_pin_reservations"`
	FinalFDs              int                     `json:"final_fixture_fds"`
	GenerationDrained     bool                    `json:"generation_drained"`
}

func (r scanMemoryReport) validate(isolatedCgroup bool) error {
	if r.Seed != scanMemorySeed || r.Arch == "" || r.GoVersion == "" || r.ConfiguredConcurrency < 1 ||
		(r.Concurrency != 1 && r.Concurrency != scanMemoryQualificationReaders) ||
		r.Fixture.Rows < 2 || r.Fixture.MaxLineBytes <= 0 {
		return errors.New("invalid scan memory report identity")
	}
	if err := r.Startup.validate(); err != nil {
		return err
	}
	if r.PeakRSS < r.Startup.PeakRSS {
		return errors.New("worker peak RSS decreased after startup")
	}
	if isolatedCgroup {
		if r.CgroupPeak == nil || *r.CgroupPeak == 0 || r.CgroupUnavailable != "" {
			return errors.New("missing isolated cgroup peak")
		}
	} else if r.CgroupPeak != nil || r.CgroupUnavailable == "" {
		return errors.New("invalid cgroup unavailability evidence")
	}
	if r.CatalogFDs != 2 || r.FinalFDs != 0 || r.FinalPins != 0 || !r.GenerationDrained {
		return errors.New("incomplete catalog cleanup evidence")
	}
	if len(r.Phases) != len(scanMemoryPhases) {
		return errors.New("missing scan phase evidence")
	}
	for i, p := range r.Phases {
		phase := scanMemoryPhases[i]
		rows, _ := phase.outcome(r.Fixture.Rows)
		minPaused := uint64(r.Fixture.MaxLineBytes) * uint64(r.Concurrency)
		if p.Phase != phase || p.CompletedReaders != r.Concurrency || p.RowsPerReader != rows ||
			p.WorkLimit != scanMemoryWorkLimit(phase, r.Fixture.MaxLineBytes) || p.Limits != scanMemoryLimits(r.Concurrency) ||
			p.Allocated == 0 || p.BaselineHeap == 0 || p.PostHeap == 0 || p.HeapSamples < 3 ||
			p.PausedHeap < p.BaselineHeap || p.PausedHeap-p.BaselineHeap < minPaused || p.PeakHeap < p.PausedHeap ||
			p.PausedFDs != r.CatalogFDs || p.PostFDs != r.CatalogFDs ||
			p.PausedCaptures != uint64(r.Concurrency) || p.PostCaptures != 0 || p.PausedPins != 1 || p.PostPins != 1 {
			return fmt.Errorf("invalid %s phase evidence: %+v", phase, p)
		}
		if err := p.checkBudget(); err != nil {
			return err
		}
	}
	return nil
}

type scanMemoryWorkerOutput struct {
	ready  *scanMemoryStartup
	report *scanMemoryReport
	budget string
}

func decodeScanMemoryLine(data []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return fmt.Errorf("unexpected data after scan memory record: %v", err)
	}
	return nil
}

func decodeScanMemoryOutput(output []byte) (records scanMemoryWorkerOutput, err error) {
	for raw := range bytes.Lines(output) {
		line := bytes.TrimSpace(raw)
		switch {
		case bytes.HasPrefix(line, []byte(scanMemoryReadyPrefix)):
			if records.ready != nil {
				return records, errors.New("duplicate scan readiness record")
			}
			var startup scanMemoryStartup
			if err := decodeScanMemoryLine(line[len(scanMemoryReadyPrefix):], &startup); err != nil {
				return records, err
			}
			if err := startup.validate(); err != nil {
				return records, err
			}
			records.ready = &startup
		case bytes.HasPrefix(line, []byte(scanMemoryReportPrefix)), bytes.HasPrefix(line, []byte(scanMemoryBudgetPrefix)):
			if records.ready == nil || records.report != nil || records.budget != "" {
				return records, errors.New("unexpected scan worker terminal record")
			}
			if data, ok := bytes.CutPrefix(line, []byte(scanMemoryReportPrefix)); ok {
				records.report = new(scanMemoryReport)
				if err := decodeScanMemoryLine(data, records.report); err != nil {
					return records, err
				}
			} else {
				if err := decodeScanMemoryLine(line[len(scanMemoryBudgetPrefix):], &records.budget); err != nil {
					return records, err
				}
				if records.budget == "" {
					return records, errors.New("empty workload budget diagnostic")
				}
			}
		}
	}
	return records, nil
}

func scanMemoryVerdict(output []byte, processErr error) (scanMemoryReport, error) {
	records, decodeErr := decodeScanMemoryOutput(output)
	if records.ready == nil {
		return scanMemoryReport{}, errors.Join(errScanWorkerStartup, processErr, decodeErr)
	}
	if records.budget != "" && decodeErr == nil && processErr != nil {
		return scanMemoryReport{}, errors.Join(fmt.Errorf("%w: %s", errScanWorkerBudget, records.budget), processErr)
	}
	if records.report == nil || processErr != nil || decodeErr != nil {
		return scanMemoryReport{}, errors.Join(errScanWorkerRun, processErr, decodeErr)
	}
	if records.report.Startup != *records.ready {
		return scanMemoryReport{}, fmt.Errorf("%w: final report differs from startup record", errScanWorkerRun)
	}
	return *records.report, nil
}
