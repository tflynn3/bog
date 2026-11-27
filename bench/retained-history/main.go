package main

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"syscall"
	"time"

	"bog/verify/capacityevidence"
)

const capacityCase = capacityevidence.Case

type fixture struct {
	SeedRows      int    `json:"seed_rows"`
	TailRows      int    `json:"tail_rows"`
	LineBytes     int    `json:"line_bytes"`
	BatchRows     int    `json:"batch_rows"`
	MemoryBytes   uint64 `json:"memory_limit_bytes"`
	scatteredTail bool
}

func fixtures(mode string) []fixture {
	sizes := []int{8192, 32768}
	if mode == "live" {
		sizes = []int{capacityevidence.SmallScale, capacityevidence.LargeScale}
	}
	var result []fixture
	for _, size := range sizes {
		result = append(result, fixture{size, capacityevidence.TailRows, capacityevidence.LineBytes, capacityevidence.BatchRows, capacityevidence.MemoryLimit, mode == "live"})
	}
	return result
}

type result struct {
	Fixture       fixture              `json:"fixture"`
	Phase         string               `json:"phase"`
	Status        string               `json:"status"`
	Workload      string               `json:"workload_status"`
	Detail        string               `json:"detail,omitempty"`
	TimedOut      bool                 `json:"timed_out"`
	StartReady    *time.Duration       `json:"start_to_ready_ns"`
	RestartReady  *time.Duration       `json:"restart_to_ready_ns"`
	IngestP99     *time.Duration       `json:"ingest_p99_ns"`
	QueryP99      *time.Duration       `json:"backfill_query_p99_ns"`
	IngestSamples int                  `json:"ingest_samples"`
	QuerySamples  int                  `json:"backfill_query_samples"`
	RecoveredRows int                  `json:"recovered_rows"`
	Compaction    *compaction          `json:"compaction,omitempty"`
	Cleanup       cleanupProof         `json:"cleanup"`
	Diagnostics   *capacityDiagnostics `json:"diagnostics,omitempty"`
	resources
}

// A capacity pass requires all three independently observed cleanup facts.
// "not_started" records a fixture that never acquired a subtree.
type cleanupProof struct {
	Status          string `json:"status"`
	CandidateReaped bool   `json:"candidate_reaped"`
	SubtreeEmpty    bool   `json:"subtree_empty"`
	SubtreeRemoved  bool   `json:"subtree_removed"`
	StoreRemoved    bool   `json:"store_removed"`
	Detail          string `json:"detail,omitempty"`
}

type resources struct {
	CgroupPeak  *uint64                      `json:"cgroup_peak_memory_bytes"`
	CgroupLimit *uint64                      `json:"cgroup_memory_limit_bytes"`
	CgroupSwap  *uint64                      `json:"cgroup_swap_max_bytes"`
	CgroupOOM   *uint64                      `json:"cgroup_oom_kill_count"`
	PeakRSS     *uint64                      `json:"peak_rss_bytes"`
	PeakFDs     *uint64                      `json:"peak_descriptors"`
	DeviceIO    map[string]map[string]uint64 `json:"device_io"`
	InitialFree *uint64                      `json:"initial_disk_free_bytes"`
	MinFree     *uint64                      `json:"minimum_disk_headroom_bytes"`
	Unavailable map[string]string            `json:"unavailable,omitempty"`
}

type outcome struct {
	Status   string   `json:"status"`
	Detail   string   `json:"detail,omitempty"`
	Evidence []result `json:"evidence,omitempty"`
}

type report struct {
	SchemaVersion int                `json:"schema_version"`
	Revision      string             `json:"revision"`
	SHA256        string             `json:"binary_sha256"`
	Fixture       string             `json:"fixture"`
	Mode          string             `json:"mode"`
	Cases         map[string]outcome `json:"cases"`
}

type options struct {
	binary, revision, fixture, report, mode, workDir, cgroupParent, control string
	timeout                                                                 time.Duration
	requireCorrelation, requireSlowOverlap                                  bool
}

func pinBinary(source, dest, revision string) (digest string, err error) {
	src, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, src.Close()) }()
	stat, err := src.Stat()
	if err != nil {
		return "", err
	}
	if !stat.Mode().IsRegular() {
		return "", errors.New("binary must be a regular file")
	}
	file, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0500)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(file, hash), src)
	digest = fmt.Sprintf("%x", hash.Sum(nil))
	if err := errors.Join(copyErr, file.Close()); err != nil {
		return digest, err
	}
	info, err := buildinfo.ReadFile(dest)
	if err != nil {
		return digest, err
	}
	settings := make(map[string]string)
	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}
	if settings["vcs"] != "git" || settings["vcs.revision"] != revision || settings["vcs.modified"] != "false" ||
		settings["GOOS"] != "linux" || settings["GOARCH"] != runtime.GOARCH || info.Path != "bog/cmd/bog" {
		return digest, fmt.Errorf("expected clean bog/cmd/bog linux/%s at %s, got %s %v", runtime.GOARCH, revision, info.Path, settings)
	}
	return digest, nil
}

func writeReport(path string, r report) error {
	return (capacityevidence.ReportPublisher{}).WriteReport(path, r)
}

func createFixtureRoot(parent string) (string, error) {
	if parent == "" {
		parent = os.TempDir()
	}
	// The launcher changes into the store before executing the candidate,
	// which needs an absolute executable path.
	absoluteParent, err := filepath.Abs(parent)
	if err != nil {
		return "", fmt.Errorf("resolve fixture parent: %w", err)
	}
	return os.MkdirTemp(absoluteParent, "bog-capacity-")
}

func run(ctx context.Context, args []string, publish func(string, report) error) (err error) {
	var o options
	var cutoverReport, correlatedReport string
	f := flag.NewFlagSet("retained-history", flag.ContinueOnError)
	f.StringVar(&correlatedReport, "validate-capacity-correlation", "", "require correlated capacity proof in an existing report")
	f.BoolVar(&o.requireCorrelation, "require-capacity-correlation", false, "require replay, pin age, capacity credit, and cgroup peak for each live replacement")
	f.BoolVar(&o.requireSlowOverlap, "require-slow-response-overlap", false, "qualify the live slow-response, cancellation, and reclamation workload")
	f.StringVar(&cutoverReport, "validate-cutover", "", "validate the cutover proof in an existing capacity report")
	f.StringVar(&o.binary, "binary", "", "absolute path to pinned Linux Bog binary")
	f.StringVar(&o.revision, "revision", "", "full lowercase commit SHA")
	f.StringVar(&o.fixture, "fixture", "", "fixture ID (linux-release-v1)")
	f.StringVar(&o.report, "report", "", "schema-v1 JSON report path")
	f.StringVar(&o.mode, "mode", "live", "live or legacy")
	f.StringVar(&o.workDir, "work-dir", "", "parent for disposable stores on a local Linux filesystem")
	f.StringVar(&o.cgroupParent, "cgroup-parent", "", "delegated cgroup v2 parent (default: current cgroup)")
	f.StringVar(&o.control, "compaction-path", "/_capacity/compaction", "candidate's live compaction control path")
	f.DurationVar(&o.timeout, "timeout", 20*time.Minute, "overall probe deadline")
	if err := f.Parse(args); err != nil {
		return err
	}
	if correlatedReport != "" {
		if f.NArg() != 0 || f.NFlag() != 1 {
			return errors.New("-validate-capacity-correlation takes a report path and runs independently of fixture flags")
		}
		return validateCapacityFile(correlatedReport, capacityevidence.ValidateCorrelatedReport)
	}
	if cutoverReport != "" {
		if f.NArg() != 0 || f.NFlag() != 1 {
			return errors.New("-validate-cutover takes a report path and runs independently of fixture flags")
		}
		return validateCutoverFile(cutoverReport)
	}
	if f.NArg() != 0 || !filepath.IsAbs(o.binary) || !regexp.MustCompile("^[0-9a-f]{40}$").MatchString(o.revision) ||
		o.fixture != capacityevidence.FixtureID || o.report == "" || o.timeout <= 0 || (o.mode != "live" && o.mode != "legacy") {
		return errors.New("require absolute -binary, full lowercase -revision, -fixture linux-release-v1, and -report; see -help")
	}
	if o.requireCorrelation && o.mode != "live" {
		return errors.New("-require-capacity-correlation requires live mode")
	}
	if o.requireSlowOverlap && o.mode != "live" {
		return errors.New("-require-slow-response-overlap requires live mode")
	}
	source, sourceErr := os.Stat(o.binary)
	if dest, destErr := os.Stat(o.report); sourceErr == nil && destErr == nil && os.SameFile(source, dest) {
		return errors.New("binary and report refer to the same file")
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	r := report{1, o.revision, "", o.fixture, o.mode, make(map[string]outcome)}
	key := capacityCase
	if o.mode == "legacy" {
		key = "legacy_baseline"
		r.Cases[capacityCase] = outcome{Status: "skip", Detail: "legacy baseline cannot qualify live retained history"}
	}
	var runs []result
	defer func() {
		c := outcome{Status: "pass", Evidence: runs}
		if err != nil {
			c.Status, c.Detail = "fail", err.Error()
		}
		r.Cases[key] = c
		err = errors.Join(err, publish(o.report, r))
	}()
	dir, err := createFixtureRoot(o.workDir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	binary := filepath.Join(dir, "bog")
	r.SHA256, err = pinBinary(o.binary, binary, o.revision)
	if err != nil {
		return fmt.Errorf("pin binary: %w", err)
	}
	var failures error
	for i, size := range fixtures(o.mode) {
		run := measure(ctx, o, binary, filepath.Join(dir, fmt.Sprintf("store-%d", i)), size)
		runs = append(runs, run)
		if run.Status != "pass" {
			if run.Detail == "" && run.Cleanup.Detail == "" {
				failures = errors.Join(failures, fmt.Errorf("%d seed rows, %s: fixture status %q", size.SeedRows, run.Phase, run.Status))
			}
			if run.Detail != "" {
				failures = errors.Join(failures, fmt.Errorf("%d seed rows, %s: %s", size.SeedRows, run.Phase, run.Detail))
			}
			if run.Cleanup.Detail != "" {
				failures = errors.Join(failures, fmt.Errorf("%d seed rows, cleanup: %s", size.SeedRows, run.Cleanup.Detail))
			}
		}
	}
	return failures
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], writeReport)
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Fatal(err)
	}
}
