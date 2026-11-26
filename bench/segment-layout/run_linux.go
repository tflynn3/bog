package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

func coldCache(f *os.File) (func() error, error) {
	fd := int(f.Fd())
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_RANDOM); err != nil {
		return nil, fmt.Errorf("disable readahead: %w", err)
	}
	return func() error {
		if err := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
			return fmt.Errorf("evict file pages: %w", err)
		}
		return nil
	}, nil
}

func peakWorkerRSS() (int64, error) {
	// VmHWM describes the worker's current address space. A wait4 RSS
	// high-water mark can include the child's inherited address space before
	// exec, which may still contain the controller's fixture allocations.
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	return parsePeakRSS(status)
}

func executableDigest(path string) (_ string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	digest := sha256.New()
	if _, err := io.Copy(digest, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func collect(ctx context.Context, cfg config, executable string) (result report, err error) {
	if err := ctx.Err(); err != nil {
		return report{}, err
	}
	err = withWorkspace(cfg.dir, func(dir string) error {
		digest, err := executableDigest(executable)
		if err != nil {
			return fmt.Errorf("hash worker executable: %w", err)
		}
		var fs unix.Statfs_t
		if err := unix.Statfs(dir, &fs); err != nil {
			return err
		}
		var kernel unix.Utsname
		if err := unix.Uname(&kernel); err != nil {
			return err
		}
		layoutReports, err := prepare(ctx, dir, cfg)
		if err != nil {
			return err
		}
		// Release the fixture-building heap to reduce host memory pressure
		// before the workers begin their cold queries.
		debug.FreeOSMemory()
		if err := collectWorkers(ctx, executable, dir, layoutReports, cfg.runs); err != nil {
			return err
		}
		result = report{
			SchemaVersion: 3, Revision: cfg.revision, BuildRevision: sourceRevision(),
			BinarySHA256: digest, Layouts: layoutReports,
			Fixture: fixtureReport{
				Name: cfg.profile + "-log-v2", Scale: cfg.scale, Seed: seed, Batches: cfg.batches, BatchPattern: batchPattern,
				Rows: layoutReports[0].Rows, RewrittenRows: layoutReports[0].Backfills[0].Rows, TargetBlockSize: blockTarget,
				SamplesPerQuery: cfg.runs, MaxLineBytes: largeLineSize,
			},
			Environment: map[string]any{
				"os": runtime.GOOS, "arch": runtime.GOARCH, "go": runtime.Version(), "cpus": runtime.NumCPU(),
				"gomaxprocs": runtime.GOMAXPROCS(0), "kernel": unix.ByteSliceToString(kernel.Release[:]),
				"kernel_machine":   unix.ByteSliceToString(kernel.Machine[:]),
				"filesystem_magic": fmt.Sprintf("%#x", fs.Type), "filesystem_block_bytes": fs.Bsize,
				"cache_policy": "FADV_RANDOM on open, FADV_DONTNEED before each timed query", "work_directory": filepath.Dir(dir),
				"worker_rss_source": "/proc/self/status VmHWM",
				"zstd_level":        zstd.SpeedDefault.String(), "zstd_window_bytes": zstdWindow,
			},
		}
		return nil
	})
	return result, err
}

func run(ctx context.Context, cfg config, executable string, out io.Writer) error {
	result, err := collect(ctx, cfg, executable)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}
