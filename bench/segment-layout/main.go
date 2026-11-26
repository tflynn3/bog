package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

type queryResult struct {
	Name    string    `json:"name"`
	Literal string    `json:"literal"`
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	queryStats
	P50NS int64 `json:"cold_p50_ns"`
	P95NS int64 `json:"cold_p95_ns"`
}

type layoutResult struct {
	Name          string        `json:"layout"`
	RetainedBytes int64         `json:"retained_bytes"`
	RewriteBytes  int64         `json:"rewrite_bytes"`
	Queries       []queryResult `json:"queries"`
}

func exercise(path string, l layout, rows []checkpoint.BlockRow, blocks [][]checkpoint.BlockRow, trials int) (result layoutResult, err error) {
	result.Name = l.name()
	result.RewriteBytes, err = packFile(path, l, blocks)
	if err != nil {
		return result, err
	}
	f, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return result, err
	}
	result.RetainedBytes = info.Size()
	for _, q := range queries {
		want := reference(rows, q)
		ordinal := 0
		stats, err := scan(f, info.Size(), q, true, func(got checkpoint.BlockRow) error {
			if ordinal >= len(want) || got != want[ordinal] {
				return fmt.Errorf("oracle mismatch at result %d", ordinal)
			}
			ordinal++
			return nil
		})
		if err != nil {
			return result, fmt.Errorf("%s: %w", q.name, err)
		}
		if ordinal != len(want) {
			return result, fmt.Errorf("%s: got %d rows, expected %d", q.name, ordinal, len(want))
		}
		samples := make([]int64, trials)
		for i := range samples {
			if err := invalidate(f); err != nil {
				return result, err
			}
			start := time.Now()
			got, err := scan(f, info.Size(), q, false, nil)
			samples[i] = time.Since(start).Nanoseconds()
			if err != nil {
				return result, err
			}
			if got.Rows != stats.Rows || got.BlocksRead != stats.BlocksRead {
				return result, fmt.Errorf("%s: scan changed during timing", q.name)
			}
		}
		slices.Sort(samples)
		result.Queries = append(result.Queries, queryResult{
			Name: q.name, Literal: q.literal, Start: q.start, End: q.end, queryStats: stats,
			P50NS: samples[(trials+1)/2-1],
			P95NS: samples[(trials*95+99)/100-1],
		})
	}
	return result, nil
}

func run(parent string, smoke bool, trials int) (err error) {
	dir, err := os.MkdirTemp(parent, "bog-segment-layout-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	host, err := hostMetadata(dir)
	if err != nil {
		return err
	}
	n, oversized := 120_000, store.MaxBatch
	if smoke {
		n, oversized = 6_000, 320<<10
	}
	rows, err := fixture(n, oversized)
	if err != nil {
		return fmt.Errorf("fixture: %w", err)
	}
	blocks := partition(rows)
	results := make([]layoutResult, 0, 6)
	for _, codec := range []compression{raw, lz4Block, zstdBlock} {
		for _, summaries := range []bool{false, true} {
			l := layout{codec, summaries}
			result, err := exercise(filepath.Join(dir, l.name()+".bin"), l, rows, blocks, trials)
			if err != nil {
				return fmt.Errorf("%s: %w", l.name(), err)
			}
			results = append(results, result)
		}
	}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("missing build information")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"environment": map[string]any{
			"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH,
			"cpus": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0), "host": host,
			"build": build, "cache_policy": cachePolicy,
		},
		"fixture": map[string]any{
			"name": fixtureVersion, "seed": fixtureSeed, "smoke": smoke, "rows": len(rows),
			"oversized_line_bytes": len(rows[n].Row.Line), "oversized_frame_bytes": oversized, "regular_batch_rows": batchSizes,
			"blocks": len(blocks), "block_target_bytes": targetBlockBytes, "summary_bytes_per_block": summaryBytes,
		},
		"trials_per_query": trials, "layouts": results,
	})
}

func main() {
	smoke := flag.Bool("smoke", false, "use the small deterministic fixture")
	trials := flag.Int("trials", 0, "timed repetitions per query (default 21, smoke 1)")
	parent := flag.String("dir", "", "parent for scratch files (default OS temporary directory)")
	flag.Parse()
	if *trials == 0 {
		*trials = 21
		if *smoke {
			*trials = 1
		}
	}
	if *trials < 1 || *trials > 1000 || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(*parent, *smoke, *trials); err != nil {
		// There is no recovery if stderr itself is unavailable.
		if _, err := fmt.Fprintln(os.Stderr, err); err != nil {
			os.Exit(2)
		}
		os.Exit(1)
	}
}
