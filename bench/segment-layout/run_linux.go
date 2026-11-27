package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"bog/internal/checkpoint"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

type generationReport struct {
	Rows                      int           `json:"rows"`
	Blocks                    int           `json:"blocks"`
	RetainedBytes             int64         `json:"retained_bytes"`
	BloomMetadataBytes        int64         `json:"bloom_metadata_bytes"`
	BloomBytesPerRetainedByte float64       `json:"bloom_bytes_per_retained_byte"`
	Queries                   []measurement `json:"queries"`
}

type backfillReport struct {
	Scenario           string        `json:"scenario"`
	AppendedRows       int           `json:"appended_rows"`
	Earliest           time.Time     `json:"earliest_appended_timestamp"`
	Latest             time.Time     `json:"latest_appended_timestamp"`
	RewriteBytes       int64         `json:"rewrite_bytes"`
	WriteAmplification amplification `json:"write_amplification"`
	generationReport
}

type layoutReport struct {
	Name string `json:"layout"`
	generationReport
	RewriteBytes int64            `json:"rewrite_bytes"` // Clustered rewrite, also listed in Backfills.
	Backfills    []backfillReport `json:"backfills"`
}

type fixtureReport struct {
	Name            string `json:"name"`
	Scale           string `json:"scale"`
	Seed            uint64 `json:"seed"`
	Batches         int    `json:"batches"`
	BatchPattern    []int  `json:"batch_size_pattern"`
	Rows            int    `json:"rows"`
	RewrittenRows   int    `json:"rewritten_rows"`
	TargetBlockSize int    `json:"target_block_bytes"`
	SamplesPerQuery int    `json:"samples_per_query"`
	MaxLineBytes    int    `json:"max_line_bytes"`
}

type report struct {
	SchemaVersion int              `json:"schema_version"`
	Revision      string           `json:"revision"`
	Fixture       fixtureReport    `json:"fixture"`
	Environment   map[string]any   `json:"environment"`
	Layouts       []layoutReport   `json:"layouts"`
	Compaction    compactionReport `json:"compaction"`
}

// Each phase shares its append-order packing, queries, and oracle rows across
// the layouts. The baseline for filtered reads is local to this phase.
func runGeneration(ctx context.Context, cfg config, dir, phase string, rows []checkpoint.BlockRow) ([]generationReport, error) {
	blocks := partition(rows)
	results := make([]generationReport, len(layouts))
	for i, l := range layouts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		size, err := writeGeneration(filepath.Join(dir, phase+"-"+l.name()), blocks, l)
		if err != nil {
			return nil, fmt.Errorf("%s: write %s: %w", phase, l.name(), err)
		}
		bloomBytes := int64(l.indexWidth()-entrySize) * int64(len(blocks))
		results[i] = generationReport{
			Rows: len(rows), Blocks: len(blocks), RetainedBytes: size,
			BloomMetadataBytes: bloomBytes, BloomBytesPerRetainedByte: float64(bloomBytes) / float64(size),
		}
	}
	baselines := make(map[codec]int)
	for i, l := range layouts {
		if !l.prune {
			baselines[l.codec] = i
		}
	}
	for i, l := range layouts {
		if l.prune && results[i].RetainedBytes-results[baselines[l.codec]].RetainedBytes != results[i].BloomMetadataBytes {
			return nil, fmt.Errorf("%s: %s retained-byte delta differs from Bloom metadata size", phase, l.name())
		}
	}

	for _, q := range queries() {
		want := reference(rows, q)
		for i, l := range layouts {
			m, err := measureIsolated(ctx, cfg, filepath.Join(dir, phase+"-"+l.name()), q, want, len(rows))
			if err != nil {
				return nil, fmt.Errorf("%s: %s/%s: %w", phase, l.name(), q.name, err)
			}
			results[i].Queries = append(results[i].Queries, m)
		}
	}
	for i, l := range layouts {
		for j := range results[i].Queries {
			if err := results[i].Queries[j].compareReads(results[baselines[l.codec]].Queries[j]); err != nil {
				return nil, fmt.Errorf("%s: %s: %w", phase, l.name(), err)
			}
		}
	}
	return results, nil
}

func collect(ctx context.Context, cfg config) (_ report, err error) {
	dir, err := os.MkdirTemp(cfg.dir, "bog-segment-layout-")
	if err != nil {
		return report{}, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	environment, err := benchmarkEnvironment(dir, cfg)
	if err != nil {
		return report{}, err
	}
	input := fixture(cfg.batches, cfg.profile == "entropy", cfg.fixtureSeed)
	production, denominators, err := measureCompaction(ctx, cfg, dir, input)
	if err != nil {
		return report{}, err
	}
	initial, err := runGeneration(ctx, cfg, dir, "initial", input.rows)
	if err != nil {
		return report{}, err
	}
	results := make([]layoutReport, len(layouts))
	for i, l := range layouts {
		results[i] = layoutReport{Name: l.name(), generationReport: initial[i]}
	}

	for _, b := range input.backfills() {
		rewritten, err := runGeneration(ctx, cfg, dir, b.name, input.withBackfill(b))
		if err != nil {
			return report{}, err
		}
		earliest, latest := b.batch[0].Row.TS, b.batch[0].Row.TS
		for _, row := range b.batch[1:] {
			if row.Row.TS.Before(earliest) {
				earliest = row.Row.TS
			}
			if row.Row.TS.After(latest) {
				latest = row.Row.TS
			}
		}
		for i, l := range layouts {
			generation := rewritten[i]
			results[i].Backfills = append(results[i].Backfills, backfillReport{
				Scenario: b.name, AppendedRows: len(b.batch), Earliest: earliest, Latest: latest,
				RewriteBytes: generation.RetainedBytes, generationReport: generation,
				WriteAmplification: writeAmplification(uint64(generation.RetainedBytes), denominators[b.name], benchmarkBoundary),
			})
			if b.name == "clustered" {
				results[i].RewriteBytes = generation.RetainedBytes
			}
			if err := os.Remove(filepath.Join(dir, b.name+"-"+l.name())); err != nil {
				return report{}, err
			}
		}
	}

	return report{
		SchemaVersion: 3, Revision: cfg.revision, Layouts: results, Compaction: production,
		Fixture: fixtureReport{
			Name: cfg.profile + "-log-v2", Scale: cfg.scale, Seed: cfg.fixtureSeed, Batches: cfg.batches, BatchPattern: batchPattern,
			Rows: len(input.rows), RewrittenRows: results[0].Backfills[0].Rows, TargetBlockSize: blockTarget,
			SamplesPerQuery: cfg.runs, MaxLineBytes: largeLineSize,
		},
		Environment: environment,
	}, nil
}

func benchmarkEnvironment(dir string, cfg config) (map[string]any, error) {
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return nil, err
	}
	var kernel unix.Utsname
	if err := unix.Uname(&kernel); err != nil {
		return nil, err
	}
	env := map[string]any{
		"os": runtime.GOOS, "arch": runtime.GOARCH, "go": runtime.Version(), "cpus": runtime.NumCPU(),
		"gomaxprocs": runtime.GOMAXPROCS(0), "kernel": unix.ByteSliceToString(kernel.Release[:]),
		"filesystem_magic": fmt.Sprintf("%#x", fs.Type), "filesystem_block_bytes": fs.Bsize,
		"cache_policy": "FADV_RANDOM on open, FADV_DONTNEED before each timed query", "work_directory": filepath.Dir(dir),
		"operation_timeout_ns": cfg.timeout.Nanoseconds(),
		"godebug":              os.Getenv("GODEBUG"), "gogc": os.Getenv("GOGC"), "gomemlimit": os.Getenv("GOMEMLIMIT"), "zstd_level": zstd.SpeedDefault.String(), "zstd_window_bytes": zstdWindow,
	}
	return env, nil
}

func run(ctx context.Context, cfg config, out io.Writer) error {
	if cfg.validateReport != "" {
		f, err := os.Open(cfg.validateReport)
		if err != nil {
			return err
		}
		return errors.Join(validateQualificationJSON(f), f.Close())
	}
	if cfg.incremental {
		result, err := collectIncremental(ctx, cfg)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(result)
	}
	result, err := collect(ctx, cfg)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}
