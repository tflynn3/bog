package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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
	Scenario     string    `json:"scenario"`
	AppendedRows int       `json:"appended_rows"`
	Earliest     time.Time `json:"earliest_appended_timestamp"`
	Latest       time.Time `json:"latest_appended_timestamp"`
	RewriteBytes int64     `json:"rewrite_bytes"`
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
	Seed            int    `json:"seed"`
	Batches         int    `json:"batches"`
	BatchPattern    []int  `json:"batch_size_pattern"`
	Rows            int    `json:"rows"`
	RewrittenRows   int    `json:"rewritten_rows"`
	TargetBlockSize int    `json:"target_block_bytes"`
	SamplesPerQuery int    `json:"samples_per_query"`
	MaxLineBytes    int    `json:"max_line_bytes"`
}

type report struct {
	SchemaVersion int            `json:"schema_version"`
	Revision      string         `json:"revision"`
	Fixture       fixtureReport  `json:"fixture"`
	Environment   map[string]any `json:"environment"`
	Layouts       []layoutReport `json:"layouts"`
}

func writeGeneration(path string, blocks [][]checkpoint.BlockRow, l layout) (_ int64, err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := writeLayout(f, l, len(blocks), func(i int) ([]byte, summary, error) {
		rows := slices.Clone(blocks[i])
		slices.SortFunc(rows, ordered)
		var filter summary
		if l.prune {
			for _, row := range rows {
				filter.add(row.Row.Line)
			}
		}
		raw, err := checkpoint.EncodeBlock(rows)
		return raw, filter, err
	}); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func measure(path string, size int64, q query, want []checkpoint.BlockRow, totalRows, runs int) (_ measurement, err error) {
	f, err := os.Open(path)
	if err != nil {
		return measurement{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	fd := int(f.Fd())
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_RANDOM); err != nil {
		return measurement{}, fmt.Errorf("disable readahead: %w", err)
	}
	return measureQuery(f, size, q, want, totalRows, runs, func() error {
		if err := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
			return fmt.Errorf("evict file pages: %w", err)
		}
		return nil
	})
}

// Each phase shares its append-order packing, queries, and oracle rows across
// the layouts. The baseline for filtered reads is local to this phase.
func runGeneration(dir, phase string, rows []checkpoint.BlockRow, runs int) ([]generationReport, error) {
	blocks := partition(rows)
	paths := make([]string, len(layouts))
	results := make([]generationReport, len(layouts))
	for i, l := range layouts {
		paths[i] = filepath.Join(dir, phase+"-"+l.name())
		size, err := writeGeneration(paths[i], blocks, l)
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
		samples := make([]measurement, len(layouts))
		for i, l := range layouts {
			m, err := measure(paths[i], results[i].RetainedBytes, q, want, len(rows), runs)
			if err != nil {
				return nil, fmt.Errorf("%s: %s/%s: %w", phase, l.name(), q.name, err)
			}
			samples[i] = m
		}
		for i, l := range layouts {
			if err := samples[i].compareReads(samples[baselines[l.codec]]); err != nil {
				return nil, fmt.Errorf("%s: %s: %w", phase, l.name(), err)
			}
			results[i].Queries = append(results[i].Queries, samples[i])
		}
	}
	return results, nil
}

func collect(cfg config) (_ report, err error) {
	dir, err := os.MkdirTemp(cfg.dir, "bog-segment-layout-")
	if err != nil {
		return report{}, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return report{}, err
	}
	var kernel unix.Utsname
	if err := unix.Uname(&kernel); err != nil {
		return report{}, err
	}
	input := fixture(cfg.batches, cfg.profile == "entropy")
	initial, err := runGeneration(dir, "initial", input.rows, cfg.runs)
	if err != nil {
		return report{}, err
	}
	results := make([]layoutReport, len(layouts))
	for i, l := range layouts {
		results[i] = layoutReport{Name: l.name(), generationReport: initial[i]}
	}

	for _, b := range input.backfills() {
		rewritten, err := runGeneration(dir, b.name, input.withBackfill(b), cfg.runs)
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
		SchemaVersion: 2, Revision: cfg.revision, Layouts: results,
		Fixture: fixtureReport{
			Name: cfg.profile + "-log-v2", Scale: cfg.scale, Seed: seed, Batches: cfg.batches, BatchPattern: batchPattern,
			Rows: len(input.rows), RewrittenRows: results[0].Backfills[0].Rows, TargetBlockSize: blockTarget,
			SamplesPerQuery: cfg.runs, MaxLineBytes: largeLineSize,
		},
		Environment: map[string]any{
			"os": runtime.GOOS, "arch": runtime.GOARCH, "go": runtime.Version(), "cpus": runtime.NumCPU(),
			"gomaxprocs": runtime.GOMAXPROCS(0), "kernel": unix.ByteSliceToString(kernel.Release[:]),
			"filesystem_magic": fmt.Sprintf("%#x", fs.Type), "filesystem_block_bytes": fs.Bsize,
			"cache_policy": "FADV_RANDOM on open, FADV_DONTNEED before each timed query", "work_directory": filepath.Dir(dir),
			"zstd_level": zstd.SpeedDefault.String(), "zstd_window_bytes": zstdWindow,
		},
	}, nil
}

func run(cfg config, out io.Writer) error {
	result, err := collect(cfg)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}
