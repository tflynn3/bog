package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"bog/internal/checkpoint"
)

type generationReport struct {
	Rows                      int           `json:"rows"`
	Blocks                    int           `json:"blocks"`
	RetainedBytes             int64         `json:"retained_bytes"`
	BloomMetadataBytes        int64         `json:"bloom_metadata_bytes"`
	BloomBytesPerRetainedByte float64       `json:"bloom_bytes_per_retained_byte"`
	OracleSHA256              string        `json:"oracle_sha256"`
	Queries                   []measurement `json:"queries"`
}

type backfillReport struct {
	Scenario     string    `json:"scenario"`
	AppendedRows int       `json:"appended_rows"`
	Earliest     time.Time `json:"earliest_appended_timestamp"`
	Latest       time.Time `json:"latest_appended_timestamp"`
	RewriteBytes int64     `json:"rewrite_bytes"`
	RewriteNS    int64     `json:"rewrite_ns"`
	generationReport
}

type layoutReport struct {
	Name               string `json:"layout"`
	PeakWorkerRSSBytes int64  `json:"peak_worker_rss_bytes"`
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
	BuildRevision string         `json:"build_revision"`
	BinarySHA256  string         `json:"binary_sha256"`
	Revision      string         `json:"revision"`
	Fixture       fixtureReport  `json:"fixture"`
	Environment   map[string]any `json:"environment"`
	Layouts       []layoutReport `json:"layouts"`
}

func writeGeneration(ctx context.Context, path string, blocks [][]checkpoint.BlockRow, l layout) (_ int64, err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := writeLayout(f, l, len(blocks), func(i int) ([]byte, summary, error) {
		if err := ctx.Err(); err != nil {
			return nil, summary{}, err
		}
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

// The phase order is shared by the preparer, worker, and report assembly.
var phases = [...]string{"initial", "clustered", "scattered"}

func generationPath(dir string, phase string, l layout) string {
	return filepath.Join(dir, phase+"-"+l.name())
}

func oraclePath(dir string, phase string, q query) string {
	return filepath.Join(dir, phase+"-"+q.name+".oracle")
}

func prepareGeneration(ctx context.Context, dir, phase string, rows []checkpoint.BlockRow) ([]generationReport, []int64, error) {
	blocks := partition(rows)
	results := make([]generationReport, len(layouts))
	writeNS := make([]int64, len(layouts))
	for i, l := range layouts {
		start := time.Now()
		size, err := writeGeneration(ctx, generationPath(dir, phase, l), blocks, l)
		writeNS[i] = time.Since(start).Nanoseconds()
		if err != nil {
			return nil, nil, fmt.Errorf("%s: write %s: %w", phase, l.name(), err)
		}
		bloomBytes := int64(l.indexWidth()-entrySize) * int64(len(blocks))
		results[i] = generationReport{
			Rows: len(rows), Blocks: len(blocks), RetainedBytes: size,
			BloomMetadataBytes: bloomBytes, BloomBytesPerRetainedByte: float64(bloomBytes) / float64(size),
		}
	}
	fingerprint := sha256.New()
	for _, q := range queries() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if err := writeOracle(ctx, oraclePath(dir, phase, q), len(rows), reference(rows, q), fingerprint); err != nil {
			return nil, nil, fmt.Errorf("%s: write %s oracle: %w", phase, q.name, err)
		}
	}
	digest := hex.EncodeToString(fingerprint.Sum(nil))
	for i := range results {
		results[i].OracleSHA256 = digest
	}
	return results, writeNS, nil
}

func prepare(ctx context.Context, dir string, cfg config) ([]layoutReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	input := fixture(cfg.batches, cfg.profile == "entropy")
	initial, _, err := prepareGeneration(ctx, dir, phases[0], input.rows)
	if err != nil {
		return nil, err
	}
	results := make([]layoutReport, len(layouts))
	for i, l := range layouts {
		results[i] = layoutReport{Name: l.name(), generationReport: initial[i]}
	}
	for _, b := range input.backfills() {
		rewritten, writeNS, err := prepareGeneration(ctx, dir, b.name, input.withBackfill(b))
		if err != nil {
			return nil, err
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
		for i, gen := range rewritten {
			results[i].Backfills = append(results[i].Backfills, backfillReport{
				Scenario: b.name, AppendedRows: len(b.batch), Earliest: earliest, Latest: latest,
				RewriteBytes: gen.RetainedBytes, RewriteNS: writeNS[i], generationReport: gen,
			})
			if b.name == "clustered" {
				results[i].RewriteBytes = gen.RetainedBytes
			}
		}
	}
	return results, nil
}

func withWorkspace(parent string, work func(string) error) (err error) {
	dir, err := os.MkdirTemp(parent, "bog-segment-layout-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	return work(dir)
}
