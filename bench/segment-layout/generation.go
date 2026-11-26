package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"bog/internal/checkpoint"
)

type measureFile func(string, int64, query, []checkpoint.BlockRow, int, int) (measurement, error)

// Fingerprint the append input independently of packing and compression.
func inputDigest(rows []checkpoint.BlockRow) (string, error) {
	h := sha256.New()
	var fields [28]byte
	for _, row := range rows {
		le.PutUint64(fields[:8], uint64(row.Row.TS.Unix()))
		le.PutUint32(fields[8:12], uint32(row.Row.TS.Nanosecond()))
		le.PutUint64(fields[12:20], row.Ordinal)
		le.PutUint64(fields[20:28], uint64(len(row.Row.Line)))
		if _, err := h.Write(fields[:]); err != nil {
			return "", err
		}
		if _, err := io.WriteString(h, row.Row.Line); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
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

// Each phase shares its append-order packing, queries, and oracle rows across
// the layouts. The baseline for filtered reads is local to this phase.
func runGeneration(dir, phase string, rows []checkpoint.BlockRow, runs int, measure measureFile) ([]generationReport, error) {
	digest, err := inputDigest(rows)
	if err != nil {
		return nil, fmt.Errorf("%s: fingerprint append input: %w", phase, err)
	}
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
			InputSHA256: digest, Rows: len(rows), Blocks: len(blocks), RetainedBytes: size,
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
			if q.name == falsePositiveQuery && (len(want) != 0 || m.Blocks == 0) {
				return nil, fmt.Errorf("%s: %s did not exercise the Bloom false-positive probe", phase, l.name())
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

func collectIn(dir string, cfg config, env reportEnvironment, measure measureFile) (report, error) {
	input := fixture(cfg.batches, cfg.profile == "entropy")
	initial, err := runGeneration(dir, "initial", input.rows, cfg.runs, measure)
	if err != nil {
		return report{}, err
	}
	results := make([]layoutReport, len(layouts))
	for i, l := range layouts {
		results[i] = layoutReport{Name: l.name(), generationReport: initial[i]}
	}

	for _, b := range input.backfills() {
		rewritten, err := runGeneration(dir, b.name, input.withBackfill(b), cfg.runs, measure)
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
		SchemaVersion: reportVersion, Revision: cfg.revision, Layouts: results,
		Fixture: fixtureReport{
			Name: cfg.profile + fixtureVersion, Scale: cfg.scale, Seed: seed, Batches: cfg.batches, BatchPattern: batchPattern,
			Rows: len(input.rows), RewrittenRows: results[0].Backfills[0].Rows, TargetBlockSize: blockTarget,
			SamplesPerQuery: cfg.runs, MaxLineBytes: largeLineSize,
		},
		Environment: env,
	}, nil
}
