package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"bog/internal/checkpoint"
)

type incrementalReport struct {
	SchemaVersion int                      `json:"schema_version"`
	Mode          string                   `json:"mode"`
	Revision      string                   `json:"revision"`
	Environment   map[string]any           `json:"environment"`
	Fixture       incrementalFixtureReport `json:"fixture"`
	Layouts       []incrementalLayout      `json:"layouts"`
}

type incrementalFixtureReport struct {
	Name    string `json:"name"`
	Scale   string `json:"scale"`
	Seed    uint64 `json:"seed"`
	Batches int    `json:"batches"`
	Runs    int    `json:"samples_per_compaction"`
}

type incrementalLayout struct {
	Codec        string              `json:"codec"`
	SegmentBytes int                 `json:"target_segment_bytes"`
	Publications []publicationResult `json:"publications"`
}

type publicationResult struct {
	Phase         string `json:"phase"`
	Rows          int    `json:"rows"`
	AppendedRows  int    `json:"appended_rows"`
	Segments      int    `json:"segments"`
	ManifestBytes int64  `json:"manifest_bytes"`
	segmentBytes
	Decode          decodeSample       `json:"decode"`
	QueryCompaction *compactionLatency `json:"query_during_compaction,omitempty"`
}

type decodeRequest struct {
	Manifest string
	Target   int
}

func decodeWorker(ctx context.Context, req decodeRequest, out io.Writer) error {
	current, err := readSnapshot(req.Manifest)
	if err != nil {
		return err
	}
	stats, err := collectDecode(ctx, filepath.Dir(req.Manifest), current, req.Target)
	if err != nil {
		return err
	}
	stats.PeakRSSBytes, err = selfPeakRSS()
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(stats)
}

func measureSnapshotDecode(ctx context.Context, cfg config, manifest string, target int) (decodeSample, error) {
	arg, err := json.Marshal(workerRequest{Decode: &decodeRequest{manifest, target}})
	if err != nil {
		return decodeSample{}, err
	}
	sampleCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	var sample decodeSample
	_, err = runWorker(sampleCtx, []string{workerFlag, string(arg)}, func(r io.Reader) error {
		dec := json.NewDecoder(r)
		if err := dec.Decode(&sample); err != nil {
			return err
		}
		if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
			return fmt.Errorf("decode worker sent extra data: %v", err)
		}
		return nil
	})
	return sample, err
}

func collectIncremental(ctx context.Context, cfg config) (_ incrementalReport, err error) {
	dir, err := os.MkdirTemp(cfg.dir, "bog-segment-layout-")
	if err != nil {
		return incrementalReport{}, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	env, err := benchmarkEnvironment(dir, cfg)
	if err != nil {
		return incrementalReport{}, err
	}
	env["cache_policy"] = "filesystem cache left in place"
	input := fixture(cfg.batches, cfg.profile == "entropy", cfg.fixtureSeed)
	result := incrementalReport{
		SchemaVersion: 4, Mode: "incremental", Revision: cfg.revision, Environment: env,
		Fixture: incrementalFixtureReport{cfg.profile + "-successive-v1", cfg.scale, cfg.fixtureSeed, cfg.batches, cfg.runs},
	}
	for _, target := range segmentTargets {
		for c := rawCodec; c <= zstdCodec; c++ {
			l := layout{codec: c}
			work := filepath.Join(dir, fmt.Sprintf("%s-%d", l.name(), target))
			if err := os.Mkdir(work, 0700); err != nil {
				return incrementalReport{}, err
			}
			pub, err := runPublications(ctx, cfg, work, input, l, target)
			if err != nil {
				return incrementalReport{}, fmt.Errorf("%s/%d: %w", l.name(), target, err)
			}
			result.Layouts = append(result.Layouts, incrementalLayout{l.name(), target, pub})
			if err := os.RemoveAll(work); err != nil {
				return incrementalReport{}, err
			}
		}
	}
	return result, nil
}

func runPublications(ctx context.Context, cfg config, dir string, input fixtureInput, l layout, target int) ([]publicationResult, error) {
	phases := append([]backfill{{"initial", input.rows}}, input.successiveAppends()...)
	all := query{name: "all", start: firstTime, end: endTime}
	// Copy the append history so verification never mutates the seed rows.
	expected := make([]checkpoint.BlockRow, 0, len(input.rows)+12)
	var previous snapshot
	results := make([]publicationResult, 0, len(phases))
	for i, phase := range phases {
		manifest, latency, err := measurePublication(ctx, cfg, dir, i, previous, expected, phase.batch, l, target)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", phase.name, err)
		}
		current, err := readSnapshot(manifest)
		if err != nil {
			return nil, err
		}
		bytes, err := accountSegments(previous, current)
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(manifest)
		if err != nil {
			return nil, err
		}
		expected = append(expected, phase.batch...)
		verifyCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		rows, err := querySnapshot(verifyCtx, dir, current, target, all)
		cancel()
		if err != nil {
			return nil, err
		}
		if err := checkSnapshotRows(rows, reference(expected, all)); err != nil {
			return nil, fmt.Errorf("%s: %w", phase.name, err)
		}
		stats, err := measureSnapshotDecode(ctx, cfg, manifest, target)
		if err != nil {
			return nil, err
		}
		results = append(results, publicationResult{
			Phase: phase.name, Rows: len(expected), AppendedRows: len(phase.batch), Segments: len(current.Segments),
			ManifestBytes: info.Size(), segmentBytes: bytes, Decode: stats, QueryCompaction: latency,
		})
		previous = current
	}
	return results, nil
}

func measurePublication(ctx context.Context, cfg config, dir string, generation int, previous snapshot, history, batch []checkpoint.BlockRow, l layout, target int) (string, *compactionLatency, error) {
	if generation == 0 {
		trialCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
		path, err := publishSegments(trialCtx, dir, "00-0000", previous, batch, l, target)
		return path, nil, err
	}
	q := query{"sparse", sparseLiteral, firstTime, endTime}
	want := reference(history, q)
	latency := &compactionLatency{Query: q.name, MatchingRows: len(want)}
	const attemptsPerSample = 5
	for i := range attemptsPerSample * cfg.runs {
		trialCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		var rows []checkpoint.BlockRow
		path, elapsed, overlap, err := compactWithQuery(trialCtx, func() error {
			var err error
			rows, err = querySnapshot(trialCtx, dir, previous, target, q)
			return err
		}, func() (string, error) {
			return publishSegments(trialCtx, dir, fmt.Sprintf("%02d-%04d", generation, i), previous, batch, l, target)
		})
		cancel()
		if err != nil {
			return "", nil, err
		}
		if err := checkSnapshotRows(rows, want); err != nil {
			return "", nil, err
		}
		if overlap > 0 {
			if err := latency.add(elapsed, overlap); err != nil {
				return "", nil, err
			}
			if len(latency.SamplesNS) == cfg.runs {
				latency.P95NS = p95(latency.SamplesNS)
				return path, latency, nil
			}
		}
		current, err := readSnapshot(path)
		if err != nil {
			return "", nil, err
		}
		if err := removeTrial(dir, path, previous, current); err != nil {
			return "", nil, err
		}
	}
	return "", nil, fmt.Errorf("collected %d of %d overlapping compaction samples in %d attempts",
		len(latency.SamplesNS), cfg.runs, attemptsPerSample*cfg.runs)
}
