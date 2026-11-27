package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

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

func runQualificationWorker(ctx context.Context, req workerRequest, result any) error {
	arg, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = runWorker(ctx, []string{workerFlag, string(arg)}, func(r io.Reader) error {
		dec := json.NewDecoder(r)
		if err := dec.Decode(result); err != nil {
			return err
		}
		if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
			return fmt.Errorf("worker sent extra data: %v", err)
		}
		return nil
	})
	return err
}

func collectIncremental(ctx context.Context, cfg config) (_ incrementalReport, err error) {
	if !knownRevision(cfg.revision) {
		return incrementalReport{}, errors.New("source revision unavailable; supply -revision")
	}
	dir, err := os.MkdirTemp(cfg.dir, "bog-segment-layout-")
	if err != nil {
		return incrementalReport{}, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	env, err := benchmarkEnvironment(dir, cfg)
	if err != nil {
		return incrementalReport{}, err
	}
	env["cache_policy"] = qualificationCache
	result := incrementalReport{
		SchemaVersion: 4, Mode: "incremental", Revision: cfg.revision, Environment: env,
		Fixture: incrementalFixtureReport{cfg.profile + "-successive-v1", cfg.scale, cfg.fixtureSeed, cfg.batches, cfg.runs},
	}
	baseline, err := qualifyLayout(ctx, cfg, dir, 0, rawCodec)
	if err != nil {
		return incrementalReport{}, fmt.Errorf("%s: %w", productionV1, err)
	}
	result.Baseline = productionLayout{productionV1, baseline}
	for _, target := range segmentTargets {
		for c := rawCodec; c <= zstdCodec; c++ {
			name := (layout{codec: c}).name()
			pubs, err := qualifyLayout(ctx, cfg, dir, target, c)
			if err != nil {
				return incrementalReport{}, fmt.Errorf("%s/%d: %w", name, target, err)
			}
			result.Layouts = append(result.Layouts, incrementalLayout{name, target, pubs})
		}
	}
	return result, result.validate()
}

type qualificationRequest struct {
	Dir, Profile, Query string
	Batches, Target     int
	Seed                uint64
	Codec               codec
	Timeout             time.Duration
}

func qualifyLayout(ctx context.Context, cfg config, dir string, target int, c codec) ([]publicationResult, error) {
	var pubs []publicationResult
	for _, name := range qualificationQueries {
		accepted := 0
		const attemptsPerSample = 5
		for range attemptsPerSample * cfg.runs {
			req := qualificationRequest{
				Profile: cfg.profile, Query: name, Batches: cfg.batches, Target: target,
				Seed: cfg.fixtureSeed, Codec: c, Timeout: cfg.timeout,
			}
			trial, err := qualifyTrial(ctx, dir, req, target != 0 && pubs == nil)
			if err != nil {
				return nil, fmt.Errorf("%s trial: %w", name, err)
			}
			if err := mergeTrial(&pubs, trial, name); err != nil {
				return nil, err
			}
			if trial != nil {
				accepted++
				if accepted == cfg.runs {
					break
				}
			}
		}
		if accepted != cfg.runs {
			return nil, fmt.Errorf("%s: collected %d of %d trials with overlap in every append phase", name, accepted, cfg.runs)
		}
	}
	for i := range pubs {
		pub := &pubs[i]
		for j := range pub.Queries {
			pub.Queries[j].summarize()
		}
		if i > 0 {
			q := pub.Queries[0]
			sparse := compactionLatency{Query: q.Query, MatchingRows: q.MatchingRows, P95NS: q.QueryP95NS}
			for _, sample := range q.Samples {
				if err := sparse.add(time.Duration(sample.QueryNS), time.Duration(sample.OverlapNS)); err != nil {
					return nil, fmt.Errorf("%s sparse sample: %w", pub.Phase, err)
				}
			}
			pub.QueryCompaction = &sparse
		}
	}
	return pubs, nil
}

func qualifyTrial(ctx context.Context, dir string, req qualificationRequest, decode bool) (_ []publicationResult, err error) {
	req.Dir, err = os.MkdirTemp(dir, "trial-")
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(req.Dir)) }()
	// Bound the child even if it is blocked in a filesystem syscall between
	// deadline checks. Individual phases enforce their own shorter limits.
	trialCtx, cancel := context.WithTimeout(ctx, 5*req.Timeout)
	defer cancel()
	var trial []publicationResult
	if err := runQualificationWorker(trialCtx, workerRequest{Qualification: &req}, &trial); err != nil {
		return nil, err
	}
	if decode {
		for i := range trial {
			path := filepath.Join(req.Dir, fmt.Sprintf("%02d.json", i))
			var sample decodeSample
			decodeCtx, cancel := context.WithTimeout(ctx, req.Timeout)
			err := runQualificationWorker(decodeCtx, workerRequest{Decode: &decodeRequest{path, req.Target}}, &sample)
			cancel()
			if err != nil {
				return nil, fmt.Errorf("%s decode: %w", trial[i].Phase, err)
			}
			trial[i].Decode = &sample
		}
	}
	return trial, nil
}

func mergeTrial(pubs *[]publicationResult, trial []publicationResult, name string) error {
	if trial == nil { // One or more scans finished without observing overlap.
		return nil
	}
	if len(trial) != len(qualificationPhases) {
		return errors.New("incomplete qualification worker response")
	}
	for i, p := range trial {
		if p.Phase != qualificationPhases[i] || len(p.Queries) != 1 ||
			p.Queries[0].Query != name || len(p.Queries[0].Samples) != 1 {
			return fmt.Errorf("invalid worker response for %s/%s", p.Phase, name)
		}
		if *pubs == nil {
			continue
		}
		dst := &(*pubs)[i]
		if p.Rows != dst.Rows || p.AppendedRows != dst.AppendedRows || p.Segments != dst.Segments ||
			p.ManifestBytes != dst.ManifestBytes || p.segmentBytes != dst.segmentBytes {
			return fmt.Errorf("%s: logical publication counters changed across trials", p.Phase)
		}
		q := p.Queries[0]
		last := &dst.Queries[len(dst.Queries)-1]
		if last.Query != name {
			dst.Queries = append(dst.Queries, q)
			continue
		}
		if last.MatchingRows != q.MatchingRows {
			return fmt.Errorf("%s/%s: row count changed across trials", p.Phase, name)
		}
		last.Samples = append(last.Samples, q.Samples[0])
	}
	if *pubs == nil {
		*pubs = trial
	}
	return nil
}
