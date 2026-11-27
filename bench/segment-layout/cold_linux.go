package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"golang.org/x/sys/unix"
)

type coldRequest struct {
	Path, Query string
	Production  bool
}

type coldTarget struct {
	name       string
	path       string
	production bool
	phases     []coldPhase
}

func collectCold(ctx context.Context, cfg config) (_ coldMatrixReport, err error) {
	if err := ctx.Err(); err != nil {
		return coldMatrixReport{}, err
	}
	dir, err := os.MkdirTemp(cfg.dir, "bog-segment-cold-")
	if err != nil {
		return coldMatrixReport{}, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	env, err := benchmarkEnvironment(dir, cfg)
	if err != nil {
		return coldMatrixReport{}, err
	}
	env["cache_policy"] = coldCachePolicy
	var runID [16]byte
	if _, err := rand.Read(runID[:]); err != nil {
		return coldMatrixReport{}, fmt.Errorf("cold pairing run ID: %w", err)
	}
	result := coldMatrixReport{
		SchemaVersion: coldMatrixSchema, Mode: "cold-matrix", Revision: cfg.revision,
		RunID: hex.EncodeToString(runID[:]), Environment: env,
		Protocol: matrixProtocol(cfg.profile, cfg.runs),
	}
	for _, profile := range result.Protocol.ProfileOrder {
		profileDir := filepath.Join(dir, profile)
		if err := os.Mkdir(profileDir, 0700); err != nil {
			return coldMatrixReport{}, err
		}
		cfg.profile = profile
		report, err := collectColdProfile(ctx, cfg, profileDir, result.RunID, env)
		if err != nil {
			return coldMatrixReport{}, fmt.Errorf("%s: %w", profile, err)
		}
		result.Profiles = append(result.Profiles, report)
		if err := os.RemoveAll(profileDir); err != nil {
			return coldMatrixReport{}, err
		}
	}
	if err := result.validateHeader(); err != nil {
		return coldMatrixReport{}, err
	}
	return result, ctx.Err()
}

func collectColdProfile(ctx context.Context, cfg config, dir, runID string, env map[string]any) (coldReport, error) {
	input := fixture(cfg.batches, cfg.profile == "entropy", cfg.fixtureSeed)
	result := coldReport{
		SchemaVersion: coldMatrixSchema, Mode: "cold", Revision: cfg.revision, Environment: env,
		Fixture: coldFixture{cfg.profile, cfg.scale, cfg.fixtureSeed, cfg.batches, cfg.runs, blockTarget},
		Oracle:  coldOracle, Live: liveReport{Status: "unavailable", Reason: "no pinned binary supplied"},
	}
	result.Pairing = &coldPairing{RunID: runID, FixtureSHA256: coldFixtureDigest(result.Fixture, input)}
	bursts := append([]backfill{{"initial", nil}}, input.backfills()...)
	targets := make([]coldTarget, 1+len(layouts))
	targets[0].name = productionV1
	for i, l := range layouts {
		targets[i+1].name = l.name()
	}
	denominators := make(map[string]uint64)
	for phaseIndex, burst := range bursts {
		rows := input.rows
		if phaseIndex != 0 {
			rows = input.withBackfill(burst)
		}
		prepareCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		err := prepareColdTargets(prepareCtx, cfg, dir, burst, rows, targets)
		cancel()
		if err != nil {
			return coldReport{}, fmt.Errorf("%s publication: %w", burst.name, err)
		}
		if phaseIndex != 0 {
			denominators[burst.name] = targets[0].phases[phaseIndex].Rewrite.AppendedWALBytes
		}
		if err := sampleColdPhase(ctx, cfg, targets, rows, phaseIndex, result.Pairing); err != nil {
			return coldReport{}, fmt.Errorf("%s: %w", burst.name, err)
		}
		if err := os.RemoveAll(targets[0].path); err != nil {
			return coldReport{}, err
		}
	}
	result.Production = coldLayout{productionV1, targets[0].phases}
	for i, l := range layouts {
		result.Experimental = append(result.Experimental, coldLayout{l.name(), targets[i+1].phases})
	}
	if cfg.binary != "" {
		live, err := measureLive(ctx, cfg, dir, input, denominators)
		if err != nil {
			return coldReport{}, err
		}
		result.Live = live
	}
	if err := result.sealPairs(input); err != nil {
		return coldReport{}, err
	}
	return result, ctx.Err()
}

func prepareColdTargets(ctx context.Context, cfg config, dir string, burst backfill, rows []checkpoint.BlockRow, targets []coldTarget) error {
	productionDir := filepath.Join(dir, productionV1+"-"+burst.name)
	if err := os.Mkdir(productionDir, 0700); err != nil {
		return err
	}
	var baseline coldPhase
	req := componentRequest{productionDir, cfg.profile, burst.name, cfg.batches, cfg.fixtureSeed}
	if err := runQualificationWorker(ctx, workerRequest{ColdPrepare: &req}, &baseline); err != nil {
		return fmt.Errorf("%s: %w", productionV1, err)
	}
	if baseline.Phase != burst.name || baseline.Rows != len(rows) ||
		(burst.name != "initial" && (baseline.Rewrite == nil || baseline.Rewrite.AppendedWALBytes == 0)) {
		return errors.New("unexpected production publication returned by worker")
	}
	baseline.StoredBytes = new(baseline.storedBytes())
	targets[0].path, targets[0].production = productionDir, true
	targets[0].phases = append(targets[0].phases, baseline)
	for i, l := range layouts {
		target := &targets[i+1]
		layoutDir := filepath.Join(dir, l.name())
		pub, path, err := prepareColdExperimental(ctx, layoutDir, burst, rows, l, baseline.Rewrite)
		if err != nil {
			return fmt.Errorf("%s: %w", l.name(), err)
		}
		pub.StoredBytes = new(pub.storedBytes())
		target.path = path
		target.phases = append(target.phases, pub)
	}
	return nil
}

func prepareColdProduction(ctx context.Context, req componentRequest, out io.Writer) error {
	if req.Batches < 1 || req.Batches > maxBatches || (req.Profile != "mixed" && req.Profile != "entropy") {
		return errors.New("invalid cold fixture request")
	}
	input := fixture(req.Batches, req.Profile == "entropy", req.Seed)
	pub := coldPhase{Phase: req.Scenario, Rows: len(input.rows)}
	err := withSeededPublication(ctx, req.Dir, input, func(p publicationRun, initial *checkpoint.Manifest) error {
		if initial == nil || initial.Version != 1 || initial.NextOrdinal != uint64(pub.Rows) {
			return errors.New("initial production cutoff disagrees with fixture")
		}
		current := initial
		var previous *checkpoint.Manifest
		if req.Scenario != "initial" {
			found := false
			for _, burst := range input.backfills() {
				if burst.name != req.Scenario {
					continue
				}
				found = true
				pub.Rows += len(burst.batch)
				if err := p.append(burst.batch); err != nil {
					return err
				}
				next, sample, err := p.publish(ctx, initial, pub.Rows)
				if err != nil {
					return err
				}
				previous, current, pub.Rewrite = initial, next, &sample.WriteAmplification
			}
			if !found {
				return fmt.Errorf("unknown cold scenario %q", req.Scenario)
			}
		}
		info, err := os.Stat(filepath.Join(req.Dir, "bog.checkpoint"))
		if err != nil {
			return err
		}
		pub.segmentBytes = accountProduction(previous, current)
		pub.Segments, pub.ManifestBytes = len(current.Segments), info.Size()
		return nil
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(pub)
}

func prepareColdExperimental(ctx context.Context, dir string, burst backfill, rows []checkpoint.BlockRow, l layout, baseline *amplification) (_ coldPhase, _ string, err error) {
	var before snapshot
	appended := rows
	if burst.name == "initial" {
		if err := os.Mkdir(dir, 0700); err != nil {
			return coldPhase{}, "", err
		}
	} else {
		before, err = readSnapshot(filepath.Join(dir, "initial.json"))
		if err != nil {
			return coldPhase{}, "", err
		}
		appended = rows[len(rows)-len(burst.batch):]
	}
	path, err := publishSegments(ctx, dir, burst.name, before, appended, l, blockTarget)
	if err != nil {
		return coldPhase{}, "", err
	}
	handle, err := os.Open(dir)
	if err != nil {
		return coldPhase{}, "", err
	}
	if err := errors.Join(handle.Sync(), handle.Close()); err != nil {
		return coldPhase{}, "", err
	}
	next, err := readSnapshot(path)
	if err != nil {
		return coldPhase{}, "", err
	}
	account, err := accountSegments(before, next)
	if err != nil {
		return coldPhase{}, "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return coldPhase{}, "", err
	}
	pub := coldPhase{Phase: burst.name, Rows: len(rows), Segments: len(next.Segments), ManifestBytes: info.Size(), segmentBytes: account}
	if l.prune {
		pub.BloomBytes = int64(len(next.Segments)) * int64(len(summary{}))
	}
	if baseline != nil {
		amp := writeAmplification(uint64(pub.RewrittenBytes)+uint64(pub.ManifestBytes), baseline.AppendedWALBytes, coldRewriteBoundary)
		pub.Rewrite = &amp
	}
	return pub, path, nil
}

func sampleColdPhase(ctx context.Context, cfg config, targets []coldTarget, rows []checkpoint.BlockRow, phase int, pairing *coldPairing) error {
	for qi, q := range coldQueryPlan(coldMatrixSchema) {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := reference(rows, q)
		digest := coldRowsDigest(want)
		selectivity := measureColdSelectivity(rows, q, len(want))
		for i := range targets {
			pub := &targets[i].phases[phase]
			pub.Queries = append(pub.Queries, coldQueryReport{Query: q.name, Literal: q.literal, Start: q.start, End: q.end, Matches: len(want), Selectivity: &selectivity})
		}
		for sample := range coldQueryRuns(coldMatrixSchema, q, cfg.runs) {
			round := sample + 1
			pairID := pairing.pairID(coldPhases[phase], q, round)
			// Rotate the leading layout to reduce systematic sample-order effects.
			for offset := range targets {
				i := (phase + qi + sample + offset) % len(targets)
				target := &targets[i]
				req := coldRequest{Path: target.path, Query: q.name, Production: target.production}
				dst := &target.phases[phase].Queries[qi]
				if err := sampleColdQuery(ctx, cfg, req, want, dst); err != nil {
					return fmt.Errorf("%s %s sample %d: %w", target.name, q.name, round, err)
				}
				// The worker exited successfully and every row matched the
				// oracle stream, so its ordered result has this digest.
				dst.Samples[sample].Proof = &coldSampleProof{
					Round: round, PairID: pairID, Layout: target.name, RowsSHA256: digest,
				}
			}
		}
		for i := range targets {
			targets[i].phases[phase].Queries[qi].summarize()
		}
	}
	return nil
}

func sampleColdQuery(ctx context.Context, cfg config, req coldRequest, want []checkpoint.BlockRow, dst *coldQueryReport) error {
	arg, err := json.Marshal(workerRequest{Cold: &req})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	var sample coldSample
	_, err = runWorker(ctx, []string{workerFlag, string(arg)}, func(r io.Reader) error {
		return readQuerySample(r, want, &sample)
	})
	if err != nil {
		return err
	}
	if sample.Proof != nil || (sample.Scan == nil) != req.Production {
		return errors.New("invalid pairing or scan evidence in worker output")
	}
	dst.Samples = append(dst.Samples, sample)
	return nil
}

func coldWorker(ctx context.Context, req coldRequest, out io.Writer) error {
	for _, q := range coldQueryPlan(coldMatrixSchema) {
		if q.name == req.Query {
			if req.Production {
				return coldProductionQuery(ctx, req.Path, q, out)
			}
			return coldExperimentalQuery(ctx, req.Path, q, out)
		}
	}
	return fmt.Errorf("unknown cold query %q", req.Query)
}

func coldProductionQuery(ctx context.Context, dir string, q query, out io.Writer) (err error) {
	var catalog history.Catalog
	defer func() { err = errors.Join(err, catalog.Close()) }()
	if _, err := catalog.Replace(ctx, dir); err != nil {
		return err
	}
	reader, err := catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	// The catalog has already validated this manifest and every named segment.
	// The private trial directory is immutable for the duration of sampling.
	data, err := os.ReadFile(filepath.Join(dir, "bog.checkpoint"))
	if err != nil {
		return err
	}
	var envelope struct{ Payload checkpoint.Manifest }
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	for _, seg := range envelope.Payload.Segments {
		if err := adviseCold(ctx, filepath.Join(dir, seg.Name)); err != nil {
			return err
		}
	}
	return timeColdQuery(out, func() (rows []checkpoint.BlockRow, sample coldSample, err error) {
		err = reader.Scan(ctx, history.Query{Start: q.start, End: q.end, Contains: q.literal}, func(row checkpoint.BlockRow) error {
			rows = append(rows, row)
			return nil
		})
		return rows, sample, err
	})
}

func coldExperimentalQuery(ctx context.Context, path string, q query, out io.Writer) error {
	current, err := readSnapshot(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	for _, ref := range current.Segments {
		if !validSegmentRef(ref) {
			return errLayout
		}
		if err := adviseCold(ctx, filepath.Join(dir, ref.Name)); err != nil {
			return err
		}
	}
	return timeColdQuery(out, func() ([]checkpoint.BlockRow, coldSample, error) {
		return scanColdSnapshot(ctx, dir, current, q)
	})
}

func adviseCold(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	advice := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	if err := errors.Join(advice, f.Close()); err != nil {
		return fmt.Errorf("evict %s: %w", filepath.Base(path), err)
	}
	return nil
}

func timeColdQuery(out io.Writer, scan func() ([]checkpoint.BlockRow, coldSample, error)) error {
	var first, last runtime.MemStats
	runtime.ReadMemStats(&first)
	before, err := currentReadIO()
	if err != nil {
		return err
	}
	start := time.Now()
	rows, sample, err := scan()
	sample.ElapsedNS = time.Since(start).Nanoseconds()
	if err != nil {
		return err
	}
	after, err := currentReadIO()
	if err != nil {
		return err
	}
	sample.ReadBytes, sample.StorageReadBytes, err = after.delta(before)
	if err != nil {
		return err
	}
	runtime.ReadMemStats(&last)
	sample.GoAllocationBytes = last.TotalAlloc - first.TotalAlloc
	sample.PeakRSSBytes, err = selfPeakRSS()
	if err != nil {
		return err
	}
	return writeQuerySample(out, sample, rows)
}

func currentReadIO() (readIO, error) {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return readIO{}, err
	}
	return parseReadIO(data)
}
