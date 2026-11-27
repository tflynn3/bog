package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

type compactionReport struct {
	Component []publicationMeasurement `json:"checkpoint_publication_component"`
	Live      liveReport               `json:"live_server"`
}

type componentRequest struct {
	Dir, Profile, Scenario string
	Batches                int
	Seed                   uint64
}

func measureCompaction(ctx context.Context, cfg config, dir string, input fixtureInput) (compactionReport, map[string]uint64, error) {
	result := compactionReport{Live: liveReport{Status: "unavailable", Reason: "no pinned binary supplied"}}
	denominators := make(map[string]uint64)
	for _, burst := range input.backfills() {
		target := filepath.Join(dir, "component-"+burst.name)
		if err := os.Mkdir(target, 0700); err != nil {
			return result, nil, err
		}
		arg, err := json.Marshal(workerRequest{Component: &componentRequest{target, cfg.profile, burst.name, cfg.batches, cfg.fixtureSeed}})
		if err != nil {
			return result, nil, err
		}
		var sample publicationMeasurement
		sampleCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		_, err = runWorker(sampleCtx, []string{workerFlag, string(arg)}, func(r io.Reader) error {
			dec := json.NewDecoder(r)
			if err := dec.Decode(&sample); err != nil {
				return err
			}
			if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
				return fmt.Errorf("unexpected component worker output after result: %v", err)
			}
			return nil
		})
		cancel()
		if err != nil {
			return result, nil, fmt.Errorf("%s checkpoint component: %w", burst.name, err)
		}
		if sample.Scenario != burst.name || sample.CoveredRows != len(input.rows)+len(burst.batch) ||
			sample.PeakRSSBytes == 0 || sample.WriteAmplification.AppendedWALBytes == 0 {
			return result, nil, errors.New("invalid component worker result")
		}
		result.Component = append(result.Component, sample)
		denominators[burst.name] = sample.WriteAmplification.AppendedWALBytes
		if err := os.RemoveAll(target); err != nil {
			return result, nil, err
		}
	}
	if cfg.binary != "" {
		live, err := measureLive(ctx, cfg, dir, input, denominators)
		if err != nil {
			return result, nil, err
		}
		result.Live = live
	}
	return result, denominators, nil
}

// Keep ordinary batch boundaries and put the oversized row and sentinels in their own frames.
func (input fixtureInput) batches() iter.Seq[[]checkpoint.BlockRow] {
	return func(yield func([]checkpoint.BlockRow) bool) {
		if !yield(input.rows[:1]) {
			return
		}
		for b, anchor := range input.anchors {
			start := int(anchor.Ordinal)
			end := start + batchPattern[b%len(batchPattern)]
			if !yield(input.rows[start:end]) {
				return
			}
			if b == len(input.anchors)/2 && !yield(input.rows[end:end+1]) {
				return
			}
		}
		for i := len(input.rows) - 3; i < len(input.rows); i++ {
			if !yield(input.rows[i : i+1]) {
				return
			}
		}
	}
}

func encodeBatch(batch []checkpoint.BlockRow) ([]byte, []store.Row, error) {
	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	rows := make([]store.Row, len(batch))
	for i, row := range batch {
		rows[i] = row.Row
		if err := enc.Encode(row.Row); err != nil {
			return nil, nil, err
		}
	}
	// Omit the final newline to fit the maximum-sized singleton frame.
	return bytes.TrimSuffix(payload.Bytes(), []byte{'\n'}), rows, nil
}

type publicationRun struct {
	log       *wal.Log
	publisher *checkpoint.Publisher
}

func withPublication(dir string, run func(publicationRun, *os.File) error) (err error) {
	log, err := wal.Open(dir, &store.Store{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, log.Close()) }()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	handle, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, handle.Close()) }()
	p := publicationRun{log, checkpoint.NewPublisher(root, handle, checkpoint.NewBudget(checkpoint.Limits{checkpoint.StagingBytes: ^uint64(0)}))}
	return run(p, handle)
}

func withSeededPublication(ctx context.Context, dir string, input fixtureInput, run func(publicationRun, *checkpoint.Manifest) error) error {
	return withPublication(dir, func(p publicationRun, _ *os.File) error {
		if err := p.seed(ctx, input); err != nil {
			return err
		}
		initial, _, err := p.publish(ctx, nil, len(input.rows))
		if err != nil || run == nil {
			return err
		}
		return run(p, initial)
	})
}

func (p publicationRun) seed(ctx context.Context, input fixtureInput) error {
	for batch := range input.batches() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.append(batch); err != nil {
			return err
		}
	}
	return nil
}

func (p publicationRun) append(batch []checkpoint.BlockRow) error {
	body, rows, err := encodeBatch(batch)
	if err != nil {
		return err
	}
	return p.log.Append(body, rows)
}

func (p publicationRun) publish(ctx context.Context, previous *checkpoint.Manifest, count int) (_ *checkpoint.Manifest, sample publicationMeasurement, err error) {
	capture, err := p.log.Capture(ctx)
	if err != nil {
		return nil, sample, err
	}
	defer func() { err = errors.Join(err, capture.Release()) }()
	if capture.NextOrdinal() != uint64(count) {
		return nil, sample, fmt.Errorf("captured %d rows, want %d", capture.NextOrdinal(), count)
	}
	hash := sha256.New()
	end := capture.CommittedEnd()
	if _, err := io.CopyN(hash, io.NewSectionReader(capture, 0, end), end); err != nil {
		return nil, sample, err
	}
	source := fmt.Sprintf("%x", hash.Sum(nil))
	if previous == nil {
		m, err := p.publisher.PublishCaptured(ctx, capture, end, nil, uint64(count), source)
		return m, sample, err
	}
	if end <= previous.CoveredOffset {
		return nil, sample, errors.New("checkpoint replacement requires a positive WAL suffix")
	}
	before, err := processIO("self", "wchar")
	if err != nil {
		return nil, sample, err
	}
	start := time.Now()
	m, err := p.publisher.PublishCaptured(ctx, capture, end, previous, uint64(count), source)
	elapsed := time.Since(start)
	if err != nil {
		return nil, sample, err
	}
	after, err := processIO("self", "wchar")
	if err != nil {
		return nil, sample, err
	}
	if after <= before || m.NextOrdinal != uint64(count) || m.CoveredOffset != end {
		return nil, sample, errors.New("invalid checkpoint publication counters or cutoff")
	}
	sample.CoveredRows, sample.ElapsedNS = count, elapsed.Nanoseconds()
	sample.WriteAmplification = writeAmplification(after-before, uint64(end-previous.CoveredOffset), componentBoundary)
	return m, sample, nil
}

func measureComponent(ctx context.Context, req componentRequest) (publicationMeasurement, error) {
	input := fixture(req.Batches, req.Profile == "entropy", req.Seed)
	for _, burst := range input.backfills() {
		if req.Scenario != burst.name {
			continue
		}
		rows := input.withBackfill(burst)
		var sample publicationMeasurement
		err := withSeededPublication(ctx, req.Dir, input, func(p publicationRun, previous *checkpoint.Manifest) error {
			if err := p.append(burst.batch); err != nil {
				return err
			}
			var err error
			_, sample, err = p.publish(ctx, previous, len(rows))
			return err
		})
		sample.Scenario = burst.name
		if err != nil {
			return sample, err
		}
		return sample, verifyPublished(req.Dir, rows)
	}
	return publicationMeasurement{}, fmt.Errorf("unknown component scenario %q", req.Scenario)
}

func verifyPublished(dir string, rows []checkpoint.BlockRow) (err error) {
	legacy, err := checkpoint.OpenLegacy(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, legacy.Close()) }()
	p, err := legacy.OpenPublished()
	if err != nil {
		return err
	}
	if p.Manifest().NextOrdinal != uint64(len(rows)) {
		return errors.New("published cutoff does not cover the fixture")
	}
	q := query{start: firstTime, end: endTime}
	want := reference(rows, q)
	i := 0
	if err := p.Scan(func(row checkpoint.BlockRow) error {
		if i >= len(want) {
			return errors.New("extra published row")
		}
		expected := want[i]
		if row.Ordinal != expected.Ordinal || !row.Row.TS.Equal(expected.Row.TS) || row.Row.Line != expected.Row.Line {
			return fmt.Errorf("published row %d disagrees with exact oracle", i)
		}
		i++
		return nil
	}); err != nil {
		return err
	}
	if i != len(want) {
		return fmt.Errorf("published %d rows, want %d", i, len(want))
	}
	return nil
}

func processIO(pid, field string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join("/proc", pid, "io"))
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(data)) {
		key, value, ok := strings.Cut(line, ":")
		if ok && key == field {
			return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		}
	}
	return 0, fmt.Errorf("missing /proc/%s/io counter %s", pid, field)
}
