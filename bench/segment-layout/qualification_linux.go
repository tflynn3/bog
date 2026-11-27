package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
)

type qualificationState struct {
	req      qualificationRequest
	run      publicationRun
	dir      *os.File
	catalog  history.Catalog
	baseline *checkpoint.Manifest
	snapshot snapshot
	end      int64
}

func qualificationWorker(ctx context.Context, req qualificationRequest, out io.Writer) error {
	if req.Batches < 1 || req.Batches > maxBatches || req.Timeout <= 0 ||
		(req.Target != 0 && (!slices.Contains(segmentTargets[:], req.Target) || req.Codec > zstdCodec)) ||
		(req.Query != "sparse" && req.Query != "dense") {
		return errors.New("invalid qualification worker request")
	}
	input := fixture(req.Batches, req.Profile == "entropy", req.Seed)
	results, err := runQualification(ctx, req, input)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(results)
}

func runQualification(ctx context.Context, req qualificationRequest, input fixtureInput) (results []publicationResult, err error) {
	literal := sparseLiteral
	if req.Query == "dense" {
		literal = denseLiteral
	}
	q := query{req.Query, literal, firstTime, endTime}
	phases := append([]backfill{{"initial", input.rows}}, input.successiveAppends()...)
	expected := make([]checkpoint.BlockRow, 0, len(input.rows)+12)
	err = withPublication(req.Dir, func(p publicationRun, handle *os.File) (err error) {
		state := qualificationState{req: req, run: p, dir: handle, end: int64(len("BOGWAL01"))}
		defer func() { err = errors.Join(err, state.catalog.Close()) }()
		for i, phase := range phases {
			stepCtx, cancel := context.WithTimeout(ctx, req.Timeout)
			pub, err := state.step(stepCtx, input, phase, i, expected, q)
			cancel()
			if err != nil {
				return fmt.Errorf("%s: %w", phase.name, err)
			}
			if i > 0 && pub.Queries[0].Samples[0].OverlapNS == 0 {
				results = nil
				return nil
			}
			results = append(results, pub)
			expected = append(expected, phase.batch...)
		}
		return nil
	})
	return results, err
}

func (s *qualificationState) step(ctx context.Context, input fixtureInput, phase backfill, index int, prior []checkpoint.BlockRow, q query) (pub publicationResult, err error) {
	covered := len(prior) + len(phase.batch)
	if index == 0 {
		err = s.run.seed(ctx, input)
	} else {
		err = s.run.append(phase.batch)
	}
	if err != nil {
		return pub, err
	}
	capture, err := s.run.log.Capture(ctx)
	if err != nil {
		return pub, err
	}
	defer func() { err = errors.Join(err, capture.Release()) }()
	end := capture.CommittedEnd()
	if end <= s.end || capture.NextOrdinal() != uint64(covered) {
		return pub, errors.New("WAL capture disagrees with fixture cutoff")
	}
	var next *checkpoint.Manifest
	var path string
	var publish func() error
	// Both paths touch the same committed WAL prefix before timing.
	hash := sha256.New()
	if _, err := io.CopyN(hash, io.NewSectionReader(capture, 0, end), end); err != nil {
		return pub, err
	}
	if s.req.Target == 0 {
		source := fmt.Sprintf("%x", hash.Sum(nil))
		publish = func() error {
			var err error
			next, err = s.run.publisher.PublishCaptured(ctx, capture, end, s.baseline, uint64(covered), source)
			return err
		}
	} else {
		publish = func() error {
			var err error
			path, err = publishSegments(ctx, s.req.Dir, fmt.Sprintf("%02d", index), s.snapshot, phase.batch,
				layout{codec: s.req.Codec}, s.req.Target)
			if err != nil {
				return err
			}
			return s.dir.Sync()
		}
	}
	var got []checkpoint.BlockRow
	var scan func() error
	read := func() error {
		var err error
		got, err = s.scan(ctx, q)
		return err
	}
	if index > 0 {
		scan = read
	}
	sample, err := timeQualification(ctx, uint64(end-s.end), publish, scan)
	if err != nil {
		return pub, err
	}
	pub = publicationResult{Phase: phase.name, Rows: covered, AppendedRows: len(phase.batch)}
	if err := s.install(ctx, &pub, next, end, path); err != nil {
		return pub, err
	}
	expected := append(prior, phase.batch...)
	all := query{start: firstTime, end: endTime}
	rows, err := s.scan(ctx, all)
	if err != nil {
		return pub, err
	}
	if err := checkSnapshotRows(rows, reference(expected, all)); err != nil {
		return pub, fmt.Errorf("verify published generation: %w", err)
	}
	s.end = end
	queryRows := prior
	if index == 0 {
		queryRows = expected
		start := time.Now()
		if err := read(); err != nil {
			return pub, err
		}
		sample.QueryNS = time.Since(start).Nanoseconds()
		sample.PeakRSSBytes, err = selfPeakRSS()
		if err != nil {
			return pub, err
		}
	}
	want := reference(queryRows, q)
	if err := checkSnapshotRows(got, want); err != nil {
		return pub, fmt.Errorf("%s timed scan: %w", q.name, err)
	}
	pub.Queries = []qualificationScan{{Query: q.name, MatchingRows: len(want), Samples: []qualificationSample{sample}}}
	return pub, nil
}

func (s *qualificationState) scan(ctx context.Context, q query) (rows []checkpoint.BlockRow, err error) {
	if s.req.Target != 0 {
		return querySnapshot(ctx, s.req.Dir, s.snapshot, s.req.Target, q)
	}
	reader, err := s.catalog.AcquireCurrentUntil(ctx, time.Time{})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	err = reader.Scan(ctx, history.Query{Start: q.start, End: q.end, Contains: q.literal}, func(row checkpoint.BlockRow) error {
		rows = append(rows, row)
		return nil
	})
	return rows, err
}

func (s *qualificationState) install(ctx context.Context, pub *publicationResult, next *checkpoint.Manifest, end int64, path string) error {
	if s.req.Target == 0 {
		if next == nil || next.Version != 1 || next.NextOrdinal != uint64(pub.Rows) || next.CoveredOffset != end {
			return errors.New("production manifest disagrees with fixture cutoff")
		}
		if _, err := s.catalog.ReplacePinned(ctx, s.dir); err != nil {
			return err
		}
		pub.segmentBytes = accountProduction(s.baseline, next)
		pub.Segments = len(next.Segments)
		s.baseline = next
		path = filepath.Join(s.req.Dir, "bog.checkpoint")
	} else {
		current, err := readSnapshot(path)
		if err != nil {
			return err
		}
		pub.segmentBytes, err = accountSegments(s.snapshot, current)
		if err != nil {
			return err
		}
		pub.Segments, s.snapshot = len(current.Segments), current
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	pub.ManifestBytes = info.Size()
	return nil
}

func accountProduction(before, after *checkpoint.Manifest) segmentBytes {
	var bytes segmentBytes
	old := make(map[string]checkpoint.Segment)
	if before != nil {
		for _, segment := range before.Segments {
			old[segment.Name] = segment
			bytes.RetiredBytes += segment.Size
		}
	}
	for _, segment := range after.Segments {
		bytes.RetainedBytes += segment.Size
		if old[segment.Name] == segment {
			bytes.ReusedBytes += segment.Size
			bytes.RetiredBytes -= segment.Size
		} else {
			bytes.RewrittenBytes += segment.Size
		}
	}
	return bytes
}

func timeQualification(ctx context.Context, appended uint64, publish, scan func() error) (qualificationSample, error) {
	before, err := processIO("self", "write_bytes")
	if err != nil {
		return qualificationSample{}, err
	}
	var queryTime, publicationTime, overlap time.Duration
	if scan != nil {
		queryTime, publicationTime, overlap, err = compactWithQuery(ctx, scan, publish)
	} else {
		start := time.Now()
		err = publish()
		publicationTime = time.Since(start)
		err = errors.Join(err, ctx.Err())
	}
	if err != nil {
		return qualificationSample{}, err
	}
	after, err := processIO("self", "write_bytes")
	if err != nil {
		return qualificationSample{}, err
	}
	if after <= before {
		return qualificationSample{}, errors.New("publication charged no Linux write_bytes; choose a disk-backed work directory")
	}
	rss, err := selfPeakRSS()
	if err != nil {
		return qualificationSample{}, err
	}
	return qualificationSample{
		QueryNS: queryTime.Nanoseconds(), PublicationNS: publicationTime.Nanoseconds(),
		OverlapNS: overlap.Nanoseconds(), PeakRSSBytes: rss,
		WriteAmplification: writeAmplification(after-before, appended, physicalBoundary),
	}, nil
}
