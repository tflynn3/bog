package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
	"unsafe"

	"bog/internal/checkpoint"
)

var segmentTargets = [...]int{16 << 10, 64 << 10, 256 << 10}

type segmentRef struct {
	Name      string `json:"name"`
	FileBytes int64  `json:"file_bytes"`
	RawBytes  int    `json:"raw_bytes"`
}

type snapshot struct {
	Segments []segmentRef `json:"segments"`
}

type segmentBytes struct {
	ReusedBytes    int64 `json:"reused_bytes"`
	RewrittenBytes int64 `json:"rewritten_bytes"`
	RetainedBytes  int64 `json:"retained_bytes"`
	RetiredBytes   int64 `json:"retired_bytes"`
}

func accountSegments(before, after snapshot) (segmentBytes, error) {
	old := make(map[string]segmentRef, len(before.Segments))
	var sum segmentBytes
	for _, segment := range before.Segments {
		if old[segment.Name].Name != "" || !validSegmentRef(segment) {
			return segmentBytes{}, errLayout
		}
		old[segment.Name] = segment
		sum.RetiredBytes += segment.FileBytes
	}
	seen := make(map[string]bool, len(after.Segments))
	for _, segment := range after.Segments {
		if seen[segment.Name] || !validSegmentRef(segment) {
			return segmentBytes{}, errLayout
		}
		seen[segment.Name] = true
		sum.RetainedBytes += segment.FileBytes
		if previous, ok := old[segment.Name]; ok {
			if previous != segment {
				return segmentBytes{}, fmt.Errorf("carried segment %s changed metadata", segment.Name)
			}
			sum.ReusedBytes += segment.FileBytes
			sum.RetiredBytes -= segment.FileBytes
		} else {
			sum.RewrittenBytes += segment.FileBytes
		}
	}
	return sum, nil
}

func validSegmentRef(ref segmentRef) bool {
	return filepath.IsLocal(ref.Name) && filepath.Base(ref.Name) == ref.Name &&
		strings.HasSuffix(ref.Name, ".segment") &&
		ref.RawBytes >= blockHeader+blockRowWidth && ref.RawBytes <= maxBlock &&
		ref.FileBytes > headerSize+entrySize && ref.FileBytes <= int64(headerSize+entrySize+ref.RawBytes)
}

const blockRowWidth = 24

func encodedRowBytes(row checkpoint.BlockRow) int {
	return blockRowWidth + len(row.Row.Line)
}

func rawBlockBytes(rows []checkpoint.BlockRow) int {
	size := blockHeader
	for _, row := range rows {
		size += encodedRowBytes(row)
	}
	return size
}

// A publication keeps every sealed segment and repacks only the previous tail
// and new input. The predecessor remains readable until its readers finish.
func publishSegments(ctx context.Context, dir, id string, previous snapshot, appended []checkpoint.BlockRow, l layout, target int) (_ string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	keep := previous.Segments
	rows := appended
	if len(keep) > 0 && len(appended) > 0 && keep[len(keep)-1].RawBytes+encodedRowBytes(appended[0]) <= target {
		var decoder blockDecoder
		defer decoder.close()
		tail := keep[len(keep)-1]
		if _, err := decodeSegment(dir, tail, target, &decoder, func(decoded []checkpoint.BlockRow) {
			rows = append(decoded, appended...)
		}); err != nil {
			return "", err
		}
		slices.SortFunc(rows, func(a, b checkpoint.BlockRow) int { return cmp.Compare(a.Ordinal, b.Ordinal) })
		keep = keep[:len(keep)-1]
	}
	next := snapshot{Segments: slices.Clone(keep)}
	var encoder blockEncoder
	defer func() { err = errors.Join(err, encoder.close()) }()
	for i, block := range partitionAt(rows, target) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		name := fmt.Sprintf("%s-%06d.segment", id, i)
		size, err := writeGenerationUsing(filepath.Join(dir, name), [][]checkpoint.BlockRow{block}, l, &encoder)
		if err != nil {
			return "", fmt.Errorf("write %s: %w", name, err)
		}
		next.Segments = append(next.Segments, segmentRef{name, size, rawBlockBytes(block)})
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path := filepath.Join(dir, id+".json")
	return path, writeSnapshot(path, next)
}

func writeSnapshot(path string, next snapshot) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := json.NewEncoder(f).Encode(next); err != nil {
		return err
	}
	return f.Sync()
}

func readSnapshot(path string) (snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return snapshot{}, err
	}
	var current snapshot
	if err := json.Unmarshal(data, &current); err != nil {
		return snapshot{}, err
	}
	if len(current.Segments) == 0 {
		return snapshot{}, errLayout
	}
	return current, nil
}

const decodedRowBytes = int(unsafe.Sizeof(checkpoint.BlockRow{}))

// Each row consumes 24 wire bytes plus its line. Charge the larger of
// one decoded row per 24 bytes and one copied line byte per wire byte.
func decodeBound(rawBytes int) int {
	payload := rawBytes - blockHeader
	return 2*rawBytes + payload + (payload/blockRowWidth)*max(0, decodedRowBytes-blockRowWidth)
}

func oversizedDecodeBound(rawBytes int) int {
	return 2*rawBytes + decodedRowBytes + rawBytes - blockHeader - blockRowWidth
}

type decodeWorkingSet struct {
	PeakWorkingBytes   int `json:"peak_working_bytes"`
	WorkingBoundBytes  int `json:"working_bound_bytes"`
	PeakOrdinaryBytes  int `json:"peak_ordinary_working_bytes"`
	OrdinaryBoundBytes int `json:"ordinary_working_bound_bytes"`
	PeakOversizedBytes int `json:"peak_oversized_working_bytes"`
	OversizedSegments  int `json:"oversized_segments"`
}

func decodeSegment(dir string, ref segmentRef, target int, decoder *blockDecoder, visit func([]checkpoint.BlockRow)) (peak int, err error) {
	if !validSegmentRef(ref) {
		return 0, errLayout
	}
	f, err := os.Open(filepath.Join(dir, ref.Name))
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() != ref.FileBytes || !info.Mode().IsRegular() {
		return 0, errLayout
	}
	reads, err := decoder.scan(f, ref.FileBytes, ref.RawBytes, true, query{start: firstTime, end: endTime}, func(raw []byte, buffers int) error {
		if len(raw) != ref.RawBytes {
			return errLayout
		}
		bound := decodeBound(len(raw))
		if len(raw) > target {
			if le.Uint32(raw[4:8]) != 1 {
				return fmt.Errorf("oversized block is not a singleton: %w", errLayout)
			}
			bound = oversizedDecodeBound(len(raw))
		}
		rows, err := checkpoint.DecodeBlock(raw)
		if err != nil {
			return err
		}
		peak = buffers + len(rows)*decodedRowBytes + len(raw) - blockHeader - blockRowWidth*len(rows)
		if peak > bound {
			return fmt.Errorf("decode working set %d exceeds bound %d", peak, bound)
		}
		if visit != nil {
			visit(rows)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if reads.Blocks != 1 {
		return 0, errLayout
	}
	return peak, nil
}

func scanSnapshot(ctx context.Context, dir string, current snapshot, target int, visit func([]checkpoint.BlockRow)) (decodeWorkingSet, error) {
	stats := decodeWorkingSet{OrdinaryBoundBytes: decodeBound(target), WorkingBoundBytes: decodeBound(target)}
	var decoder blockDecoder
	defer decoder.close()
	for _, ref := range current.Segments {
		if err := ctx.Err(); err != nil {
			return decodeWorkingSet{}, err
		}
		peak, err := decodeSegment(dir, ref, target, &decoder, visit)
		if err != nil {
			return decodeWorkingSet{}, fmt.Errorf("decode %s: %w", ref.Name, err)
		}
		stats.PeakWorkingBytes = max(stats.PeakWorkingBytes, peak)
		if ref.RawBytes > target {
			stats.OversizedSegments++
			stats.PeakOversizedBytes = max(stats.PeakOversizedBytes, peak)
			stats.WorkingBoundBytes = max(stats.WorkingBoundBytes, oversizedDecodeBound(ref.RawBytes))
		} else {
			stats.PeakOrdinaryBytes = max(stats.PeakOrdinaryBytes, peak)
		}
	}
	return stats, ctx.Err()
}

func querySnapshot(ctx context.Context, dir string, current snapshot, target int, q query) ([]checkpoint.BlockRow, error) {
	var matches []checkpoint.BlockRow
	_, err := scanSnapshot(ctx, dir, current, target, func(rows []checkpoint.BlockRow) {
		for _, row := range rows {
			if !row.Row.TS.Before(q.start) && row.Row.TS.Before(q.end) && strings.Contains(row.Row.Line, q.literal) {
				matches = append(matches, row)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(matches, ordered)
	return matches, ctx.Err()
}

func checkSnapshotRows(got, want []checkpoint.BlockRow) error {
	if len(got) != len(want) {
		return fmt.Errorf("oracle: %d rows, want %d", len(got), len(want))
	}
	for i, row := range got {
		if !row.Row.TS.Equal(want[i].Row.TS) || row.Ordinal != want[i].Ordinal || row.Row.Line != want[i].Row.Line {
			return fmt.Errorf("oracle: row %d differs in timestamp, append ordinal or line", i)
		}
	}
	return nil
}

func removeTrial(dir, manifest string, before, after snapshot) error {
	if _, err := accountSegments(before, after); err != nil {
		return err
	}
	old := make(map[string]bool, len(before.Segments))
	for _, ref := range before.Segments {
		old[ref.Name] = true
	}
	err := os.Remove(manifest)
	for _, ref := range after.Segments {
		if !old[ref.Name] {
			err = errors.Join(err, os.Remove(filepath.Join(dir, ref.Name)))
		}
	}
	return err
}

type compactionLatency struct {
	Query        string  `json:"query"`
	MatchingRows int     `json:"matching_rows"`
	SamplesNS    []int64 `json:"samples_ns"`
	OverlapNS    []int64 `json:"overlap_ns"`
	P95NS        int64   `json:"p95_ns"`
}

func (m *compactionLatency) add(elapsed, overlap time.Duration) error {
	if elapsed <= 0 || overlap <= 0 || elapsed < overlap {
		return fmt.Errorf("invalid compaction overlap %s for query sample %s", overlap, elapsed)
	}
	m.SamplesNS = append(m.SamplesNS, elapsed.Nanoseconds())
	m.OverlapNS = append(m.OverlapNS, overlap.Nanoseconds())
	return nil
}

func compactWithQuery(ctx context.Context, q func() error, compact func() (string, error)) (string, time.Duration, time.Duration, error) {
	type result struct {
		path       string
		start, end time.Time
		err        error
	}
	start := make(chan struct{})
	started := make(chan struct{})
	done := make(chan result, 1)
	go func() {
		<-start
		began := time.Now()
		close(started)
		path, err := compact()
		done <- result{path, began, time.Now(), err}
	}()
	queryStart := time.Now()
	close(start)
	<-started
	queryErr := q()
	queryEnd := time.Now()
	written := <-done
	overlapEnd := written.end
	if queryEnd.Before(overlapEnd) {
		overlapEnd = queryEnd
	}
	return written.path, queryEnd.Sub(queryStart), overlapEnd.Sub(written.start),
		errors.Join(queryErr, written.err, ctx.Err())
}

type decodeSample struct {
	decodeWorkingSet
	AllocationBytes uint64 `json:"allocation_bytes"`
	Allocations     uint64 `json:"allocations"`
	PeakRSSBytes    uint64 `json:"peak_rss_bytes"`
}

func collectDecode(ctx context.Context, dir string, current snapshot, target int) (decodeSample, error) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	stats, err := scanSnapshot(ctx, dir, current, target, nil)
	runtime.ReadMemStats(&after)
	if err != nil {
		return decodeSample{}, err
	}
	return decodeSample{
		decodeWorkingSet: stats, AllocationBytes: after.TotalAlloc - before.TotalAlloc,
		Allocations: after.Mallocs - before.Mallocs,
	}, nil
}
