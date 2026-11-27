package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

func (f fixture) slot(ordinal int) int {
	group := ordinal / 4
	ordered := f.SeedRows / 4 * 3 / 4
	if group < ordered {
		return group * 2
	}
	if ordinal < f.SeedRows {
		return (f.SeedRows/4-group-1)*2 + 1
	}
	tailGroup := group - f.SeedRows/4
	if f.scatteredTail {
		// Distribute tied groups through the broad time window, with
		// consecutive groups alternating between its two halves.
		groups := (f.TailRows + 3) / 4
		position := tailGroup/2 + (tailGroup%2)*((groups+1)/2)
		return 1 + position*(f.SeedRows/4-1)/groups
	}
	return tailGroup % (ordered * 2)
}

func (f fixture) row(ordinal int) row {
	key := ordinal
	if ordinal%4 == 1 {
		key--
	}
	literal := [...]string{"needle .*", "needle .*", "needle .*", "Needle .*"}[key%4]
	prefix := fmt.Sprintf("%s %08d ", literal, key)
	return row{epoch.Add(time.Duration(f.slot(ordinal)) * time.Millisecond), prefix + strings.Repeat("x", f.LineBytes-len(prefix))}
}

func (f fixture) batch(lo, hi int) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for i := lo; i < hi; i++ {
		if err := enc.Encode(f.row(i)); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func (f fixture) newWalk(cutoff int, broad bool) *walk {
	q := filter{epoch, epoch.Add(time.Duration(f.SeedRows) * time.Millisecond), ""}
	if broad {
		q.start = q.start.Add(time.Millisecond)
		q.end = epoch.Add(time.Duration(f.SeedRows/4) * time.Millisecond)
		q.literal = "needle .*"
	}
	return f.walkFor(cutoff, q)
}

func (f fixture) walkFor(cutoff int, q filter) *walk {
	var want []int
	for i := 0; i < cutoff; i++ {
		ts := epoch.Add(time.Duration(f.slot(i)) * time.Millisecond)
		if !ts.Before(q.start) && ts.Before(q.end) && (q.literal == "" || i%4 != 3) {
			want = append(want, i)
		}
	}
	slices.SortFunc(want, func(a, b int) int {
		return cmp.Or(cmp.Compare(f.slot(a), f.slot(b)), cmp.Compare(a, b))
	})
	return &walk{filter: q, want: want, seen: make(map[string]bool)}
}

func p99(samples []time.Duration) *time.Duration {
	if len(samples) == 0 {
		return nil
	}
	slices.Sort(samples)
	k := len(samples) - len(samples)/100
	return &samples[k-1]
}

type ingestSample struct {
	latency time.Duration
	ack     time.Time
}

func (f fixture) appendRange(ctx context.Context, s service, lo, hi int, samples *[]ingestSample) error {
	for i := lo; i < hi; i += f.BatchRows {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := f.batch(i, min(i+f.BatchRows, hi))
		if err != nil {
			return err
		}
		d, err := s.ingest(ctx, b)
		if err != nil {
			return fmt.Errorf("append at ordinal %d: %w", i, err)
		}
		if samples != nil {
			*samples = append(*samples, ingestSample{d, time.Now()})
		}
	}
	return nil
}

func (f fixture) load(ctx context.Context, s service, o options, r *result, dir string) error {
	live := o.mode == "live"
	var clustered, scattered *backfillTimeline
	seedEnd := f.SeedRows
	if live {
		if r.Diagnostics == nil {
			r.Diagnostics = &capacityDiagnostics{}
		}
		if len(r.Diagnostics.Backfills) == 0 {
			r.Diagnostics.Backfills = newBackfillTimelines(f)
		}
		clustered, scattered = r.Diagnostics.Backfills[0], r.Diagnostics.Backfills[1]
		seedEnd = clustered.HeldCutoff
		r.Phase = "live_capability"
		if _, err := s.compact(ctx, o.control, "status", 0); err != nil {
			return err
		}
	}
	r.Phase = "seed"
	// Bound the uncovered WAL tail while preparing the retained prefix.
	const prepareRows = 8192
	var generation uint64
	for lo := 0; lo < seedEnd; lo += prepareRows {
		hi := min(lo+prepareRows, seedEnd)
		if err := f.appendRange(ctx, s, lo, hi, nil); err != nil {
			return err
		}
		if live {
			var err error
			generation, err = s.prepareCheckpoint(ctx, o.control, hi, generation)
			if err != nil {
				return err
			}
		}
	}
	if !live {
		return f.legacyMixed(ctx, s, r)
	}

	// These final seed rows are reverse backfills in adjacent timestamp
	// slots. A narrow walk keeps the extra proof independent of seed size.
	first, last := f.row(clustered.HeldCutoff).TS, f.row(clustered.FinalCutoff-1).TS
	window := filter{
		start:   last,
		end:     first.Add(7 * time.Millisecond),
		literal: "needle .*",
	}
	clusteredWalk := f.walkFor(clustered.HeldCutoff, window)
	clusteredWalk.firstResumeLimit = 1
	c, err := f.runBackfill(ctx, s, o, r, dir, clustered, clusteredWalk, generation)
	if err != nil {
		return err
	}
	// Restore the established seed boundary before the release proof.
	r.Phase = "seed_checkpoint"
	generation, err = s.prepareCheckpoint(ctx, o.control, f.SeedRows, c.Generation)
	if err != nil {
		return err
	}
	_, err = f.runBackfill(ctx, s, o, r, dir, scattered, f.newWalk(f.SeedRows, true), generation)
	return err
}

func (s service) prepareCheckpoint(ctx context.Context, path string, cutoff int, previous uint64) (uint64, error) {
	if _, err := s.compact(ctx, path, "start", cutoff); err != nil {
		return 0, err
	}
	c, err := s.waitCompaction(ctx, path, cutoff, previous)
	if err != nil {
		return 0, err
	}
	if c.CommittedRows != cutoff {
		return 0, fmt.Errorf("unexpected committed rows during fixture preparation: %+v", c)
	}
	return c.Generation, nil
}

func (f fixture) legacyMixed(ctx context.Context, s service, r *result) error {
	r.Phase = "mixed_load"
	w := f.newWalk(f.SeedRows, true)
	w.phase = "seed_capture"
	if _, err := w.page(ctx, s, f); err != nil {
		return fmt.Errorf("capture seed cutoff: %w", err)
	}
	var writes []ingestSample
	var reads []time.Duration
	defer func() { r.recordMixed(writes, reads, time.Time{}) }()
	first := f.SeedRows + f.BatchRows
	if err := f.appendRange(ctx, s, f.SeedRows, first, &writes); err != nil {
		return err
	}
	mixed, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := make(chan error, 1)
	go func() {
		err := f.appendRange(mixed, s, first, f.SeedRows+f.TailRows, &writes)
		if err != nil {
			cancel()
		}
		writer <- err
	}()
	w.phase = "mixed_continuation"
	var walkErr error
	for w.cursor != "" {
		d, err := w.page(mixed, s, f)
		if err != nil {
			walkErr = fmt.Errorf("walk captured cutoff %d: %w", f.SeedRows, err)
			cancel()
			break
		}
		reads = append(reads, d)
	}
	return errors.Join(<-writer, walkErr)
}

func (r *result) recordMixed(writes []ingestSample, reads []time.Duration, runningThrough time.Time) {
	r.IngestSamples, r.QuerySamples = len(writes), len(reads)
	all := make([]time.Duration, 0, len(writes))
	var overlapping []time.Duration
	for i, sample := range writes {
		all = append(all, sample.latency)
		// The first batch precedes compaction start.
		if i > 0 && !sample.ack.After(runningThrough) {
			overlapping = append(overlapping, sample.latency)
		}
	}
	r.IngestP99, r.QueryP99 = p99(all), p99(reads)
	if r.Diagnostics != nil {
		r.Diagnostics.RunningSamples = len(overlapping)
		r.Diagnostics.RunningIngestP99 = p99(overlapping)
	}
}

func (f fixture) recover(ctx context.Context, s service) (int, error) {
	w := f.newWalk(f.SeedRows+f.TailRows, false)
	w.phase = "restart_walk"
	if f.scatteredTail {
		w.pageLimit = 1000
	}
	_, err := w.exhaust(ctx, s, f)
	return w.offset, err
}
