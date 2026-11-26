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
	return (group - f.SeedRows/4) % (ordered * 2)
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
	var want []int
	for i := 0; i < cutoff; i++ {
		ts := epoch.Add(time.Duration(f.slot(i)) * time.Millisecond)
		if !ts.Before(q.start) && ts.Before(q.end) && (!broad || i%4 != 3) {
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

func (f fixture) appendRange(ctx context.Context, s service, lo, hi int, samples *[]time.Duration) error {
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
			*samples = append(*samples, d)
		}
	}
	return nil
}

func (f fixture) load(ctx context.Context, s service, o options, r *result) error {
	if o.mode == "live" {
		r.Phase = "live_capability"
		if _, err := s.compact(ctx, o.control, "status", 0); err != nil {
			return err
		}
	}
	r.Phase = "seed"
	// Prepare the growing prefix in bounded increments so compaction can
	// keep the uncovered WAL tail below the memory limit even during seeding.
	const prepareRows = 8192
	var generation uint64
	for lo := 0; lo < f.SeedRows; lo += prepareRows {
		hi := min(lo+prepareRows, f.SeedRows)
		if err := f.appendRange(ctx, s, lo, hi, nil); err != nil {
			return err
		}
		if o.mode == "live" {
			if _, err := s.compact(ctx, o.control, "start", hi); err != nil {
				return err
			}
			c, err := s.waitCompaction(ctx, o.control, hi, generation)
			if err != nil {
				return err
			}
			if c.CommittedRows != hi {
				return fmt.Errorf("unexpected committed rows during fixture preparation: %+v", c)
			}
			generation = c.Generation
		}
	}
	r.Phase = "mixed_load"
	w := f.newWalk(f.SeedRows, true)
	if _, err := w.page(ctx, s, f); err != nil {
		return fmt.Errorf("capture seed cutoff: %w", err)
	}
	var writes, reads []time.Duration
	defer func() {
		r.IngestSamples, r.QuerySamples = len(writes), len(reads)
		r.IngestP99, r.QueryP99 = p99(writes), p99(reads)
	}()
	// Append one acknowledged batch before resuming the cursor. Later
	// writes have a single producer so tie order stays deterministic.
	first := f.SeedRows + f.BatchRows
	if err := f.appendRange(ctx, s, f.SeedRows, first, &writes); err != nil {
		return err
	}
	if o.mode == "live" {
		r.Phase = "mixed_compaction"
		c, err := s.compact(ctx, o.control, "start", first)
		if err != nil {
			return err
		}
		if c.State != "running" || c.Generation != generation || c.CoveredRows != f.SeedRows || c.CommittedRows != first {
			return fmt.Errorf("invalid opening compaction state: %+v", c)
		}
	}
	mixed, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := make(chan error, 1)
	firstWrite := make(chan struct{})
	go func() {
		next := first + f.BatchRows
		err := f.appendRange(mixed, s, first, next, &writes)
		close(firstWrite)
		if err == nil {
			err = f.appendRange(mixed, s, next, f.SeedRows+f.TailRows, &writes)
		}
		if err != nil {
			cancel()
		}
		writer <- err
	}()
	var mixedErr error
	// Leave a continuation pending until after live publication so the walk
	// exercises the same pin on both sides of the generation change.
	for w.cursor != "" && (o.mode != "live" || len(w.want)-w.offset > 509) {
		d, err := w.page(mixed, s, f)
		if err != nil {
			mixedErr = fmt.Errorf("walk captured cutoff %d: %w", f.SeedRows, err)
			cancel()
			break
		}
		reads = append(reads, d)
		if o.mode == "live" && len(reads) == 1 {
			<-firstWrite
			c, err := s.compact(mixed, o.control, "status", first)
			if err == nil && (c.State != "running" || c.Generation != generation ||
				c.CoveredRows != f.SeedRows || c.CommittedRows < first+f.BatchRows) {
				err = fmt.Errorf("compaction did not span a continuation and an acknowledged batch: %+v", c)
			}
			if err != nil {
				mixedErr = err
				cancel()
				break
			}
		}
	}
	if err := errors.Join(<-writer, mixedErr); err != nil {
		return err
	}
	if o.mode == "live" {
		c, err := s.waitCompaction(ctx, o.control, first, generation)
		r.Compaction = &c
		if err != nil {
			return err
		}
		if c.CommittedRows != f.SeedRows+f.TailRows {
			return fmt.Errorf("unexpected final uncovered tail: %+v", c)
		}
		for w.cursor != "" {
			if _, err := w.page(ctx, s, f); err != nil {
				return fmt.Errorf("resume after compaction: %w", err)
			}
		}
	}
	return nil
}

func (f fixture) recover(ctx context.Context, s service) (int, error) {
	w := f.newWalk(f.SeedRows+f.TailRows, false)
	for w.pages == 0 || w.cursor != "" {
		if _, err := w.page(ctx, s, f); err != nil {
			return w.offset, err
		}
	}
	return w.offset, nil
}
