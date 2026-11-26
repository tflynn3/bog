package querymodel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// AcknowledgeBatch submits one read-only fixture batch in slice order. Return nil only
// after a durable acknowledgement (HTTP 204 for Bog). An ambiguous result
// aborts the scenario because its append ordinals can no longer be known.
type AcknowledgeBatch func(context.Context, []Row) error

// WireFixtureStats records accepted data from a seeded scenario.
type WireFixtureStats struct {
	AcknowledgedRows int
	Held             WireStats
	Fresh            WireStats
	Unfiltered       WireStats
	Empty            WireStats
}

const (
	sparseLiteral = "needle [wire]"
	seededTrimCap = 256
)

type seededWireData struct {
	initial, later [][]Row
	filter         Filter
}

func makeSeededWireData(seed int64, epoch time.Time) seededWireData {
	filter := Filter{
		Start:    epoch,
		End:      epoch.Add(26 * time.Millisecond),
		Contains: sparseLiteral,
	}
	// Unordered background fills the range between matching records and
	// extends past the last one. Batch membership is deterministic from seed.
	initial := SeededBatches(seed, epoch, 104, false)
	tie := epoch.Add(time.Millisecond)
	duplicate := Row{Time: tie, Line: sparseLiteral + " duplicate"}
	initial = append(initial, []Row{
		{Time: epoch.Add(20 * time.Millisecond), Line: sparseLiteral + " later"},
		{Time: tie.In(fixtureOffsets[2]), Line: sparseLiteral + " " + strings.Repeat("界", 28)},
		duplicate,
		{Time: filter.End, Line: sparseLiteral + " exclusive upper bound"},
		{Time: tie, Line: "Needle [wire] case miss"},
	}, []Row{
		{Time: filter.Start, Line: sparseLiteral + " inclusive lower bound"},
		duplicate,
		{Time: filter.Start.Add(-time.Nanosecond), Line: sparseLiteral + " before lower bound"},
	})
	later := [][]Row{{
		{Time: tie, Line: sparseLiteral + " new tie"},
		{Time: filter.Start, Line: sparseLiteral + " new backfill"},
	}, {
		{Time: epoch.Add(22 * time.Millisecond), Line: sparseLiteral + " new later match"},
	}}
	return seededWireData{initial: initial, later: later, filter: filter}
}

// VerifySeededWireFixture exercises an initially empty, exclusively owned
// endpoint. It acknowledges seed batches, verifies a held sparse walk across
// two more writes, then verifies fresh sparse, unfiltered and empty walks.
// The sparse rows include equal UTC instants with different offsets, exact
// duplicates, backfill, and literal punctuation. For a server configured with
// a 256-byte page cap the opening sparse page must be trimmed by that cap.
func VerifySeededWireFixture(ctx context.Context, seed int64, epoch time.Time, limits WireLimits, appendBatch AcknowledgeBatch, fetch FetchWirePage) (WireFixtureStats, error) {
	var stats WireFixtureStats
	data := makeSeededWireData(seed, epoch)
	walk := WireWalk{
		Seed: seed, Filter: data.filter, RowLimits: []int{4, 1, 7}, Limits: limits,
	}
	// At 256 bytes the first two rows fit and the third does not. This
	// makes the trimmed boundary as observable as the changing limits.
	if limits.MaxPageBytes == seededTrimCap {
		walk.FirstPageRows = 2
	}
	trace := wireTrace{walk: walk}
	if appendBatch == nil || fetch == nil {
		return stats, trace.failure(errors.New("append acknowledgement and fetch adapters are required"))
	}
	if err := walk.validate(); err != nil {
		return stats, trace.failure(err)
	}
	if limits.MaxPages < 2 {
		return stats, trace.failure(errors.New("seeded fixture requires a page budget of at least two"))
	}

	// Apply a single scenario deadline to ingest and all four walks, so that
	// starting a fresh capture cannot extend the time available to the suite.
	ctx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	var log Log
	appendBatches := func(ctx context.Context, phase string, batches [][]Row) error {
		for i, batch := range batches {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := appendBatch(ctx, batch); err != nil {
				return fmt.Errorf("%s batch %d: %w", phase, i+1, err)
			}
			stats.AcknowledgedRows = log.Append(batch)
		}
		return ctx.Err()
	}

	if err := appendBatches(ctx, "seed", data.initial); err != nil {
		trace.highWater = log.HighWater()
		return stats, trace.failure(err)
	}
	appendLater := func(ctx context.Context, _ string) error {
		return appendBatches(ctx, "post-capture", data.later)
	}
	held, err := VerifyWireWalk(ctx, &log, walk, fetch, appendLater)
	stats.Held = held
	if err != nil {
		return stats, fmt.Errorf("held sparse walk: %w", err)
	}

	walk.FirstPageRows = 0

	for _, check := range []struct {
		name, contains string
		rowLimits      []int
		stats          *WireStats
	}{
		{"fresh sparse", sparseLiteral, []int{1, 9, 2}, &stats.Fresh},
		{"fresh unfiltered", "", []int{12, 3}, &stats.Unfiltered},
		{"fresh empty", "[no fixture row]", []int{2}, &stats.Empty},
	} {
		walk.Filter.Contains, walk.RowLimits = check.contains, check.rowLimits
		*check.stats, err = VerifyWireWalk(ctx, &log, walk, fetch, nil)
		if err != nil {
			return stats, fmt.Errorf("%s walk: %w", check.name, err)
		}
	}
	return stats, nil
}
