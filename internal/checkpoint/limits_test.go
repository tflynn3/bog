package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"testing"
)

func requireExhaustion(t *testing.T, err error, resource Resource, limit, inUse, need uint64) {
	t.Helper()
	var exhausted *ExhaustedError
	if !errors.Is(err, ErrExhausted) || !errors.As(err, &exhausted) ||
		*exhausted != (ExhaustedError{Resource: resource, Limit: limit, InUse: inUse, Need: need}) {
		t.Fatalf("exhaustion = %v, details = %+v", err, exhausted)
	}
}

func TestBudgetExactLimitsAndSharedOwnership(t *testing.T) {
	for _, resource := range []Resource{StagingBytes, ReplayTailBytes, PinnedGenerations, LiveCaptures} {
		t.Run(resource.String(), func(t *testing.T) {
			var limits Limits
			limits[resource] = math.MaxUint64
			budget := NewBudget(limits)
			charge, err := budget.Reserve(t.Context(), resource, math.MaxUint64)
			requirePublicationOK(t, err)
			pin := charge.Retain()
			charge.Release()
			charge.Release()
			if charge.Retain() != nil {
				t.Fatal("released handle retained a charge")
			}
			_, err = budget.Reserve(t.Context(), resource, 1)
			requireExhaustion(t, err, resource, math.MaxUint64, math.MaxUint64, 1)
			pin.Release()
			if usage := budget.Usage(); usage.InUse[resource] != 0 || usage.Peak[resource] != math.MaxUint64 {
				t.Fatalf("usage after drain = %+v", usage)
			}
		})
	}
	budget := NewBudget(Limits{})
	_, err := budget.Reserve(t.Context(), StagingBytes, 1)
	requireExhaustion(t, err, StagingBytes, 0, 0, 1)
	zero, err := budget.ReserveReplayTail(t.Context(), 8, 8)
	requirePublicationOK(t, err)
	zero.Release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := budget.Reserve(ctx, ReplayTailBytes, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission = %v", err)
	}
	for _, pair := range [][2]int64{{-1, 0}, {9, 8}} {
		if _, err := budget.ReserveReplayTail(t.Context(), pair[0], pair[1]); err == nil {
			t.Fatalf("accepted invalid bounds %v", pair)
		}
	}
}

func TestBudgetConcurrentReserveRetainAndRelease(t *testing.T) {
	const capacity = 5
	budget := NewBudget(Limits{ReplayTailBytes: capacity})
	for range 20 {
		var workers sync.WaitGroup
		start := make(chan struct{})
		admitted := make(chan *Reservation, 20)
		for range cap(admitted) {
			workers.Go(func() {
				<-start
				reservation, err := budget.Reserve(t.Context(), ReplayTailBytes, 1)
				if err == nil {
					admitted <- reservation
				} else if !errors.Is(err, ErrExhausted) {
					t.Errorf("admission: %v", err)
				}
			})
		}
		close(start)
		workers.Wait()
		close(admitted)
		if len(admitted) != capacity {
			t.Fatalf("admitted %d, want %d", len(admitted), capacity)
		}
		for reservation := range admitted {
			workers.Go(func() {
				if ref := reservation.Retain(); ref != nil {
					ref.Release()
				}
			})
			workers.Go(reservation.Release)
		}
		workers.Wait()
		if usage := budget.Usage(); usage.InUse[ReplayTailBytes] != 0 || usage.Peak[ReplayTailBytes] != capacity {
			t.Fatalf("usage after concurrent drain = %+v", usage)
		}
	}
}

func TestReplayBudgetAcrossConcurrentAppendAndCapturedFrames(t *testing.T) {
	_, l, wal := capturedPublicationFixture(t)
	start, midpoint := int64(len("BOGWAL01")), l.end
	full := append(bytes.Clone(wal), wal[start:]...)
	end := int64(len(full))
	limit := uint64(end - start)
	budget := NewBudget(Limits{ReplayTailBytes: limit})
	scanning, resume := make(chan struct{}), make(chan struct{})
	type result struct {
		tail *ReplayedTail
		err  error
	}
	done := make(chan result, 1)
	var once sync.Once
	input := capturedSuffix{ReaderAt: bytes.NewReader(full), start: start, before: func() {
		once.Do(func() { close(scanning); <-resume })
	}}
	go func() {
		tail, err := budget.ReplayCapturedTail(t.Context(), input, start, end, 7)
		done <- result{tail, err}
	}()
	select {
	case <-scanning:
	case r := <-done:
		t.Fatalf("replay returned before reaching reader: %v", r.err)
	}
	_, contention := budget.ReserveReplayTail(t.Context(), end, end+1)
	close(resume)
	r := <-done
	requireExhaustion(t, contention, ReplayTailBytes, limit, limit, 1)
	requirePublicationOK(t, r.err)
	if r.tail.NextOrdinal != 11 || len(r.tail.Batches) != 2 ||
		r.tail.Batches[0].End != midpoint || r.tail.Batches[1].Start != midpoint ||
		r.tail.Batches[0].FirstOrdinal != 7 || r.tail.Batches[1].FirstOrdinal != 9 ||
		r.tail.Batches[0].Rows[0] != r.tail.Batches[1].Rows[1] {
		t.Fatalf("replayed tail = %+v", r.tail)
	}
	held := r.tail.Batches[0].Reservation.Retain()
	for _, batch := range r.tail.Batches {
		batch.Reservation.Release()
	}
	if inUse := budget.Usage().InUse[ReplayTailBytes]; inUse != uint64(midpoint-start) {
		t.Fatalf("bytes held by captured frame = %d", inUse)
	}
	appendCharge, err := budget.ReserveReplayTail(t.Context(), midpoint, end)
	requirePublicationOK(t, err)
	appendCharge.Release()
	held.Release()
	if inUse := budget.Usage().InUse[ReplayTailBytes]; inUse != 0 {
		t.Fatalf("replay bytes after capture drained = %d", inUse)
	}

	for _, tt := range []struct {
		name string
		edit func([]byte)
		ctx  func() context.Context
		from uint64
		want error
	}{
		{name: "late committed corruption", edit: func(b []byte) { b[len(b)-1] ^= 1 }, want: ErrIntegrity},
		{name: "ordinal overflow", from: math.MaxUint64 - 2, want: ErrIntegrity},
		{name: "cancelled", ctx: func() context.Context {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			return ctx
		}, want: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, data := t.Context(), bytes.Clone(full)
			if tt.ctx != nil {
				ctx = tt.ctx()
			}
			if tt.edit != nil {
				tt.edit(data)
			}
			got, err := budget.ReplayCapturedTail(ctx, bytes.NewReader(data), start, end, tt.from)
			if got != nil || !errors.Is(err, tt.want) || budget.Usage().InUse[ReplayTailBytes] != 0 {
				t.Fatalf("replay result = %+v, error = %v, usage = %+v", got, err, budget.Usage())
			}
		})
	}
}
