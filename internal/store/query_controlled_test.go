package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestControlledQuerySnapshotAndBudgets(t *testing.T) {
	s := &Store{}
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Publish([]Row{{t0.Add(time.Second), "x"}, {t0, "x"}, {t0, "x"}})
	limits := QueryLimits{100, 1000, 10000}
	rows, count, next, valid, err := s.QueryContext(context.Background(), t0, t0.Add(2*time.Second), "x", 1, nil, nil, true, limits)
	if err != nil || !valid || len(rows) != 1 || next == nil || next.Ordinal != 1 {
		t.Fatalf("first page: %v %d %v %v %v", rows, count, next, valid, err)
	}
	s.Publish([]Row{{t0, "x"}})
	rows, _, next, valid, err = s.QueryContext(context.Background(), t0, t0.Add(2*time.Second), "x", 10, &count, next, true, limits)
	if err != nil || !valid || len(rows) != 2 || next != nil {
		t.Fatalf("continuation: %v %v %v %v", rows, next, valid, err)
	}
	_, _, _, _, err = s.QueryContext(context.Background(), t0, t0.Add(2*time.Second), "x", 1, nil, nil, true, QueryLimits{100, 1, 10000})
	if !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("lookahead: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, _, err = s.QueryContext(ctx, t0, t0.Add(2*time.Second), "", 1, nil, nil, false, limits)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}
