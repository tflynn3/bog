package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLimitedQueryLookaheadAndSnapshot(t *testing.T) {
	s := &Store{}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Publish([]Row{{base, "hit"}, {base.Add(time.Second), strings.Repeat("x", 1000)}, {base.Add(2 * time.Second), "hit"}})
	start, end := base, base.Add(time.Minute)
	_, _, _, _, err := s.QueryPageContext(context.Background(), start, end, "hit", 1, nil, nil, QueryLimits{Work: 100})
	if !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("lookahead: %v", err)
	}
	first, count, next, ok, err := s.QueryPageContext(context.Background(), start, end, "hit", 1, nil, nil, QueryLimits{Work: 10000})
	if err != nil || !ok || next == nil || len(first) != 1 {
		t.Fatalf("first: %v %v %v", ok, next, err)
	}
	s.Publish([]Row{{base.Add(time.Second), "hit backfill"}})
	rest, _, _, ok, err := s.QueryPageContext(context.Background(), start, end, "hit", 10, &count, next, QueryLimits{Work: 10000})
	if err != nil || !ok || !reflect.DeepEqual(rest, []Row{{base.Add(2 * time.Second), "hit"}}) {
		t.Fatalf("snapshot: %#v %v %v", rest, ok, err)
	}
}
func TestLimitedQueryCancellationAndBudget(t *testing.T) {
	s := &Store{}
	base := time.Now()
	rows := make([]Row, 3000)
	for i := range rows {
		rows[i] = Row{base.Add(time.Duration(i) * time.Second), "ordinary"}
	}
	s.Publish(rows)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.QueryContext(ctx, base, base.Add(time.Hour), "absent", 10, QueryLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := s.QueryContext(context.Background(), base, base.Add(time.Hour), "absent", 10, QueryLimits{Work: 10}); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("budget: %v", err)
	}
}

func TestLimitedQueryParity(t *testing.T) {
	s := &Store{}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for batch := 0; batch < 20; batch++ {
		rows := make([]Row, 157)
		for i := range rows {
			n := batch*len(rows) + i
			rows[i] = Row{base.Add(time.Duration((n*79)%2000) * time.Second), "miss"}
			if n%113 == 0 {
				rows[i].Line = "rare match"
			}
		}
		s.Publish(rows)
	}
	for _, literal := range []string{"", "rare", "absent"} {
		for _, limit := range []int{1, 17, 1000} {
			want := s.Query(base, base.Add(time.Hour), literal, limit)
			got, err := s.QueryContext(context.Background(), base, base.Add(time.Hour), literal, limit, QueryLimits{Work: 1000000})
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%q limit %d: %v", literal, limit, err)
			}
		}
	}
}

func BenchmarkLimitedAbsentQuery(b *testing.B) {
	s := &Store{}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]Row, 100000)
	for i := range rows {
		rows[i] = Row{base.Add(time.Duration(i) * time.Second), "ordinary log message"}
	}
	s.Publish(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.QueryContext(context.Background(), base, base.Add(100000*time.Second), "absent", 10, QueryLimits{Work: 1 << 30})
		if err != nil {
			b.Fatal(err)
		}
	}
}
