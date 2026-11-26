package querymodel_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"bog/internal/store"
	model "bog/verify/query-model"
)

// storeAdapter maps fields at the API boundary. All expectations come from Log.
type storeAdapter struct {
	store store.Store
}

func (s *storeAdapter) publish(batch []model.Row) {
	rows := make([]store.Row, len(batch))
	for i, row := range batch {
		rows[i] = store.Row{TS: row.Time, Line: row.Line}
	}
	s.store.Publish(rows)
}

func (s *storeAdapter) query(req model.Request) (model.Page, error) {
	var snapshot *int
	var after *store.Position
	if req.Cursor != nil {
		count := req.Cursor.HighWater
		snapshot = &count
		after = &store.Position{Time: req.Cursor.After.Time, Ordinal: req.Cursor.After.Ordinal}
	}
	filter := req.Filter
	rows, count, next, ok := s.store.QueryPage(filter.Start, filter.End, filter.Contains, req.Limit, snapshot, after)
	if !ok {
		return model.Page{}, fmt.Errorf("Store.QueryPage rejected request")
	}
	page := model.Page{Rows: make([]model.Row, len(rows)), HighWater: count}
	for i, row := range rows {
		page.Rows[i] = model.Row{Time: row.TS, Line: row.Line}
	}
	if next != nil {
		page.Next = &model.Position{Time: next.Time, Ordinal: next.Ordinal}
	}
	return page, nil
}

func (s *storeAdapter) queryLocated(req model.Request) (model.Page, error) {
	var snapshot, after *int
	if req.Cursor != nil {
		snapshot, after = &req.Cursor.HighWater, &req.Cursor.After.Ordinal
	}
	f := req.Filter
	got, err := s.store.QuerySnapshotPageWithWork(f.Start, f.End, f.Contains, req.Limit, snapshot, after, nil)
	if err != nil {
		return model.Page{}, err
	}
	if len(got.Rows) != len(got.Ordinals) {
		return model.Page{}, fmt.Errorf("got %d rows and %d ordinals", len(got.Rows), len(got.Ordinals))
	}
	page := model.Page{Rows: make([]model.Row, len(got.Rows)), HighWater: got.HighWater}
	for i, row := range got.Rows {
		page.Rows[i] = model.Row{Time: row.TS, Line: row.Line}
	}
	if got.More {
		if len(got.Rows) == 0 {
			return model.Page{}, fmt.Errorf("empty page with more matches")
		}
		last := len(got.Rows) - 1
		page.Next = &model.Position{Time: got.Rows[last].TS, Ordinal: got.Ordinals[last]}
	}
	return page, nil
}

var epoch = time.Date(2025, 1, 2, 0, 0, 0, 123, time.UTC)

func loadHistory(batches [][]model.Row) (*storeAdapter, *model.Log) {
	s, log := new(storeAdapter), new(model.Log)
	for _, batch := range batches {
		s.publish(batch)
		log.Append(batch)
	}
	return s, log
}

func checkWalk(t *testing.T, seed int64, log *model.Log, filter model.Filter, limits []int, query model.QueryPage) {
	t.Helper()
	if err := model.VerifyWalk(seed, log, log.HighWater(), filter, limits, query); err != nil {
		t.Fatal(err)
	}
}

func TestStoreSnapshotWalk(t *testing.T) {
	start, middle, end := epoch, epoch.Add(time.Second), epoch.Add(2*time.Second)
	west := time.FixedZone("west", -7*60*60)
	east := time.FixedZone("east", (5*60+30)*60)
	duplicate := model.Row{Time: middle, Line: "needle tie"}
	ordered := [][]model.Row{
		{{Time: start.Add(-time.Nanosecond), Line: "needle before"},
			{Time: start.In(east), Line: "needle start"}},
		{duplicate, duplicate, {Time: middle.In(west), Line: "needle west"}},
		{{Time: middle.In(east), Line: "Needle upper"}, {Time: middle, Line: "literal .* [x]"},
			{Time: end.Add(-time.Nanosecond), Line: "needle near end"},
			{Time: end.In(west), Line: "needle end"}},
	}
	merged := [][]model.Row{
		{ordered[2][3], duplicate, ordered[0][0]},
		{ordered[2][2], ordered[0][1], duplicate},
		{ordered[1][2], ordered[2][0], ordered[2][1]},
	}
	tests := []struct {
		name     string
		batches  [][]model.Row
		contains string
		limits   []int
		empty    bool
	}{
		{name: "ordered ties and half-open bounds", batches: ordered, contains: "needle", limits: []int{1}},
		{name: "merged ties and changing limits", batches: merged, contains: "needle", limits: []int{2, 1, 3}},
		{name: "merged exact terminal limit", batches: merged, contains: "needle", limits: []int{5}},
		{name: "merged full terminal continuation", batches: merged, contains: "needle", limits: []int{2, 3}},
		{name: "all rows", batches: ordered, limits: []int{3, 40}},
		{name: "case sensitive", batches: merged, contains: "Needle", limits: []int{1}},
		{name: "literal punctuation", batches: merged, contains: ".* [", limits: []int{1}},
		{name: "no matching rows", batches: ordered, contains: "NEEDLE", limits: []int{2}},
		{name: "empty interval", batches: merged, limits: []int{2}, empty: true},
		{name: "empty log", limits: []int{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, log := loadHistory(tt.batches)
			filter := model.Filter{Start: start.In(west), End: end.In(east), Contains: tt.contains}
			if tt.empty {
				filter.End = start.In(east)
			}
			checkWalk(t, 0, log, filter, tt.limits, s.query)
		})
	}
}

func TestStoreSnapshotWalkWithInterleavedAppends(t *testing.T) {
	const seed = 17
	s, log := loadHistory([][]model.Row{{
		{Time: epoch.Add(3 * time.Second), Line: "last"},
		{Time: epoch.Add(time.Second), Line: "first"},
		{Time: epoch.Add(2 * time.Second), Line: "tie"},
		{Time: epoch.Add(2 * time.Second), Line: "tie"},
	}})
	filter := model.Filter{Start: epoch, End: epoch.Add(10 * time.Second)}
	late := [][]model.Row{
		{{Time: epoch, Line: "before cursor"}, {Time: epoch.Add(4 * time.Second), Line: "newer"}},
		{{Time: epoch.Add(2 * time.Second), Line: "tie"}, {Time: epoch.Add(2500 * time.Millisecond), Line: "backfill"}},
	}
	published := 0
	pin := log.HighWater()
	checkWalk(t, seed, log, filter, []int{1, 1, 2}, func(req model.Request) (model.Page, error) {
		if req.Cursor != nil && published < len(late) {
			batch := late[published]
			s.publish(batch)
			log.Append(batch)
			published++
		}
		return s.query(req)
	})
	if published != len(late) || log.HighWater() != pin+4 {
		t.Fatalf("seed=%d: published %d late batches, high-water mark=%d", seed, published, log.HighWater())
	}
	checkWalk(t, seed, log, filter, []int{1, 3}, s.query)
}

func TestSeededStoreSnapshotWalk(t *testing.T) {
	for _, ordered := range []bool{true, false} {
		for _, seed := range []int64{0, 1, -17, 43, 92821} {
			t.Run(fmt.Sprintf("ordered=%t/seed=%d", ordered, seed), func(t *testing.T) {
				s, log := loadHistory(model.SeededBatches(seed, epoch, 1536, ordered))
				for _, contains := range []string{"", "needle", "Needle", "needle界", ".* [", "absent literal"} {
					for _, interval := range [][2]time.Duration{{0, time.Second}, {13 * time.Millisecond, 77 * time.Millisecond}} {
						filter := model.Filter{Start: epoch.Add(interval[0]), End: epoch.Add(interval[1]), Contains: contains}
						checkWalk(t, seed, log, filter, []int{1, 37, 128, 3, 17}, s.query)
					}
				}
			})
		}
	}
}

func TestStoreSnapshotWalkConcurrentPublish(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query func(*storeAdapter, model.Request) (model.Page, error)
	}{
		{"timestamp cursor", (*storeAdapter).query},
		{"ordinal cursor", (*storeAdapter).queryLocated},
	} {
		t.Run(tc.name, func(t *testing.T) { checkConcurrentStoreWalk(t, tc.query) })
	}
}

func checkConcurrentStoreWalk(t *testing.T, query func(*storeAdapter, model.Request) (model.Page, error)) {
	t.Helper()
	const seed = 43
	s, log := loadHistory(model.SeededBatches(seed, epoch, 1536, true))
	filter := model.Filter{Start: epoch, End: epoch.Add(time.Second)}
	// Start from within the original time range, then extend past it.
	late := model.SeededBatches(seed+1, epoch.Add(300*time.Millisecond), 1024, false)
	published := 0
	checkWalk(t, seed, log, filter, []int{1, 11, 17}, func(req model.Request) (model.Page, error) {
		if req.Cursor == nil || published == len(late) {
			return query(s, req)
		}
		batch := late[published]
		published++
		// Each pair starts together after capture. Join the writer before
		// returning the page, even when the query fails.
		var writer sync.WaitGroup
		ready, start := make(chan struct{}), make(chan struct{})
		writer.Go(func() {
			close(ready)
			<-start
			s.publish(batch)
		})
		<-ready
		close(start)
		page, err := query(s, req)
		writer.Wait()
		log.Append(batch)
		return page, err
	})
	if published != len(late) {
		t.Fatalf("seed=%d: published %d late batches, want %d", seed, published, len(late))
	}
	checkWalk(t, seed, log, filter, []int{53}, func(req model.Request) (model.Page, error) { return query(s, req) })
}
