package querymodel

import "fmt"

// Cursor pins a continuation to the first page's append high-water mark.
type Cursor struct {
	HighWater int
	After     Position
}

// Request starts a new walk when Cursor is nil.
type Request struct {
	Filter Filter
	Limit  int
	Cursor *Cursor
}

// Page contains rows and the captured append count. Next is nil on the terminal
// page. Otherwise it is the position of the last row returned on this page.
type Page struct {
	Rows      []Row
	HighWater int
	Next      *Position
}

// QueryPage adapts the system under test to the oracle's request and response types.
// The adapter treats the request and its cursor as read-only.
type QueryPage func(Request) (Page, error)

// VerifyWalk checks each page against the fixed log prefix, cycling through
// positive page limits. The first request must capture highWater in the system
// under test. The adapter can interleave appends during continuation requests.
// Errors include the fixture seed and the actual request sequence.
func VerifyWalk(seed int64, log *Log, highWater int, filter Filter, limits []int, query QueryPage) error {
	var requests []Request
	fail := func(err error) error {
		var sequence []byte
		for i, req := range requests {
			sequence = fmt.Appendf(sequence, "\n  %d. start=%s end=%s contains=%q limit=%d cursor=",
				i+1, req.Filter.Start, req.Filter.End, req.Filter.Contains, req.Limit)
			if req.Cursor == nil {
				sequence = append(sequence, "nil"...)
			} else {
				sequence = fmt.Appendf(sequence, "{high_water=%d after=%+v}", req.Cursor.HighWater, req.Cursor.After)
			}
		}
		return fmt.Errorf("seed=%d high_water=%d: %w\nrequest sequence (%d requests):%s",
			seed, highWater, err, len(requests), sequence)
	}
	if len(limits) == 0 {
		return fail(fmt.Errorf("at least one page limit is required"))
	}
	for _, limit := range limits {
		if limit <= 0 {
			return fail(fmt.Errorf("page limit must be positive, got %d", limit))
		}
	}
	matches, err := log.Match(highWater, filter)
	if err != nil {
		return fail(err)
	}
	var cursor *Cursor
	for {
		limit := limits[len(requests)%len(limits)]
		req := Request{Filter: filter, Limit: limit, Cursor: cursor}
		requests = append(requests, req)
		page, err := query(req)
		if err != nil {
			return fail(fmt.Errorf("query page %d: %w", len(requests), err))
		}
		if page.HighWater != highWater {
			return fail(fmt.Errorf("got high-water mark %d, want %d", page.HighWater, highWater))
		}
		n := min(limit, len(matches))
		if len(page.Rows) != n {
			return fail(fmt.Errorf("got %d rows, want %d", len(page.Rows), n))
		}
		for i, got := range page.Rows {
			want := matches[i]
			if !got.Time.Equal(want.Time) || got.Line != want.Line {
				return fail(fmt.Errorf("row %d: got (%s, %q), want (%s, %q) at append ordinal %d",
					i, got.Time, got.Line, want.Time, want.Line, want.Ordinal))
			}
		}
		if len(matches) == n {
			if page.Next != nil {
				return fail(fmt.Errorf("terminal page has cursor %+v", *page.Next))
			}
			return nil
		}
		last := matches[n-1]
		want := Position{Time: last.Time, Ordinal: last.Ordinal}
		if page.Next == nil {
			return fail(fmt.Errorf("missing continuation, want position %+v", want))
		}
		if page.Next.Ordinal != want.Ordinal || !page.Next.Time.Equal(want.Time) {
			return fail(fmt.Errorf("got continuation %+v, want %+v", *page.Next, want))
		}
		cursor = &Cursor{HighWater: page.HighWater, After: *page.Next}
		matches = matches[n:]
	}
}
