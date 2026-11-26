package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func wireConfig() model.WireWalk {
	return model.WireWalk{
		Seed: -73, Filter: model.Filter{Start: epoch, End: epoch.Add(time.Second)},
		RowLimits: []int{2, 1, 4},
		Limits:    model.WireLimits{MaxPages: 32, MaxPageBytes: 256, Timeout: time.Second},
	}
}

func TestWireWalkInterleavesAcknowledgedAppend(t *testing.T) {
	t.Parallel()
	first := model.Row{Time: epoch, Line: "early"}
	tie := model.Row{Time: epoch.Add(time.Nanosecond), Line: "duplicate"}
	late := model.Row{Time: epoch.Add(2 * time.Nanosecond), Line: "late"}
	added := model.Row{Time: tie.Time, Line: "new tie"}
	var log model.Log
	log.Append([]model.Row{tie, late, first, tie})
	walk := wireConfig()
	tokens := []string{"", "opaque: first+/=?", "opaque: second+/=?"}
	pages := []model.WirePage{
		{Rows: []model.Row{first}, NextCursor: tokens[1], BodyBytes: 180},
		{Rows: []model.Row{tie}, NextCursor: tokens[2], BodyBytes: 60},
		{Rows: []model.Row{tie, late}, BodyBytes: 90},
	}
	requests, hooks := 0, 0
	fetch := func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
		i := requests
		requests++
		if i >= len(pages) {
			t.Fatal("request after terminal page")
		}
		want := model.WireRequest{Filter: walk.Filter, Limit: walk.RowLimits[i], Cursor: tokens[i], MaxPageBytes: 256}
		if req != want || hooks != min(i, 1) {
			t.Fatalf("page %d request = %+v, hook count = %d, want %+v and %d", i+1, req, hooks, want, min(i, 1))
		}
		return pages[i], nil
	}
	hook := func(_ context.Context, cursor string) error {
		hooks++
		if cursor != tokens[1] {
			t.Fatalf("first continuation changed: %q", cursor)
		}
		log.Append([]model.Row{added})
		return nil
	}
	stats, err := model.VerifyWireWalk(t.Context(), &log, walk, fetch, hook)
	wantStats := model.WireStats{Pages: 3, Rows: 4, BodyBytes: 330, ByteTrimmedPages: 1}
	if err != nil || stats != wantStats || hooks != 1 || requests != 3 {
		t.Fatalf("held walk = %+v, %v, requests=%d hooks=%d", stats, err, requests, hooks)
	}

	walk.RowLimits = []int{10}
	stats, err = model.VerifyWireWalk(t.Context(), &log, walk, func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
		if req.Cursor != "" {
			t.Fatal("fresh walk reused the held cursor")
		}
		return model.WirePage{Rows: []model.Row{first, tie, tie, added, late}, BodyBytes: 240}, nil
	}, nil)
	if err != nil || stats.Rows != 5 || stats.Pages != 1 {
		t.Fatalf("fresh walk = %+v, %v", stats, err)
	}
}

func TestWireWalkRejectsInvalidContinuationsAndRows(t *testing.T) {
	t.Parallel()
	first := model.Row{Time: epoch, Line: "first"}
	second := model.Row{Time: epoch, Line: "second"}
	rows := []model.Row{first, first, second, first}
	valid := model.WirePage{Rows: rows[:1], NextCursor: "token-A", BodyBytes: 42}
	page := func(rows []model.Row, cursor string) model.WirePage {
		return model.WirePage{Rows: rows, NextCursor: cursor, BodyBytes: int64(42 * len(rows))}
	}

	for _, tc := range []struct {
		name      string
		pages     []model.WirePage
		detail    string
		accepted  int
		rowLimits []int
	}{
		{"empty continuation", []model.WirePage{page(nil, "token-A")}, "empty page issued a continuation", 0, nil},
		{"dropped duplicate", []model.WirePage{valid, page(rows[2:3], "token-B")}, "append ordinal 1", 1, nil},
		{"missing continuation", []model.WirePage{page(rows[:2], "")}, "2 expected rows remaining", 0, nil},
		{"repeated cursor", []model.WirePage{valid, page(rows[1:2], "token-B"), page(rows[2:3], "token-A")}, "first issued by page 1", 2, nil},
		{"terminal cursor", []model.WirePage{page(rows, "token-A")}, "after terminal exhaustion", 0, []int{4}},
		{"timestamp mismatch", []model.WirePage{page([]model.Row{{Time: epoch.Add(time.Second), Line: "first"}}, "token-A")}, "append ordinal 0", 0, nil},
		{"row ceiling", []model.WirePage{page(rows[:3], "token-A")}, "at most 2 expected", 0, nil},
		{"surplus match", []model.WirePage{valid, page(rows, "")}, "at most 3 expected", 1, []int{4}},
		{"bytes over cap", []model.WirePage{{Rows: rows[:1], BodyBytes: 257}}, "encoded body has 257 bytes", 0, nil},
		{"missing byte count", []model.WirePage{{Rows: rows[:1], BodyBytes: -1}}, "encoded body has -1 bytes", 0, nil},
		{"inconsistent byte count", []model.WirePage{{Rows: rows[:1]}}, "0 bytes for 1 rows", 0, nil},
		{"oversize cursor", []model.WirePage{page(rows[:1], strings.Repeat("a", 4097))}, "continuation exceeds", 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var log model.Log
			log.Append(rows)
			walk := wireConfig()
			if tc.rowLimits != nil {
				walk.RowLimits = tc.rowLimits
			}
			i := 0
			stats, err := model.VerifyWireWalk(t.Context(), &log, walk, func(context.Context, model.WireRequest) (model.WirePage, error) {
				if i == len(tc.pages) {
					t.Fatal("fetched beyond invalid page")
				}
				p := tc.pages[i]
				i++
				return p, nil
			}, nil)
			if err == nil || !strings.Contains(err.Error(), tc.detail) || stats.Pages != tc.accepted || i != len(tc.pages) {
				t.Fatalf("walk = %+v, %v, calls=%d; want %q after %d accepted pages", stats, err, i, tc.detail, tc.accepted)
			}
		})
	}
}

func TestWireWalkBoundsAndDiagnostics(t *testing.T) {
	t.Parallel()
	record := model.Row{Time: epoch, Line: "row"}
	var log model.Log
	log.Append(slices.Repeat([]model.Row{record}, 30))
	t.Run("page budget", func(t *testing.T) {
		walk := wireConfig()
		walk.Limits.MaxPages = 10
		requests := 0
		stats, err := model.VerifyWireWalk(t.Context(), &log, walk, func(_ context.Context, _ model.WireRequest) (model.WirePage, error) {
			requests++
			return model.WirePage{Rows: []model.Row{record}, NextCursor: fmt.Sprintf("opaque-token-%d", requests), BodyBytes: 42}, nil
		}, nil)
		if !errors.Is(err, model.ErrWirePageBudget) || stats.Pages != 10 || stats.Rows != 10 || requests != 10 {
			t.Fatalf("walk = %+v, %v, requests=%d", stats, err, requests)
		}
		for _, fragment := range []string{"seed=-73", "high_water=30", "omitted 2 earlier", "page 10 limit=2 rows=1 bytes=42 continued=true"} {
			if !strings.Contains(err.Error(), fragment) {
				t.Fatalf("missing %q in %v", fragment, err)
			}
		}
		if strings.Contains(err.Error(), "opaque-token") {
			t.Fatalf("diagnostic included a cursor: %v", err)
		}
	})
	t.Run("bounded and redacted cause", func(t *testing.T) {
		const cursor = "private cursor"
		cause := errors.New("fetch cursor=private+cursor: " + strings.Repeat("x", 15000))
		stats, err := model.VerifyWireWalk(t.Context(), &log, wireConfig(), func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
			if req.Cursor != "" {
				return model.WirePage{}, cause
			}
			return model.WirePage{Rows: []model.Row{record}, NextCursor: cursor, BodyBytes: 40}, nil
		}, nil)
		if !errors.Is(err, cause) || stats.Pages != 1 || len(err.Error()) > 1024 ||
			strings.Contains(err.Error(), "private") || !strings.Contains(err.Error(), "<cursor>") {
			t.Fatalf("walk = %+v, diagnostic length=%d and detail %.250q", stats, len(fmt.Sprint(err)), err)
		}
	})
	t.Run("terminal fits final allowance", func(t *testing.T) {
		walk := wireConfig()
		walk.Limits.MaxPages = 1
		var empty model.Log
		stats, err := model.VerifyWireWalk(t.Context(), &empty, walk, func(context.Context, model.WireRequest) (model.WirePage, error) {
			return model.WirePage{}, nil
		}, nil)
		if err != nil || stats != (model.WireStats{Pages: 1}) {
			t.Fatalf("empty walk = %+v, %v", stats, err)
		}
	})
}

func TestWireWalkHookAndCancellation(t *testing.T) {
	t.Parallel()
	r := model.Row{Time: epoch}
	var log model.Log
	log.Append([]model.Row{r, r})
	continued := model.WirePage{Rows: []model.Row{r}, NextCursor: "cursor", BodyBytes: 38}
	hookErr := errors.New("acknowledgement failed")
	for _, tc := range []struct {
		name, detail string
		page         model.WirePage
		cancelBefore bool
		cancelFetch  bool
		cancelHook   bool
		firstRows    int
		want         error
		calls, hooks int
		accepted     int
	}{
		{name: "canceled before fetch", cancelBefore: true, want: context.Canceled},
		{name: "canceled before accepting fetch", page: continued, cancelFetch: true, want: context.Canceled, calls: 1},
		{name: "canceled during hook", page: continued, cancelHook: true, want: context.Canceled, calls: 1, hooks: 1, accepted: 1},
		{name: "hook error", page: continued, want: hookErr, calls: 1, hooks: 1, accepted: 1},
		{name: "incorrect trimmed boundary before hook", page: continued, firstRows: 2, detail: "first page has 1 rows, want 2", calls: 1},
		{name: "terminal before hook", page: model.WirePage{Rows: []model.Row{r, r}, BodyBytes: 76}, detail: "requires a first-page continuation", calls: 1},
		{name: "invalid before hook", page: model.WirePage{NextCursor: "cursor"}, detail: "empty page", calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelBefore {
				cancel()
			}
			calls, hooks := 0, 0
			walk := wireConfig()
			walk.FirstPageRows = tc.firstRows
			stats, err := model.VerifyWireWalk(ctx, &log, walk, func(context.Context, model.WireRequest) (model.WirePage, error) {
				calls++
				if calls > tc.calls {
					t.Fatal("unexpected fetch")
				}
				if tc.cancelFetch {
					cancel()
				}
				return tc.page, nil
			}, func(context.Context, string) error {
				hooks++
				if tc.cancelHook {
					cancel()
				}
				return hookErr
			})
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) || !strings.Contains(err.Error(), tc.detail) ||
				calls != tc.calls || hooks != tc.hooks || stats.Pages != tc.accepted {
				t.Fatalf("walk=%+v, %v, fetches=%d hooks=%d", stats, err, calls, hooks)
			}
		})
	}
}

func TestWireWalkRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	for _, change := range []func(*model.WireWalk){
		func(w *model.WireWalk) { w.Limits.MaxPages = 0 },
		func(w *model.WireWalk) { w.Limits.MaxPageBytes = 0 },
		func(w *model.WireWalk) { w.Limits.MaxPageBytes = 1 << 62 },
		func(w *model.WireWalk) { w.Limits.Timeout = -time.Second },
		func(w *model.WireWalk) { w.Filter.End = w.Filter.Start },
		func(w *model.WireWalk) { w.RowLimits = nil },
		func(w *model.WireWalk) { w.RowLimits = []int{1, 0} },
		func(w *model.WireWalk) { w.FirstPageRows = w.RowLimits[0] + 1 },
	} {
		var log model.Log
		walk := wireConfig()
		change(&walk)
		if _, err := model.VerifyWireWalk(t.Context(), &log, walk, func(context.Context, model.WireRequest) (model.WirePage, error) {
			t.Fatal("fetched with invalid configuration")
			return model.WirePage{}, nil
		}, nil); err == nil {
			t.Fatalf("accepted configuration %+v", walk)
		}
	}
}
