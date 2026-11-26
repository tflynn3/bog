package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	querymodel "bog/verify/query-model"
)

func parityModel(rows []row) (*querymodel.Log, error) {
	model := new(querymodel.Log)
	return model, appendParityModel(model, rows)
}

func appendParityModel(model *querymodel.Log, rows []row) error {
	batch := make([]querymodel.Row, 0, len(rows))
	for _, r := range rows {
		ts, err := time.Parse(time.RFC3339Nano, r.TS)
		if err != nil {
			return err
		}
		batch = append(batch, querymodel.Row{Time: ts, Line: r.Line})
	}
	model.Append(batch)
	return nil
}

func parityMatches(pager *releasePager, model *querymodel.Log, cutoff int, filter string) ([]row, error) {
	entries, err := model.Match(cutoff, querymodel.Filter{Start: pager.query.Start, End: pager.query.End, Contains: filter})
	if err != nil {
		return nil, err
	}
	want := make([]row, 0, len(entries))
	for _, item := range entries {
		want = append(want, row{item.Time.Format(time.RFC3339Nano), item.Line})
	}
	return want, nil
}

func checkParityWalks(ctx context.Context, pager *releasePager, model *querymodel.Log, cutoff int, stage string) error {
	for _, filter := range []string{"", "[match]", "absent"} {
		want, err := parityMatches(pager, model, cutoff, filter)
		if err != nil {
			return err
		}
		for _, limit := range []int{1, 3, 10} {
			w := parityWalk{stage: stage, filter: filter, want: want}
			for w.pages == 0 || w.cursor != "" {
				if err := w.page(ctx, pager, limit); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type parityWalkFailure struct {
	Stage      string `json:"stage"`
	Filter     string `json:"filter"`
	Page       int    `json:"page"`
	Limit      int    `json:"limit"`
	Offset     int    `json:"offset"`
	HTTPStatus int    `json:"http_status,omitempty"`
	Detail     string `json:"detail"`
	cause      error
}

func (f *parityWalkFailure) Error() string {
	return fmt.Sprintf("%s filter %q page %d, limit %d, offset %d: %s", f.Stage, f.Filter, f.Page, f.Limit, f.Offset, f.Detail)
}

func (f *parityWalkFailure) Unwrap() error { return f.cause }

type parityWalk struct {
	stage, filter, cursor string
	pages, offset         int
	want                  []row
	seen                  map[string]bool
}

func (w *parityWalk) page(ctx context.Context, pager *releasePager, limit int) error {
	page, res, err := pager.fetch(ctx, w.filter, limit, w.cursor)
	n := len(page.Rows)
	remaining := len(w.want) - w.offset
	if err == nil {
		err = checkPageRows(page.Rows, w.want[w.offset:])
	}
	if err == nil {
		switch {
		case n > limit:
			err = fmt.Errorf("returned %d rows beyond limit %d", n, limit)
		case page.NextCursor == "" && n < remaining:
			err = fmt.Errorf("walk ended with %d rows missing", remaining-n)
		case page.NextCursor != "" && (n == 0 || n >= remaining || w.seen[page.NextCursor]):
			err = fmt.Errorf("repeated cursor or continuation without remaining rows")
		}
	}
	if err != nil {
		failure := &parityWalkFailure{
			Stage: w.stage, Filter: w.filter, Page: w.pages + 1, Limit: limit,
			Offset: w.offset, Detail: err.Error(), cause: err,
		}
		if res != nil {
			failure.HTTPStatus = res.StatusCode
		}
		return failure
	}
	if w.seen == nil {
		w.seen = make(map[string]bool)
	}
	w.pages++
	w.offset += n
	w.cursor = page.NextCursor
	w.seen[w.cursor] = true
	return nil
}

func replaceParityGeneration(ctx context.Context, client *http.Client, base string, cutoff int) (proof parityGeneration, err error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	method, action := http.MethodPost, "start"
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		res, err := request(ctx, client, method, fmt.Sprintf("%s/_capacity/compaction?action=%s&cutoff=%d", base, action, cutoff), nil)
		if err != nil {
			return proof, fmt.Errorf("compaction %s: %w", action, err)
		}
		if res.status != http.StatusOK {
			return proof, fmt.Errorf("compaction %s: HTTP %d: %.200q", action, res.status, res.body)
		}
		var state struct {
			State         string `json:"state"`
			Generation    uint64 `json:"generation"`
			CoveredRows   int    `json:"covered_rows"`
			CommittedRows int    `json:"committed_rows"`
		}
		if err := json.Unmarshal(res.body, &state); err != nil {
			return proof, fmt.Errorf("decode compaction %s: %w", action, err)
		}
		if action == "start" {
			proof.PreviousGeneration = state.Generation
		}
		proof.Generation, proof.CoveredRows = state.Generation, state.CoveredRows
		switch {
		case state.CommittedRows != cutoff:
			return proof, fmt.Errorf("compaction %s: committed %d rows, want %d", action, state.CommittedRows, cutoff)
		case action == "status" && state.State == "complete" && state.Generation > proof.PreviousGeneration && state.CoveredRows == cutoff:
			return proof, nil
		case state.State != "running" || state.Generation != proof.PreviousGeneration || state.CoveredRows >= cutoff:
			return proof, fmt.Errorf("unexpected compaction %s state: %+v", action, state)
		}
		method, action = http.MethodGet, "status"
		select {
		case <-ctx.Done():
			return proof, fmt.Errorf("wait for compaction publication: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestParityMatchesCapturedPrefix(t *testing.T) {
	t.Parallel()
	const earlier = "2019-12-31T23:59:59Z"
	const later = "2020-01-01T00:00:02Z"
	start := row{fixtureStart, "backfill [match]"}
	first := row{later, "first [match]"}
	duplicate := row{later, "duplicate [match]"}
	model, err := parityModel([]row{
		first, duplicate, duplicate, start,
		{earlier, "[match] before range"}, {fixtureEnd, "[match] at end"}, {later, "[MATCH]"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cutoff := model.HighWater()
	appended := row{later, "appended [match]"}
	if err := appendParityModel(model, []row{appended}); err != nil {
		t.Fatal(err)
	}
	pager, err := newReleasePager("http://127.0.0.1:80", &http.Client{}, 256)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		cutoff int
		want   []row
	}{
		{cutoff, []row{start, first, duplicate, duplicate}},
		{model.HighWater(), []row{start, first, duplicate, duplicate, appended}},
	} {
		got, err := parityMatches(pager, model, tc.cutoff, "[match]")
		if err != nil || !slices.Equal(got, tc.want) {
			t.Fatalf("cutoff %d: got %v, %v; want %v", tc.cutoff, got, err, tc.want)
		}
	}
}

func TestParityContinuationKeepsFirstFailure(t *testing.T) {
	t.Parallel()
	const ndjson = "Content-Type: application/x-ndjson\r\n"
	first := row{fixtureStart, "first"}
	second := row{fixtureStart, "second"}
	last := row{fixtureStart, "last"}
	token1, token2 := strings.Repeat("A", 44), strings.Repeat("B", 44)
	wireCursor := func(token string) string { return nextCursor + ": " + token + "\r\n" }
	wireRow := func(r row) string { return fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", r.TS, r.Line) }

	for _, tc := range []struct {
		name, response, problem string
		status                  int
	}{
		{"valid", lengthResponse(200, ndjson+wireCursor(token2), wireRow(second)), "", 200},
		{"wrong order", lengthResponse(200, ndjson+wireCursor(token2), wireRow(last)), "unexpected row", 200},
		{"early exhaustion", lengthResponse(200, ndjson, wireRow(second)), "walk ended", 200},
		{"repeated cursor", lengthResponse(200, ndjson+wireCursor(token1), wireRow(second)), "does not advance", 200},
		{"HTTP failure", lengthResponse(503, "", "query work budget exhausted\n"), "503", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, req *http.Request) {
				switch cursor := req.URL.Query().Get("cursor"); cursor {
				case "":
					writeWireResponse(t, w, lengthResponse(200, ndjson+wireCursor(token1), wireRow(first)))
				case token1:
					writeWireResponse(t, w, tc.response)
				case token2:
					writeWireResponse(t, w, lengthResponse(200, ndjson, wireRow(last)))
				default:
					t.Errorf("unexpected request cursor %q", cursor)
				}
			})
			pager, err := newReleasePager(server.URL, server.Client(), 256)
			if err != nil {
				t.Fatal(err)
			}
			walk := parityWalk{stage: "held_across_compaction", want: []row{first, second, last}}
			if err := walk.page(t.Context(), pager, 1); err != nil {
				t.Fatal(err)
			}
			err = walk.page(t.Context(), pager, 1)
			checkProblem(t, err, tc.problem)
			if err == nil {
				if err := walk.page(t.Context(), pager, 1); err != nil || walk.offset != 3 || walk.cursor != "" {
					t.Fatalf("exhaustion: offset=%d cursor=%q error=%v", walk.offset, walk.cursor, err)
				}
				return
			}
			var failure *parityWalkFailure
			if !errors.As(err, &failure) || failure.Stage != walk.stage || failure.Offset != 1 ||
				failure.Page != 2 || failure.Limit != 1 || failure.HTTPStatus != tc.status {
				t.Fatalf("incorrect failure details: %+v", failure)
			}
			if walk.offset != 1 || walk.cursor != token1 {
				t.Fatal("failed response advanced the verified walk")
			}
		})
	}
}

func TestReplaceParityGeneration(t *testing.T) {
	t.Parallel()
	const running = `{"state":"running","generation":3,"covered_rows":3,"committed_rows":7}`
	const complete = `{"state":"complete","generation":4,"covered_rows":7,"committed_rows":7}`
	for _, tc := range []struct {
		name, start, end, problem string
	}{
		{"complete", running, complete, ""},
		{"wrong cutoff", running, strings.Replace(complete, "covered_rows\":7", "covered_rows\":6", 1), "unexpected compaction status state"},
		{"same generation", running, strings.Replace(complete, "generation\":4", "generation\":3", 1), "unexpected compaction status state"},
		{"wrong committed count", strings.Replace(running, "committed_rows\":7", "committed_rows\":6", 1), "", "committed 6 rows"},
		{"bad reply", running, "{", "decode compaction status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != "/_capacity/compaction" || req.URL.Query().Get("cutoff") != "7" {
					t.Errorf("unexpected compaction URL %s", req.URL)
				}
				body := tc.start
				if req.Method == http.MethodGet && req.URL.Query().Get("action") == "status" {
					body = tc.end
				} else if req.Method != http.MethodPost || req.URL.Query().Get("action") != "start" {
					t.Errorf("unexpected compaction method %s and query %s", req.Method, req.URL.RawQuery)
				}
				if _, err := io.WriteString(w, body); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			proof, err := replaceParityGeneration(t.Context(), server.Client(), server.URL, 7)
			checkProblem(t, err, tc.problem)
			if err == nil && proof != (parityGeneration{3, 4, 7}) {
				t.Fatalf("unexpected generation evidence: %+v", proof)
			}
		})
	}
}

func TestReplaceParityGenerationStopsOnCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cancel()
	}))
	defer server.Close()
	_, err := replaceParityGeneration(ctx, server.Client(), server.URL, 7)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wanted canceled compaction probe, got %v", err)
	}
}
