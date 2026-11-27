package querymodel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/client"
	model "bog/verify/query-model"
)

func TestFixedBudgetTerminalReplayFailures(t *testing.T) {
	t.Parallel()
	workRefused := &model.FixedBudgetHTTPError{
		StatusCode: http.StatusServiceUnavailable, CleanBudgetRejection: true,
		Cause: errors.New("query work budget exhausted"),
	}
	expired := &model.FixedBudgetHTTPError{StatusCode: http.StatusGone, Cause: errors.New("query capture expired")}
	const openingToken = "first/+"
	const terminalToken = "terminal/+"
	const unexpectedToken = "unexpected/+"
	transport := fmt.Errorf("request cursor=%s response cursor=%s", url.QueryEscape(terminalToken), unexpectedToken)
	for _, tc := range []struct {
		name, diagnostic string
		change           func(*model.WirePage)
		err              error
		status           int
	}{
		{name: "budget refusal", diagnostic: "query work budget exhausted", err: workRefused, status: http.StatusServiceUnavailable},
		{name: "expired capture", diagnostic: "query capture expired", err: expired, status: http.StatusGone},
		{name: "transport with cursor details", diagnostic: "cursor=<cursor>", err: transport, change: func(p *model.WirePage) {
			p.NextCursor = unexpectedToken
		}},
		{name: "malformed success", diagnostic: "invalid NDJSON", status: http.StatusOK, err: &model.FixedBudgetHTTPError{
			StatusCode: http.StatusOK, Cause: errors.New("invalid NDJSON"),
		}},
		{name: "empty replay", diagnostic: "missing continuation", change: func(p *model.WirePage) {
			*p = model.WirePage{}
		}},
		{name: "repeated terminal row", diagnostic: "page has 2 rows", change: func(p *model.WirePage) {
			p.Rows = append(p.Rows, p.Rows[0])
			p.BodyBytes *= 2
		}},
		{name: "issued cursor", diagnostic: "after terminal exhaustion", change: func(p *model.WirePage) {
			p.NextCursor = unexpectedToken
		}},
		{name: "earlier match", diagnostic: "row 3 (append ordinal 30)", change: func(p *model.WirePage) {
			p.Rows[0].Line = "@next"
		}},
		{name: "leaked backfill", diagnostic: "row 3 (append ordinal 30)", change: func(p *model.WirePage) {
			p.Rows[0].Line = "@new late tie"
		}},
		{name: "wrong timestamp", diagnostic: "row 3 (append ordinal 30)", change: func(p *model.WirePage) {
			p.Rows[0].Time = p.Rows[0].Time.Add(time.Nanosecond)
		}},
		{name: "zero body bytes", diagnostic: "encoded body has 0 bytes for 1 rows", change: func(p *model.WirePage) {
			p.BodyBytes = 0
		}},
		{name: "over byte cap", diagnostic: "encoded body has 97 bytes", change: func(p *model.WirePage) {
			p.BodyBytes = 97
		}},
		{name: "changed encoded length", diagnostic: "replayed terminal body has 46 bytes, want 45", change: func(p *model.WirePage) {
			p.BodyBytes++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := model.FixedBudgetCases()[2]
			start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			opening := []model.Row{{Time: start, Line: "@last"}, {Time: start, Line: "@tied"}}
			middle := model.Row{Time: start.Add(3 * time.Second), Line: "@next"}
			terminal := model.Row{Time: start.Add(40 * time.Second), Line: "@late"}
			// Reuse the response buffer on both terminal fetches. Mutating it
			// during replay must still compare against the captured ledger.
			var terminalBuffer [1]model.Row
			var events []string
			var terminalRequest model.WireRequest
			result, err := model.RunFixedBudgetCase(t.Context(), c, func(_ context.Context, batch []model.Row) error {
				switch len(events) {
				case 0:
					if len(batch) != 46 {
						t.Fatalf("seed has %d rows", len(batch))
					}
					events = append(events, "seed")
				case 2, 5:
					if len(batch) != 1 {
						t.Fatalf("backfill has %d rows", len(batch))
					}
					events = append(events, batch[0].Line)
				default:
					t.Fatalf("unexpected acknowledgement after %v", events)
				}
				return nil
			}, func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
				switch len(events) {
				case 1:
					events = append(events, "opening")
					if req.Cursor != "" || req.Limit != 4 {
						t.Fatalf("opening request %+v", req)
					}
					return model.WirePage{Rows: opening, BodyBytes: 90, NextCursor: openingToken}, nil
				case 4:
					events = append(events, "resume")
					if req.Cursor != openingToken || req.Limit != 1 {
						t.Fatalf("first resume request %+v", req)
					}
					return model.WirePage{Rows: []model.Row{middle}, BodyBytes: 45, NextCursor: terminalToken}, nil
				case 6:
					events = append(events, "terminal")
					if req.Cursor != terminalToken || req.Limit != 1 {
						t.Fatalf("terminal resume request %+v", req)
					}
					terminalRequest = req
					terminalBuffer[0] = terminal
					return model.WirePage{Rows: terminalBuffer[:], BodyBytes: 45}, nil
				case 7:
					events = append(events, "replay")
					if req != terminalRequest {
						t.Fatalf("replay request %+v, want %+v", req, terminalRequest)
					}
					page := model.WirePage{Rows: terminalBuffer[:], BodyBytes: 45}
					if tc.change != nil {
						tc.change(&page)
					}
					return page, tc.err
				default:
					t.Fatalf("unexpected query after %v", events)
					return model.WirePage{}, nil
				}
			}, func(_ context.Context, cursor string) error {
				if cursor != openingToken || !slices.Equal(events, []string{"seed", "opening", "@new"}) {
					t.Fatalf("replacement hook got %q after %v", cursor, events)
				}
				events = append(events, "replacement")
				return nil
			})
			wantEvents := []string{"seed", "opening", "@new", "replacement", "resume", "@new late tie", "terminal", "replay"}
			wantStats := model.WireStats{Pages: 3, Rows: 4, BodyBytes: 180, ByteTrimmedPages: 1}
			status := tc.status
			if tc.err == nil {
				status = http.StatusOK
			}
			if err == nil || tc.err != nil && !errors.Is(err, tc.err) || !slices.Equal(events, wantEvents) ||
				result.Outcome != model.FixedBudgetIssuedCursor || result.Phase != "terminal_replay" ||
				result.Page != 3 || result.Limit != 1 || result.HTTPStatus != status || result.WireStats != wantStats {
				t.Fatalf("result=%+v events=%q error=%v", result, events, err)
			}
			details := []string{`fixture="sparse"`, "reference_work_budget=314", "phase=terminal_replay", "page 3 limit=1", tc.diagnostic}
			if status != 0 {
				details = append(details, fmt.Sprintf("HTTP %d", status))
			}
			for _, detail := range details {
				if !strings.Contains(err.Error(), detail) {
					t.Errorf("missing %q in %v", detail, err)
				}
			}
			for _, token := range []string{openingToken, terminalToken, url.QueryEscape(terminalToken), unexpectedToken} {
				if strings.Contains(err.Error(), token) {
					t.Errorf("cursor leaked in %v", err)
				}
			}
		})
	}
}

func TestFixedBudgetStallOverHTTP(t *testing.T) {
	t.Parallel()
	const (
		workRefusal   = "query work budget exhausted\n"
		issuingBudget = 17
	)
	for _, test := range []struct {
		name, phase   string
		budget        int
		refuseRequest int
		page          int
		acceptedPages int
		outcome       model.FixedBudgetOutcome
	}{
		{"opening rejection below boundary", "opening", issuingBudget - 1, 1, 1, 0, model.FixedBudgetCleanRejection},
		{"first resume stalls after replacement", "continuation", issuingBudget, 2, 2, 1, model.FixedBudgetIssuedCursor},
		{"terminal resume stalls after second backfill", "terminal_resume", issuingBudget, 3, 3, 2, model.FixedBudgetIssuedCursor},
		{"terminal replay stalls at issuing budget", "terminal_replay", issuingBudget, 4, 3, 3, model.FixedBudgetIssuedCursor},
		{"higher budget control", "terminal_replay", issuingBudget + 1, 0, 3, 3, model.FixedBudgetExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := model.FixedBudgetCases()[1]
			p := &fixturePeer{captures: make(map[string]scriptedCapture), trimCap: int(c.ResponseByteCap)}
			var requests atomic.Int32
			var replacement atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/query" {
					p.ServeHTTP(w, r)
					return
				}
				n := int(requests.Add(1))
				if n > 1 && !replacement.Load() {
					http.Error(w, "replacement hook missing", http.StatusInternalServerError)
					return
				}
				if test.budget <= issuingBudget && n == test.refuseRequest {
					w.Header().Set("Content-Length", strconv.Itoa(len(workRefusal)))
					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(http.StatusServiceUnavailable)
					if _, err := w.Write([]byte(workRefusal)); err != nil {
						t.Errorf("write refusal: %v", err)
					}
					return
				}
				p.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			clientConn, fetch := useWireClient(t, server.URL, c.ResponseByteCap)
			acks := 0
			acknowledge := func(ctx context.Context, rows []model.Row) error {
				var body bytes.Buffer
				encoder := json.NewEncoder(&body)
				for _, row := range rows {
					if err := encoder.Encode(httpFixtureRow{Time: row.Time, Line: row.Line}); err != nil {
						return err
					}
				}
				outcome, err := clientConn.Ingest(ctx, body.Bytes())
				if err != nil {
					return err
				}
				if outcome != client.IngestCommitted {
					return fmt.Errorf("ingest outcome %v", outcome)
				}
				acks++
				return nil
			}
			var verified []string
			result, err := model.RunFixedBudgetCase(t.Context(), c, acknowledge, func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
				page, err := fetch(ctx, req)
				if err != nil {
					if status, ok := err.(*client.HTTPError); ok {
						// The scripted peer sends an explicit fixed-length refusal
						// with no cursor. The client has read and closed its body.
						return page, &model.FixedBudgetHTTPError{
							StatusCode: status.StatusCode, Cause: err,
							CleanBudgetRejection: status.StatusCode == http.StatusServiceUnavailable &&
								status.Body == workRefusal && status.Cause == nil,
						}
					}
					return page, err
				}
				for _, row := range page.Rows {
					verified = append(verified, row.Line)
				}
				return page, nil
			}, func(_ context.Context, token string) error {
				if acks != 2 || token != httpToken(1) || replacement.Swap(true) {
					return errors.New("replacement did not follow the first acknowledged backfill")
				}
				return nil
			})
			wantRequests := test.refuseRequest
			wantStatus := http.StatusServiceUnavailable
			wantRows := []string{"c", "c", "b"}[:test.acceptedPages]
			if test.refuseRequest == 0 {
				wantRequests, wantStatus = 4, http.StatusOK
				wantRows = []string{"c", "c", "b", "b"}
			}
			if result.Outcome != test.outcome || result.Phase != test.phase || result.Page != test.page ||
				result.Pages != test.acceptedPages || result.Rows != test.acceptedPages || result.Limit != 1 ||
				result.HTTPStatus != wantStatus || int(requests.Load()) != wantRequests ||
				acks != min(wantRequests, 3) || !slices.Equal(verified, wantRows) {
				t.Fatalf("trial work_budget=%d result=%+v requests=%d acks=%d rows=%q error=%v",
					test.budget, result, requests.Load(), acks, verified, err)
			}
			if test.outcome != model.FixedBudgetIssuedCursor {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var status *model.FixedBudgetHTTPError
			attempt := fmt.Sprintf("phase=%s page %d limit=1 HTTP 503", test.phase, test.page)
			if !errors.As(err, &status) || !status.CleanBudgetRejection || !strings.Contains(err.Error(), attempt) {
				t.Fatalf("trial work_budget=%d expected issuing-budget stall at %s, got %v", test.budget, attempt, err)
			}
		})
	}
}

func TestFixedBudgetReplayUsesScenarioDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := model.FixedBudgetCases()[0]
	var deadline time.Time
	var seed []model.Row
	acks, fetches := 0, 0
	checkDeadline := func(ctx context.Context) {
		t.Helper()
		got, ok := ctx.Deadline()
		if !ok || !got.Equal(deadline) {
			t.Fatalf("deadline %s (present=%t), want %s", got, ok, deadline)
		}
	}
	result, err := model.RunFixedBudgetCase(ctx, c, func(ctx context.Context, batch []model.Row) error {
		acks++
		if acks == 1 {
			deadline, _ = ctx.Deadline()
			seed = slices.Clone(batch)
		}
		checkDeadline(ctx)
		return nil
	}, func(ctx context.Context, _ model.WireRequest) (model.WirePage, error) {
		fetches++
		checkDeadline(ctx)
		if fetches > 4 {
			t.Fatal("retried canceled replay")
		}
		token := ""
		if fetches < 3 {
			token = fmt.Sprintf("token-%d", fetches)
		}
		page := model.WirePage{Rows: seed[min(fetches, 3)-1 : min(fetches, 3)], BodyBytes: 41, NextCursor: token}
		if fetches == 4 {
			cancel()
			<-ctx.Done()
			// Even a valid page cannot complete a canceled replay.
		}
		return page, nil
	}, func(ctx context.Context, _ string) error {
		checkDeadline(ctx)
		return nil
	})
	if !errors.Is(err, context.Canceled) || result.Outcome != model.FixedBudgetIssuedCursor ||
		result.Phase != "terminal_replay" || result.Page != 3 || result.Pages != 3 ||
		result.HTTPStatus != http.StatusOK || fetches != 4 || acks != 3 {
		t.Fatalf("result=%+v fetches=%d acks=%d error=%v", result, fetches, acks, err)
	}
}

func TestFixedBudgetCaseRequiresExpectedTerminalPage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		firstLimit int
		firstRows  int
		pageRows   [][]int
		cursors    []string
		diagnostic string
		cause      error
		phase      string
		wantStats  model.WireStats
	}{
		{
			name: "terminal too early", firstLimit: 4, firstRows: 3,
			pageRows: [][]int{{0, 1, 2}, {30}}, cursors: []string{"first", ""},
			diagnostic: "expected to exhaust on page 3, exhausted on page 2", phase: "continuation",
			wantStats: model.WireStats{Pages: 2, Rows: 4, BodyBytes: 180, ByteTrimmedPages: 1},
		},
		{
			name: "no terminal within walk bound", firstLimit: 1, firstRows: 1,
			pageRows: [][]int{{0}, {1}, {2}}, cursors: []string{"first", "second", "third"},
			diagnostic: "page budget exhausted before terminal exhaustion", cause: model.ErrWirePageBudget, phase: "terminal_resume",
			wantStats: model.WireStats{Pages: 3, Rows: 3, BodyBytes: 135},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := model.FixedBudgetCases()[2]
			c.FirstLimit, c.FirstRows, c.ResponseByteCap = tc.firstLimit, tc.firstRows, 140
			acks, fetches, hooks := 0, 0, 0
			var seed []model.Row
			result, err := model.RunFixedBudgetCase(t.Context(), c, func(_ context.Context, rows []model.Row) error {
				acks++
				if acks == 1 {
					seed = slices.Clone(rows)
				}
				return nil
			}, func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
				fetches++
				if fetches > len(tc.pageRows) {
					t.Fatal("requested replay without an accepted terminal page at the expected boundary")
				}
				var rows []model.Row
				for _, i := range tc.pageRows[fetches-1] {
					rows = append(rows, seed[i])
				}
				if fetches > 1 && (req.Cursor != tc.cursors[fetches-2] || req.Limit != c.ResumeLimit) {
					t.Fatalf("continuation request %+v", req)
				}
				return model.WirePage{
					Rows: rows, BodyBytes: 45 * int64(len(rows)), NextCursor: tc.cursors[fetches-1],
				}, nil
			}, func(_ context.Context, cursor string) error {
				hooks++
				if cursor != "first" || acks != 2 {
					return fmt.Errorf("unexpected replacement state after %d acknowledgements", acks)
				}
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.diagnostic) || tc.cause != nil && !errors.Is(err, tc.cause) ||
				result.Outcome != model.FixedBudgetIssuedCursor || result.Phase != tc.phase ||
				result.Page != len(tc.pageRows) || result.HTTPStatus != http.StatusOK ||
				result.WireStats != tc.wantStats || acks != len(tc.pageRows) || fetches != len(tc.pageRows) || hooks != 1 {
				t.Fatalf("result=%+v acks=%d fetches=%d hooks=%d error=%v", result, acks, fetches, hooks, err)
			}
		})
	}
}
