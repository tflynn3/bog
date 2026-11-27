package querymodel_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	model "bog/verify/query-model"
)

func TestQueryCostsBothPageSources(t *testing.T) {
	t.Parallel()
	for _, retained := range []bool{false, true} {
		t.Run(pageSourceName(retained), func(t *testing.T) {
			t.Parallel()
			report, err := model.VerifyQueryCosts(t.Context(), seededTraceConfig(2709), func(_ context.Context, budget int, run func(model.QueryTraceAdapter) error) (err error) {
				fixture := &runtimeQueryFixture{retained: retained, budget: budget, meterScans: retained}
				if err := fixture.start(); err != nil {
					return errors.Join(err, fixture.stop())
				}
				defer func() { err = errors.Join(err, fixture.stop()) }()
				return run(fixture.adapter())
			})
			if err != nil {
				t.Fatal(err)
			}
			checkQueryCostReport(t, report)
			for _, sample := range report.Samples {
				checkQueryCostAvailability(t, sample, retained)
				if sample.Replacement != nil {
					wantCutoff := 0
					if retained {
						wantCutoff = 114
					}
					if sample.Replacement.PublishedReplacementCutoff != wantCutoff ||
						!reflect.DeepEqual(sample.QueryCostWalk, sample.Replacement.Held) {
						t.Fatalf("held evidence changed across replacement: %+v", sample)
					}
				}
			}
			if retained {
				broad := report.Samples[2]
				opening, resume := broad.PageObservations[0], broad.PageObservations[1]
				if opening.ScannedBytes.Value <= resume.ScannedBytes.Value || opening.ChargedWorkUnits.Value <= resume.ChargedWorkUnits.Value ||
					resume.ScannedBytes.Value <= uint64(resume.ResponseBytes) {
					t.Fatalf("missing opening certification or replay scan input: opening=%+v resume=%+v", opening, resume)
				}
			}
			logQueryReport(t, "fixture_query_cost", report)
		})
	}
}

func checkQueryCostReport(t *testing.T, report model.QueryCostReport) {
	t.Helper()
	if report.AcknowledgedRows != 112 || len(report.Samples) != 3 {
		t.Fatalf("incomplete report: %+v", report)
	}
	for i, want := range []struct {
		name string
		rows int
	}{{"empty", 0}, {"sparse", 5}, {"broad", 110}} {
		got := report.Samples[i]
		if got.Query != want.name || got.ReturnedRows != want.rows ||
			got.RejectedWorkBudgetUnits < 1 || got.MinimumIssuingWorkBudgetUnits != got.RejectedWorkBudgetUnits+1 {
			t.Fatalf("incomplete %s evidence: %+v", want.name, got)
		}
		checkQueryCostWalk(t, got.QueryCostWalk, report.RowLimits)
		if i == 0 {
			if got.Replacement != nil {
				t.Fatal("absent literal issued a held capture")
			}
			continue
		}
		if got.Replacement == nil || got.Replacement.AcknowledgedRows != 115 ||
			got.Replacement.WorkBudgetUnits != report.MaxWorkBudget ||
			got.Replacement.Held.ReturnedRows != want.rows || got.Replacement.Fresh.ReturnedRows != want.rows+3 {
			t.Fatalf("incomplete replacement walks: %+v", got)
		}
		checkQueryCostWalk(t, got.Replacement.Held, report.RowLimits)
		checkQueryCostWalk(t, got.Replacement.Fresh, report.RowLimits)
	}
	empty, sparse, broad := report.Samples[0], report.Samples[1], report.Samples[2]
	if empty.ResponseBytes != 0 || empty.Pages != 1 || sparse.ResponseBytes <= 0 || sparse.Pages != 3 ||
		broad.ResponseBytes <= sparse.ResponseBytes || broad.Pages <= sparse.Pages {
		t.Fatalf("unexpected wire totals: %+v", report.Samples)
	}
}

func checkQueryCostWalk(t *testing.T, walk model.QueryCostWalk, limits []int) {
	t.Helper()
	if walk.Pages == 0 || walk.Pages != len(walk.PageObservations) || walk.ResponseBytesSource == "" {
		t.Fatalf("incomplete page evidence: %+v", walk)
	}
	var rows int
	var response int64
	var scanned, charged uint64
	for i, page := range walk.PageObservations {
		if page.Page != i+1 || page.RowLimit != limits[i%len(limits)] || page.ResponseBytes > 256 {
			t.Fatalf("invalid page evidence: %+v", page)
		}
		rows += page.ReturnedRows
		response += page.ResponseBytes
		if page.ScannedBytes != nil {
			scanned += page.ScannedBytes.Value
		}
		if page.ChargedWorkUnits != nil {
			charged += page.ChargedWorkUnits.Value
		}
	}
	if walk.ReturnedRows != rows || walk.ResponseBytes != response ||
		walk.ScannedBytes != nil && walk.ScannedBytes.Value != scanned ||
		walk.ChargedWorkUnits != nil && walk.ChargedWorkUnits.Value != charged {
		t.Fatalf("walk counters do not sum the pages: %+v", walk)
	}
}

func checkQueryCostAvailability(t *testing.T, sample model.QueryCostSample, observed bool) {
	t.Helper()
	walks := []model.QueryCostWalk{sample.QueryCostWalk}
	budgets := []int{sample.MinimumIssuingWorkBudgetUnits}
	if sample.Replacement != nil {
		walks = append(walks, sample.Replacement.Held, sample.Replacement.Fresh)
		budgets = append(budgets, sample.Replacement.WorkBudgetUnits, sample.Replacement.WorkBudgetUnits)
	}
	for i, walk := range walks {
		if (walk.ScannedBytes != nil) != observed || (walk.ChargedWorkUnits != nil) != observed ||
			(walk.Unavailable["scanned_bytes"] == "") != observed || (walk.Unavailable["charged_work_units"] == "") != observed ||
			(walk.ScannedBytesPerReturnedRow != nil) != (observed && walk.ReturnedRows > 0) {
			t.Fatalf("invalid walk availability: %+v", walk)
		}
		for _, page := range walk.PageObservations {
			if (page.ScannedBytes != nil) != observed || (page.ChargedWorkUnits != nil) != observed ||
				(page.Unavailable["scanned_bytes"] == "") != observed || (page.Unavailable["charged_work_units"] == "") != observed {
				t.Fatalf("invalid page availability: %+v", page)
			}
			if observed && (page.ScannedBytes.Source != fixtureScanBytesSource || page.ChargedWorkUnits.Source != fixtureScanWorkSource ||
				page.ScannedBytes.Value == 0 || page.ScannedBytes.Value > page.ChargedWorkUnits.Value ||
				page.ChargedWorkUnits.Value > uint64(budgets[i])) {
				t.Fatalf("invalid fixture counter: %+v", page)
			}
		}
	}
}

func TestQueryCostsMeteredWireTrial(t *testing.T) {
	t.Parallel()
	const source = "scripted HTTP fixture encoded JSON row payload decoded during capture"
	for _, tc := range []struct {
		name, fail, want string
	}{
		{name: "metered issuing walk"},
		{name: "refusal leaks cursor", fail: "cursor", want: "ordered walk"},
		{name: "refusal has read or close error", fail: "cause", want: "ordered walk"},
		{name: "broad continuation refusal", fail: "resume", want: "broad query: work budget 23: ordered walk"},
		{name: "empty body must be witnessed", fail: "empty", want: "terminal response is missing its encoded body witness"},
		{name: "broad terminal replay drift", fail: "replay", want: "broad query: work budget 23: terminal replay"},
		{name: "meter error", fail: "meter", want: "meter failed"},
		{name: "meter source missing", fail: "source", want: "scanned_bytes observation requires a source"},
		{name: "meter source changed", fail: "mixed", want: "scanned_bytes observation changed source"},
		{name: "meter overflow", fail: "overflow", want: "scanned_bytes observation overflows walk total"},
		{name: "fresh terminal replay drift", fail: "fresh replay", want: "sparse query backfill trial: work budget 32: fresh walk: terminal replay"},
		{name: "incorrect replacement cutoff", fail: "cutoff", want: "bad replacement cutoff 114 after 112"},
		{name: "launcher cleanup error", fail: "cleanup", want: "cleanup failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := seededTraceConfig(2709)
			cfg.MaxWorkBudget = 32
			var seed [][]model.Row
			var expectedScanBytes uint64
			seen := make(map[string]map[int]bool)
			report, err := model.VerifyQueryCosts(t.Context(), cfg, func(_ context.Context, budget int, run func(model.QueryTraceAdapter) error) error {
				peer := &fixturePeer{trimCap: 256, captures: make(map[string]scriptedCapture), scanInput: make([]json.RawMessage, 0)}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					peer.mu.Lock()
					refused := peer.refuseOpening
					peer.mu.Unlock()
					if refused && r.Method == http.MethodGet && r.URL.Query().Get("page") == "1" {
						switch tc.fail {
						case "cursor":
							w.Header().Set(cursorHeader, httpToken(1))
						case "cause":
							const body = "query work budget exhausted\n"
							w.Header().Set("Content-Type", "text/plain")
							w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
							w.WriteHeader(http.StatusServiceUnavailable)
							if _, err := io.WriteString(w, body); err != nil {
								t.Errorf("write truncated refusal: %v", err)
							}
							return
						}
					}
					peer.ServeHTTP(w, r)
				}))
				defer server.Close()
				c, fetch, err := newWitnessClient(server.URL, server.Client().Transport, 256)
				if err != nil {
					return err
				}
				defer c.CloseIdleConnections()
				var batches [][]model.Row
				var terminalSeen bool
				var cutoff, seedBatchCount int
				var observedRequests int
				var minimum int
				fetchPage := func(ctx context.Context, req model.WireRequest, replay bool) (model.WirePage, error) {
					if cutoff == 0 {
						return model.WirePage{}, errors.New("fetch before seed publication")
					}
					switch req.Filter.Contains {
					case "[no fixture row]":
						minimum = 7
					case "needle [wire]":
						minimum = 17
					case "[all]":
						minimum = 23
					default:
						return model.WirePage{}, errors.New("unexpected filter")
					}
					if seen[req.Filter.Contains] == nil {
						seen[req.Filter.Contains] = make(map[int]bool)
					}
					seen[req.Filter.Contains][budget] = true
					peer.mu.Lock()
					peer.refuseOpening = budget < minimum
					augmented := len(peer.rows) == 115
					peer.mu.Unlock()
					if tc.fail == "resume" && req.Filter.Contains == "[all]" && budget == minimum && req.Cursor != "" {
						return model.WirePage{}, &model.FixedBudgetHTTPError{StatusCode: http.StatusServiceUnavailable, CleanBudgetRejection: true}
					}
					page, err := fetch(ctx, req)
					if err == nil && page.NextCursor == "" {
						switch {
						case tc.fail == "empty" && req.Filter.Contains == "[no fixture row]":
							page.EncodedBody = nil
						case tc.fail == "replay" && req.Filter.Contains == "[all]" && budget == minimum && replay:
							page.EncodedBody[0] ^= 1
						case tc.fail == "fresh replay" && augmented && replay && terminalSeen:
							page.EncodedBody[0] ^= 1
						}
						terminalSeen = terminalSeen || replay
					}
					return page, err
				}
				runErr := run(model.QueryTraceAdapter{
					Acknowledge: func(ctx context.Context, rows []model.Row) error {
						batches = append(batches, slices.Clone(rows))
						return acknowledgeFixtureBatch(ctx, c, rows)
					},
					Publish: func(ctx context.Context, requested int) error {
						peer.mu.Lock()
						defer peer.mu.Unlock()
						if cutoff == 0 {
							if requested != len(peer.rows) || requested != 112 || peer.scannedBytes != 0 {
								return fmt.Errorf("bad seed publication at cutoff %d", requested)
							}
							seedBatchCount = len(batches)
						} else if tc.fail == "cutoff" || cutoff != 112 || requested != 114 || requested != len(peer.rows) || observedRequests != 1 {
							return fmt.Errorf("bad replacement cutoff %d after %d", requested, cutoff)
						}
						cutoff = requested
						return ctx.Err()
					},
					Fetch: func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
						return fetchPage(ctx, req, true)
					},
					FetchObserved: func(ctx context.Context, req model.WireRequest) (model.WirePage, model.QueryScanObservation, error) {
						peer.mu.Lock()
						before := peer.scannedBytes
						peer.mu.Unlock()
						page, err := fetchPage(ctx, req, false)
						if err != nil {
							return page, model.QueryScanObservation{}, err
						}
						observedRequests++
						if tc.fail == "meter" {
							return page, model.QueryScanObservation{}, errors.New("meter failed")
						}
						peer.mu.Lock()
						defer peer.mu.Unlock()
						if expectedScanBytes == 0 {
							for _, encoded := range peer.scanInput {
								expectedScanBytes += uint64(len(encoded))
							}
						}
						bytes := model.QueryCostCounter{Source: source, Value: peer.scannedBytes - before}
						switch {
						case tc.fail == "source":
							bytes.Source = " "
						case tc.fail == "mixed" && req.Cursor != "":
							bytes.Source = "another boundary"
						case tc.fail == "overflow" && req.Cursor != "":
							bytes.Value = ^uint64(0)
						}
						return page, model.QueryScanObservation{
							ScannedBytes:     &bytes,
							ChargedWorkUnits: &model.QueryCostCounter{Source: "scripted units per accepted request", Value: 2},
						}, nil
					},
				})
				batches = batches[:seedBatchCount]
				if seed == nil {
					seed = batches
				}
				if !reflect.DeepEqual(batches, seed) {
					return errors.Join(runErr, errors.New("acknowledged seed differed between probes"))
				}
				if tc.fail == "cleanup" && budget < minimum {
					return errors.Join(runErr, errors.New("cleanup failed"))
				}
				return runErr
			})
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error=%v, want %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			checkQueryCostReport(t, report)
			for i, sample := range report.Samples {
				want := []int{7, 17, 23}[i]
				if sample.MinimumIssuingWorkBudgetUnits != want || !seen[sample.Contains][want-1] || !seen[sample.Contains][want] ||
					sample.ScannedBytes == nil || sample.ScannedBytes.Source != source ||
					expectedScanBytes == 0 || sample.ScannedBytes.Value != expectedScanBytes || sample.Unavailable["scanned_bytes"] != "" ||
					sample.ChargedWorkUnits == nil || sample.ChargedWorkUnits.Value != uint64(2*sample.Pages) {
					t.Fatalf("incorrect budget or scan evidence: %+v", sample)
				}
				for j, page := range sample.PageObservations {
					wantBytes := uint64(0)
					if j == 0 {
						wantBytes = expectedScanBytes
					}
					if page.ScannedBytes == nil || page.ScannedBytes.Value != wantBytes || page.ChargedWorkUnits.Value != 2 {
						t.Fatalf("incorrect page counters: %+v", page)
					}
				}
				if sample.Replacement != nil && sample.Replacement.PublishedReplacementCutoff != 114 {
					t.Fatalf("incorrect replacement: %+v", sample.Replacement)
				}
				if sample.ReturnedRows == 0 {
					if sample.ScannedBytesPerReturnedRow != nil {
						t.Fatalf("empty query reported a scan ratio: %+v", sample)
					}
				} else if sample.ScannedBytesPerReturnedRow == nil ||
					*sample.ScannedBytesPerReturnedRow != float64(expectedScanBytes)/float64(sample.ReturnedRows) {
					t.Fatalf("incorrect scan ratio: %+v", sample)
				}
			}
			logQueryReport(t, "metered_query_cost", report)
		})
	}
}
