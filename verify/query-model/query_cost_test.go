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
				fixture := &runtimeQueryFixture{retained: retained, budget: budget}
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
				if sample.ScannedBytes != nil || sample.ScannedBytesPerReturnedRow != nil || sample.Unavailable["scanned_bytes"] == "" {
					t.Fatalf("expected scan bytes to be explicitly unavailable: %+v", sample)
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
			got.RejectedWorkBudgetUnits < 1 || got.MinimumIssuingWorkBudgetUnits != got.RejectedWorkBudgetUnits+1 ||
			got.Unavailable["charged_work_units"] == "" {
			t.Fatalf("incomplete %s evidence: %+v", want.name, got)
		}
	}
	empty, sparse, broad := report.Samples[0], report.Samples[1], report.Samples[2]
	if empty.ResponseBytes != 0 || empty.Pages != 1 || sparse.ResponseBytes <= 0 || sparse.Pages != 3 ||
		broad.ResponseBytes <= sparse.ResponseBytes || broad.Pages <= sparse.Pages {
		t.Fatalf("unexpected wire totals: %+v", report.Samples)
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
		{name: "meter error", fail: "meter", want: "observe scanned bytes"},
		{name: "meter source missing", fail: "source", want: "scan byte observation requires a source"},
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
				var terminalSeen, published bool
				var minimum int
				runErr := run(model.QueryTraceAdapter{
					Acknowledge: func(ctx context.Context, rows []model.Row) error {
						batches = append(batches, slices.Clone(rows))
						return acknowledgeFixtureBatch(ctx, c, rows)
					},
					Publish: func(ctx context.Context, cutoff int) error {
						peer.mu.Lock()
						defer peer.mu.Unlock()
						if published || cutoff != len(peer.rows) || cutoff != 112 || peer.scannedBytes != 0 {
							return fmt.Errorf("bad seed publication at cutoff %d", cutoff)
						}
						published = true
						return ctx.Err()
					},
					Fetch: func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
						if !published {
							return model.WirePage{}, errors.New("fetch before seed publication")
						}
						switch req.Filter.Contains {
						case "[no fixture row]":
							minimum = 7
						case "needle [wire]":
							minimum = 17
						case "":
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
						peer.mu.Unlock()
						if tc.fail == "resume" && req.Filter.Contains == "" && budget == minimum && req.Cursor != "" {
							return model.WirePage{}, &model.FixedBudgetHTTPError{StatusCode: http.StatusServiceUnavailable, CleanBudgetRejection: true}
						}
						page, err := fetch(ctx, req)
						if err == nil && page.NextCursor == "" {
							switch {
							case tc.fail == "empty" && req.Filter.Contains == "[no fixture row]":
								page.EncodedBody = nil
							case tc.fail == "replay" && req.Filter.Contains == "" && budget == minimum && terminalSeen:
								page.EncodedBody[0] ^= 1
							}
							terminalSeen = true
						}
						return page, err
					},
					ObserveScanBytes: func(ctx context.Context) (model.QueryScanBytes, error) {
						if tc.fail == "meter" {
							return model.QueryScanBytes{}, errors.New("meter failed")
						}
						if tc.fail == "source" {
							return model.QueryScanBytes{Source: " "}, nil
						}
						peer.mu.Lock()
						defer peer.mu.Unlock()
						if expectedScanBytes == 0 {
							for _, encoded := range peer.scanInput {
								expectedScanBytes += uint64(len(encoded))
							}
						}
						return model.QueryScanBytes{Source: source, Bytes: peer.scannedBytes}, ctx.Err()
					},
				})
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
					expectedScanBytes == 0 || sample.ScannedBytes.Bytes != expectedScanBytes || sample.Unavailable["scanned_bytes"] != "" {
					t.Fatalf("incorrect budget or scan evidence: %+v", sample)
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
