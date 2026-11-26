package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

func TestShippedQueryFixedBudgetWalks(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(second int, line string) store.Row {
		return store.Row{TS: start.Add(time.Duration(second) * time.Second), Line: line}
	}
	ties := []store.Row{at(0, "c"), at(0, "a"), at(0, "b")}
	sparse := []store.Row{at(0, "@last"), at(0, "@tied"), at(3, "@next")}
	for second := 4; second <= 30; second++ {
		sparse = append(sparse, at(second, "miss"))
	}
	sparse = append(sparse, at(40, "@late"))
	for second := 41; second <= 55; second++ {
		sparse = append(sparse, at(second, "miss"))
	}
	for _, tc := range []struct {
		name, literal string
		seed, want    []store.Row
		byteCap       int
		firstLimit    int
		firstRows     int
		reject        []uint64
		issue         []uint64
	}{
		{"ties", "", ties, ties, DefaultQueryMaxResponseBytes, 1, 1, []uint64{11}, []uint64{64}},
		{"sparse", "@", sparse, []store.Row{sparse[0], sparse[1], sparse[2], at(40, "@late")}, 96, 4, 2, nil, []uint64{314, 512}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := url.Values{
				"start": {start.Format(time.RFC3339)},
				"end":   {start.Add(10 * time.Minute).Format(time.RFC3339)},
			}
			if tc.literal != "" {
				filter.Set("contains", tc.literal)
			}
			query := "/query?" + filter.Encode()
			want := make([]string, len(tc.want))
			for i, row := range tc.want {
				want[i] = jsonRow(t, row)
			}

			// Probe against fresh handlers at each budget, like the verifier.
			// Every issuance is followed through exhaustion on the same server.
			probe := func(t *testing.T, budget uint64) bool {
				t.Helper()
				request := startFixedBudgetServer(t, budget, tc.byteCap)
				var body strings.Builder
				for _, row := range tc.seed {
					body.WriteString(jsonRow(t, row))
				}
				ingest, response := request(http.MethodPost, "/ingest", body.String())
				if ingest.StatusCode != http.StatusNoContent || response != "" {
					t.Fatalf("ingest: status=%d body=%q", ingest.StatusCode, response)
				}

				page, response := request(http.MethodGet, query+"&page=1&limit="+strconv.Itoa(tc.firstLimit), "")
				if page.StatusCode == http.StatusServiceUnavailable {
					checkFixedBudgetFrame(t, page, response, "query work budget exhausted\n", "text/plain; charset=utf-8")
					if len(page.Header.Values("Bog-Next-Cursor")) != 0 {
						t.Fatal("rejected first page exposed a cursor")
					}
					return false
				}
				if page.StatusCode != http.StatusOK {
					t.Fatalf("budget %d: status=%d body=%q", budget, page.StatusCode, response)
				}
				checkFixedBudgetFrame(t, page, response, strings.Join(want[:tc.firstRows], ""), "application/x-ndjson")
				token := checkFixedBudgetCursor(t, page, true)

				// Ingest a match before the first captured timestamp and
				// another tie. Retries must still see the original membership.
				backfill := jsonRow(t, at(-1, tc.literal+"backfill")) + jsonRow(t, at(0, tc.literal+"new tie"))
				ingest, response = request(http.MethodPost, "/ingest", backfill)
				if ingest.StatusCode != http.StatusNoContent || response != "" {
					t.Fatalf("backfill: status=%d body=%q", ingest.StatusCode, response)
				}
				seen := map[string]bool{token: true}
				for offset := tc.firstRows; offset < len(want); offset++ {
					path := query + "&limit=1&cursor=" + url.QueryEscape(token)
					page, response = request(http.MethodGet, path, "")
					if page.StatusCode != http.StatusOK {
						t.Fatalf("budget %d, offset %d: status=%d body=%q", budget, offset, page.StatusCode, response)
					}
					checkFixedBudgetFrame(t, page, response, want[offset], "application/x-ndjson")
					token = checkFixedBudgetCursor(t, page, offset+1 < len(want))
					if token != "" && seen[token] {
						t.Fatalf("budget %d, offset %d: repeated cursor", budget, offset)
					}
					if token != "" {
						seen[token] = true
					}
					retry, retryBody := request(http.MethodGet, path, "")
					checkFixedBudgetFrame(t, retry, retryBody, want[offset], "application/x-ndjson")
					if retry.StatusCode != http.StatusOK || checkFixedBudgetCursor(t, retry, token != "") != token {
						t.Fatalf("budget %d, offset %d: retry changed the continuation", budget, offset)
					}
				}
				return true
			}

			check := func(budget uint64, wantIssue bool) {
				t.Helper()
				t.Run(fmt.Sprintf("budget_%d", budget), func(t *testing.T) {
					if issued := probe(t, budget); issued != wantIssue {
						t.Fatalf("budget %d issued a cursor = %t, want %t", budget, issued, wantIssue)
					}
				})
			}
			for _, budget := range tc.reject {
				check(budget, false)
			}
			for _, budget := range tc.issue {
				check(budget, true)
			}
			low, high := uint64(1), tc.issue[0]
			for low < high {
				budget := low + (high-low)/2
				var issued bool
				if !t.Run(fmt.Sprintf("probe_%d", budget), func(t *testing.T) {
					issued = probe(t, budget)
				}) {
					return
				}
				if issued {
					high = budget
				} else {
					low = budget + 1
				}
			}
			t.Logf("first issuing budget = %d", low)
			check(low-1, false)
			check(low, true)
		})
	}
}

func startFixedBudgetServer(t *testing.T, budget uint64, byteCap int) func(string, string, string) (*http.Response, string) {
	t.Helper()
	s := new(store.Store)
	handler := NewWithQueryLimits(s, publishingAppender{s}, byteCap, 1, budget)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
		defer cancel()
		if err := handler.(*API).Close(ctx); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	httpClient := server.Client()
	return func(method, path, body string) (*http.Response, string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return res, string(readHTTPTestBody(t, res.Body))
	}
}

func checkFixedBudgetFrame(t *testing.T, page *http.Response, body, want, contentType string) {
	t.Helper()
	if body != want || page.Header.Get("Content-Type") != contentType ||
		page.Header.Get("Content-Length") != strconv.Itoa(len(want)) ||
		page.ContentLength != int64(len(want)) || len(page.TransferEncoding) != 0 || len(page.Trailer) != 0 {
		t.Fatalf("status=%d headers=%v transfer=%v trailers=%v body=%q, want body %q",
			page.StatusCode, page.Header, page.TransferEncoding, page.Trailer, body, want)
	}
}

func checkFixedBudgetCursor(t *testing.T, page *http.Response, more bool) string {
	t.Helper()
	values := page.Header.Values("Bog-Next-Cursor")
	if !more && len(values) == 0 {
		return ""
	}
	if !more || len(values) != 1 || values[0] == "" {
		t.Fatalf("more=%t cursor=%q", more, values)
	}
	return values[0]
}
