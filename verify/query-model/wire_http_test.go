package querymodel_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/client"
	model "bog/verify/query-model"
)

const cursorHeader = "Bog-Next-Cursor"

type httpFixtureRow struct {
	Time time.Time `json:"ts"`
	Line string    `json:"line"`
}

func useWireClient(t *testing.T, endpoint string, cap int64) (*client.Client, model.FetchWirePage) {
	t.Helper()
	c, err := client.New(endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.MaxPageBytes = cap
	t.Cleanup(c.CloseIdleConnections)
	return c, func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
		if req.MaxPageBytes != c.MaxPageBytes {
			return model.WirePage{}, errors.New("test client cap mismatch")
		}
		q := client.Query{Start: req.Filter.Start, End: req.Filter.End, Contains: req.Filter.Contains, Limit: req.Limit}
		p, err := c.FetchPage(ctx, q, req.Cursor)
		if err != nil {
			return model.WirePage{}, err
		}
		var result model.WirePage
		result.NextCursor = p.NextCursor
		// This peer uses a known encoder. The production client validates
		// actual framing and enforces the wire ceiling before this conversion.
		for _, r := range p.Rows {
			result.Rows = append(result.Rows, model.Row{Time: r.TS, Line: r.Line})
			wire, err := json.Marshal(httpFixtureRow{r.TS, r.Line})
			if err != nil {
				return model.WirePage{}, err
			}
			result.BodyBytes += int64(len(wire) + 1)
		}
		return result, nil
	}
}

func httpToken(n int) string {
	// It has valid public header syntax, with an uninterpreted payload.
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("scripted-peer-continuation-token:%016d", n)))
}

type scriptedCapture struct {
	start, end time.Time
	literal    string
	rows       []httpFixtureRow
	after      int
}

type fixturePeer struct {
	mu       sync.Mutex
	rows     []httpFixtureRow
	captures map[string]scriptedCapture
	serial   int
	trimCap  int
	requests []string
}

func (p *fixturePeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Method == http.MethodPost && r.URL.Path == "/ingest" {
		var batch []httpFixtureRow
		dec := json.NewDecoder(r.Body)
		for {
			var row httpFixtureRow
			err := dec.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			batch = append(batch, row)
		}
		p.rows = append(p.rows, batch...)
		p.requests = append(p.requests, "ingest")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet || r.URL.Path != "/query" {
		http.NotFound(w, r)
		return
	}

	params := r.URL.Query()
	start, startErr := time.Parse(time.RFC3339Nano, params.Get("start"))
	end, endErr := time.Parse(time.RFC3339Nano, params.Get("end"))
	limit, limitErr := strconv.Atoi(params.Get("limit"))
	literal, cursor := params.Get("contains"), params.Get("cursor")
	if startErr != nil || endErr != nil || limitErr != nil || limit < 1 || r.Header.Get("Accept-Encoding") != "identity" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var capture scriptedCapture
	if cursor == "" {
		if params.Get("page") != "1" || params.Has("cursor") {
			http.Error(w, "bad capture request", http.StatusBadRequest)
			return
		}
		p.requests = append(p.requests, fmt.Sprintf("first:%s:%d", literal, limit))
		capture = scriptedCapture{start: start, end: end, literal: literal}
		for _, row := range p.rows {
			if row.Time.Compare(start) >= 0 && row.Time.Compare(end) < 0 && strings.Contains(row.Line, literal) {
				row.Time = row.Time.UTC()
				capture.rows = append(capture.rows, row)
			}
		}
		slices.SortStableFunc(capture.rows, func(a, b httpFixtureRow) int { return a.Time.Compare(b.Time) })
	} else {
		prior, ok := p.captures[cursor]
		if !ok || params.Has("page") || !prior.start.Equal(start) || !prior.end.Equal(end) || prior.literal != literal {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		capture = prior
		p.requests = append(p.requests, fmt.Sprintf("resume:%s:%d", literal, limit))
	}

	var body bytes.Buffer
	for sent := 0; sent < limit && capture.after < len(capture.rows); sent++ {
		encoded, err := json.Marshal(capture.rows[capture.after])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if body.Len()+len(encoded)+1 > p.trimCap {
			break
		}
		body.Write(encoded)
		body.WriteByte('\n')
		capture.after++
	}
	if capture.after < len(capture.rows) {
		p.serial++
		next := httpToken(p.serial)
		p.captures[next] = capture
		w.Header().Set(cursorHeader, next)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Length", strconv.Itoa(body.Len()))
	if _, err := w.Write(body.Bytes()); err != nil {
		p.requests = append(p.requests, "write error: "+err.Error())
	}
}

func TestSeededWireFixtureOverHTTP(t *testing.T) {
	t.Parallel()
	for _, seed := range []int64{-17, 0, 2709} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			const cap = 256
			peer := &fixturePeer{captures: make(map[string]scriptedCapture), trimCap: cap}
			server := httptest.NewServer(peer)
			defer server.Close()
			c, fetch := useWireClient(t, server.URL, cap)
			appendBatch := func(ctx context.Context, rows []model.Row) error {
				var body bytes.Buffer
				encoder := json.NewEncoder(&body)
				for _, r := range rows {
					if err := encoder.Encode(httpFixtureRow{r.Time, r.Line}); err != nil {
						return err
					}
				}
				outcome, err := c.Ingest(ctx, body.Bytes())
				if err != nil {
					return err
				}
				if outcome != client.IngestCommitted {
					return fmt.Errorf("ingest outcome %d", outcome)
				}
				return nil
			}
			limits := model.WireLimits{MaxPages: 128, MaxPageBytes: cap, Timeout: 5 * time.Second}
			stats, err := model.VerifySeededWireFixture(t.Context(), seed, epoch, limits, appendBatch, fetch)
			if err != nil {
				t.Fatal(err)
			}
			if stats.AcknowledgedRows != 115 || stats.Held.Rows != 5 || stats.Held.Pages != 3 ||
				stats.Held.ByteTrimmedPages != 1 || stats.Fresh.Rows != 8 || stats.Unfiltered.Rows != 113 ||
				stats.Empty != (model.WireStats{Pages: 1}) {
				t.Fatalf("unexpected fixture results %+v", stats)
			}

			peer.mu.Lock()
			defer peer.mu.Unlock()
			const first = "first:needle [wire]:4"
			want := []string{first, "ingest", "ingest", "resume:needle [wire]:1", "resume:needle [wire]:7", "first:needle [wire]:1"}
			at := slices.Index(peer.requests, first)
			if at < 0 || len(peer.requests)-at < len(want) || !slices.Equal(peer.requests[at:at+len(want)], want) {
				t.Fatalf("request ordering: %v", peer.requests)
			}
		})
	}
}

func TestWireFixtureStopsAtUnacknowledgedBatch(t *testing.T) {
	t.Parallel()
	want := errors.New("ambiguous ingest")
	calls := 0
	stats, err := model.VerifySeededWireFixture(t.Context(), -11, epoch, wireConfig().Limits,
		func(context.Context, []model.Row) error {
			calls++
			if calls == 2 {
				return want
			}
			return nil
		}, func(context.Context, model.WireRequest) (model.WirePage, error) {
			t.Fatal("queried after an ambiguous ingest")
			return model.WirePage{}, nil
		})
	if !errors.Is(err, want) || calls != 2 || stats.AcknowledgedRows != len(model.SeededBatches(-11, epoch, 104, false)[0]) ||
		!strings.Contains(err.Error(), "seed batch 2") {
		t.Fatalf("fixture = %+v, %v, batches=%d", stats, err, calls)
	}
}

func TestWireWalkRejectsMalformedHTTPPages(t *testing.T) {
	t.Parallel()
	const line = "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"row\"}\n"
	for _, tc := range []struct {
		name, body, detail string
		headers            http.Header
	}{
		{"missing newline", strings.TrimSuffix(line, "\n"), "invalid query record framing", nil},
		{"short body", line, "unexpected EOF", http.Header{"Content-Length": {strconv.Itoa(1 + len(line))}}},
		{"empty cursor header", line, "invalid next cursor header", http.Header{cursorHeader: {""}}},
		{"cursor trailer", line, "unexpected next cursor trailer", http.Header{"Trailer": {cursorHeader}}},
		{"declared length over cap", line, "response exceeds size limit", http.Header{"Content-Length": {"257"}}},
		{"streamed bytes over cap", strings.Repeat(line, 7), "response exceeds size limit", http.Header{"Trailer": {"X-Fixture-Trailer"}}},
		{"malformed after valid prefix", line + "{broken}\n", "decode query record", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reads atomic.Int32
			writeErrors := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads.Add(1)
				for key, values := range tc.headers {
					for _, value := range values {
						w.Header().Add(key, value)
					}
				}
				w.Header().Set("Content-Type", "application/x-ndjson")
				_, err := io.WriteString(w, tc.body)
				writeErrors <- err
			}))
			defer server.Close()
			_, fetch := useWireClient(t, server.URL, 256)
			var log model.Log
			log.Append([]model.Row{{Time: epoch, Line: "row"}})
			stats, err := model.VerifyWireWalk(t.Context(), &log, wireConfig(), fetch, nil)
			if err == nil || !strings.Contains(err.Error(), tc.detail) || stats != (model.WireStats{}) || reads.Load() != 1 {
				t.Fatalf("walk = %+v, %v, requests=%d; want %q", stats, err, reads.Load(), tc.detail)
			}
			select {
			case err := <-writeErrors:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("scripted response write did not finish")
			}
		})
	}
}

func TestWireWalkCancelsDuringHTTPBodyDelivery(t *testing.T) {
	t.Parallel()
	const body = "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"row\"}\n"
	for _, tc := range []struct {
		name string
		want error
	}{
		{"canceled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, finished, release := make(chan error, 1), make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(finished)
				w.Header().Set("Content-Type", "application/x-ndjson")
				w.Header().Set("Content-Length", strconv.Itoa(2*len(body)))
				w.Header().Set(cursorHeader, httpToken(1))
				_, err := io.WriteString(w, body)
				if err == nil {
					err = http.NewResponseController(w).Flush()
				}
				started <- err
				select {
				case <-req.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_, fetch := useWireClient(t, server.URL, 256)
			var log model.Log
			log.Append([]model.Row{{Time: epoch, Line: "row"}})
			walk := wireConfig()
			if tc.want == context.DeadlineExceeded {
				walk.Limits.Timeout = 300 * time.Millisecond
			}
			type result struct {
				stats model.WireStats
				err   error
			}
			results := make(chan result, 1)
			go func() {
				stats, err := model.VerifyWireWalk(ctx, &log, walk, fetch, nil)
				results <- result{stats, err}
			}()
			select {
			case err := <-started:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("peer never began delivering its body")
			}
			if tc.want == context.Canceled {
				cancel()
			}
			select {
			case got := <-results:
				if !errors.Is(got.err, tc.want) || got.stats != (model.WireStats{}) {
					t.Fatalf("walk = %+v, %v, want %v", got.stats, got.err, tc.want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("walk did not return after cancellation")
			}
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP request context did not cancel")
			}
		})
	}
}

// The release budget probe owns threshold discovery. Once a peer issues a
// cursor, the oracle must surface a rejection at that same configuration
// without retrying, reducing the limit again, or silently restarting.
func TestWireWalkSurfacesFixedBudgetContinuationRejection(t *testing.T) {
	t.Parallel()
	const body = "{\"ts\":\"2025-01-02T00:00:00.000000123Z\",\"line\":\"row\"}\n"
	const rejection = "query work budget exhausted\n"
	token := httpToken(91)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		switch calls.Add(1) {
		case 1:
			if q.Get("page") != "1" || q.Has("cursor") || q.Get("limit") != "2" {
				t.Errorf("unexpected capture query %s", req.URL.RawQuery)
			}
			w.Header().Set(cursorHeader, token)
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			if _, err := io.WriteString(w, body); err != nil {
				t.Error(err)
			}
		case 2:
			if q.Has("page") || q.Get("cursor") != token || q.Get("limit") != "1" {
				t.Error("unexpected continuation request")
			}
			http.Error(w, strings.TrimSuffix(rejection, "\n"), http.StatusServiceUnavailable)
		default:
			t.Error("retried a rejected continuation")
			http.Error(w, "unexpected query", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	_, fetch := useWireClient(t, server.URL, 256)
	var log model.Log
	log.Append([]model.Row{{Time: epoch, Line: "row"}, {Time: epoch, Line: "later"}})
	stats, err := model.VerifyWireWalk(t.Context(), &log, wireConfig(), fetch, nil)
	var httpErr *client.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable || httpErr.Body != rejection ||
		stats.Pages != 1 || stats.Rows != 1 || calls.Load() != 2 {
		t.Fatalf("walk = %+v, %v, calls=%d", stats, err, calls.Load())
	}
}
