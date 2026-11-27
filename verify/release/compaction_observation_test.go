package main

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

type backfillCompaction struct {
	state              string
	generation         uint64
	covered, committed int
}

type compactionObservation struct {
	backfillCompaction
	retiredGenerationPins uint64
}

// readCompactionObservation requires a complete status snapshot. The pin count
// is an observation only when requirePins is true and the request succeeds.
func readCompactionObservation(ctx context.Context, client *http.Client, base, method, action string, cutoff int, requirePins bool) (compactionObservation, error) {
	target := base + "/_capacity/compaction?action=" + action + "&cutoff=" + strconv.Itoa(cutoff)
	res, err := request(ctx, client, method, target, nil)
	if err != nil {
		return compactionObservation{}, fmt.Errorf("compaction %s cutoff %d: %w", action, cutoff, err)
	}
	if res.status != http.StatusOK {
		return compactionObservation{}, fmt.Errorf("compaction %s cutoff %d returned HTTP %d: %.200s", action, cutoff, res.status, res.body)
	}
	got, err := decodeCompactionObservation(res.body, requirePins)
	if err != nil {
		var syntax *jsontext.SyntacticError
		if errors.As(err, &syntax) {
			return compactionObservation{}, fmt.Errorf("decode compaction %s cutoff %d: %w", action, cutoff, err)
		}
		return compactionObservation{}, fmt.Errorf("invalid compaction %s cutoff %d: %w", action, cutoff, err)
	}
	return got, nil
}

func decodeCompactionObservation(body []byte, requirePins bool) (compactionObservation, error) {
	var got compactionObservation
	var metrics jsontext.Value
	fields := map[string]any{
		"state":          &got.state,
		"generation":     &got.generation,
		"covered_rows":   &got.covered,
		"committed_rows": &got.committed,
	}
	if requirePins {
		fields["metrics"] = &metrics
	}
	if _, err := decodeFields(body, fields); err != nil {
		return compactionObservation{}, err
	}
	if got.state != "running" && got.state != "complete" {
		return compactionObservation{}, fmt.Errorf("unexpected state %q", got.state)
	}
	if got.covered < 0 || got.committed < got.covered {
		return compactionObservation{}, fmt.Errorf("invalid row counters: covered=%d committed=%d", got.covered, got.committed)
	}
	if requirePins {
		if _, err := decodeFields(metrics, map[string]any{"retired_generation_pins": &got.retiredGenerationPins}); err != nil {
			return compactionObservation{}, fmt.Errorf("compaction metrics: %w", err)
		}
	}
	return got, nil
}

func TestDecodeCompactionObservation(t *testing.T) {
	t.Parallel()
	const snapshot = `{"state":"complete","generation":7,"covered_rows":8,"committed_rows":10}`
	withMetrics := func(metrics string) string {
		return strings.TrimSuffix(snapshot, "}") + `,"metrics":` + metrics + "}"
	}
	published := compactionObservation{backfillCompaction: backfillCompaction{"complete", 7, 8, 10}}
	for _, tc := range []struct {
		name, body, wantErr string
		requirePins         bool
		want                compactionObservation
	}{
		{name: "publication without metrics", body: snapshot, want: published},
		{name: "publication with optional null metrics", body: withMetrics("null"), want: published},
		{name: "zero predecessor", body: `{"state":"running","generation":0,"covered_rows":0,"committed_rows":0,"extra":true}`,
			want: compactionObservation{backfillCompaction: backfillCompaction{state: "running"}}},
		{name: "positive pin count", body: withMetrics(`{"retired_generation_pins":2,"pending_compactions":0}`), requirePins: true,
			want: compactionObservation{backfillCompaction: published.backfillCompaction, retiredGenerationPins: 2}},
		{name: "explicit zero pin count", body: withMetrics(`{"retired_generation_pins":0}`), requirePins: true, want: published},
		{name: "missing metrics", body: snapshot, requirePins: true, wantErr: `missing or null "metrics"`},
		{name: "null metrics", body: withMetrics("null"), requirePins: true, wantErr: `missing or null "metrics"`},
		{name: "non-object metrics", body: withMetrics("[]"), requirePins: true, wantErr: "malformed JSON object"},
		{name: "missing pin count", body: withMetrics("{}"), requirePins: true, wantErr: `missing or null "retired_generation_pins"`},
		{name: "null pin count", body: withMetrics(`{"retired_generation_pins":null}`), requirePins: true, wantErr: `missing or null "retired_generation_pins"`},
		{name: "mis-cased pin count", body: withMetrics(`{"Retired_Generation_Pins":0}`), requirePins: true, wantErr: `missing or null "retired_generation_pins"`},
		{name: "duplicate pin count", body: withMetrics(`{"retired_generation_pins":1,"retired_generation_pins":0}`), requirePins: true, wantErr: "duplicate"},
		{name: "negative pin count", body: withMetrics(`{"retired_generation_pins":-1}`), requirePins: true, wantErr: "retired_generation_pins"},
		{name: "duplicate generation with escaped name", body: strings.Replace(snapshot, `"generation":7`, `"generation":7,"generatio\u006e":8`, 1),
			wantErr: "duplicate"},
		{name: "mis-cased coverage", body: strings.Replace(snapshot, "covered_rows", "Covered_Rows", 1), wantErr: `missing or null "covered_rows"`},
		{name: "unknown state", body: strings.Replace(snapshot, "complete", "pending", 1), wantErr: "unexpected state"},
		{name: "negative coverage", body: strings.Replace(snapshot, `"covered_rows":8`, `"covered_rows":-1`, 1), wantErr: "invalid row counters"},
		{name: "coverage ahead of commit", body: strings.Replace(snapshot, `"committed_rows":10`, `"committed_rows":7`, 1), wantErr: "invalid row counters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeCompactionObservation([]byte(tc.body), tc.requirePins)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || got != (compactionObservation{}) {
					t.Fatalf("observation = %+v, error = %v, want no observation and error containing %q", got, err, tc.wantErr)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("observation = %+v, error = %v, want %+v", got, err, tc.want)
			}
		})
	}
	const zeroSnapshot = `{"state":"complete","generation":0,"covered_rows":0,"committed_rows":0}`
	for _, member := range []struct{ name, value string }{
		{"state", `"complete"`}, {"generation", "0"}, {"covered_rows", "0"}, {"committed_rows", "0"},
	} {
		for _, mode := range []string{"missing", "null"} {
			t.Run(mode+"/"+member.name, func(t *testing.T) {
				t.Parallel()
				body := zeroSnapshot
				key := strconv.Quote(member.name)
				if mode == "missing" {
					body = strings.Replace(body, ","+key+":"+member.value, "", 1)
					body = strings.Replace(body, key+":"+member.value+",", "", 1)
				} else {
					body = strings.Replace(body, key+":"+member.value, key+":null", 1)
				}
				for _, requirePins := range []bool{false, true} {
					reply := body
					if requirePins {
						reply = strings.TrimSuffix(body, "}") + `,"metrics":{"retired_generation_pins":0}}`
					}
					got, err := decodeCompactionObservation([]byte(reply), requirePins)
					if err == nil || !strings.Contains(err.Error(), "missing or null "+key) || got != (compactionObservation{}) {
						t.Fatalf("requirePins=%t: observation = %+v, error = %v, want missing or null %s", requirePins, got, err, key)
					}
				}
			})
		}
	}
}

type compactionFixtureBody struct {
	io.Reader
	closeErr error
	closed   bool
}

func (b *compactionFixtureBody) Close() error {
	b.closed = true
	return b.closeErr
}

func TestReadCompactionObservation(t *testing.T) {
	t.Parallel()
	const snapshot = `{"state":"complete","generation":7,"covered_rows":8,"committed_rows":10}`
	closeErr := errors.New("fixture close failure")
	for _, tc := range []struct {
		name, method, action, body, wantErr string
		status                              int
		closeErr                            error
	}{
		{name: "status", method: http.MethodGet, action: "status", status: http.StatusOK, body: snapshot},
		{name: "start", method: http.MethodPost, action: "start", status: http.StatusOK, body: snapshot},
		{name: "non-200", method: http.MethodGet, action: "status", status: http.StatusServiceUnavailable, body: "not available",
			wantErr: "compaction status cutoff 8 returned HTTP 503: not available"},
		{name: "truncated JSON", method: http.MethodGet, action: "status", status: http.StatusOK, body: "{",
			wantErr: "decode compaction status cutoff 8"},
		{name: "missing counter", method: http.MethodGet, action: "status", status: http.StatusOK, body: `{"state":"complete","generation":7,"covered_rows":8}`,
			wantErr: "invalid compaction status cutoff 8"},
		{name: "close failure", method: http.MethodGet, action: "status", status: http.StatusOK, body: snapshot, closeErr: closeErr,
			wantErr: "compaction status cutoff 8: fixture close failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := &compactionFixtureBody{Reader: strings.NewReader(tc.body), closeErr: tc.closeErr}
			calls := 0
			client := &http.Client{Transport: budgetRoundTripper(func(req *http.Request) (*http.Response, error) {
				calls++
				wantPath := "/_capacity/compaction?action=" + tc.action + "&cutoff=8"
				if req.Method != tc.method || req.URL.RequestURI() != wantPath || req.Body != nil {
					t.Fatalf("request = %s %s, body = %v, want %s %s with no body", req.Method, req.URL.RequestURI(), req.Body, tc.method, wantPath)
				}
				return &http.Response{StatusCode: tc.status, Body: body}, nil
			})}
			got, err := readCompactionObservation(t.Context(), client, "http://127.0.0.1", tc.method, tc.action, 8, false)
			if calls != 1 || !body.closed {
				t.Fatalf("requests = %d, body closed = %t, want one request and a closed body", calls, body.closed)
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || got != (compactionObservation{}) {
					t.Fatalf("observation = %+v, error = %v, want no observation and error containing %q", got, err, tc.wantErr)
				}
				if tc.closeErr != nil && !errors.Is(err, tc.closeErr) {
					t.Fatalf("error = %v, want wrapped close error", err)
				}
			} else if want := (compactionObservation{backfillCompaction: backfillCompaction{"complete", 7, 8, 10}}); err != nil || got != want {
				t.Fatalf("observation = %+v, error = %v, want %+v", got, err, want)
			}
		})
	}
}
