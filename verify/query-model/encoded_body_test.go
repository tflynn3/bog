package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	model "bog/verify/query-model"
)

func TestFixedBudgetEncodedBodyContract(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		strict    bool
		original  string
		replay    string
		reuse     bool
		invalidAt int
		want      string
	}{
		{name: "legacy callers"},
		{name: "strict identical bytes", strict: true, original: "one", replay: "one"},
		{name: "only original", original: "one", want: "both require"},
		{name: "only replay", replay: "one", want: "both require"},
		{name: "strict neither", strict: true, want: "missing"},
		{name: "strict original only", strict: true, original: "one", want: "both require"},
		{name: "same size changed bytes with aliased buffer", original: "one", replay: "two", reuse: true, want: "differs"},
		{name: "opening size mismatch", invalidAt: 1, want: "witness has"},
		{name: "terminal size mismatch", invalidAt: 3, want: "witness has"},
		{name: "replay size mismatch", invalidAt: 4, want: "witness has"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var seed []model.Row
			calls := 0
			shared := make([]byte, 3)
			ack := func(_ context.Context, rows []model.Row) error {
				if seed == nil {
					seed = rows
				}
				return nil
			}
			fetch := func(_ context.Context, req model.WireRequest) (model.WirePage, error) {
				calls++
				if calls > 4 {
					return model.WirePage{}, errors.New("unexpected query")
				}
				p := model.WirePage{Rows: seed[min(calls-1, 2):min(calls, 3)], BodyBytes: 3}
				if calls < 3 {
					p.NextCursor = strconv.Itoa(calls)
				}
				body := ""
				switch calls {
				case 3:
					body = tc.original
				case 4:
					body = tc.replay
				}
				if body != "" {
					p.EncodedBody = []byte(body)
					if tc.reuse {
						copy(shared, body)
						p.EncodedBody = shared
					}
				}
				if calls == tc.invalidAt {
					p.EncodedBody = []byte{}
				}
				return p, nil
			}
			run := model.RunFixedBudgetCase
			if tc.strict {
				run = model.RunStrictFixedBudgetCase
			}
			got, err := run(t.Context(), model.FixedBudgetCases()[0], ack, fetch)
			if tc.want == "" {
				if err != nil || got.Outcome != model.FixedBudgetExhausted || calls != 4 {
					t.Fatalf("calls=%d result=%+v error=%v", calls, got, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) || got.Outcome == model.FixedBudgetExhausted {
				t.Fatalf("calls=%d result=%+v error=%v, want %q", calls, got, err, tc.want)
			}
		})
	}
}

func TestStrictFixedBudgetSameLengthHTTPEncodingDrift(t *testing.T) {
	t.Parallel()
	var requests, appends atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ingest" {
			appends.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		const ts = `"ts":"2020-01-01T00:00:00Z"`
		var body string
		switch requests.Add(1) {
		case 1:
			w.Header().Set(cursorHeader, httpToken(1))
			body = "{" + ts + ",\"line\":\"c\"}\n"
		case 2:
			w.Header().Set(cursorHeader, httpToken(2))
			body = "{" + ts + ",\"line\":\"a\"}\n"
		case 3:
			body = "{" + ts + ",\"line\":\"b\"}\n"
		case 4:
			body = "{\"line\":\"b\"," + ts + "}\n"
		default:
			http.Error(w, "unexpected query", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if _, err := fmt.Fprint(w, body); err != nil {
			t.Error(err)
		}
	}))
	defer peer.Close()
	c, fetch, err := newWitnessClient(peer.URL, peer.Client().Transport, 96)
	if err != nil {
		t.Fatal(err)
	}
	got, err := model.RunStrictFixedBudgetCase(t.Context(), model.FixedBudgetCases()[0],
		func(ctx context.Context, batch []model.Row) error { return acknowledgeFixtureBatch(ctx, c, batch) }, fetch)
	if err == nil || !strings.Contains(err.Error(), "replayed terminal body differs") ||
		got.Outcome != model.FixedBudgetIssuedCursor || requests.Load() != 4 || appends.Load() != 3 {
		t.Fatalf("requests=%d appends=%d result=%+v error=%v", requests.Load(), appends.Load(), got, err)
	}
}
