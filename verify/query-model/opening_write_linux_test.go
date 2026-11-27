package querymodel_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	model "bog/verify/query-model"
)

type failedOpeningWriter struct{ http.ResponseWriter }

func (failedOpeningWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestQueryCaptureSurvivesFailedOpeningWrite(t *testing.T) {
	t.Parallel()
	for _, retained := range []bool{false, true} {
		t.Run(strconv.FormatBool(retained), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			f := &runtimeQueryFixture{retained: retained, budget: 1024}
			if err := f.start(); err != nil {
				t.Fatal(errors.Join(err, f.stop()))
			}
			t.Cleanup(func() {
				if err := f.stop(); err != nil {
					t.Error(err)
				}
			})
			cfg := seededTraceConfig(0)
			seed := []model.Row{{Time: cfg.Epoch, Line: "third"}, {Time: cfg.Epoch, Line: "first"}, {Time: cfg.Epoch, Line: "second"}}
			if err := acknowledgeFixtureBatch(ctx, f.client, seed); err != nil {
				t.Fatal(err)
			}
			if retained {
				if err := f.publish(ctx, len(seed)); err != nil {
					t.Fatal(err)
				}
			}

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// The real handler commits the cursor and declared length.
				// Delivery of its first record then fails.
				f.handler.ServeHTTP(failedOpeningWriter{w}, r)
			})
			fault := httptest.NewServer(handler)
			defer fault.Close()
			filter := model.Filter{Start: cfg.Epoch, End: cfg.Epoch.Add(time.Second)}
			params := url.Values{
				"page": {"1"}, "limit": {"1"},
				"start": {filter.Start.Format(time.RFC3339Nano)}, "end": {filter.End.Format(time.RFC3339Nano)},
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fault.URL+"/query?"+params.Encode(), nil)
			if err != nil {
				t.Fatal(err)
			}
			res, err := fault.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			token := res.Header.Get(cursorHeader)
			body, readErr := io.ReadAll(io.LimitReader(res.Body, 257))
			closeErr := res.Body.Close()
			if res.StatusCode != http.StatusOK || token == "" || res.ContentLength <= 0 ||
				len(body) != 0 || !errors.Is(readErr, io.ErrUnexpectedEOF) || closeErr != nil {
				t.Fatalf("failed opening status=%d cursor_present=%t length=%d received=%d read=%v close=%v",
					res.StatusCode, token != "", res.ContentLength, len(body), readErr, closeErr)
			}

			if err := acknowledgeFixtureBatch(ctx, f.client, []model.Row{{Time: cfg.Epoch, Line: "after interrupted opening"}}); err != nil {
				t.Fatal(err)
			}
			if retained {
				if err := f.publish(ctx, len(seed)+1); err != nil {
					t.Fatal(err)
				}
			}
			// The opening's first row precedes the committed cursor. Check
			// the complete suffix against the pre-interruption ledger.
			var suffix model.Log
			suffix.Append(seed[1:])
			walk := model.WireWalk{Filter: filter, RowLimits: []int{4}, Limits: cfg.Limits}
			stats, err := model.VerifyWireWalk(ctx, &suffix, walk, func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
				req.Cursor = token
				return f.fetch(ctx, req)
			}, nil)
			if err != nil || stats.Pages != 1 || stats.Rows != 2 {
				t.Fatalf("failed-opening resume: %+v err=%v", stats, err)
			}
			if retained {
				if err := f.api.RetireQueryCapture(token); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
