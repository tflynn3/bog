package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func seededTraceConfig(seed int64) model.QueryTraceConfig {
	return model.QueryTraceConfig{
		Seed: seed, Epoch: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		MaxWorkBudget: 16384,
		Limits:        model.WireLimits{MaxPages: 128, MaxPageBytes: 256, Timeout: 20 * time.Second},
	}
}

func TestSeededQueryTraceBoundaryOverHTTP(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		fail string
		want string
	}{
		{name: "clean adjacent threshold"},
		{name: "unclean lower rejection", fail: "opening", want: "held walk"},
		{name: "continuation refusal at threshold", fail: "resume", want: "held walk"},
		{name: "same length replay drift at threshold", fail: "replay", want: "differs from the original"},
		{name: "launch discards callback error", fail: "ignore", want: "held terminal replay"},
		{name: "cleanup after clean rejection", fail: "cleanup", want: "cleanup failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := seededTraceConfig(-17)
			cfg.MaxWorkBudget = 64
			const minimum = 17
			var observedBudgets []int
			result, err := model.VerifyQueryTrace(t.Context(), cfg, func(_ context.Context, budget int, run func(model.QueryTraceAdapter) error) error {
				observedBudgets = append(observedBudgets, budget)
				peer := &fixturePeer{trimCap: 256, captures: make(map[string]scriptedCapture), refuseOpening: budget < minimum}
				server := httptest.NewServer(peer)
				defer server.Close()
				c, fetch, err := newWitnessClient(server.URL, server.Client().Transport, 256)
				if err != nil {
					return err
				}
				var terminalSeen bool
				var cutoffs []int
				runErr := run(model.QueryTraceAdapter{
					Acknowledge: func(ctx context.Context, rows []model.Row) error { return acknowledgeFixtureBatch(ctx, c, rows) },
					Publish: func(_ context.Context, cutoff int) error {
						peer.mu.Lock()
						defer peer.mu.Unlock()
						if cutoff != len(peer.rows) || len(cutoffs) > 0 && (cutoff <= cutoffs[0] || len(peer.captures) != 1) {
							return fmt.Errorf("bad publication at cutoff %d", cutoff)
						}
						cutoffs = append(cutoffs, cutoff)
						return nil
					},
					Fetch: func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
						page, err := fetch(ctx, req)
						switch {
						case budget < minimum && tc.fail == "opening":
							page.EncodedBody = []byte{}
						case budget == minimum && tc.fail == "resume" && req.Cursor != "":
							return model.WirePage{}, &model.FixedBudgetHTTPError{StatusCode: http.StatusServiceUnavailable, CleanBudgetRejection: true}
						case budget == minimum && (tc.fail == "replay" || tc.fail == "ignore") && err == nil && page.NextCursor == "":
							if terminalSeen {
								page.EncodedBody[0] ^= 1
							}
							terminalSeen = true
						}
						return page, err
					},
				})
				if budget < minimum && tc.fail == "cleanup" {
					return errors.Join(runErr, errors.New("cleanup failed"))
				}
				if tc.fail == "ignore" {
					return nil
				}
				if runErr == nil && (len(cutoffs) == 0 || budget >= minimum && len(cutoffs) != 2) {
					return fmt.Errorf("unexpected publication cutoffs %v", cutoffs)
				}
				return runErr
			})
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("budgets=%v result=%+v error=%v, want %q", observedBudgets, result, err, tc.want)
				}
				return
			}
			if err != nil || result.RejectedBudget != minimum-1 || result.MinimumWorkBudget != minimum ||
				result.BoundaryHeld.Rows != 5 || result.BoundaryHeld.ByteTrimmedPages != 1 || result.Fresh.Rows != 8 {
				t.Fatalf("budgets=%v result=%+v error=%v", observedBudgets, result, err)
			}
		})
	}
}
