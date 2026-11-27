package querymodel_test

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func TestPinnedQueryModelCosts(t *testing.T) {
	binary := pinnedQueryTestBinary(t)
	t.Logf("query cost candidate os=%s arch=%s", runtime.GOOS, runtime.GOARCH)
	for _, mode := range []struct {
		name     string
		retained bool
	}{{name: "ordinary"}, {name: "retained", retained: true}} {
		t.Run(mode.name, func(t *testing.T) {
			cfg := seededTraceConfig(2709)
			cfg.Limits.Timeout = 90 * time.Second
			dir := t.TempDir()
			report, err := model.VerifyQueryCosts(t.Context(), cfg, func(ctx context.Context, budget int, run func(model.QueryTraceAdapter) error) (err error) {
				trial, err := os.MkdirTemp(dir, "probe-")
				if err != nil {
					return err
				}
				defer func() { err = errors.Join(err, os.RemoveAll(trial)) }()
				return servePinnedQuery(t, ctx, binary, trial, budget, mode.retained, run)
			})
			if err != nil {
				t.Fatal(err)
			}
			checkQueryCostReport(t, report)
			logQueryReport(t, "pinned_query_cost", report)
		})
	}
}
