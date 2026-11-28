package querymodel_test

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func TestPinnedQueryModelSelectiveAdmission(t *testing.T) {
	binary := pinnedQueryTestBinary(t)
	for _, scenario := range []model.SelectiveAdmissionScenario{model.SelectiveAlongsideCursor, model.SelectiveRetainedCaptures} {
		t.Run(string(scenario), func(t *testing.T) {
			runPinnedSelectiveAdmission(t, binary, scenario, 45*time.Second)
		})
	}
}

func TestPinnedQueryModelSelectiveAdmissionExpiry(t *testing.T) {
	if os.Getenv("BOG_QUERY_VERIFY_136_EXPIRY") != "1" {
		t.Skip("set BOG_QUERY_VERIFY_136_EXPIRY=1 for the full candidate cursor lifetime")
	}
	binary := pinnedQueryTestBinary(t)
	runPinnedSelectiveAdmission(t, binary, model.SelectiveScanExpiry, 16*time.Minute)
}

func runPinnedSelectiveAdmission(t *testing.T, binary string, scenario model.SelectiveAdmissionScenario, timeout time.Duration) {
	t.Helper()
	cfg := selectiveAdmissionConfig()
	cfg.SeedRows = 65536
	if scenario == model.SelectiveAlongsideCursor {
		// The pre-24ca046 worst-case reservation exceeded the remaining
		// budget beside a cursor this large, even for an empty result.
		cfg.SeedRows = 400_000
	}
	if scenario == model.SelectiveScanExpiry {
		cfg.ScanBytes = 24 << 20 // Legacy compatibility issuer.
	} else {
		cfg.ScanBytes = 64 << 20 // Default retained issuer.
	}
	cfg.ExpiryGrace = 5 * time.Second
	cfg.Limits.Timeout = timeout

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	t.Logf("selective admission arch=%s scan_limit_source=issuer_default capture_slots=64", runtime.GOARCH)
	logQueryReport(t, "selective_admission_settings", cfg)
	err := withConfiguredPinnedQueryEndpoint(t, ctx, binary, t.TempDir(), int(cfg.WorkBudget), cfg.QueryConcurrency, false, func(f model.CaptureDeliveryFixture) error {
		report, err := model.VerifySelectiveAdmission(ctx, cfg, scenario, model.SelectiveAdmissionAdapter{
			Acknowledge: f.Acknowledge, Fetch: f.Fetch, Refuse: f.Refuse,
		})
		logQueryReport(t, "pinned_selective_admission", report)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
