package querymodel_test

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	model "bog/verify/query-model"
)

var errNoCompaction = errors.New("candidate has no retained compaction control")

// The native linux-release race step already runs this package from the gate
// checkout and supplies BINARY, the same pinned executable used by admission.
func TestPinnedQueryModelReplay(t *testing.T) {
	binary := pinnedQueryTestBinary(t)

	for _, retained := range []bool{false, true} {
		name := "ordinary"
		if retained {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			cfg := seededTraceConfig(2709)
			cfg.Limits.Timeout = 45 * time.Second
			dir := t.TempDir()
			if retained {
				check := func(model.QueryTraceAdapter) error { return nil }
				err := servePinnedQuery(t, t.Context(), binary, dir, cfg.MaxWorkBudget, true, check)
				if err == errNoCompaction {
					t.Skip("retained shipped qualification pending: candidate has no compaction control")
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			trial := 0
			result, err := model.VerifyQueryTrace(t.Context(), cfg, func(ctx context.Context, budget int, run func(model.QueryTraceAdapter) error) error {
				trial++
				path := filepath.Join(dir, strconv.Itoa(trial))
				if err := os.Mkdir(path, 0700); err != nil {
					return err
				}
				return servePinnedQuery(t, ctx, binary, path, budget, retained, run)
			})
			if err != nil {
				t.Fatalf("trace=%+v: %v", result, err)
			}
			t.Logf("adjacent rejection=%d issuing=%d exact terminal replay with held=%+v fresh=%+v",
				result.RejectedBudget, result.MinimumWorkBudget, result.Held, result.Fresh)
		})
	}
}

func pinnedQueryTestBinary(t *testing.T) string {
	t.Helper()
	binary, revision := os.Getenv("BOG_RELEASE_BINARY"), os.Getenv("BOG_RELEASE_REVISION")
	if binary == "" && os.Getenv("GITHUB_WORKFLOW_SHA") != "" {
		binary, revision = os.Getenv("BINARY"), os.Getenv("REVISION")
	}
	if binary == "" {
		t.Skip("pinned Linux release candidate required")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := pinnedQueryDigest(binary, revision)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate revision=%s binary_sha256=%x", revision, digest)

	return binary
}

func TestPinnedQueryModel104(t *testing.T) {
	requireQuery104(t)
	binary := pinnedQueryTestBinary(t)
	for _, retained := range []bool{false, true} {
		t.Run(pageSourceName(retained), func(t *testing.T) {
			for _, limit := range []int{1, 10} {
				t.Run(fmt.Sprintf("opening-limit-%d", limit), func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
					defer cancel()
					err := withPinnedQueryEndpoint(t, ctx, binary, t.TempDir(), 32<<10, retained, func(f model.CaptureDeliveryFixture) error {
						return model.VerifyFittingPages(ctx, seededTraceConfig(0).Epoch, captureWireLimits, limit, f)
					})
					if err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func pinnedQueryDigest(binary, revision string) ([]byte, error) {
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		return nil, fmt.Errorf("read pinned build identity: %w", err)
	}
	settings := make(map[string]string, len(info.Settings))
	for _, item := range info.Settings {
		settings[item.Key] = item.Value
	}
	if revision == "" || settings["vcs.revision"] != revision || settings["vcs.modified"] != "false" {
		return nil, fmt.Errorf("pinned query binary revision=%q modified=%q, want clean revision %q",
			settings["vcs.revision"], settings["vcs.modified"], revision)
	}
	f, err := os.Open(binary)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, f)
	return hash.Sum(nil), errors.Join(readErr, f.Close())
}

func servePinnedQuery(t *testing.T, ctx context.Context, binary, dir string, budget int, retained bool, run func(model.QueryTraceAdapter) error) error {
	t.Helper()
	return withPinnedQueryEndpoint(t, ctx, binary, dir, budget, retained, func(f model.CaptureDeliveryFixture) error {
		return run(model.QueryTraceAdapter{Acknowledge: f.Acknowledge, Fetch: f.Fetch, Publish: f.Publish})
	})
}

func withPinnedQueryEndpoint(t *testing.T, ctx context.Context, binary, dir string, budget int, retained bool, run func(model.CaptureDeliveryFixture) error) error {
	t.Helper()
	return withConfiguredPinnedQueryEndpoint(t, ctx, binary, dir, budget, 1, retained, run)
}

func withConfiguredPinnedQueryEndpoint(t *testing.T, ctx context.Context, binary, dir string, budget, concurrency int, retained bool, run func(model.CaptureDeliveryFixture) error) error {
	t.Helper()
	command := func(addr string) *exec.Cmd {
		cmd := exec.Command(binary, "-addr="+addr, "-data-dir="+dir, "-query-concurrency="+strconv.Itoa(concurrency),
			"-query-max-response-bytes=256", "-query-work-budget="+strconv.Itoa(budget))
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "BOG_CAPACITY_CONTROL=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		if retained {
			cmd.Env = append(cmd.Env, "BOG_CAPACITY_CONTROL=1")
		}
		return cmd
	}
	return withPinnedQueryCandidate(ctx, command, t.Logf, func(raw *http.Client, base string) error {
		c, fetch, err := newWitnessClient(base, raw.Transport, 256)
		if err != nil {
			return err
		}
		fixture := model.CaptureDeliveryFixture{
			Acknowledge: func(ctx context.Context, rows []model.Row) error { return acknowledgeFixtureBatch(ctx, c, rows) },
			Fetch:       fetch,
			Refuse: func(ctx context.Context, q model.WireRequest, status int, body string) error {
				return observeQueryRefusal(ctx, raw, base, q, status, body)
			},
		}
		if retained {
			control := pinnedCompaction{raw: raw, base: base}
			initial, err := control.read(ctx, http.MethodGet, "status", 0)
			if err != nil {
				var statusErr *pinnedCompactionHTTPError
				if errors.As(err, &statusErr) && statusErr.status == http.StatusNotFound {
					return errNoCompaction
				}
				return err
			}
			if initial.State != "complete" || initial.CoveredRows != 0 || initial.CommittedRows != 0 {
				return errors.New("compaction trial did not start at an empty published point")
			}
			previous := initial.CompactionObservation
			fixture.Publish = func(ctx context.Context, cutoff int) error {
				ctx, cancel := context.WithTimeout(ctx, pinnedCompactionTimeout)
				defer cancel()
				current, err := control.publish(ctx, previous, cutoff, cutoff)
				if err != nil {
					return err
				}
				previous = current
				return nil
			}
		}
		return run(fixture)
	})
}
