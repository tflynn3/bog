package querymodel_test

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
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

const capacityControl = "/_capacity/compaction"

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
	command := func(addr string) *exec.Cmd {
		cmd := exec.Command(binary, "-addr="+addr, "-data-dir="+dir, "-query-concurrency=1",
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
			status, initial, err := control.read(ctx, http.MethodGet, "status", 0)
			if err != nil {
				return err
			}
			if status == http.StatusNotFound {
				return errNoCompaction
			}
			if status != http.StatusOK {
				return fmt.Errorf("compaction capability returned HTTP %d", status)
			}
			if *initial.State != "complete" || *initial.Covered != 0 || *initial.Committed != 0 {
				return errors.New("compaction trial did not start at an empty published point")
			}
			control.last = initial
			fixture.Publish = control.publish
		}
		return run(fixture)
	})
}

func pinnedRequest(ctx context.Context, raw *http.Client, method, path string) (status int, body []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, method, path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	res, err := raw.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	const maxBody = 8192
	body, err = io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err == nil && (len(body) > maxBody || res.ContentLength >= 0 && res.ContentLength != int64(len(body))) {
		err = errors.New("invalid control response body length")
	}
	return res.StatusCode, body, err
}

type pinnedGeneration struct {
	State      *string `json:"state"`
	Generation *uint64 `json:"generation"`
	Covered    *int    `json:"covered_rows"`
	Committed  *int    `json:"committed_rows"`
}

type pinnedCompaction struct {
	raw  *http.Client
	base string
	last pinnedGeneration
}

func (c *pinnedCompaction) read(ctx context.Context, method, action string, cutoff int) (int, pinnedGeneration, error) {
	path := c.base + capacityControl + "?action=" + action + "&cutoff=" + strconv.Itoa(cutoff)
	status, body, err := pinnedRequest(ctx, c.raw, method, path)
	if err != nil || status != http.StatusOK {
		return status, pinnedGeneration{}, err
	}
	var g pinnedGeneration
	if err := json.Unmarshal(body, &g); err != nil {
		return status, g, err
	}
	if g.State == nil || g.Generation == nil || g.Covered == nil || g.Committed == nil ||
		(*g.State != "complete" && *g.State != "running") || *g.Covered < 0 || *g.Committed < *g.Covered {
		return status, g, errors.New("incomplete candidate compaction observation")
	}
	return status, g, nil
}

func (c *pinnedCompaction) publish(ctx context.Context, cutoff int) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	generation, covered := uint64(0), 0
	if c.last.Generation != nil {
		generation, covered = *c.last.Generation, *c.last.Covered
	}
	observe := func(method, action string) (pinnedGeneration, error) {
		status, current, err := c.read(ctx, method, action, cutoff)
		if err != nil {
			return current, err
		}
		if status != http.StatusOK {
			return current, fmt.Errorf("compaction %s at cutoff %d returned HTTP %d", action, cutoff, status)
		}
		if *current.Committed != cutoff ||
			(*current.State == "running" && (*current.Generation != generation || *current.Covered != covered)) ||
			(*current.State == "complete" && (*current.Generation <= generation || *current.Covered != cutoff)) {
			return current, fmt.Errorf("compaction %s cutoff=%d returned state=%s generation=%d covered=%d committed=%d",
				action, cutoff, *current.State, *current.Generation, *current.Covered, *current.Committed)
		}
		return current, nil
	}
	started, err := observe(http.MethodPost, "start")
	if err != nil {
		return err
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := observe(http.MethodGet, "status")
		if err != nil {
			return err
		}
		if *started.State == "complete" && (*current.State != "complete" || *current.Generation != *started.Generation) {
			return errors.New("completed compaction changed before status observation")
		}
		if *current.State == "complete" {
			c.last = current
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
