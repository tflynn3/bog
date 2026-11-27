package querymodel_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
	"bog/verify/capacityevidence"
	model "bog/verify/query-model"
)

// The pinned executable supplies the durable activation and wire evidence.
// The in-process socket fixture independently observes admission during delivery.
func TestPinnedQueryGenerationReplacement(t *testing.T) {
	binary := pinnedQueryTestBinary(t)
	cfg := replacementConfig()
	cfg.Limits.Timeout = 45 * time.Second
	result, err := model.VerifyGenerationReplacementReplay(t.Context(), cfg, func(ctx context.Context, budget int, seed model.GenerationReplacementSeed, run func(model.GenerationReplacementAdapter) error) error {
		dir, err := prepareReplacementRestore(ctx, t.TempDir(), seed)
		if err != nil {
			return fmt.Errorf("prepare tail-only restore: %w", err)
		}
		command := func(addr string) *exec.Cmd {
			cmd := exec.Command(binary, "-addr="+addr, "-data-dir="+dir, "-query-concurrency=4",
				"-query-max-response-bytes="+strconv.FormatInt(cfg.Limits.MaxPageBytes, 10),
				"-query-work-budget="+strconv.Itoa(budget))
			for _, env := range os.Environ() {
				if !strings.HasPrefix(env, "BOG_CAPACITY_CONTROL=") {
					cmd.Env = append(cmd.Env, env)
				}
			}
			cmd.Env = append(cmd.Env, "BOG_CAPACITY_CONTROL=1")
			return cmd
		}
		return withPinnedQueryCandidate(ctx, command, t.Logf, func(raw *http.Client, base string) error {
			c, fetch, err := newWitnessClient(base, raw.Transport, cfg.Limits.MaxPageBytes)
			if err != nil {
				return err
			}
			control := replacementControl{pinnedCompaction{raw: raw, base: base}}
			initial, err := control.read(ctx, http.MethodGet, "status", 0)
			if err != nil {
				return err
			}
			err = run(model.GenerationReplacementAdapter{
				Initial: replacementGeneration(initial.CompactionObservation),
				Acknowledge: func(ctx context.Context, rows []model.Row) error {
					return acknowledgeFixtureBatch(ctx, c, rows)
				},
				Fetch: fetch, Activate: control.activate, RetiredPins: control.retiredPins,
				Retire: control.retire,
				Refuse: func(ctx context.Context, q model.WireRequest, status int, body string) error {
					return observeQueryRefusal(ctx, raw, base, q, status, body)
				},
			})
			return errors.Join(err, requireReplacementTailOnly(dir))
		})
	})
	if err != nil {
		t.Fatalf("pinned tail-only replay result=%+v: %v", result, err)
	}
	if !result.TerminalReplay || !result.CaptureReleased || result.AcrossReplacementReplay != [2]bool{true, true} {
		t.Fatalf("incomplete pinned replacement replay: %+v", result)
	}
	logQueryReport(t, "pinned_query_replacement", struct {
		Result             model.GenerationReplacementResult `json:"result"`
		PinObservation     string                            `json:"pin_observation"`
		ReleaseObservation string                            `json:"release_observation"`
		Unavailable        map[string]string                 `json:"unavailable"`
	}{
		Result:             result,
		PinObservation:     "retired_generation_pins from capacity status after each completed cutoff",
		ReleaseObservation: "elapsed nanoseconds from before the retirement request to the first zero retired_generation_pins sample",
		Unavailable: map[string]string{
			"peak_prepared_bytes":         "candidate HTTP interface has no server-side preparation barrier",
			"peak_scan_buffer_bytes":      "capacity status does not expose per-query scan buffers",
			"peak_live_captures":          "capacity status provides aggregate retired pins at sampled points",
			"capture_lifetimes":           "capacity status provides aggregate retired pins without source acquisition and release timestamps",
			"release_after_held_delivery": "the candidate case uses idle retirement",
			"release_after_cancel":        "candidate HTTP interface has no deterministic server-side cancellation barrier",
		},
	})
}

type replacementControl struct{ pinnedCompaction }

func replacementGeneration(g capacityevidence.CompactionObservation) model.ReplacementGeneration {
	return model.ReplacementGeneration{
		Generation: g.Generation, CoveredRows: g.CoveredRows, CommittedRows: g.CommittedRows, Complete: g.State == "complete",
	}
}

func (c replacementControl) activate(ctx context.Context, cutoff int) (model.ReplacementGeneration, error) {
	ctx, cancel := context.WithTimeout(ctx, pinnedCompactionTimeout)
	defer cancel()
	previous, err := c.read(ctx, http.MethodGet, "status", cutoff)
	if err != nil {
		return model.ReplacementGeneration{}, err
	}
	if cutoff >= previous.CommittedRows {
		return model.ReplacementGeneration{}, fmt.Errorf("compaction cutoff %d leaves no committed suffix at %d", cutoff, previous.CommittedRows)
	}
	current, err := c.publish(ctx, previous.CompactionObservation, cutoff, previous.CommittedRows)
	return replacementGeneration(current), err
}

func (c replacementControl) retiredPins(ctx context.Context) (uint64, error) {
	reply, err := c.read(ctx, http.MethodGet, "status", 0)
	if err != nil {
		return 0, fmt.Errorf("pin observation: %w", err)
	}
	metrics, issues, err := capacityevidence.DecodeRuntimeMetrics(reply.metrics)
	if err != nil {
		return 0, fmt.Errorf("decode retire pin metrics: %w", err)
	}
	if metrics.RetiredPins == nil {
		return 0, fmt.Errorf("retired_generation_pins unavailable (observation issue %d)", issues["retired_generation_pins"])
	}
	return *metrics.RetiredPins, nil
}

func (c replacementControl) retire(ctx context.Context, _ model.Filter, cursor string) error {
	params := url.Values{"action": {"retire"}, "cursor": {cursor}}
	status, body, err := pinnedRequest(ctx, c.raw, http.MethodPost, c.base+capacityControl+"?"+params.Encode())
	if err != nil {
		return err
	}
	if status != http.StatusNoContent || len(body) != 0 {
		return fmt.Errorf("retirement returned HTTP %d with %d body bytes, want empty HTTP 204", status, len(body))
	}
	return nil
}

// The fixture has no running writer while Export reads these private files.
type replacementSnapshot struct {
	root  *os.Root
	point backup.Point
}

func (s replacementSnapshot) Point() backup.Point { return s.point }
func (s replacementSnapshot) OpenManifest() (io.ReadCloser, error) {
	return s.root.Open("bog.checkpoint")
}
func (s replacementSnapshot) OpenSegment(name string) (io.ReadCloser, error) {
	return s.root.Open(name)
}
func (s replacementSnapshot) OpenWAL() (backup.WALReader, error) {
	return s.root.Open("bog.wal")
}

func appendReplacementFixture(dir string, rows []model.Row) (err error) {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	batch := make([]store.Row, 0, len(rows))
	for _, row := range rows {
		stored := store.Row{TS: row.Time, Line: row.Line}
		if err := enc.Encode(stored); err != nil {
			return err
		}
		batch = append(batch, stored)
	}
	log, err := wal.Open(dir, new(store.Store))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, log.Close()) }()
	return log.Append(body.Bytes(), batch)
}

func prepareReplacementRestore(ctx context.Context, parent string, seed model.GenerationReplacementSeed) (_ string, err error) {
	source := filepath.Join(parent, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		return "", err
	}
	if err := appendReplacementFixture(source, seed.Checkpoint); err != nil {
		return "", err
	}
	legacy, err := checkpoint.OpenLegacy(source)
	if err != nil {
		return "", err
	}
	m, publishErr := legacy.Publish()
	if err := errors.Join(publishErr, legacy.Close()); err != nil {
		return "", err
	}
	if err := appendReplacementFixture(source, seed.Tail); err != nil {
		return "", err
	}
	legacy, err = checkpoint.OpenLegacy(source)
	if err != nil {
		return "", err
	}
	end := legacy.CommittedOffset()
	if err := legacy.Close(); err != nil {
		return "", err
	}
	if m.NextOrdinal != uint64(len(seed.Checkpoint)) || end <= m.CoveredOffset {
		return "", errors.New("restore seed needs exact checkpoint coverage and a nonempty committed tail")
	}

	root, err := os.OpenRoot(source)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	raw, err := root.ReadFile("bog.checkpoint")
	if err != nil {
		return "", err
	}
	var manifest struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return "", err
	}
	point := backup.Point{
		Generation: sha256.Sum256(manifest.Payload), CoveredOffset: m.CoveredOffset,
		NextOrdinal: m.NextOrdinal, CommittedEnd: end,
	}
	bundle, target := filepath.Join(parent, "bundle"), filepath.Join(parent, "restored")
	if err := backup.Export(ctx, replacementSnapshot{root, point}, bundle); err != nil {
		return "", err
	}
	if err := backup.Restore(ctx, bundle, target); err != nil {
		return "", err
	}
	restored, err := backup.Verify(ctx, target)
	if err != nil {
		return "", err
	}
	if restored != point {
		return "", errors.New("restored recovery point differs from the exported point")
	}
	return target, requireReplacementTailOnly(target)
}

func requireReplacementTailOnly(dir string) error {
	switch _, err := os.Lstat(filepath.Join(dir, "bog.wal")); {
	case err == nil:
		return errors.New("tail-only directory contains a full WAL")
	case !errors.Is(err, os.ErrNotExist):
		return err
	default:
		return nil
	}
}
