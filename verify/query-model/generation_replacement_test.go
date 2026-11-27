package querymodel_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func replacementConfig() model.GenerationReplacementConfig {
	return model.GenerationReplacementConfig{
		Epoch: epoch, WorkBudget: 2048,
		Limits: model.WireLimits{MaxPageBytes: 256, MaxPages: 12, Timeout: 15 * time.Second},
	}
}

func launchReplacementFixture(t *testing.T, ctx context.Context, budget int, seed model.GenerationReplacementSeed, run func(model.GenerationReplacementAdapter) error) (err error) {
	t.Helper()
	f := &runtimeQueryFixture{retained: true, budget: budget, concurrency: 4}
	f.startIssuer()
	defer func() { err = errors.Join(err, f.closeIssuer()) }()
	s := newQuerySocketFixture(t, f.handler)
	defer s.server.Close()
	ack := func(ctx context.Context, rows []model.Row) error {
		return acknowledgeFixtureBatch(ctx, s.client, rows)
	}
	if err := ack(ctx, seed.Checkpoint); err != nil {
		return fmt.Errorf("acknowledge fixture checkpoint: %w", err)
	}
	if _, err := f.activateTail(ctx, len(seed.Checkpoint)); err != nil {
		return fmt.Errorf("prepare fixture checkpoint: %w", err)
	}
	if err := ack(ctx, seed.Tail); err != nil {
		return fmt.Errorf("acknowledge fixture tail: %w", err)
	}
	f.mu.Lock()
	initial := f.replacementPointLocked()
	f.mu.Unlock()
	return run(model.GenerationReplacementAdapter{
		Initial: initial, Acknowledge: ack, Fetch: s.fetch, Activate: f.activateTail,
		Refuse: s.refuse, RetiredPins: f.retiredPins, HoldResume: s.holdResume,
		Retire: func(ctx context.Context, _ model.Filter, cursor string) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return f.api.RetireQueryCapture(cursor)
		},
	})
}

func TestGenerationReplacementOverTCP(t *testing.T) {
	t.Parallel()
	for _, offset := range []int{0, -7 * 3600, 5*3600 + 30*60} {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			t.Parallel()
			cfg := replacementConfig()
			cfg.Epoch = cfg.Epoch.In(time.FixedZone("fixture", offset))
			calls := 0
			result, err := model.VerifyGenerationReplacement(t.Context(), cfg, func(ctx context.Context, budget int, seed model.GenerationReplacementSeed, run func(model.GenerationReplacementAdapter) error) error {
				calls++
				if budget != cfg.WorkBudget {
					return fmt.Errorf("launch at %d, want %d", budget, cfg.WorkBudget)
				}
				return launchReplacementFixture(t, ctx, budget, seed, run)
			})
			if err != nil {
				t.Fatalf("result=%+v: %v", result, err)
			}
			want := [2]model.ReplacementGeneration{
				{Generation: 2, CoveredRows: 9, CommittedRows: 10, Complete: true},
				{Generation: 3, CoveredRows: 12, CommittedRows: 14, Complete: true},
			}
			if calls != 1 || result.WorkBudget != cfg.WorkBudget || result.Replacements != want ||
				result.Held.Rows != 6 || result.Held.Pages != 3 || result.Fresh.Rows != 12 ||
				!result.TerminalReplay || !result.AdmittedReplay || !result.CaptureReleased {
				t.Fatalf("incomplete replacement evidence from %d launches: %+v", calls, result)
			}
		})
	}
}

func TestGenerationReplacementReplayOverTCP(t *testing.T) {
	t.Parallel()
	result, err := model.VerifyGenerationReplacementReplay(t.Context(), replacementConfig(), func(ctx context.Context, budget int, seed model.GenerationReplacementSeed, run func(model.GenerationReplacementAdapter) error) error {
		return launchReplacementFixture(t, ctx, budget, seed, func(f model.GenerationReplacementAdapter) error {
			f.HoldResume = nil
			return run(f)
		})
	})
	if err != nil || !result.TerminalReplay || !result.CaptureReleased || result.AdmittedReplay {
		t.Fatalf("idle retirement result=%+v error=%v", result, err)
	}
}

func TestGenerationReplacementRejectsBrokenObservations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, want string
		alter      func(*model.GenerationReplacementAdapter)
	}{
		{"missing gate", "requires acknowledgement", func(f *model.GenerationReplacementAdapter) {
			f.HoldResume = nil
		}},
		{"empty initial suffix", "initial checkpoint and committed tail", func(f *model.GenerationReplacementAdapter) {
			f.Initial.CommittedRows = f.Initial.CoveredRows
		}},
		{"unacknowledged batch", "replacement 1 acknowledge batch 1", func(f *model.GenerationReplacementAdapter) {
			f.Acknowledge = func(context.Context, []model.Row) error { return errors.New("ack lost") }
			f.Activate = func(context.Context, int) (model.ReplacementGeneration, error) {
				panic("unacknowledged data must not activate")
			}
		}},
		{"unchanged generation", "did not advance", func(f *model.GenerationReplacementAdapter) {
			activate := f.Activate
			f.Activate = func(ctx context.Context, cutoff int) (model.ReplacementGeneration, error) {
				g, err := activate(ctx, cutoff)
				g.Generation = f.Initial.Generation
				return g, err
			}
		}},
		{"wrong second cutoff", "replacement 2: observed", func(f *model.GenerationReplacementAdapter) {
			activate := f.Activate
			f.Activate = func(ctx context.Context, cutoff int) (model.ReplacementGeneration, error) {
				g, err := activate(ctx, cutoff)
				if g.Generation == f.Initial.Generation+2 {
					g.CoveredRows--
				}
				return g, err
			}
		}},
		{"lost committed suffix", "want complete coverage", func(f *model.GenerationReplacementAdapter) {
			activate := f.Activate
			f.Activate = func(ctx context.Context, cutoff int) (model.ReplacementGeneration, error) {
				g, err := activate(ctx, cutoff)
				g.CommittedRows = cutoff
				return g, err
			}
		}},
		{"budget refused on first resume", "held walk at work budget", func(f *model.GenerationReplacementAdapter) {
			fetch := f.Fetch
			f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
				if req.Cursor != "" {
					return model.WirePage{}, &model.FixedBudgetHTTPError{StatusCode: http.StatusServiceUnavailable, CleanBudgetRejection: true}
				}
				return fetch(ctx, req)
			}
		}},
		{"same-length terminal drift", "differs from the original encoded body", func(f *model.GenerationReplacementAdapter) {
			fetch := f.Fetch
			terminals := 0
			f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
				page, err := fetch(ctx, req)
				if err == nil && req.Cursor != "" && page.NextCursor == "" {
					terminals++
					if terminals == 2 {
						driftReplacementBody(&page)
					}
				}
				return page, err
			}
		}},
		{"terminal witness missing", "missing its encoded body witness", func(f *model.GenerationReplacementAdapter) {
			fetch := f.Fetch
			f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
				page, err := fetch(ctx, req)
				if req.Cursor != "" && page.NextCursor == "" {
					page.EncodedBody = nil
				}
				return page, err
			}
		}},
		{"wrong fresh backfill", "fresh walk after both replacements", func(f *model.GenerationReplacementAdapter) {
			fetch := f.Fetch
			opens := 0
			f.Fetch = func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
				page, err := fetch(ctx, req)
				if err == nil && req.Cursor == "" {
					opens++
					if opens == 2 && len(page.Rows) > 0 {
						page.Rows[0].Line = "stale generation"
					}
				}
				return page, err
			}
		}},
		{"gate never held", "did not witness an admitted resume", func(f *model.GenerationReplacementAdapter) {
			f.HoldResume = func(ctx context.Context, req model.WireRequest, _ func() error) (model.WirePage, error) {
				return f.Fetch(ctx, req)
			}
		}},
		{"new resume allowed", "new resume during retirement", func(f *model.GenerationReplacementAdapter) {
			f.Retire = func(context.Context, model.Filter, string) error { return nil }
		}},
		{"early release", "retirement released the paused capture", func(f *model.GenerationReplacementAdapter) {
			retired := false
			retire, pins := f.Retire, f.RetiredPins
			f.Retire = func(ctx context.Context, filter model.Filter, cursor string) error {
				retired = true
				return retire(ctx, filter, cursor)
			}
			f.RetiredPins = func(ctx context.Context) (uint64, error) {
				if retired {
					return 0, nil
				}
				return pins(ctx)
			}
		}},
		{"admitted replay corrupt", "admitted terminal response after retirement", func(f *model.GenerationReplacementAdapter) {
			hold := f.HoldResume
			f.HoldResume = func(ctx context.Context, req model.WireRequest, callback func() error) (model.WirePage, error) {
				page, err := hold(ctx, req, callback)
				if err == nil {
					driftReplacementBody(&page)
				}
				return page, err
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := model.VerifyGenerationReplacement(t.Context(), replacementConfig(), func(ctx context.Context, budget int, seed model.GenerationReplacementSeed, run func(model.GenerationReplacementAdapter) error) error {
				return launchReplacementFixture(t, ctx, budget, seed, func(f model.GenerationReplacementAdapter) error {
					tc.alter(&f)
					return run(f)
				})
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) || result.CaptureReleased {
				t.Fatalf("result=%+v error=%v, want %q before release evidence", result, err, tc.want)
			}
		})
	}
}

func driftReplacementBody(page *model.WirePage) {
	page.EncodedBody = bytes.Clone(page.EncodedBody)
	page.EncodedBody[0] ^= 1
}

func TestGenerationReplacementTailFrameBoundary(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cfg := replacementConfig()
	seed := model.GenerationReplacementSeed{
		Checkpoint: []model.Row{{Time: cfg.Epoch, Line: "first"}},
		Tail: []model.Row{
			{Time: cfg.Epoch, Line: "second"},
			{Time: cfg.Epoch, Line: "third"},
		},
	}
	err := launchReplacementFixture(t, ctx, cfg.WorkBudget, seed, func(f model.GenerationReplacementAdapter) error {
		if _, err := f.Activate(ctx, 2); err == nil {
			return errors.New("split tail frame was accepted")
		}
		g, err := f.Activate(ctx, 3)
		if err != nil {
			return err
		}
		if !g.Complete || g.Generation != f.Initial.Generation+1 || g.CoveredRows != 3 || g.CommittedRows != 3 {
			return fmt.Errorf("split attempt changed the predecessor or journal: %+v", g)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGenerationReplacementReleaseWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		fail error
	}{
		{name: "eventually drains"},
		{name: "failed observation", fail: errors.New("pin probe failed")},
		{name: "canceled observation", fail: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			samples := 0
			result, err := model.VerifyGenerationReplacement(ctx, replacementConfig(), func(ctx context.Context, budget int, seed model.GenerationReplacementSeed, run func(model.GenerationReplacementAdapter) error) error {
				return launchReplacementFixture(t, ctx, budget, seed, func(f model.GenerationReplacementAdapter) error {
					hold, pins := f.HoldResume, f.RetiredPins
					joined := false
					f.HoldResume = func(ctx context.Context, req model.WireRequest, callback func() error) (model.WirePage, error) {
						page, err := hold(ctx, req, callback)
						joined = err == nil
						return page, err
					}
					f.RetiredPins = func(ctx context.Context) (uint64, error) {
						if !joined {
							return pins(ctx)
						}
						samples++
						if tc.fail == context.Canceled {
							cancel()
							return 1, nil
						}
						if tc.fail != nil {
							return 0, tc.fail
						}
						if samples == 1 {
							return 1, nil
						}
						return pins(ctx)
					}
					return run(f)
				})
			})
			if tc.fail == nil {
				if err != nil || samples != 2 || !result.CaptureReleased {
					t.Fatalf("release samples=%d result=%+v error=%v", samples, result, err)
				}
			} else if !errors.Is(err, tc.fail) || samples != 1 || result.CaptureReleased {
				t.Fatalf("release samples=%d result=%+v error=%v, want %v", samples, result, err, tc.fail)
			}
		})
	}
}
