package querymodel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"
)

// ReplacementGeneration is an observation of the selected generation and its
// committed suffix from one snapshot. Row counts include nonmatching rows.
type ReplacementGeneration struct {
	Generation    uint64
	CoveredRows   int
	CommittedRows int
	Complete      bool
}

// GenerationReplacementSeed supplies the ordered batches for an isolated
// launch. Initialize the predecessor from Checkpoint and leave Tail committed
// in its suffix. Neither batch may be modified.
type GenerationReplacementSeed struct {
	Checkpoint []Row
	Tail       []Row
}

// GenerationReplacementAdapter binds a single endpoint for the entire trial.
// Activate must request the supplied whole-batch cutoff and return an observed
// completion, including the committed rows still above the cutoff. Refuse
// witnesses the complete status and plain-text body without a cursor. Retire
// authenticates the input cursor and closes admission. RetiredPins samples
// the live pins on retired generations without waiting for drain.
type GenerationReplacementAdapter struct {
	Initial     ReplacementGeneration
	Acknowledge AcknowledgeBatch
	Fetch       FetchWirePage
	Activate    func(ctx context.Context, cutoff int) (ReplacementGeneration, error)
	Refuse      func(context.Context, WireRequest, int, string) error
	Retire      func(context.Context, Filter, string) error
	RetiredPins func(context.Context) (uint64, error)

	// HoldResume runs the callback once, synchronously, with an admitted
	// resume paused before delivery. It resumes and joins both sides on
	// every exit and returns that request's complete page on success.
	HoldResume func(context.Context, WireRequest, func() error) (WirePage, error)
}

// LaunchGenerationReplacement configures the endpoint with the given fixed
// per-request budget and the seed's exact committed state. It invokes run
// once, synchronously, and returns setup, scenario and cleanup errors.
type LaunchGenerationReplacement func(
	ctx context.Context, workBudget int, seed GenerationReplacementSeed,
	run func(GenerationReplacementAdapter) error,
) error

type GenerationReplacementConfig struct {
	Epoch      time.Time
	WorkBudget int
	Limits     WireLimits
}

// GenerationReplacementResult describes the accepted evidence. AdmittedReplay
// identifies a byte-exact response received from the paused worker after
// retirement. Replay-only trials leave that field false.
type GenerationReplacementResult struct {
	WorkBudget               int                      `json:"work_budget"`
	Initial                  ReplacementGeneration    `json:"initial"`
	Replacements             [2]ReplacementGeneration `json:"replacements"`
	Held                     WireStats                `json:"held"`
	Fresh                    WireStats                `json:"fresh"`
	TerminalReplay           bool                     `json:"terminal_replay"`
	AdmittedReplay           bool                     `json:"admitted_replay"`
	CaptureReleased          bool                     `json:"capture_released"`
	AcrossReplacementReplay  [2]bool                  `json:"across_replacement_replay"`
	RetiredPinsAtReplacement [2]uint64                `json:"retired_pins_at_replacement"`
	RetirementToZeroObserved time.Duration            `json:"retirement_to_zero_observed_ns"`
}

type replacementData struct {
	seed   GenerationReplacementSeed
	cycles [2][][]Row
	walk   WireWalk
}

func makeReplacementData(cfg GenerationReplacementConfig) replacementData {
	start := cfg.Epoch.UTC()
	tie := start.Add(4 * time.Millisecond)
	end := start.Add(time.Second)
	const literal = "keep"
	duplicate := Row{Time: tie, Line: "keep duplicate"}
	return replacementData{
		seed: GenerationReplacementSeed{
			Checkpoint: []Row{
				{Time: tie, Line: "keep first tie"},
				{Time: start.Add(8 * time.Millisecond), Line: "keep last"},
				duplicate,
			},
			Tail: []Row{
				{Time: start.Add(2 * time.Millisecond), Line: "keep opening"},
				{Time: tie.In(fixtureOffsets[2]), Line: "keep tail tie"},
				duplicate,
				{Time: tie, Line: "KEEP misses"},
			},
		},
		cycles: [2][][]Row{
			{
				{{Time: tie, Line: "keep cycle one tie"}, duplicate},
				{{Time: start.Add(time.Millisecond), Line: "keep cycle one backfill"}},
			},
			{
				{{Time: tie, Line: "keep cycle two tie"}, {Time: end, Line: "keep exclusive end"}},
				{{Time: start, Line: "keep cycle two backfill"}, duplicate},
			},
		},
		walk: WireWalk{
			Filter: Filter{Start: start, End: end, Contains: literal},
			// First issue with two rows, then take one row between the two
			// replacements. The same pattern also bounds the fresh walk.
			RowLimits: []int{2, 1, 3}, FirstPageRows: 2, Limits: cfg.Limits,
		},
	}
}

// VerifyGenerationReplacement checks one capture across two tail-only
// replacements, exact terminal replay and retirement with a paused resume.
func VerifyGenerationReplacement(ctx context.Context, cfg GenerationReplacementConfig, launch LaunchGenerationReplacement) (GenerationReplacementResult, error) {
	return verifyGenerationReplacement(ctx, cfg, launch, true)
}

// VerifyGenerationReplacementReplay checks the same walk and idle retirement
// on an endpoint without a server-side delivery gate. The admitted-response
// lifecycle is measured by VerifyGenerationReplacement.
func VerifyGenerationReplacementReplay(ctx context.Context, cfg GenerationReplacementConfig, launch LaunchGenerationReplacement) (GenerationReplacementResult, error) {
	return verifyGenerationReplacement(ctx, cfg, launch, false)
}

func verifyGenerationReplacement(ctx context.Context, cfg GenerationReplacementConfig, launch LaunchGenerationReplacement, pause bool) (GenerationReplacementResult, error) {
	result := GenerationReplacementResult{WorkBudget: cfg.WorkBudget}
	if launch == nil || cfg.WorkBudget <= 0 || cfg.Epoch.IsZero() {
		return result, errors.New("generation replacement requires a launcher, a positive work budget and an epoch")
	}
	data := makeReplacementData(cfg)
	if err := data.walk.validate(); err != nil {
		return result, err
	}
	// Twelve rows match after both cycles. Even without byte trimming the
	// row-limit schedule needs six pages to exhaust the fresh walk.
	if cfg.Limits.MaxPages < 6 {
		return result, errors.New("generation replacement requires at least six pages")
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Limits.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}

	seed := GenerationReplacementSeed{
		Checkpoint: slices.Clone(data.seed.Checkpoint),
		Tail:       slices.Clone(data.seed.Tail),
	}
	var calls int
	var trialErr error
	launchErr := launch(ctx, cfg.WorkBudget, seed, func(f GenerationReplacementAdapter) error {
		calls++
		if calls != 1 {
			trialErr = errors.Join(trialErr, errors.New("launcher ran more than one replacement trial"))
		} else {
			trialErr = runGenerationReplacement(ctx, data, f, pause, &result)
		}
		return trialErr
	})
	if calls == 0 && launchErr == nil {
		launchErr = errors.New("launcher did not run the replacement trial")
	}
	if launchErr != nil && !errors.Is(launchErr, trialErr) {
		trialErr = errors.Join(trialErr, launchErr)
	} else if launchErr != nil {
		trialErr = launchErr
	}
	return result, errors.Join(trialErr, ctx.Err())
}

func runGenerationReplacement(ctx context.Context, data replacementData, f GenerationReplacementAdapter, pause bool, result *GenerationReplacementResult) error {
	if f.Acknowledge == nil || f.Fetch == nil || f.Activate == nil || f.Refuse == nil ||
		f.Retire == nil || f.RetiredPins == nil || pause && f.HoldResume == nil {
		return errors.New("generation replacement requires acknowledgement, fetch, activation, retirement, refusal and pin observations, plus a delivery gate for paused trials")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var ledger Log
	covered := ledger.Append(data.seed.Checkpoint)
	highWater := ledger.Append(data.seed.Tail)
	if err := checkReplacementPoint(f.Initial, covered, highWater); err != nil {
		return fmt.Errorf("initial checkpoint and committed tail: %w", err)
	}
	result.Initial = f.Initial
	if err := requireReplacementPins(ctx, f, false); err != nil {
		return fmt.Errorf("initial retired pins: %w", err)
	}
	matches, err := ledger.Match(highWater, data.walk.Filter)
	if err != nil {
		return err
	}

	var opening string
	var terminal terminalReplay
	resumes := 0
	var retryRequest WireRequest
	var retryBody []byte
	checkRetry := func(page WirePage) error {
		if err := checkWirePage(retryRequest, page, matches[2:], 2, nil); err != nil {
			return err
		}
		if page.EncodedBody == nil || !bytes.Equal(retryBody, page.EncodedBody) {
			return errors.New("continuation body changed across replacement")
		}
		return nil
	}
	fetchHeld := func(ctx context.Context, req WireRequest) (WirePage, error) {
		// The opening hook takes the first replacement. The successful
		// one-row continuation then straddles the two activations.
		if req.Cursor != "" {
			resumes++
			if resumes == 2 {
				if err := replaceGeneration(ctx, &ledger, data, 1, f, result); err != nil {
					return WirePage{}, err
				}
			}
		}
		page, err := f.Fetch(ctx, req)
		if err == nil && req == retryRequest {
			if err := checkRetry(page); err != nil {
				return WirePage{}, err
			}
			result.AcrossReplacementReplay[0] = true
		}
		if err == nil && req.Cursor != "" && page.NextCursor == "" {
			terminal.record(req, page)
		}
		return page, err
	}
	result.Held, err = VerifyWireWalk(ctx, &ledger, data.walk, fetchHeld, func(ctx context.Context, cursor string) error {
		opening = cursor
		retryRequest = WireRequest{Filter: data.walk.Filter, Limit: data.walk.RowLimits[1], Cursor: cursor, MaxPageBytes: data.walk.Limits.MaxPageBytes}
		page, err := f.Fetch(ctx, retryRequest)
		if err != nil {
			return redactWireFailure(fmt.Errorf("read witness before replacement: %w", err), cursor, page.NextCursor)
		}
		if err := checkWirePage(retryRequest, page, matches[2:], 2, nil); err != nil {
			return err
		}
		if page.EncodedBody == nil {
			return errors.New("continuation before replacement is missing its encoded body witness")
		}
		retryBody = bytes.Clone(page.EncodedBody)
		return replaceGeneration(ctx, &ledger, data, 0, f, result)
	})
	if err != nil {
		return fmt.Errorf("held walk at work budget %d: %w", result.WorkBudget, err)
	}
	if resumes < 2 || terminal.req.Cursor == "" {
		return errors.New("held walk did not resume through both replacements to a terminal continuation")
	}
	page, err := f.Fetch(ctx, retryRequest)
	if err == nil {
		err = checkRetry(page)
	}
	if err != nil {
		return redactWireFailure(fmt.Errorf("retry after second replacement: %w", err), retryRequest.Cursor, page.NextCursor)
	}
	result.AcrossReplacementReplay[1] = true
	if err := terminal.verify(ctx, f.Fetch, matches, result.Held.Rows, true); err != nil {
		return fmt.Errorf("terminal replay at work budget %d: %w", result.WorkBudget, err)
	}
	result.TerminalReplay = true

	result.Fresh, err = VerifyWireWalk(ctx, &ledger, data.walk, f.Fetch, nil)
	if err != nil {
		return fmt.Errorf("fresh walk after both replacements: %w", err)
	}
	if result.Fresh.Rows <= result.Held.Rows {
		return errors.New("fresh walk did not include later matching rows")
	}

	return drainReplacementCapture(ctx, f, opening, terminal, matches, pause, result)
}

func checkReplacementPoint(current ReplacementGeneration, covered, committed int) error {
	if !current.Complete || current.CoveredRows != covered || current.CommittedRows != committed || committed <= covered {
		return fmt.Errorf("observed %+v, want complete coverage at %d with %d committed rows", current, covered, committed)
	}
	return nil
}

func replaceGeneration(ctx context.Context, ledger *Log, data replacementData, index int, f GenerationReplacementAdapter, result *GenerationReplacementResult) error {
	previous := result.Initial
	if index > 0 {
		previous = result.Replacements[index-1]
	}
	phase := fmt.Sprintf("replacement %d", index+1)
	var cutoff int
	for i, batch := range data.cycles[index] {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.Acknowledge(ctx, slices.Clone(batch)); err != nil {
			return fmt.Errorf("%s acknowledge batch %d: %w", phase, i+1, err)
		}
		committed := ledger.Append(batch)
		if i == 0 {
			cutoff = committed
		}
	}
	current, err := f.Activate(ctx, cutoff)
	if err != nil {
		return fmt.Errorf("%s activate cutoff %d: %w", phase, cutoff, err)
	}
	if err := checkReplacementPoint(current, cutoff, ledger.HighWater()); err != nil {
		return fmt.Errorf("%s: %w", phase, err)
	}
	if current.Generation <= previous.Generation || current.CoveredRows <= previous.CoveredRows {
		return fmt.Errorf("%s did not advance beyond %+v: %+v", phase, previous, current)
	}
	result.Replacements[index] = current
	pins, err := observeReplacementPins(ctx, f, true)
	if err != nil {
		return fmt.Errorf("%s predecessor pin: %w", phase, err)
	}
	result.RetiredPinsAtReplacement[index] = pins
	return nil
}

func requireReplacementPins(ctx context.Context, f GenerationReplacementAdapter, held bool) error {
	_, err := observeReplacementPins(ctx, f, held)
	return err
}

func observeReplacementPins(ctx context.Context, f GenerationReplacementAdapter, held bool) (uint64, error) {
	pins, err := f.RetiredPins(ctx)
	if err != nil {
		return pins, err
	}
	if (pins > 0) != held {
		return pins, fmt.Errorf("retired pins=%d, want live predecessor pin=%t", pins, held)
	}
	return pins, ctx.Err()
}

func drainReplacementCapture(ctx context.Context, f GenerationReplacementAdapter, opening string, terminal terminalReplay, matches []Entry, pause bool, result *GenerationReplacementResult) error {
	req := terminal.req
	expired := func() error {
		for _, cursor := range []string{opening, req.Cursor} {
			resume := req
			resume.Cursor = cursor
			if err := f.Refuse(ctx, resume, http.StatusGone, expiredCaptureBody); err != nil {
				return redactWireFailure(err, opening, req.Cursor)
			}
		}
		return ctx.Err()
	}
	var retirement time.Time
	retire := func() error {
		retirement = time.Now()
		if err := f.Retire(ctx, req.Filter, req.Cursor); err != nil {
			return redactWireFailure(fmt.Errorf("retire predecessor capture: %w", err), opening, req.Cursor)
		}
		return nil
	}
	if err := requireReplacementPins(ctx, f, true); err != nil {
		return fmt.Errorf("before retirement: %w", err)
	}

	if pause {
		var calls int
		var callbackErr error
		page, err := f.HoldResume(ctx, req, func() error {
			calls++
			if calls > 1 {
				callbackErr = errors.Join(callbackErr, errors.New("delivery gate ran callback more than once"))
			} else {
				callbackErr = retireWhilePaused(retire, expired, func() error {
					return requireReplacementPins(ctx, f, true)
				})
			}
			return callbackErr
		})
		if err != nil || callbackErr != nil {
			return redactWireFailure(fmt.Errorf("paused resume: %w", errors.Join(err, callbackErr)), opening, req.Cursor, page.NextCursor)
		}
		if calls != 1 {
			return errors.New("delivery gate did not witness an admitted resume")
		}
		// The returned page must come from the admitted worker. Reissuing
		// the terminal request here would test a different request.
		if err := terminal.verify(ctx, func(context.Context, WireRequest) (WirePage, error) {
			return page, nil
		}, matches, result.Held.Rows, true); err != nil {
			return fmt.Errorf("admitted terminal response after retirement: %w", err)
		}
		result.AdmittedReplay = true
	} else if err := retire(); err != nil {
		return err
	}

	if err := expired(); err != nil {
		return fmt.Errorf("new resume after retirement: %w", err)
	}
	if err := waitForReplacementRelease(ctx, f.RetiredPins); err != nil {
		return fmt.Errorf("after final response: %w", err)
	}
	result.RetirementToZeroObserved = time.Since(retirement)
	result.CaptureReleased = true
	return nil
}

func retireWhilePaused(retire, expired, pinned func() error) error {
	if err := retire(); err != nil {
		return err
	}
	if err := expired(); err != nil {
		return fmt.Errorf("new resume during retirement: %w", err)
	}
	if err := pinned(); err != nil {
		return fmt.Errorf("retirement released the paused capture: %w", err)
	}
	return nil
}

func waitForReplacementRelease(ctx context.Context, pins func(context.Context) (uint64, error)) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for retired capture to drain: %w", err)
		}
		live, err := pins(ctx)
		if err != nil {
			return err
		}
		if live == 0 {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %d retired pins to drain: %w", live, ctx.Err())
		case <-ticker.C:
		}
	}
}
