package querymodel

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// OpeningFault selects the transport boundary for one opening request.
type OpeningFault uint8

const (
	BeforeHeaders OpeningFault = iota + 1
	DuringBody
)

// InterruptedOpening contains client observations from the failed exchange.
// The adapter must also independently observe a server write failure before
// returning success. Cursor and DeclaredBytes are zero before header delivery.
type InterruptedOpening struct {
	Cursor        string
	DeclaredBytes int64
	Prefix        []byte
}

// CaptureDeliveryFixture supplies an initially empty, exclusive endpoint.
// Refuse checks a complete plain-text response, including absence of a
// continuation in headers and trailers. HoldResume calls its callback only
// when an admitted resume is held before delivery and joins the worker on
// every exit. It returns the accepted page after releasing the worker.
type CaptureDeliveryFixture struct {
	Acknowledge AcknowledgeBatch
	Fetch       FetchWirePage
	Refuse      func(context.Context, WireRequest, int, string) error
	Interrupt   func(context.Context, WireRequest, OpeningFault) (InterruptedOpening, error)
	HoldResume  func(context.Context, WireRequest, func() error) (WirePage, error)
	Retire      func(context.Context, Filter, string) error
	Publish     func(context.Context, int) error
}

const (
	legacyCaptureLimit  = 64
	captureCapacityBody = "query capacity exhausted\n"
	expiredCaptureBody  = "query capture expired\n"
)

// CaptureDeliveryScenario checks one observable lifecycle boundary.
type CaptureDeliveryScenario uint8

const (
	ReuseBeforeHeaders CaptureDeliveryScenario = iota + 1
	ResumeAfterHeaders
	DrainAtCapacity
)

func deliveryFixture(epoch time.Time, limits WireLimits) ([]Row, WireWalk) {
	tie := epoch.Add(time.Millisecond)
	rows := []Row{
		{Time: tie, Line: "third in query order"},
		{Time: epoch, Line: "first in query order"},
		{Time: tie, Line: "fourth in query order"},
		{Time: epoch, Line: "second in query order"},
	}
	walk := WireWalk{
		Filter:    Filter{Start: epoch, End: epoch.Add(time.Second)},
		RowLimits: []int{1, 4}, Limits: limits,
	}
	return rows, walk
}

// VerifyCaptureDelivery uses a fresh fixture for each boundary. No test of
// capture capacity interprets tokens, retries a failed open, or waits for TTL.
func VerifyCaptureDelivery(ctx context.Context, epoch time.Time, limits WireLimits, scenario CaptureDeliveryScenario, f CaptureDeliveryFixture) error {
	rows, walk := deliveryFixture(epoch, limits)
	if err := walk.validate(); err != nil {
		return err
	}
	if f.Acknowledge == nil || f.Fetch == nil {
		return errors.New("capture delivery requires acknowledgement and page fetch")
	}
	switch scenario {
	case ReuseBeforeHeaders:
		if f.Interrupt == nil || f.Refuse == nil {
			return errors.New("slot reuse requires opening interruption and refusal witnesses")
		}
	case ResumeAfterHeaders:
		if f.Interrupt == nil {
			return errors.New("interrupted resume requires an opening interruption witness")
		}
	case DrainAtCapacity:
		if f.HoldResume == nil || f.Retire == nil || f.Refuse == nil {
			return errors.New("retirement requires hold, retirement, and refusal controls")
		}
	default:
		return fmt.Errorf("unknown capture delivery scenario %d", scenario)
	}

	ctx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	var ledger Log
	if err := f.Acknowledge(ctx, rows); err != nil {
		return fmt.Errorf("seed delivery fixture: %w", err)
	}
	ledger.Append(rows)
	if f.Publish != nil {
		if err := f.Publish(ctx, ledger.HighWater()); err != nil {
			return fmt.Errorf("publish seed: %w", err)
		}
	}
	open := WireRequest{Filter: walk.Filter, Limit: 1, MaxPageBytes: limits.MaxPageBytes}
	matches, err := ledger.Match(ledger.HighWater(), walk.Filter)
	if err != nil {
		return err
	}
	switch scenario {
	case ReuseBeforeHeaders:
		return verifySlotReuse(ctx, f, open, matches)
	case ResumeAfterHeaders:
		return verifyInterruptedSuffix(ctx, f, open, walk, &ledger, matches)
	default:
		return verifyRetirementDrain(ctx, f, open, matches)
	}
}

func openCaptures(ctx context.Context, f CaptureDeliveryFixture, req WireRequest, matches []Entry, count int) (map[string]int, string, error) {
	seen := make(map[string]int, count)
	var last string
	for i := 1; i <= count; i++ {
		var err error
		last, err = openCapture(ctx, f.Fetch, req, matches, seen)
		if err != nil {
			return nil, "", fmt.Errorf("open capture %d of %d: %w", i, count, err)
		}
		seen[last] = i
	}
	return seen, last, nil
}

func openCapture(ctx context.Context, fetch FetchWirePage, req WireRequest, matches []Entry, seen map[string]int) (string, error) {
	page, err := fetch(ctx, req)
	if err != nil {
		return "", redactWireFailure(err, page.NextCursor)
	}
	if err := checkWirePage(req, page, matches, 0, seen); err != nil {
		return "", err
	}
	return page.NextCursor, nil
}

func verifySlotReuse(ctx context.Context, f CaptureDeliveryFixture, open WireRequest, matches []Entry) error {
	seen, _, err := openCaptures(ctx, f, open, matches, legacyCaptureLimit-1)
	if err != nil {
		return err
	}
	fault, err := f.Interrupt(ctx, open, BeforeHeaders)
	if err != nil {
		return fmt.Errorf("interrupt opening before headers: %w", err)
	}
	if fault.Cursor != "" || fault.DeclaredBytes != 0 || len(fault.Prefix) != 0 {
		return errors.New("pre-header failure delivered response bytes")
	}
	if _, err := openCapture(ctx, f.Fetch, open, matches, seen); err != nil {
		return fmt.Errorf("capture slot %d was not reusable after the server-observed pre-header failure: %w", legacyCaptureLimit, err)
	}
	return f.Refuse(ctx, open, http.StatusServiceUnavailable, captureCapacityBody)
}

func verifyInterruptedSuffix(ctx context.Context, f CaptureDeliveryFixture, open WireRequest, walk WireWalk, ledger *Log, matches []Entry) error {
	// The fault adapter has already discarded the incomplete opening body.
	// Preserve the actual suffix expected at this cursor before publishing more.
	var suffix Log
	for _, entry := range matches[1:] {
		suffix.Append([]Row{entry.Row})
	}
	fault, err := f.Interrupt(ctx, open, DuringBody)
	if err != nil {
		return fmt.Errorf("interrupt opening body: %w", err)
	}
	if fault.Cursor == "" || len(fault.Cursor) > maxOpaqueCursorBytes || fault.DeclaredBytes > open.MaxPageBytes ||
		len(fault.Prefix) == 0 || int64(len(fault.Prefix)) >= fault.DeclaredBytes {
		return errors.New("expected a received cursor and a strictly incomplete opening body")
	}
	backfill := []Row{
		{Time: walk.Filter.Start, Line: "later tie"},
		{Time: walk.Filter.Start.Add(500 * time.Microsecond), Line: "later backfill"},
	}
	if err := f.Acknowledge(ctx, backfill); err != nil {
		return fmt.Errorf("acknowledge backfill: %w", err)
	}
	ledger.Append(backfill)
	if f.Publish != nil {
		if err := f.Publish(ctx, ledger.HighWater()); err != nil {
			return fmt.Errorf("publish backfill: %w", err)
		}
	}
	var witness terminalReplay
	stats, err := VerifyWireWalk(ctx, &suffix, walk, func(ctx context.Context, req WireRequest) (WirePage, error) {
		if req.Cursor == "" {
			req.Cursor = fault.Cursor
		}
		page, err := f.Fetch(ctx, req)
		if err != nil {
			return WirePage{}, redactWireFailure(err, req.Cursor, page.NextCursor)
		}
		if page.NextCursor == "" {
			witness.record(req, page)
		}
		return page, nil
	}, nil)
	if err != nil {
		return fmt.Errorf("resume interrupted opening: %w", err)
	}
	if err := witness.verify(ctx, f.Fetch, matches[1:], stats.Rows, true); err != nil {
		return fmt.Errorf("replay interrupted opening's terminal request: %w", err)
	}
	if _, err := VerifyWireWalk(ctx, ledger, walk, f.Fetch, nil); err != nil {
		return fmt.Errorf("fresh walk after interrupted opening: %w", err)
	}
	return nil
}

func verifyRetirementDrain(ctx context.Context, f CaptureDeliveryFixture, open WireRequest, matches []Entry) error {
	seen, token, err := openCaptures(ctx, f, open, matches, legacyCaptureLimit)
	if err != nil {
		return err
	}
	full := func() error {
		return f.Refuse(ctx, open, http.StatusServiceUnavailable, captureCapacityBody)
	}
	if err := full(); err != nil {
		return fmt.Errorf("capture ceiling: %w", err)
	}
	req := open
	req.Cursor, req.Limit = token, len(matches)
	page, err := f.Fetch(ctx, req)
	if err != nil {
		return redactWireFailure(fmt.Errorf("read terminal page: %w", err), token, page.NextCursor)
	}
	if err := checkWirePage(req, page, matches[1:], 1, nil); err != nil {
		return err
	}
	if page.NextCursor != "" || len(page.Rows) != len(matches)-1 {
		return errors.New("capacity fixture did not return the expected terminal page")
	}
	var witness terminalReplay
	witness.record(req, page)
	// Change a single base64url character without decoding the cursor.
	forged := "A" + token[1:]
	if forged == token {
		forged = "B" + token[1:]
	}
	if err := f.Retire(ctx, req.Filter, forged); err == nil {
		return errors.New("retirement accepted a modified cursor")
	}
	if err := witness.verify(ctx, f.Fetch, matches, len(matches), true); err != nil {
		return fmt.Errorf("terminal replay before retirement: %w", err)
	}

	expired := func() error {
		return f.Refuse(ctx, req, http.StatusGone, expiredCaptureBody)
	}
	checkHeld := func(page WirePage) error {
		// Feed the independently received page to the same byte and row
		// witness used above. No network retry can stand in for this worker.
		return witness.verify(ctx, func(context.Context, WireRequest) (WirePage, error) {
			return page, nil
		}, matches, len(matches), true)
	}
	outer, err := f.HoldResume(ctx, req, func() error {
		inner, err := f.HoldResume(ctx, req, func() error {
			if err := f.Retire(ctx, req.Filter, token); err != nil {
				return redactWireFailure(fmt.Errorf("retire capture at the %d-slot limit: %w", legacyCaptureLimit, err), token)
			}
			if err := expired(); err != nil {
				return fmt.Errorf("new resume during retirement: %w", err)
			}
			return full()
		})
		if err != nil {
			return err
		}
		if err := checkHeld(inner); err != nil {
			return fmt.Errorf("first admitted resume: %w", err)
		}
		return full()
	})
	if err != nil {
		return redactWireFailure(fmt.Errorf("drain admitted resumes: %w", err), token)
	}
	if err := checkHeld(outer); err != nil {
		return fmt.Errorf("final admitted resume: %w", err)
	}
	if err := expired(); err != nil {
		return fmt.Errorf("resume after retirement drained: %w", err)
	}
	if err := f.Retire(ctx, req.Filter, token); err != nil {
		return redactWireFailure(fmt.Errorf("repeat retirement: %w", err), token)
	}
	if _, err := openCapture(ctx, f.Fetch, open, matches, seen); err != nil {
		return fmt.Errorf("capacity did not return after final admitted resume: %w", err)
	}
	return full()
}
