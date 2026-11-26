package httpapi

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	maxQueryLimit             = 1000
	maxQueryWindows           = 4096
	maxQueryProofPositions    = 256 << 10
	queryCertificationPasses  = 16
	maxQueryCertificationWork = 2 << 30
)

var errInvalidPage = errors.New("invalid result from query page source")

// A window records a repeatable source request and a bound on preparing any
// suffix of its rows. A cursor inside the window replays that same request,
// then starts delivery after the signed offset. This certifies smaller limits
// and byte cuts without probing every possible limit at every row.
type queryWindow struct {
	after, last                   PagePosition
	limit, count                  int
	more, oversized               bool
	scanCost, cost, nextTokenCost uint64
}

func (a *API) certifyQuery(ctx context.Context, c *queryCapture, filter PageFilter, after PagePosition) error {
	// Each replay must fit one request. Cap the total proof work separately,
	// including failed trials, so certification cannot walk unbounded history.
	total := min(a.queryWorkBudget, maxQueryCertificationWork/queryCertificationPasses) * queryCertificationPasses
	proof := store.NewQueryWork(ctx, total)
	req := PageRequest{Filter: filter, Snapshot: true, Capture: c.sourceID, After: after, Limit: maxQueryLimit}
	measure := queryResponse{maxBytes: a.maxResponseBytes}
	positionBytes := len(after)
	for len(c.windows) < maxQueryWindows {
		if !a.captures.now().Before(c.deadline) {
			return ErrCaptureExpired
		}
		window, err := a.certifyWindow(ctx, req, &measure, proof)
		if errors.Is(err, store.ErrQueryWorkExhausted) && req.Limit > 1 && proof.Remaining() > 0 {
			req.Limit /= 2
			continue
		}
		if err != nil {
			return err
		}
		positionBytes += len(window.last)
		if positionBytes > maxQueryProofPositions {
			return store.ErrQueryWorkExhausted
		}
		c.windows = append(c.windows, window)
		if !window.more || window.oversized {
			return nil
		}
		req.After = window.last
	}
	return store.ErrQueryWorkExhausted
}

func (a *API) certifyWindow(ctx context.Context, req PageRequest, measure *queryResponse, proof *store.QueryWork) (queryWindow, error) {
	work := proof.WithLimit(a.queryWorkBudget)
	before := work.Remaining()
	// Charge at least one unit per probe, even for an empty source result.
	if err := work.Charge(1); err != nil {
		return queryWindow{}, err
	}
	scanStart := work.Remaining()
	page, err := a.source.Page(ctx, req, work)
	if err != nil {
		return queryWindow{}, err
	}
	if err := validateSnapshotResult(req, page); err != nil {
		return queryWindow{}, err
	}
	window := queryWindow{
		after: req.After, limit: req.Limit, count: len(page.Rows), more: page.More,
		scanCost: scanStart - work.Remaining(),
	}
	maxPosition := len(req.After)
	for _, row := range page.Rows {
		if err := work.Charge(1); err != nil {
			return queryWindow{}, err
		}
		stamp, err := row.Row.TS.MarshalJSON()
		if err != nil {
			return queryWindow{}, err
		}
		size, err := measure.measureLine(row.Row.Line, stamp, measure.maxBytes, false, work)
		if err != nil {
			return queryWindow{}, err
		}
		if size > measure.maxBytes {
			// Every walk reaching this record will return the established 422.
			window.oversized = true
			break
		}
		if err := work.Charge(queryDeliveryUnits(size)); err != nil {
			return queryWindow{}, err
		}
		maxPosition = max(maxPosition, len(row.Position))
	}
	window.last = PagePosition(strings.Clone(string(page.Rows[len(page.Rows)-1].Position)))
	window.cost = before - work.Remaining()

	window.nextTokenCost = cursorUnits(maxPosition)
	// Reserve the worst authenticated position entering this window and the
	// worst token that a shorter or byte-trimmed response can leave from it.
	if err := work.Charge(uint64(len(req.Filter.Contains)), cursorUnits(maxPosition), window.nextTokenCost); err != nil {
		return queryWindow{}, err
	}
	return window, nil
}

func validateSnapshotResult(req PageRequest, page PageResult) error {
	if len(page.Rows) > req.Limit || (page.More && len(page.Rows) == 0) {
		return errInvalidPage
	}
	if req.Capture == "" {
		if page.Capture == "" || len(page.Capture) > maxCursorPosition || !utf8.ValidString(page.Capture) {
			return errInvalidPage
		}
	} else if (page.Capture != "" && page.Capture != req.Capture) || len(page.Rows) == 0 {
		// The preceding result established a further match.
		return errInvalidPage
	}
	previous := req.After
	for _, row := range page.Rows {
		if !validPagePosition(row.Position) || row.Position == previous {
			return errInvalidPage
		}
		previous = row.Position
	}
	return nil
}
