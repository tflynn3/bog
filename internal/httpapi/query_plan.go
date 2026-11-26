package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	maxQueryLimit     = 1000
	maxCertifiedScans = 512
	maxOpaqueBytes    = 1024
)

// A plan stores one anchor per source scan rather than retaining rows or an
// entry for each possible client page. The cursor selects a scan and an offset
// into its results. Both are authenticated along with the last emitted position.
type certifiedScan struct {
	after PagePosition
	count int
}

var errPageSourceContract = errors.New("invalid result from query page source")

func validOpaque(value string) bool {
	return len(value) > 0 && len(value) <= maxOpaqueBytes && utf8.ValidString(value)
}

func checkSnapshotScan(result PageResult, req PageRequest, work *store.QueryWork) error {
	if (result.Capture != "" && result.Capture != req.Capture) ||
		len(result.Rows) > req.Limit ||
		(len(result.Rows) == 0 && (result.More || req.After != "")) {
		return errPageSourceContract
	}
	previous := req.After
	for _, row := range result.Rows {
		if err := work.Charge(1); err != nil {
			return err
		}
		if !validOpaque(string(row.Position)) || row.Position == req.After || row.Position == previous {
			return errPageSourceContract
		}
		previous = row.Position
	}
	return nil
}

func (a *API) startQuery(w http.ResponseWriter, ctx context.Context, filter PageFilter, limit int, work *store.QueryWork) {
	capture, err := a.captures.reserve()
	if err != nil {
		writeQueryError(w, err)
		return
	}
	published := false
	defer func() { a.captures.done(capture, !published) }()

	req := PageRequest{Filter: filter, Limit: limit, Snapshot: true}
	result, err := a.captures.source.Page(ctx, req, work)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	capture.sourceID = result.Capture
	if !validOpaque(capture.sourceID) {
		writeQueryError(w, errPageSourceContract)
		return
	}
	capture.sourceID = strings.Clone(capture.sourceID)
	if !result.ExpiresAt.IsZero() && result.ExpiresAt.Before(capture.deadline) {
		capture.deadline = result.ExpiresAt
	}
	if !time.Now().Before(capture.deadline) {
		writeQueryError(w, ErrCaptureExpired)
		return
	}
	req.Capture = capture.sourceID
	response := queryResponse{maxBytes: a.maxResponseBytes}
	last, more, err := a.prepareFirstPage(ctx, req, result, &response, work)
	if err == nil {
		err = response.reserveDelivery(work)
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if !more {
		response.write(w, "")
		return
	}

	// Start the replay plan at the actual byte/row cut, including when the
	// source's first result already proved exhaustion beyond the cut.
	req.After = PagePosition(strings.Clone(string(last)))
	req.Limit = maxQueryLimit
	if err := a.certifyQuery(ctx, capture, req, work); err != nil {
		writeQueryError(w, err)
		return
	}
	// A replay can validate one token and create another. It can hash the
	// first anchor once, and certification already covers later positions.
	// Reserve that allowance in the first request, whose cursor work fits it.
	if err := work.Charge(2*cursorTokenWork, uint64(len(filter.Contains)), cursorBlockWork(len(last))); err != nil {
		writeQueryError(w, err)
		return
	}
	cursor := capture.cursor(a.filterDigest(filter), 0, 0, last)
	token := a.encodeCursor(cursor)
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	if err := a.captures.publish(capture); err != nil {
		writeQueryError(w, err)
		return
	}
	published = true
	response.write(w, token)
}

// A source may return a short scan with lookahead. Keep filling the HTTP
// page until its row or byte cap is reached, or the source proves exhaustion.
func (a *API) prepareFirstPage(ctx context.Context, req PageRequest, result PageResult, response *queryResponse, work *store.QueryWork) (PagePosition, bool, error) {
	last := req.After
	for {
		if err := checkSnapshotScan(result, req, work); err != nil {
			return "", false, err
		}
		n, err := response.appendRows(result.Rows, work)
		if err != nil {
			return "", false, err
		}
		if n != 0 {
			last = result.Rows[n-1].Position
		}
		if n < len(result.Rows) {
			return last, true, nil
		}
		req.Limit -= n
		if req.Limit == 0 || !result.More {
			return last, result.More, nil
		}
		req.After = last
		if err := work.Charge(1); err != nil {
			return "", false, err
		}
		result, err = a.captures.source.Page(ctx, req, work)
		if err != nil {
			return "", false, err
		}
	}
}

// Each replay visits a subset of the certified scans, at most once per
// request. Its encoder handles each considered row at most once. Certification
// measures every row (including a potential byte-trimmed row), and the sum
// of the rounded delivery charges bounds delivery of any subset.
func (a *API) certifyQuery(ctx context.Context, capture *queryCapture, req PageRequest, work *store.QueryWork) error {
	measure := queryResponse{}
	for len(capture.scans) < maxCertifiedScans {
		if err := work.Charge(1); err != nil {
			return err
		}
		result, err := a.captures.source.Page(ctx, req, work)
		if err != nil {
			return err
		}
		// The first response was trimmed or had lookahead. Each later scan
		// was preceded by lookahead. Both imply at least one match here.
		if err := checkSnapshotScan(result, req, work); err != nil {
			return err
		}
		for _, row := range result.Rows {
			// Reserve row setup and any cursor anchor hashing during replay.
			if err := work.Charge(1, cursorBlockWork(len(row.Position))); err != nil {
				return err
			}
			_, size, _, err := measure.measureRow(row.Row, a.maxResponseBytes, false, work)
			if err != nil {
				return err
			}
			if size > a.maxResponseBytes {
				return &oversizedQueryRecord{a.maxResponseBytes, size}
			}
			if err := work.Charge(queryDeliveryWork(size)); err != nil {
				return err
			}
		}
		capture.scans = append(capture.scans, certifiedScan{req.After, len(result.Rows)})
		if !result.More {
			return nil
		}
		req.After = PagePosition(strings.Clone(string(result.Rows[len(result.Rows)-1].Position)))
	}
	return store.ErrQueryWorkExhausted
}

func (c *queryCapture) cursor(filter [32]byte, scan, offset int, last PagePosition) cursorData {
	if offset == c.scans[scan].count {
		scan++
		offset = 0
	}
	return cursorData{Capture: c.id, Filter: filter, Scan: uint16(scan), Offset: uint16(offset), After: positionDigest(last)}
}

func (a *API) resumeQuery(w http.ResponseWriter, ctx context.Context, filter PageFilter, limit int, raw string, work *store.QueryWork) {
	if len(raw) != cursorTokenSize {
		writeQueryError(w, store.ErrInvalidQueryCursor)
		return
	}
	if err := work.Charge(cursorTokenWork, uint64(len(filter.Contains))); err != nil {
		writeQueryError(w, err)
		return
	}
	cursor, err := a.decodeCursor(raw)
	if err != nil || cursor.Filter != a.filterDigest(filter) {
		writeQueryError(w, store.ErrInvalidQueryCursor)
		return
	}
	capture, err := a.captures.acquire(cursor.Capture)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	defer a.captures.done(capture, false)

	response := queryResponse{maxBytes: a.maxResponseBytes}
	token, err := a.replayQuery(ctx, capture, cursor, filter, limit, &response, work)
	if err != nil {
		if errors.Is(err, ErrCaptureExpired) {
			a.captures.retire(capture)
		}
		writeQueryError(w, err)
		return
	}
	response.write(w, token)
}

func (a *API) replayQuery(ctx context.Context, capture *queryCapture, cursor cursorData, filter PageFilter, limit int, response *queryResponse, work *store.QueryWork) (string, error) {
	scan, offset := int(cursor.Scan), int(cursor.Offset)
	if scan >= len(capture.scans) || offset >= capture.scans[scan].count {
		return "", store.ErrInvalidQueryCursor
	}
	var last PagePosition
	for scan < len(capture.scans) {
		step := capture.scans[scan]
		if err := work.Charge(1); err != nil {
			return "", err
		}
		req := PageRequest{Filter: filter, Limit: maxQueryLimit, Snapshot: true, Capture: capture.sourceID, After: step.after}
		result, err := a.captures.source.Page(ctx, req, work)
		if err != nil {
			return "", err
		}
		if err := checkSnapshotScan(result, req, work); err != nil {
			return "", err
		}
		if len(result.Rows) != step.count || result.More != (scan+1 < len(capture.scans)) {
			return "", errPageSourceContract
		}
		if result.More && result.Rows[step.count-1].Position != capture.scans[scan+1].after {
			return "", errPageSourceContract
		}
		if scan == int(cursor.Scan) {
			anchor := step.after
			if offset > 0 {
				anchor = result.Rows[offset-1].Position
			}
			if err := work.Charge(cursorBlockWork(len(anchor))); err != nil {
				return "", err
			}
			if positionDigest(anchor) != cursor.After {
				return "", errPageSourceContract
			}
		}
		rows := result.Rows[offset:min(offset+limit, step.count)]
		n, err := response.appendRows(rows, work)
		if err != nil {
			return "", err
		}
		if n != 0 {
			offset += n
			last = rows[n-1].Position
			limit -= n
		}
		if offset < step.count || (limit == 0 && result.More) {
			if err := work.Charge(cursorTokenWork, cursorBlockWork(len(last))); err != nil {
				return "", err
			}
			if err := response.reserveDelivery(work); err != nil {
				return "", err
			}
			return a.encodeCursor(capture.cursor(cursor.Filter, scan, offset, last)), nil
		}
		scan++
		offset = 0
	}
	return "", response.reserveDelivery(work)
}
