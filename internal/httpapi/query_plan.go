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
	after   PagePosition
	count   int
	compact bool // the first scan records the work reservation for the walk
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

	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	compact := false
	if source, ok := a.captures.source.(*legacyPageSource); ok && filter.Contains == "" &&
		a.queryWorkBudget >= cursorAuthenticationWork(filter) && source.store.PublishedCount() <= compactSnapshotRows {
		if err := work.Charge(a.queryWorkBudget); err != nil {
			writeQueryError(w, err)
			return
		}
		work = store.NewQueryWork(ctx, compactSnapshotBudget(a.queryWorkBudget))
		compact = true
	}

	// Acquire with one row and lookahead. The following scans both fill
	// the response and certify its suffix, sharing the same source work.
	req := PageRequest{Filter: filter, Limit: 1, Snapshot: true}
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
	if err := checkSnapshotScan(result, req, work); err != nil {
		writeQueryError(w, err)
		return
	}
	if _, err := response.appendRows(result.Rows, work); err != nil {
		writeQueryError(w, err)
		return
	}
	if !result.More {
		if err := response.reserveDelivery(work); err != nil {
			writeQueryError(w, err)
			return
		}
		response.write(w, "")
		return
	}

	last := result.Rows[0].Position
	req.After = last
	req.Limit = maxQueryLimit
	cut, err := a.certifyQuery(ctx, capture, req, limit-1, last, &response, work)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if len(capture.scans) == 0 {
		response.write(w, "")
		return
	}
	capture.scans[0].compact = compact
	token := a.encodeCursor(capture.cursor(a.filterDigest(filter), 0, cut.offset, cut.last))
	if err := a.captures.publish(capture); err != nil {
		writeQueryError(w, err)
		return
	}
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	published = true
	response.write(w, token)
}

// firstPageCut is the first unconsumed offset in the replay plan and the
// position of the last emitted row.
type firstPageCut struct {
	offset int
	last   PagePosition
}

// Issuance pays for the initial row and a disjoint chain of scans through
// exhaustion. The first page takes its remaining rows from that chain. Its
// cut may land inside a scan, so keep the whole scan's anchor and row count.
//
// Each resume replays a subset of those exact source requests, once each,
// regardless of the requested limit. Every replayed row was validated and
// measured in full, including any row that replay may inspect and byte-trim.
// Rounded per-record delivery reservations cover every possible body. Position
// hash reservations cover distinct incoming and outgoing anchors because a
// cursor can only follow progress. A cut at a scan boundary may need a separate
// reservation for the initial row's position.
func (a *API) certifyQuery(ctx context.Context, capture *queryCapture, req PageRequest, remaining int, last PagePosition, response *queryResponse, work *store.QueryWork) (firstPageCut, error) {
	cut := firstPageCut{last: last}
	// Both the first page and any replay deliver at most one body. Credit
	// the per-record delivery reservations against the first page as well.
	var delivery uint64
	maxDelivery := queryDeliveryWork(response.maxBytes)
	for {
		if len(capture.scans) == maxCertifiedScans {
			return cut, store.ErrQueryWorkExhausted
		}
		result, err := a.scanSnapshot(ctx, req, work)
		if err != nil {
			return cut, err
		}
		offset := 0
		for i, row := range result.Rows {
			stamp, size, _, err := response.measureRow(row.Row, response.maxBytes, false, work)
			if err != nil {
				return cut, err
			}
			if size > response.maxBytes {
				return cut, &oversizedQueryRecord{response.maxBytes, size}
			}
			rowDelivery := queryDeliveryWork(size)
			if err := work.Charge(rowDelivery, cursorBlockWork(len(row.Position))); err != nil {
				return cut, err
			}
			delivery += min(rowDelivery, maxDelivery-delivery)
			if remaining > 0 && size <= response.maxBytes-response.size {
				if err := response.appendMeasuredRow(row.Row, stamp, size, work); err != nil {
					return cut, err
				}
				remaining--
				cut.last = row.Position
				offset = i + 1
			} else {
				remaining = 0
			}
		}
		if offset < len(result.Rows) {
			if len(capture.scans) == 0 {
				cut.offset = offset
			}
			capture.scans = append(capture.scans, certifiedScan{
				after: PagePosition(strings.Clone(string(req.After))),
				count: len(result.Rows),
			})
		}
		if !result.More {
			break
		}
		req.After = result.Rows[len(result.Rows)-1].Position
	}
	bodyDelivery := queryDeliveryWork(response.size)
	if err := work.Charge(bodyDelivery - min(bodyDelivery, delivery)); err != nil {
		return cut, err
	}
	if len(capture.scans) == 0 {
		return cut, nil
	}
	// A cut within the first stored scan was covered by that row's
	// reservation. A cut at a scan boundary can also use the initial row.
	var cutWork uint64
	if cut.offset == 0 {
		cutWork = cursorBlockWork(len(cut.last))
	}
	return cut, work.Charge(cursorAuthenticationWork(req.Filter), cursorTokenWork, cutWork)
}

func (a *API) scanSnapshot(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	if err := work.Charge(1); err != nil {
		return PageResult{}, err
	}
	result, err := a.captures.source.Page(ctx, req, work)
	if err != nil {
		return PageResult{}, err
	}
	return result, checkSnapshotScan(result, req, work)
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
	authWork := cursorAuthenticationWork(filter)
	if err := work.Charge(authWork); err != nil {
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

	if capture.scans[0].compact {
		// Authentication already spent part of the request allowance. Use the
		// issuing reservation even if the live store has grown since capture.
		if err := work.Charge(a.queryWorkBudget - authWork); err != nil {
			writeQueryError(w, err)
			return
		}
		work = store.NewQueryWork(ctx, compactSnapshotBudget(a.queryWorkBudget)-authWork)
	}

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
		req := PageRequest{Filter: filter, Limit: maxQueryLimit, Snapshot: true, Capture: capture.sourceID, After: step.after}
		result, err := a.scanSnapshot(ctx, req, work)
		if err != nil {
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
			if last == "" {
				return "", errPageSourceContract
			}
			if err := work.Charge(cursorTokenWork, cursorBlockWork(len(last))); err != nil {
				return "", err
			}
			token := a.encodeCursor(capture.cursor(cursor.Filter, scan, offset, last))
			if err := response.reserveDelivery(work); err != nil {
				return "", err
			}
			return token, nil
		}
		scan++
		offset = 0
	}
	return "", response.reserveDelivery(work)
}
