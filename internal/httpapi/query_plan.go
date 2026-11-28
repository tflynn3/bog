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

// An oversized match ends certification inside the final scan. Keep the
// original scan shape so replay uses and validates the same source call.
type certifiedOversized struct {
	offset int
	bytes  int
	more   bool
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

func (a *API) prepareRetainedPage(ctx context.Context, capture *queryCapture, params queryParams, response *queryResponse, work *store.QueryWork) (PageRequest, bool, error) {
	filter := params.filter
	req := PageRequest{Filter: filter, Limit: params.limit, Snapshot: true}
	var more bool
	for {
		err := a.withPage(ctx, req, work, func(result PageResult) error {
			if req.Capture == "" {
				capture.sourceID = result.Capture
				if !validOpaque(capture.sourceID) {
					return errPageSourceContract
				}
				capture.sourceID = strings.Clone(capture.sourceID)
				if !result.ExpiresAt.IsZero() && result.ExpiresAt.Before(capture.deadline) {
					capture.deadline = result.ExpiresAt
				}
				if !time.Now().Before(capture.deadline) {
					return ErrCaptureExpired
				}
				req.Capture = capture.sourceID
			}
			if err := checkSnapshotScan(result, req, work); err != nil {
				return err
			}
			n, err := response.appendRows(result.Rows, work)
			if err != nil {
				return err
			}
			if n != 0 {
				req.After = PagePosition(strings.Clone(string(result.Rows[n-1].Position)))
			}
			req.Limit -= n
			more = n < len(result.Rows) || result.More
			// The byte cap can end the page while the row limit has room.
			if n < len(result.Rows) {
				req.Limit = 0
			}
			return nil
		})
		if err != nil {
			return req, false, err
		}
		if req.Limit == 0 || !more {
			break
		}
		// The source can return a short scan with lookahead. Fill the
		// remaining row allowance using successive scans.
		if err := work.Charge(1); err != nil {
			return req, false, err
		}
	}
	return req, more, nil
}

func (a *API) prepareRetainedOpening(ctx context.Context, capture *queryCapture, params queryParams, response *queryResponse, work *store.QueryWork) (string, error) {
	req, more, err := a.prepareRetainedPage(ctx, capture, params, response, work)
	if err != nil {
		return "", err
	}
	if err := response.reserveDelivery(work); err != nil {
		return "", err
	}
	if !more {
		return "", nil
	}

	// Start the replay plan at the actual byte/row cut, including when the
	// source's first result already proved exhaustion beyond the cut.
	req.Limit = maxQueryLimit
	if err := a.certifyQuery(ctx, capture, req, work); err != nil {
		return "", err
	}
	// A replay can validate one token and create another. It can hash the
	// first anchor once, and certification already covers later positions.
	if err := work.Charge(2*cursorTokenWork, uint64(len(req.Filter.Contains)), cursorBlockWork(len(req.After))); err != nil {
		return "", err
	}
	cursor := capture.cursor(a.filterDigest(req.Filter), 0, 0, req.After)
	return a.encodeCursor(cursor), nil
}

// Each replay visits a subset of the certified scans, at most once per
// request. Its encoder handles each considered row at most once. Certification
// measures rows through exhaustion or the first oversized match. The sum of
// the rounded delivery charges bounds delivery of any replayable subset.
func (a *API) certifyQuery(ctx context.Context, capture *queryCapture, req PageRequest, work *store.QueryWork) error {
	measure := queryResponse{}
	for len(capture.scans) < maxCertifiedScans {
		if err := work.Charge(1); err != nil {
			return err
		}
		var more bool
		err := a.withPage(ctx, req, work, func(result PageResult) error {
			// Certification and each later scan follow proven lookahead.
			if err := checkSnapshotScan(result, req, work); err != nil {
				return err
			}
			for i, row := range result.Rows {
				if err := work.Charge(1, cursorBlockWork(len(row.Position))); err != nil {
					return err
				}
				_, size, _, err := measure.measureRow(row.Row, a.maxResponseBytes, false, work)
				if err != nil {
					return err
				}
				if size > a.maxResponseBytes {
					capture.oversized = &certifiedOversized{offset: i, bytes: size, more: result.More}
					break
				}
				if err := work.Charge(queryDeliveryWork(size)); err != nil {
					return err
				}
			}
			capture.scans = append(capture.scans, certifiedScan{after: req.After, count: len(result.Rows)})
			more = capture.oversized == nil && result.More
			if more {
				req.After = PagePosition(strings.Clone(string(result.Rows[len(result.Rows)-1].Position)))
			}
			return nil
		})
		if err != nil || !more {
			return err
		}
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

	response := a.newQueryResponse()
	defer response.release()
	token, err := a.replayQuery(ctx, capture, cursor, filter, limit, &response, work)
	if err != nil {
		response.release()
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
	for scan < len(capture.scans) {
		step := capture.scans[scan]
		if err := work.Charge(1); err != nil {
			return "", err
		}
		req := PageRequest{Filter: filter, Limit: maxQueryLimit, Snapshot: true, Capture: capture.sourceID, After: step.after}
		var token string
		err := a.withPage(ctx, req, work, func(result PageResult) error {
			if err := checkSnapshotScan(result, req, work); err != nil {
				return err
			}
			hasNextScan := scan+1 < len(capture.scans)
			fitting, more := step.count, hasNextScan
			var oversizedBytes int
			if oversized := capture.oversized; !hasNextScan && oversized != nil {
				fitting, more, oversizedBytes = oversized.offset, oversized.more, oversized.bytes
			}
			if len(result.Rows) != step.count || result.More != more {
				return errPageSourceContract
			}
			if hasNextScan && result.Rows[step.count-1].Position != capture.scans[scan+1].after {
				return errPageSourceContract
			}
			if scan == int(cursor.Scan) {
				anchor := step.after
				if offset > 0 {
					anchor = result.Rows[offset-1].Position
				}
				if err := work.Charge(cursorBlockWork(len(anchor))); err != nil {
					return err
				}
				if positionDigest(anchor) != cursor.After {
					return errPageSourceContract
				}
			}
			if offset > fitting {
				return store.ErrInvalidQueryCursor
			}
			if oversizedBytes != 0 && offset == fitting && response.size == 0 {
				return &oversizedQueryRecord{a.maxResponseBytes, oversizedBytes}
			}
			rows := result.Rows[offset:min(offset+limit, fitting)]
			n, err := response.appendRows(rows, work)
			if err != nil {
				return err
			}
			last := step.after
			if n != 0 {
				offset += n
				last = rows[n-1].Position
				limit -= n
			}
			if offset < step.count || (limit == 0 && hasNextScan) {
				if err := work.Charge(cursorTokenWork, cursorBlockWork(len(last))); err != nil {
					return err
				}
				if err := response.reserveDelivery(work); err != nil {
					return err
				}
				token = a.encodeCursor(capture.cursor(cursor.Filter, scan, offset, last))
			}
			return nil
		})
		if err != nil || token != "" {
			return token, err
		}
		scan++
		offset = 0
	}
	return "", response.reserveDelivery(work)
}
