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

// A plan keeps the exact source requests needed for replay, without retaining
// rows. A client cursor can start partway through a scan, and an HTTP page
// can span scans. The cursor authenticates both the offset and last position.
type certifiedScan struct {
	after PagePosition
	limit int
	count int
}

var errPageSourceContract = errors.New("invalid result from query page source")

func validOpaque(value string) bool {
	return len(value) > 0 && len(value) <= maxOpaqueBytes && utf8.ValidString(value)
}

func snapshotBookkeepingWork(prepaid bool) uint64 {
	if prepaid {
		return 0
	}
	return 1
}

func checkSnapshotScan(result PageResult, req PageRequest, prepaid bool, work *store.QueryWork) error {
	if (result.Capture != "" && result.Capture != req.Capture) ||
		len(result.Rows) > req.Limit ||
		(len(result.Rows) == 0 && (result.More || req.After != "")) {
		return errPageSourceContract
	}
	previous := req.After
	for _, row := range result.Rows {
		if err := work.Charge(snapshotBookkeepingWork(prepaid)); err != nil {
			return err
		}
		if !validOpaque(string(row.Position)) || row.Position == req.After || row.Position == previous {
			return errPageSourceContract
		}
		previous = row.Position
	}
	return nil
}

// The unfiltered in-memory scan prepays per-row work. It can also replay the
// opening scan from the beginning of its immutable store snapshot. The
// general PageSource contract only requires captured seeks after a row.
func (a *API) prepaidSnapshot(filter PageFilter) *legacyPageSource {
	if filter.Contains != "" {
		return nil
	}
	source, _ := a.captures.source.(*legacyPageSource)
	return source
}

func (a *API) snapshotPage(ctx context.Context, req PageRequest, local *legacyPageSource, work *store.QueryWork) (PageResult, error) {
	if local != nil {
		return local.page(req, work, true)
	}
	return a.captures.source.Page(ctx, req, work)
}

func (a *API) startQuery(w http.ResponseWriter, ctx context.Context, filter PageFilter, limit int, work *store.QueryWork) {
	local := a.prepaidSnapshot(filter)
	initialLimit := 1
	if local != nil {
		// One fixed unit covers bounded capture and cursor bookkeeping for
		// these scans. Replays reserve this unit before authentication.
		if err := work.Charge(1); err != nil {
			writeQueryError(w, err)
			return
		}
		initialLimit = maxQueryLimit
	}
	capture, err := a.captures.reserve()
	if err != nil {
		writeQueryError(w, err)
		return
	}
	published := false
	defer func() { a.captures.done(capture, !published) }()

	req := PageRequest{Filter: filter, Limit: initialLimit, Snapshot: true}
	result, err := a.snapshotPage(ctx, req, local, work)
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
	response := queryResponse{maxBytes: a.maxResponseBytes, prepaidEncoding: local != nil}
	var offset int
	var last PagePosition
	if a.legacyQueryStore != nil && local == nil && limit == 1 && result.More {
		last, err = a.certifyLegacySingleRow(ctx, capture, req, result, &response, work)
	} else {
		offset, last, err = a.certifyQuery(ctx, capture, req, result, local, limit, &response, work)
		if err == nil && local == nil && len(capture.scans) != 0 {
			err = reserveCursorReplay(filter, capture.scans[0].after, work)
		}
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if len(capture.scans) == 0 {
		response.write(w, "")
		return
	}

	token := a.encodeCursor(capture.cursor(a.filterDigest(filter), 0, offset, last))
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

// Replays authenticate one token and can sign one. The opening anchor is
// the only replay position not charged as a plan row.
func reserveCursorReplay(filter PageFilter, anchor PagePosition, work *store.QueryWork) error {
	return work.Charge(2*cursorTokenWork, uint64(len(filter.Contains)), cursorBlockWork(len(anchor)))
}

func (a *API) certifyLegacySingleRow(ctx context.Context, capture *queryCapture, req PageRequest, result PageResult, response *queryResponse, work *store.QueryWork) (PagePosition, error) {
	if err := checkSnapshotScan(result, req, false, work); err != nil {
		return "", err
	}
	if _, err := response.appendRows(result.Rows, work); err != nil {
		return "", err
	}
	if err := response.reserveDelivery(work); err != nil {
		return "", err
	}
	last := result.Rows[0].Position
	req.After = PagePosition(strings.Clone(string(last)))
	req.Limit = maxQueryLimit
	// The batch includes fixed cursor work. On replay the authentication
	// charge counts toward this reservation, and the literal is charged
	// separately.
	reserveErr := work.Charge(legacyReplayBlocks, uint64(len(req.Filter.Contains)))
	if reserveErr != nil && !errors.Is(reserveErr, store.ErrQueryWorkExhausted) {
		return "", reserveErr
	}
	certify := func(tailWork *store.QueryWork) error {
		if err := reserveCursorReplay(req.Filter, req.After, tailWork); err != nil {
			return err
		}
		if err := tailWork.Charge(1); err != nil {
			return err
		}
		tail, err := a.captures.source.Page(ctx, req, tailWork)
		if err != nil {
			return err
		}
		_, _, err = a.certifyQuery(ctx, capture, req, tail, nil, 0, response, tailWork)
		return err
	}
	err := certify(work)
	if reserveErr == nil && errors.Is(err, store.ErrQueryWorkExhausted) {
		// Retry certification on the same pinned view with the prepaid
		// batch. The first page is already complete and is never reencoded.
		capture.scans = capture.scans[:0]
		err = certify(store.NewQueryWork(ctx, legacyReplayUnits))
		capture.batchedReplay = err == nil
	}
	return last, err
}

// Build the first response and certify the suffix in one traversal. Later
// requests pay for a subset of the source scans and rows priced here, even
// when changing their limit or resuming at a byte cut in the middle of a scan.
func (a *API) certifyQuery(ctx context.Context, capture *queryCapture, req PageRequest, result PageResult, local *legacyPageSource, limit int, response *queryResponse, work *store.QueryWork) (int, PagePosition, error) {
	var offset int
	var last PagePosition
	var deliveryBytes uint64
	for {
		if err := checkSnapshotScan(result, req, local != nil, work); err != nil {
			return 0, "", err
		}
		emitted := 0
		for _, row := range result.Rows {
			// A general acquisition returns at most one row. It must fit
			// and will be emitted, so only later scans can be replayed.
			if local == nil && req.After != "" {
				if err := work.Charge(cursorBlockWork(len(row.Position))); err != nil {
					return 0, "", err
				}
			}
			stamp, size, _, err := response.measureRow(row.Row, response.maxBytes, false, work)
			if err != nil {
				return 0, "", err
			}
			if size > response.maxBytes {
				return 0, "", &oversizedQueryRecord{response.maxBytes, size}
			}
			if local != nil {
				deliveryBytes += response.recordDeliveryBytes(stamp, size)
			} else if err := work.Charge(queryDeliveryWork(uint64(size))); err != nil {
				return 0, "", err
			}
			if limit == 0 || size > response.maxBytes-response.size {
				// All later rows belong to the suffix, even if one
				// would fit the unused bytes in this page.
				limit = 0
				continue
			}
			if err := response.appendMeasuredRow(row.Row, stamp, size, work); err != nil {
				return 0, "", err
			}
			limit--
			emitted++
			last = row.Position
		}
		if emitted < len(result.Rows) {
			if len(capture.scans) == 0 {
				offset = emitted
			}
			capture.scans = append(capture.scans, certifiedScan{req.After, req.Limit, len(result.Rows)})
		}
		if !result.More {
			break
		}
		if len(capture.scans) == maxCertifiedScans {
			return 0, "", store.ErrQueryWorkExhausted
		}
		if err := work.Charge(snapshotBookkeepingWork(local != nil)); err != nil {
			return 0, "", err
		}
		req.After = PagePosition(strings.Clone(string(result.Rows[len(result.Rows)-1].Position)))
		req.Limit = maxQueryLimit
		var err error
		result, err = a.snapshotPage(ctx, req, local, work)
		if err != nil {
			return 0, "", err
		}
	}
	if local != nil {
		if err := work.Charge(queryDeliveryWork(deliveryBytes)); err != nil {
			return 0, "", err
		}
	}
	return offset, last, work.Charge()
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
	local := a.prepaidSnapshot(filter)
	authWork := cursorTokenWork
	if local != nil {
		authWork = 1
	}
	if err := work.Charge(authWork, uint64(len(filter.Contains))); err != nil {
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

	if capture.batchedReplay {
		if err := work.Charge(legacyReplayBlocks - authWork); err != nil {
			writeQueryError(w, err)
			return
		}
		work = store.NewQueryWork(ctx, legacyReplayUnits)
	}
	response := queryResponse{maxBytes: a.maxResponseBytes, prepaidEncoding: local != nil}
	token, err := a.replayQuery(ctx, capture, cursor, filter, local, limit, &response, work)
	if err != nil {
		if errors.Is(err, ErrCaptureExpired) {
			a.captures.retire(capture)
		}
		writeQueryError(w, err)
		return
	}
	response.write(w, token)
}

func (a *API) replayQuery(ctx context.Context, capture *queryCapture, cursor cursorData, filter PageFilter, local *legacyPageSource, limit int, response *queryResponse, work *store.QueryWork) (string, error) {
	scan, offset := int(cursor.Scan), int(cursor.Offset)
	if scan >= len(capture.scans) || offset >= capture.scans[scan].count {
		return "", store.ErrInvalidQueryCursor
	}
	var last PagePosition
	for scan < len(capture.scans) {
		step := capture.scans[scan]
		if err := work.Charge(snapshotBookkeepingWork(local != nil)); err != nil {
			return "", err
		}
		req := PageRequest{Filter: filter, Limit: step.limit, Snapshot: true, Capture: capture.sourceID, After: step.after}
		result, err := a.snapshotPage(ctx, req, local, work)
		if err != nil {
			return "", err
		}
		if err := checkSnapshotScan(result, req, local != nil, work); err != nil {
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
			if local == nil {
				if err := work.Charge(cursorBlockWork(len(anchor))); err != nil {
					return "", err
				}
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
			if local == nil {
				if err := work.Charge(cursorTokenWork, cursorBlockWork(len(last))); err != nil {
					return "", err
				}
			}
			if err := response.reserveDelivery(work); err != nil {
				return "", err
			}
			return a.encodeCursor(capture.cursor(cursor.Filter, scan, offset, last)), work.Charge()
		}
		scan++
		offset = 0
	}
	return "", response.reserveDelivery(work)
}
