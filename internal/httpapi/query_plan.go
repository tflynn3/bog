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

// Every scan uses the same source limit, independent of the client's page
// size. A cursor selects a scan and offset and binds the last emitted position.
type certifiedScan struct {
	after PagePosition
	count int
}

var errPageSourceContract = errors.New("invalid result from query page source")

func validOpaque(value string) bool {
	return len(value) > 0 && len(value) <= maxOpaqueBytes && utf8.ValidString(value)
}

func (a *API) snapshotScan(ctx context.Context, req PageRequest, meter *snapshotWork) (PageResult, error) {
	if err := meter.charge(16 + uint64(len(req.After))); err != nil {
		return PageResult{}, err
	}
	return a.captures.source.Page(ctx, req, meter.work)
}

func checkSnapshotScan(result PageResult, req PageRequest, meter *snapshotWork) error {
	if (result.Capture != "" && result.Capture != req.Capture) ||
		len(result.Rows) > req.Limit ||
		(len(result.Rows) == 0 && (result.More || req.After != "")) {
		return errPageSourceContract
	}
	previous := req.After
	for _, row := range result.Rows {
		if len(row.Position) > maxOpaqueBytes {
			return errPageSourceContract
		}
		if err := meter.charge(16 + 4*uint64(len(row.Position))); err != nil {
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

	meter := snapshotWork{work: work}
	req := PageRequest{Filter: filter, Limit: maxQueryLimit, Snapshot: true}
	// The registry owns any successful acquisition even if its metadata
	// cannot be used or the remaining budget cannot certify the walk.
	result, err := a.snapshotScan(ctx, req, &meter)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	capture.sourceID = result.Capture
	if len(capture.sourceID) > maxOpaqueBytes {
		writeQueryError(w, errPageSourceContract)
		return
	}
	if err := meter.charge(uint64(len(capture.sourceID))); err != nil {
		writeQueryError(w, err)
		return
	}
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
	page := snapshotPage{response: queryResponse{maxBytes: a.maxResponseBytes}, limit: limit}
	err = a.certifyQuery(ctx, capture, req, result, &page, &meter)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	var token string
	if page.more {
		// Reserve the same authentication work as a resume, including its
		// optional outgoing token, before making the capture reachable.
		if err := meter.reserveCursor(filter.Contains); err != nil {
			writeQueryError(w, err)
			return
		}
		token = a.encodeCursor(page.cursor(capture, a.filterDigest(filter)))
		if err := work.Charge(); err != nil {
			writeQueryError(w, err)
			return
		}
		if err := a.captures.publish(capture); err != nil {
			writeQueryError(w, err)
			return
		}
		published = true
	}
	page.response.write(w, token)
}

// The first request visits every scan and measures every record. It reserves
// record preparation and delivery even for rows outside its first page. Any
// replay visits a subset of these scans and rows, so both the source work and
// the rounded HTTP work fit the budget established here.
func (a *API) certifyQuery(ctx context.Context, capture *queryCapture, req PageRequest, result PageResult, page *snapshotPage, meter *snapshotWork) error {
	for scan := 0; ; scan++ {
		if err := checkSnapshotScan(result, req, meter); err != nil {
			return err
		}
		capture.scans = append(capture.scans, certifiedScan{req.After, len(result.Rows)})
		if err := page.consume(result, scan, 0, true, meter); err != nil {
			return err
		}
		if !result.More {
			return meter.work.Charge()
		}
		if len(capture.scans) == maxCertifiedScans {
			return store.ErrQueryWorkExhausted
		}
		last := result.Rows[len(result.Rows)-1].Position
		req.After = PagePosition(strings.Clone(string(last)))
		var err error
		result, err = a.snapshotScan(ctx, req, meter)
		if err != nil {
			return err
		}
	}
}

// snapshotPage tracks the cut even while certification continues past it.
type snapshotPage struct {
	response     queryResponse
	limit        int
	scan, offset int
	last         PagePosition
	more         bool
}

func (p *snapshotPage) consume(result PageResult, scan, offset int, certify bool, meter *snapshotWork) error {
	for i, row := range result.Rows[offset:] {
		if p.limit == 0 {
			p.more = true
		}
		if p.more && !certify {
			return nil
		}
		// A snapshot charges for two encoding passes, JSON escaping and
		// eventual delivery. Reserving the same amount for measured-only rows
		// makes the bound independent of the starting offset and page size.
		if err := meter.reserveRecord(row.Row.Line); err != nil {
			return err
		}
		stamp, size, _, err := p.response.measureRow(row.Row, 0, false, meter.work, true)
		if err != nil {
			return err
		}
		if size > p.response.maxBytes {
			return &oversizedQueryRecord{p.response.maxBytes, size}
		}
		if p.more || size > p.response.maxBytes-p.response.size {
			p.more = true
			continue
		}
		if err := p.response.appendMeasured(row.Row.Line, stamp, size, meter.work); err != nil {
			return err
		}
		p.scan, p.offset, p.last = scan, offset+i+1, row.Position
		p.limit--
	}
	if p.limit == 0 && result.More {
		p.more = true
	}
	return nil
}

func (p *snapshotPage) cursor(capture *queryCapture, filter [32]byte) cursorData {
	scan, offset := p.scan, p.offset
	if offset == capture.scans[scan].count {
		scan++
		offset = 0
	}
	return cursorData{Capture: capture.id, Filter: filter, Scan: uint16(scan), Offset: uint16(offset), After: positionDigest(p.last)}
}

func (a *API) resumeQuery(w http.ResponseWriter, ctx context.Context, filter PageFilter, limit int, raw string, work *store.QueryWork) {
	if len(raw) != cursorTokenSize {
		writeQueryError(w, store.ErrInvalidQueryCursor)
		return
	}
	meter := snapshotWork{work: work}
	if err := meter.reserveCursor(filter.Contains); err != nil {
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

	page := snapshotPage{response: queryResponse{maxBytes: a.maxResponseBytes}, limit: limit}
	if err := a.replayQuery(ctx, capture, cursor, filter, &page, &meter); err != nil {
		if errors.Is(err, ErrCaptureExpired) {
			a.captures.retire(capture)
		}
		writeQueryError(w, err)
		return
	}
	var token string
	if page.more {
		token = a.encodeCursor(page.cursor(capture, cursor.Filter))
	}
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	page.response.write(w, token)
}

func (a *API) replayQuery(ctx context.Context, capture *queryCapture, cursor cursorData, filter PageFilter, page *snapshotPage, meter *snapshotWork) error {
	scan, offset := int(cursor.Scan), int(cursor.Offset)
	if scan >= len(capture.scans) || offset >= capture.scans[scan].count {
		return store.ErrInvalidQueryCursor
	}
	for ; scan < len(capture.scans); scan++ {
		step := capture.scans[scan]
		req := PageRequest{Filter: filter, Limit: maxQueryLimit, Snapshot: true, Capture: capture.sourceID, After: step.after}
		result, err := a.snapshotScan(ctx, req, meter)
		if err != nil {
			return err
		}
		if err := checkSnapshotScan(result, req, meter); err != nil {
			return err
		}
		if len(result.Rows) != step.count || result.More != (scan+1 < len(capture.scans)) {
			return errPageSourceContract
		}
		if result.More && result.Rows[step.count-1].Position != capture.scans[scan+1].after {
			return errPageSourceContract
		}
		if scan == int(cursor.Scan) {
			anchor := step.after
			if offset > 0 {
				anchor = result.Rows[offset-1].Position
			}
			if positionDigest(anchor) != cursor.After {
				return errPageSourceContract
			}
		}
		if err := page.consume(result, scan, offset, false, meter); err != nil || page.more {
			return err
		}
		offset = 0
	}
	return nil
}
