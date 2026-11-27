package httpapi

import (
	"context"
	"net/http"

	"bog/internal/store"
)

const (
	// Allow a terminal source scan of tiny records to be prepared and retained
	// in a single bounded batch. Larger walks retain only scan metadata.
	inlineQueryRows      = 4
	inlineQueryLineBytes = 16
	// At most 55 framing bytes per record and six output bytes per
	// decoded line byte.
	inlineQueryBodyBytes = inlineQueryRows*55 + 6*inlineQueryLineBytes

	// Keep the first scan below the store's 512-candidate Bloom
	// fallback when the HTTP row limit permits it.
	legacyFirstScanLimit = 256
)

type legacyQueryScan struct {
	after *store.TailPosition
	count int
}

type inlineQueryRecord struct {
	body  []byte
	after [32]byte
}

type legacyQueryPlan struct {
	view       *store.PositionedSnapshot
	firstLimit int
	scans      []legacyQueryScan
	inline     []inlineQueryRecord
}

func (a *API) startLegacyQuery(w http.ResponseWriter, ctx context.Context, filter PageFilter, limit int, work *store.QueryWork) {
	capture, err := a.captures.reserve()
	if err != nil {
		writeQueryError(w, err)
		return
	}
	published := false
	defer func() { a.captures.done(capture, !published) }()

	plan := &legacyQueryPlan{
		view:       a.legacyQueryStore.CapturePositionedSnapshot(),
		firstLimit: max(legacyFirstScanLimit, limit),
	}
	rows, more, err := plan.scan(filter, nil, work)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	response := queryResponse{maxBytes: a.maxResponseBytes}
	if len(rows) == 0 {
		if err := response.reserveDelivery(work); err != nil {
			writeQueryError(w, err)
			return
		}
		response.write(w, "")
		return
	}

	var cursor cursorData
	if !more && canInlineQuery(rows) {
		cursor, err = a.prepareInlineQuery(ctx, capture, plan, filter, limit, rows, &response, work)
	} else {
		response.meterFraming = true
		cursor, err = a.certifyLegacyQuery(capture, plan, filter, limit, rows, more, &response, work)
	}
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	if cursor.Capture == 0 {
		response.write(w, "")
		return
	}
	capture.legacy = plan
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

func (p *legacyQueryPlan) scan(filter PageFilter, after *store.TailPosition, work *store.QueryWork) ([]store.PositionedRow, bool, error) {
	limit := maxQueryLimit
	if after == nil {
		limit = p.firstLimit
	}
	return p.view.Page(filter.Start, filter.End, filter.Contains, limit, 0, after, work)
}

func canInlineQuery(rows []store.PositionedRow) bool {
	if len(rows) > inlineQueryRows {
		return false
	}
	remaining := inlineQueryLineBytes
	for _, row := range rows {
		remaining -= len(row.Row.Line)
		if remaining < 0 {
			return false
		}
	}
	return true
}

func (a *API) prepareInlineQuery(ctx context.Context, capture *queryCapture, plan *legacyQueryPlan, filter PageFilter, limit int, rows []store.PositionedRow, response *queryResponse, work *store.QueryWork) (cursorData, error) {
	// The batch has at most four timestamps, 16 decoded line bytes, four
	// position digests and one fixed-size token. Charge before preparing it.
	if err := work.Charge(1); err != nil {
		return cursorData{}, err
	}
	// Escaping and cancellation still use the shared encoder. The single
	// batch charge above pays for this independent, statically bounded work.
	encoding := store.NewQueryWork(ctx, inlineQueryLineBytes)
	record := queryResponse{maxBytes: a.maxResponseBytes}
	plan.inline = make([]inlineQueryRecord, 0, len(rows))
	for _, row := range rows {
		fits, size, err := record.appendRow(row.Row, encoding)
		if err != nil {
			return cursorData{}, err
		}
		if !fits {
			return cursorData{}, &oversizedQueryRecord{a.maxResponseBytes, size}
		}
		plan.inline = append(plan.inline, inlineQueryRecord{
			body: record.rows[0], after: positionDigest(legacyPosition(row.Position)),
		})
		record.rows = record.rows[:0]
		record.size = 0
	}
	// Compact replay owns the encoded records and needs no store reference.
	plan.view = nil
	next := plan.appendInlineRows(response, 0, limit)
	if err := response.reserveInlineDelivery(work); err != nil {
		return cursorData{}, err
	}
	if next == len(rows) {
		return cursorData{}, nil
	}
	// Replay authenticates a token, may issue another, and writes at most
	// one bounded batch of bytes. Compare that bound to the whole budget.
	if 2*cursorTokenWork+uint64(len(filter.Contains))+1 > a.queryWorkBudget {
		return cursorData{}, store.ErrQueryWorkExhausted
	}
	return cursorData{Capture: capture.id, Filter: a.filterDigest(filter),
		Offset: uint16(next), After: plan.inline[next-1].after}, nil
}

func (p *legacyQueryPlan) appendInlineRows(response *queryResponse, offset, limit int) int {
	end := min(len(p.inline), offset+limit)
	for offset < end {
		body := p.inline[offset].body
		if len(body) > response.maxBytes-response.size {
			break
		}
		response.rows = append(response.rows, body)
		response.size += len(body)
		offset++
	}
	return offset
}

// certifyLegacyQuery scans to exhaustion without restarting at the HTTP cut.
// Each step stores the original store anchor and row count. The first page
// uses the measurements already needed by the proof, including when bytes
// trim the first scan's rows.
func (a *API) certifyLegacyQuery(capture *queryCapture, plan *legacyQueryPlan, filter PageFilter, limit int, rows []store.PositionedRow, more bool, response *queryResponse, work *store.QueryWork) (cursorData, error) {
	if err := work.Charge(1); err != nil { // immutable view ownership
		return cursorData{}, err
	}
	var after *store.TailPosition
	var emitted int
	fill := true
	var last store.TailPosition
	for {
		if err := work.Charge(1); err != nil { // scan plan entry
			return cursorData{}, err
		}
		for _, row := range rows {
			stamp, size, err := certifyLegacyRecord(response, row.Row, work)
			if err != nil {
				return cursorData{}, err
			}
			fill = fill && emitted < limit && size <= response.maxBytes-response.size
			if fill {
				if err := response.appendMeasuredRow(row.Row, stamp, size, work); err != nil {
					return cursorData{}, err
				}
				emitted++
				last = row.Position
			}
		}
		plan.scans = append(plan.scans, legacyQueryScan{after: after, count: len(rows)})
		if !more {
			break
		}
		if len(plan.scans) == maxCertifiedScans {
			return cursorData{}, store.ErrQueryWorkExhausted
		}
		anchor := rows[len(rows)-1].Position
		after = &anchor
		var err error
		rows, more, err = plan.scan(filter, after, work)
		if err != nil {
			return cursorData{}, err
		}
	}
	if len(plan.scans) == 1 && emitted == plan.scans[0].count {
		return cursorData{}, nil
	}
	return a.legacyFirstCursor(capture, plan, filter, emitted, last, work)
}

func certifyLegacyRecord(response *queryResponse, row store.Row, work *store.QueryWork) ([]byte, int, error) {
	if err := work.Charge(1); err != nil { // row preparation
		return nil, 0, err
	}
	stamp, size, _, err := response.measureRow(row, response.maxBytes, false, work)
	if err != nil {
		return nil, 0, err
	}
	if size > response.maxBytes {
		return nil, 0, &oversizedQueryRecord{response.maxBytes, size}
	}
	// Charge the worst-case rounded delivery for this record and the work
	// to format and hash its bounded position if selected for a cursor.
	if err := work.Charge(queryDeliveryWork(size), 1); err != nil {
		return nil, 0, err
	}
	return stamp, size, nil
}

func (a *API) legacyFirstCursor(capture *queryCapture, plan *legacyQueryPlan, filter PageFilter, emitted int, last store.TailPosition, work *store.QueryWork) (cursorData, error) {
	// Bound both token operations on a continuation, authentication of the
	// filter and starting position, and publication of the capture.
	if err := work.Charge(2*cursorTokenWork, uint64(len(filter.Contains)), 1, 1); err != nil {
		return cursorData{}, err
	}
	return plan.cursor(capture.id, a.filterDigest(filter), 0, emitted, last), nil
}

func (p *legacyQueryPlan) cursor(id uint64, filter [32]byte, step, offset int, last store.TailPosition) cursorData {
	if offset == p.scans[step].count {
		step++
		offset = 0
	}
	return cursorData{Capture: id, Filter: filter, Scan: uint16(step), Offset: uint16(offset),
		After: positionDigest(legacyPosition(last))}
}

func (a *API) replayLegacyQuery(capture *queryCapture, cursor cursorData, filter PageFilter, limit int, response *queryResponse, work *store.QueryWork) (string, error) {
	plan := capture.legacy
	if plan.inline != nil {
		return a.replayInlineQuery(plan, cursor, limit, response, work)
	}
	response.meterFraming = true
	step, offset := int(cursor.Scan), int(cursor.Offset)
	if step >= len(plan.scans) || offset >= plan.scans[step].count {
		return "", store.ErrInvalidQueryCursor
	}
	var last store.TailPosition
	for ; step < len(plan.scans); step++ {
		scan := plan.scans[step]
		if err := work.Charge(1); err != nil {
			return "", err
		}
		rows, more, err := plan.scan(filter, scan.after, work)
		if err != nil {
			return "", err
		}
		if len(rows) != scan.count || more != (step+1 < len(plan.scans)) {
			return "", errPageSourceContract
		}
		if step == int(cursor.Scan) {
			if err := work.Charge(1); err != nil {
				return "", err
			}
			anchor := scan.after
			if offset > 0 {
				anchor = &rows[offset-1].Position
			}
			if anchor == nil || cursor.After != positionDigest(legacyPosition(*anchor)) {
				return "", store.ErrInvalidQueryCursor
			}
		}
		for ; offset < len(rows) && limit > 0; offset++ {
			row := rows[offset]
			fits, required, err := response.appendRow(row.Row, work)
			if err != nil {
				return "", err
			}
			if !fits {
				if response.size == 0 {
					return "", &oversizedQueryRecord{response.maxBytes, required}
				}
				return a.finishLegacyReplay(plan, cursor, step, offset, last, response, work)
			}
			limit--
			last = row.Position
		}
		if limit == 0 && (offset < len(rows) || more) {
			return a.finishLegacyReplay(plan, cursor, step, offset, last, response, work)
		}
		offset = 0
	}
	return "", response.reserveDelivery(work)
}

func (a *API) finishLegacyReplay(plan *legacyQueryPlan, cursor cursorData, step, offset int, last store.TailPosition, response *queryResponse, work *store.QueryWork) (string, error) {
	if err := work.Charge(cursorTokenWork, 1); err != nil {
		return "", err
	}
	if err := response.reserveDelivery(work); err != nil {
		return "", err
	}
	return a.encodeCursor(plan.cursor(cursor.Capture, cursor.Filter, step, offset, last)), nil
}

func (a *API) replayInlineQuery(plan *legacyQueryPlan, cursor cursorData, limit int, response *queryResponse, work *store.QueryWork) (string, error) {
	offset := int(cursor.Offset)
	if cursor.Scan != 0 || offset < 1 || offset >= len(plan.inline) || cursor.After != plan.inline[offset-1].after {
		return "", store.ErrInvalidQueryCursor
	}
	next := plan.appendInlineRows(response, offset, limit)
	if err := response.reserveInlineDelivery(work); err != nil {
		return "", err
	}
	if next == len(plan.inline) {
		return "", nil
	}
	if err := work.Charge(cursorTokenWork); err != nil {
		return "", err
	}
	cursor.Offset = uint16(next)
	cursor.After = plan.inline[next-1].after
	return a.encodeCursor(cursor), nil
}

func (b *queryResponse) reserveInlineDelivery(work *store.QueryWork) error {
	return work.Charge(uint64((b.size + inlineQueryBodyBytes - 1) / inlineQueryBodyBytes))
}
