package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"net/http"

	"bog/internal/store"
)

const (
	legacyCursorPayload     = 8 + 4 // capture ID and next match offset
	legacyCursorSize        = legacyCursorPayload + sha256.Size
	legacyCursorReservation = (legacyCursorSize*8 + 5) / 6
	legacyCursorWork        = 1 // fixed-size signing or authentication and registry access
	maxLegacyMatches        = maxCertifiedScans * maxQueryLimit
)

// A legacy capture records the exact ordered matches, including exhaustion,
// and their encoded lengths. Replay uses random access to the pinned rows,
// so the work to find a match (including sparse gaps) is paid once.
type legacyQueryPlan struct {
	matches *store.PositionedMatches
	sizes   []int
}

func (a *API) signLegacyCursor(payload []byte, filter [sha256.Size]byte) []byte {
	var signed [legacyCursorPayload + sha256.Size]byte
	copy(signed[:legacyCursorPayload], payload)
	copy(signed[legacyCursorPayload:], filter[:])
	return a.cursorMAC(signed[:])
}

func (a *API) encodeLegacyCursor(id uint64, offset int, filter [sha256.Size]byte) string {
	var token [legacyCursorSize]byte
	binary.BigEndian.PutUint64(token[:8], id)
	binary.BigEndian.PutUint32(token[8:legacyCursorPayload], uint32(offset))
	copy(token[legacyCursorPayload:], a.signLegacyCursor(token[:legacyCursorPayload], filter))
	return base64.RawURLEncoding.EncodeToString(token[:])
}

func (a *API) decodeLegacyCursor(token string, filter [sha256.Size]byte) (uint64, int, error) {
	var b [legacyCursorSize]byte
	n, err := base64.RawURLEncoding.Strict().Decode(b[:], []byte(token))
	if err != nil || n != len(b) ||
		!hmac.Equal(b[legacyCursorPayload:], a.signLegacyCursor(b[:legacyCursorPayload], filter)) {
		return 0, 0, store.ErrInvalidQueryCursor
	}
	return binary.BigEndian.Uint64(b[:8]), int(binary.BigEndian.Uint32(b[8:legacyCursorPayload])), nil
}

func (a *API) legacySnapshotQuery(w http.ResponseWriter, params queryParams, work *store.QueryWork) {
	source := a.captures.source.(*legacyPageSource)
	if params.page {
		a.startLegacyQuery(w, params, source, work)
		return
	}

	if len(params.cursor) != legacyCursorReservation {
		writeQueryError(w, store.ErrInvalidQueryCursor)
		return
	}
	if err := work.Charge(legacyCursorWork, uint64(len(params.filter.Contains))); err != nil {
		writeQueryError(w, err)
		return
	}
	filter := a.filterDigest(params.filter)
	id, offset, err := a.decodeLegacyCursor(params.cursor, filter)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	capture, err := a.captures.acquire(id)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	defer a.captures.done(capture, false)

	plan := source.lookup(capture.sourceID).plan
	if plan == nil || offset <= 0 || offset >= len(plan.sizes) {
		writeQueryError(w, store.ErrInvalidQueryCursor)
		return
	}
	response, next, err := plan.replay(offset, params.limit, a.maxResponseBytes, work)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	var token string
	if next < len(plan.sizes) {
		token = a.encodeLegacyCursor(id, next, filter)
	}
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	response.write(w, token)
}

func (a *API) startLegacyQuery(w http.ResponseWriter, params queryParams, source *legacyPageSource, work *store.QueryWork) {
	capture, err := a.captures.reserve()
	if err != nil {
		writeQueryError(w, err)
		return
	}
	published := false
	defer func() { a.captures.done(capture, !published) }()

	filter := params.filter
	matches, err := source.store.CapturePositionedMatches(filter.Start, filter.End, filter.Contains, maxLegacyMatches, work)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	plan := &legacyQueryPlan{matches: matches, sizes: make([]int, matches.Len())}
	response, err := plan.prepare(params.limit, a.maxResponseBytes, uint64(len(filter.Contains)), a.queryWorkBudget, work)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	var token string
	first := len(response.rows)
	if first < matches.Len() {
		if err := work.Charge(legacyCursorWork, uint64(len(filter.Contains))); err != nil {
			writeQueryError(w, err)
			return
		}
		token = a.encodeLegacyCursor(capture.id, first, a.filterDigest(filter))
	}
	if token != "" {
		capture.sourceID = source.retain(legacyView{plan: plan})
		if err := a.captures.publish(capture); err != nil {
			writeQueryError(w, err)
			return
		}
	}
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	published = token != ""
	response.write(w, token)
}

// prepare measures each record once, prepares the longest opening prefix,
// and certifies the work for all prefixes a resume could emit. The scan's
// per-match fixed charge covers metadata and record setup. The line charge
// covers measurement and encoding, as in queryResponse.appendRow.
func (p *legacyQueryPlan) prepare(limit, byteCap int, literalWork, budget uint64, work *store.QueryWork) (queryResponse, error) {
	response := queryResponse{maxBytes: byteCap}
	first := true
	var window legacyReplayWindow
	for i := range p.sizes {
		if err := work.Charge(); err != nil {
			return queryResponse{}, err
		}
		row := p.matches.At(i).Row
		stamp, size, _, err := response.measureRow(row, byteCap, false, work)
		if err != nil {
			return queryResponse{}, err
		}
		if size > byteCap {
			return queryResponse{}, &oversizedQueryRecord{byteCap, size}
		}
		p.sizes[i] = size
		if first && i < limit && size <= byteCap-response.size {
			if err := appendMeasuredLegacyRow(&response, row.Line, stamp, size, work); err != nil {
				return queryResponse{}, err
			}
			continue
		}
		if first {
			first = false
			window.start = i
		}
		if err := window.certify(p, i, byteCap, literalWork, budget); err != nil {
			return queryResponse{}, err
		}
	}
	return response, response.reserveDelivery(work)
}

type legacyReplayWindow struct {
	start int
	bytes int
	work  uint64
}

// For each end offset, keep the longest suffix within both page caps.
// All shorter pages ending at the same offset cost at most as much. A
// nonterminal page pays for signing once, even if a byte cut ended it.
func (w *legacyReplayWindow) certify(p *legacyQueryPlan, end, byteCap int, literalWork, budget uint64) error {
	size := p.sizes[end]
	for end-w.start >= maxQueryLimit || size > byteCap-w.bytes {
		w.bytes -= p.sizes[w.start]
		w.work -= legacyRecordWork(p.matches.At(w.start).Row)
		w.start++
	}
	w.bytes += size
	w.work += legacyRecordWork(p.matches.At(end).Row)
	overhead := literalWork + legacyCursorWork + queryDeliveryWork(w.bytes)
	if end+1 < len(p.sizes) {
		overhead += legacyCursorWork
	}
	if overhead > budget || w.work > budget-overhead {
		return store.ErrQueryWorkExhausted
	}
	return nil
}

func legacyRecordWork(row store.Row) uint64 {
	return 1 + uint64(len(row.Line))
}

func (p *legacyQueryPlan) replay(offset, limit, byteCap int, work *store.QueryWork) (queryResponse, int, error) {
	response := queryResponse{maxBytes: byteCap}
	end := min(offset+limit, len(p.sizes))
	for offset < end {
		if err := work.Charge(); err != nil {
			return queryResponse{}, 0, err
		}
		size := p.sizes[offset]
		if size > byteCap-response.size {
			break
		}
		row := p.matches.At(offset).Row
		if err := work.Charge(legacyRecordWork(row)); err != nil {
			return queryResponse{}, 0, err
		}
		stamp, err := row.TS.MarshalJSON()
		if err != nil {
			return queryResponse{}, 0, err
		}
		if err := appendMeasuredLegacyRow(&response, row.Line, stamp, size, work); err != nil {
			return queryResponse{}, 0, err
		}
		offset++
	}
	if offset < len(p.sizes) {
		if err := work.Charge(legacyCursorWork); err != nil {
			return queryResponse{}, 0, err
		}
	}
	return response, offset, response.reserveDelivery(work)
}

// Both paths use the same timestamp encoding and escape chunks as the
// measurement pass, so the record allocation matches the certified length.
func appendMeasuredLegacyRow(response *queryResponse, line string, stamp []byte, size int, work *store.QueryWork) error {
	record := make([]byte, 0, size)
	record = append(record, queryRecordPrefix...)
	record = append(record, stamp...)
	record = append(record, queryRecordField...)
	for len(line) > 0 {
		if err := work.Charge(); err != nil {
			return err
		}
		n := queryChunkEnd(line)
		payload, err := response.escapeChunk(line[:n])
		if err != nil {
			return err
		}
		record = append(record, payload...)
		line = line[n:]
	}
	record = append(record, queryRecordSuffix...)
	response.rows = append(response.rows, record)
	response.size += len(record)
	return nil
}
