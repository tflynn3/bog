package httpapi

import (
	"net/http"

	"bog/internal/store"
)

const (
	legacyCursorWork = 1 // fixed-size signing or authentication and registry access
	maxLegacyMatches = maxCertifiedScans * maxQueryLimit
)

// A legacy capture records the exact ordered matches, including exhaustion,
// and their encoded lengths. Replay uses random access to the pinned rows,
// so the work to find a match (including sparse gaps) is paid once.
type legacyQueryPlan struct {
	matches *store.PositionedMatches
	sizes   []int
}

func (a *API) resumeLegacyQuery(w http.ResponseWriter, params queryParams, work *store.QueryWork) {
	if len(params.cursor) != legacyCursorReservation {
		writeQueryError(w, store.ErrInvalidQueryCursor)
		return
	}
	if err := work.Charge(legacyCursorWork, uint64(len(params.filter.Contains))); err != nil {
		writeQueryError(w, err)
		return
	}
	filter := a.filterDigest(params.filter)
	cursor, err := a.decodeLegacyCursor(params.cursor)
	if err != nil || cursor.filter != filter {
		writeQueryError(w, store.ErrInvalidQueryCursor)
		return
	}
	capture, err := a.captures.acquire(cursor.capture)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	defer a.captures.done(capture, false)

	plan := a.legacy.lookup(capture.sourceID).plan
	offset := cursor.offset
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
		token = a.encodeLegacyCursor(legacyCursor{cursor.capture, next, filter})
	}
	if err := work.Charge(); err != nil {
		writeQueryError(w, err)
		return
	}
	response.write(w, token)
}

func (a *API) prepareLegacyOpening(capture *queryCapture, params queryParams, work *store.QueryWork) (queryResponse, string, error) {
	filter := params.filter
	matches, err := a.legacy.store.CapturePositionedMatches(filter.Start, filter.End, filter.Contains, maxLegacyMatches, work)
	if err != nil {
		return queryResponse{}, "", err
	}
	plan := &legacyQueryPlan{matches: matches, sizes: make([]int, matches.Len())}
	response, err := plan.prepare(params.limit, a.maxResponseBytes, uint64(len(filter.Contains)), a.queryWorkBudget, work)
	if err != nil {
		return queryResponse{}, "", err
	}
	first := len(response.rows)
	if first == len(plan.sizes) {
		return response, "", nil
	}
	if err := work.Charge(legacyCursorWork, uint64(len(filter.Contains))); err != nil {
		return queryResponse{}, "", err
	}
	capture.sourceID = a.legacy.retain(legacyView{plan: plan})
	return response, a.encodeLegacyCursor(legacyCursor{capture.id, first, a.filterDigest(filter)}), nil
}

// prepare measures each record once, prepares the longest opening prefix,
// and certifies the work for all prefixes a resume could emit. The scan's
// per-match fixed charge also covers match metadata and record setup.
// Measurement reserves the line-byte work and supplies the escaped payload
// directly for opening records that fit in one chunk.
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
		p.sizes[i] = size
		if size > byteCap {
			if i == 0 {
				return queryResponse{}, &oversizedQueryRecord{byteCap, size}
			}
			if !first {
				if err := window.certify(i, size, byteCap, literalWork, budget); err != nil {
					return queryResponse{}, err
				}
			}
			// Keep the barrier's exact size. No continuation can cross it.
			p.sizes = p.sizes[:i+1]
			return response, response.reserveDelivery(work)
		}
		if first && i < limit && size <= byteCap-response.size {
			var escaped []byte
			if len(row.Line) > 0 && len(row.Line) <= queryStringChunk {
				escaped = response.escapedChunkPayload()
			}
			if err := response.appendMeasuredRow(stamp, row.Line, size, escaped, work); err != nil {
				return queryResponse{}, err
			}
			continue
		}
		if first {
			first = false
			window.start = i
		} else if err := window.certify(i, size, byteCap, literalWork, budget); err != nil {
			return queryResponse{}, err
		}
		window.add(p, i, size, byteCap, legacyRecordWork(row))
	}
	if !first {
		if err := window.certify(len(p.sizes), 0, byteCap, literalWork, budget); err != nil {
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

// Keep the longest suffix within both page caps.
func (w *legacyReplayWindow) add(p *legacyQueryPlan, offset, size, byteCap int, recordWork uint64) {
	for offset-w.start >= maxQueryLimit || size > byteCap-w.bytes {
		w.bytes -= p.sizes[w.start]
		w.work -= legacyRecordWork(p.matches.At(w.start).Row)
		w.start++
	}
	w.bytes += size
	w.work += recordWork
}

// A shorter page costs at most as much as the longest suffix. Even if
// shortening a row-limited page adds a byte-cap probe, it saves at least
// one unit by dropping a row. A zero nextSize marks exhaustion.
func (w *legacyReplayWindow) certify(end, nextSize, byteCap int, literalWork, budget uint64) error {
	overhead := literalWork + legacyCursorWork + queryDeliveryWork(w.bytes)
	if nextSize != 0 {
		overhead += legacyCursorWork
		if end-w.start < maxQueryLimit && nextSize > byteCap-w.bytes {
			overhead++ // probe of the first record excluded by the byte cap
		}
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
		if err := work.Charge(1); err != nil {
			return queryResponse{}, 0, err
		}
		size := p.sizes[offset]
		if size > byteCap-response.size {
			if response.size == 0 {
				return queryResponse{}, 0, &oversizedQueryRecord{byteCap, size}
			}
			break
		}
		row := p.matches.At(offset).Row
		if err := work.Charge(uint64(len(row.Line))); err != nil {
			return queryResponse{}, 0, err
		}
		stamp, err := row.TS.MarshalJSON()
		if err != nil {
			return queryResponse{}, 0, err
		}
		if err := response.appendMeasuredRow(stamp, row.Line, size, nil, work); err != nil {
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
