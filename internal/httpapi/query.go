package httpapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	queryBlockRows  = 1000
	maxQueryBlocks  = 4096
	maxPlanKeyBytes = 256 << 10
	maxPageKeyBytes = 2048
)

type queryBlock struct {
	after PagePosition
	count int
	more  bool
}

var errInvalidSourcePage = errors.New("invalid query source page")

type oversizedQueryRecord struct{ cap, required int }

func (e oversizedQueryRecord) Error() string {
	return fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", e.cap, e.required)
}

func writeQueryError(w http.ResponseWriter, err error) {
	var oversized oversizedQueryRecord
	switch {
	case errors.Is(err, store.ErrQueryWorkExhausted):
		http.Error(w, store.ErrQueryWorkExhausted.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, ErrQueryCapacityExhausted):
		http.Error(w, ErrQueryCapacityExhausted.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "request cancelled", http.StatusRequestTimeout)
	case errors.Is(err, store.ErrInvalidQueryCursor):
		http.Error(w, store.ErrInvalidQueryCursor.Error(), http.StatusBadRequest)
	case errors.Is(err, ErrCaptureExpired):
		http.Error(w, ErrCaptureExpired.Error(), http.StatusGone)
	case errors.As(err, &oversized):
		http.Error(w, oversized.Error(), http.StatusUnprocessableEntity)
	default:
		http.Error(w, "query failed", http.StatusInternalServerError)
	}
}

func (a *API) query(w http.ResponseWriter, r *http.Request) {
	params, err := parseQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	work := store.NewQueryWork(ctx, a.queryWorkBudget)
	var response queryResponse
	response.maxBytes = a.maxResponseBytes
	switch {
	case params.page:
		err = a.startQuery(w, ctx, params, &response, work)
	case params.cursor != "":
		err = a.resumeQuery(w, ctx, params, &response, work)
	default:
		err = a.ordinaryQuery(w, ctx, params, &response, work)
	}
	if err != nil {
		writeQueryError(w, err)
	}
}

func (a *API) ordinaryQuery(w http.ResponseWriter, ctx context.Context, p queryParams, response *queryResponse, work *store.QueryWork) error {
	page, err := a.pages.Page(ctx, PageRequest{Filter: p.filter, Limit: p.limit}, work)
	if err != nil {
		return err
	}
	if len(page.Rows) > p.limit {
		return errInvalidSourcePage
	}
	for _, row := range page.Rows {
		fits, err := response.add(row.Row, work)
		if err != nil {
			return err
		}
		if !fits {
			break
		}
	}
	if err := work.Charge(queryDeliveryWork(response.size)); err != nil {
		return err
	}
	deliverQueryPage(w, response, "")
	return nil
}

// startQuery certifies a complete sequence of bounded storage calls on the
// pinned view. A resume replays a subset of these exact calls. It may take
// fewer rows, apply a different byte cut, or cross a block boundary without
// introducing an unverified seek or scan into a late sparse gap.
func (a *API) startQuery(w http.ResponseWriter, ctx context.Context, p queryParams, response *queryResponse, work *store.QueryWork) error {
	if err := work.Charge(); err != nil {
		return err
	}
	if !a.captures.reserve() {
		return ErrQueryCapacityExhausted
	}
	c := &queryCapture{deadline: time.Now().Add(maxCaptureLifetime)}
	published := false
	defer func() {
		if published {
			a.captures.done(c)
		} else {
			a.captures.release(c)
		}
	}()

	req := PageRequest{Filter: p.filter, Limit: 1, Snapshot: true}
	var next cursorData
	var lastPosition PagePosition
	var maxPositionBytes uint64
	var measuring bool
	var planKeyBytes int
	measure := queryResponse{maxBytes: a.maxResponseBytes, measureOnly: true}
	for {
		planKeyBytes += len(req.After)
		if len(c.blocks) == maxQueryBlocks || planKeyBytes > maxPlanKeyBytes {
			return store.ErrQueryWorkExhausted
		}
		if err := work.Charge(1); err != nil {
			return err
		}
		page, err := a.pages.Page(ctx, req, work)
		if err != nil {
			return err
		}
		if req.Capture == "" {
			c.sourceID = page.Capture
			if !validPageKey(c.sourceID) {
				return errInvalidSourcePage
			}
			req.Capture = c.sourceID
			if !page.ExpiresAt.IsZero() && page.ExpiresAt.Before(c.deadline) {
				c.deadline = page.ExpiresAt
			}
		}
		if err := validateBlock(req, page, work); err != nil {
			return err
		}
		c.blocks = append(c.blocks, queryBlock{req.After, len(page.Rows), page.More})
		for i, row := range page.Rows {
			maxPositionBytes = max(maxPositionBytes, uint64(len(row.Position)))
			if !measuring && len(response.rows) < p.limit {
				fits, err := response.add(row.Row, work)
				if err != nil {
					return err
				}
				if fits {
					next.block, next.offset = uint32(len(c.blocks)-1), uint16(i)
					lastPosition = row.Position
					continue
				}
				// Work inspecting an omitted record has already been charged.
				// Measure it in isolation to cover a resume that starts with it.
			}
			measuring = true
			measure.size = 0
			_, required, err := measure.appendRow(row.Row, work)
			if err != nil {
				return err
			}
			if err := work.Charge(queryDeliveryWork(max(measure.size, required))); err != nil {
				return err
			}
		}
		if !page.More {
			break
		}
		req.After = page.Rows[len(page.Rows)-1].Position
		req.Limit = queryBlockRows
	}
	if err := work.Charge(queryDeliveryWork(response.size)); err != nil {
		return err
	}
	if !measuring {
		deliverQueryPage(w, response, "")
		return nil
	}

	// Any resume pays for incoming authentication, a position check, and
	// signing an outgoing cursor. Reserve that overhead in addition to all
	// certified scans and response work before committing the initial page.
	if err := chargeCursorValidation(work, len(p.filter.Contains)); err != nil {
		return err
	}
	if err := work.Charge(maxPositionBytes, maxPositionBytes, maxCursorTokenBytes,
		uint64(len(p.filter.Contains)), uint64(len(lastPosition)), maxCursorTokenBytes); err != nil {
		return err
	}
	digest := a.filterDigest(p.filter)
	next.position = sha256.Sum256([]byte(lastPosition))
	id, err := a.captures.publish(c)
	if err != nil {
		return err
	}
	published = true
	next.capture = id
	deliverQueryPage(w, response, a.encodeCursor(next, digest))
	return nil
}

func (a *API) resumeQuery(w http.ResponseWriter, ctx context.Context, p queryParams, response *queryResponse, work *store.QueryWork) error {
	if err := chargeCursorValidation(work, len(p.filter.Contains)); err != nil {
		return err
	}
	digest := a.filterDigest(p.filter)
	cursor, err := a.decodeCursor(p.cursor, digest)
	if err != nil {
		return err
	}
	c, err := a.captures.acquire(cursor.capture)
	if err != nil {
		return err
	}
	defer a.captures.done(c)
	if int(cursor.block) >= len(c.blocks) || int(cursor.offset) >= c.blocks[cursor.block].count {
		return store.ErrInvalidQueryCursor
	}

	start := int(cursor.block)
	offset := int(cursor.offset) + 1
	if offset == c.blocks[start].count {
		// A boundary cursor already carries the exclusive position of the
		// next certified scan. The acquisition scan is never replayed.
		start++
		offset = 0
		if start == len(c.blocks) {
			return store.ErrInvalidQueryCursor
		}
		if err := verifyCursorPosition(work, cursor, c.blocks[start].after); err != nil {
			return err
		}
	}

	next := cursor
	var lastPosition PagePosition
	var more bool
scan:
	for index := start; index < len(c.blocks); index++ {
		if err := work.Charge(1); err != nil {
			return err
		}
		block := c.blocks[index]
		req := PageRequest{Filter: p.filter, Limit: queryBlockRows, Snapshot: true, Capture: c.sourceID, After: block.after}
		page, err := a.pages.Page(ctx, req, work)
		if errors.Is(err, ErrCaptureExpired) {
			a.captures.retire(cursor.capture)
		}
		if err != nil {
			return err
		}
		if err := validateBlock(req, page, work); err != nil {
			return err
		}
		if len(page.Rows) != block.count || page.More != block.more {
			return errInvalidSourcePage
		}
		if offset > 0 {
			if err := verifyCursorPosition(work, cursor, page.Rows[offset-1].Position); err != nil {
				return err
			}
		}
		for i := offset; i < len(page.Rows); i++ {
			if len(response.rows) == p.limit {
				more = true
				break scan
			}
			fits, err := response.add(page.Rows[i].Row, work)
			if err != nil {
				return err
			}
			if !fits {
				more = true
				break scan
			}
			next.block, next.offset = uint32(index), uint16(i)
			lastPosition = page.Rows[i].Position
		}
		if len(response.rows) == p.limit {
			more = block.more
			break
		}
		offset = 0
	}

	if err := work.Charge(queryDeliveryWork(response.size)); err != nil {
		return err
	}
	var token string
	if more {
		if !a.captures.canContinue(c) {
			return ErrCaptureExpired
		}
		if err := work.Charge(uint64(len(lastPosition)), maxCursorTokenBytes); err != nil {
			return err
		}
		next.position = sha256.Sum256([]byte(lastPosition))
		token = a.encodeCursor(next, digest)
	}
	deliverQueryPage(w, response, token)
	return nil
}

func validPageKey(key string) bool {
	return key != "" && len(key) <= maxPageKeyBytes && utf8.ValidString(key)
}

func verifyCursorPosition(work *store.QueryWork, cursor cursorData, position PagePosition) error {
	if err := work.Charge(uint64(len(position))); err != nil {
		return err
	}
	if sha256.Sum256([]byte(position)) != cursor.position {
		return errInvalidSourcePage
	}
	return nil
}

func validateBlock(req PageRequest, page PageResult, work *store.QueryWork) error {
	if len(page.Rows) > req.Limit || (len(page.Rows) == 0 && (page.More || req.After != "")) ||
		(page.Capture != "" && page.Capture != req.Capture) {
		return errInvalidSourcePage
	}
	previous := req.After
	for _, row := range page.Rows {
		if err := work.Charge(1, uint64(len(row.Position))); err != nil {
			return err
		}
		if !validPageKey(string(row.Position)) || row.Position == previous {
			return errInvalidSourcePage
		}
		previous = row.Position
	}
	return nil
}

func deliverQueryPage(w http.ResponseWriter, response *queryResponse, token string) {
	if token != "" {
		w.Header().Set("Bog-Next-Cursor", token)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Length", strconv.Itoa(response.size))
	w.WriteHeader(http.StatusOK)
	for _, record := range response.rows {
		if n, err := w.Write(record); err != nil || n != len(record) {
			// The declared length lets the client detect an interrupted body.
			return
		}
	}
}
