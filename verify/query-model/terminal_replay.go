package querymodel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
)

func isCleanOpeningRejection(page WirePage, err error) bool {
	observed, ok := err.(*FixedBudgetHTTPError)
	return ok && observed != nil && observed.CleanBudgetRejection &&
		observed.StatusCode == http.StatusServiceUnavailable &&
		len(page.Rows) == 0 && page.NextCursor == "" && page.BodyBytes == 0 && page.EncodedBody == nil
}

type terminalReplay struct {
	req       WireRequest
	rows      int
	bodyBytes int64
	body      []byte
}

func (r *terminalReplay) record(req WireRequest, page WirePage) {
	r.req, r.rows, r.bodyBytes = req, len(page.Rows), page.BodyBytes
	// Copy at most one capped page before the adapter can reuse its buffer.
	// The walk will reject malformed page sizes before replay.
	if int64(len(page.EncodedBody)) == page.BodyBytes && page.BodyBytes <= req.MaxPageBytes {
		r.body = bytes.Clone(page.EncodedBody)
	}
}

func (r terminalReplay) verify(ctx context.Context, fetch FetchWirePage, matches []Entry, totalRows int, strict bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strict && r.body == nil {
		return errors.New("terminal response is missing its encoded body witness")
	}
	page, err := fetch(ctx, r.req)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		return redactWireFailure(fmt.Errorf("fetch page: %w", err), r.req.Cursor, page.NextCursor)
	}
	offset := totalRows - r.rows
	if err := checkWirePage(r.req, page, matches[offset:], offset, nil); err != nil {
		return err
	}
	if page.BodyBytes != r.bodyBytes {
		return fmt.Errorf("replayed terminal body has %d bytes, want %d", page.BodyBytes, r.bodyBytes)
	}
	if r.body != nil || page.EncodedBody != nil {
		if r.body == nil || page.EncodedBody == nil {
			return errors.New("terminal response and replay both require an encoded body witness")
		}
		if !bytes.Equal(r.body, page.EncodedBody) {
			return errors.New("replayed terminal body differs from the original encoded body")
		}
	}
	return ctx.Err()
}
