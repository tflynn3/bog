package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// decodeQueryPage is the private decoding seam for successful query responses.
// It validates the complete body and the cursor metadata before returning a
// candidate page. The response owner still has to close the body and check
// cancellation before the candidate can be published.
func decodeQueryPage(ctx context.Context, resp *http.Response, maxBytes int64, maxRows int, cursor string) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	frame, err := newQueryFrame(resp, maxBytes)
	if err != nil {
		return Page{}, fmt.Errorf("read query page: %w", err)
	}
	rows, err := decodeQueryFrame(ctx, &frame, maxRows)
	if err != nil {
		return Page{}, err
	}
	// Check cursor metadata after the body finishes. net/http may only
	// populate Trailer during the read that establishes the frame boundary.
	next, err := readNextCursor(resp, cursor)
	if err != nil {
		return Page{}, fmt.Errorf("%w: %w", ErrMalformedPage, err)
	}
	return Page{Rows: rows, NextCursor: next}, nil
}

func decodeQueryFrame(ctx context.Context, frame *queryFrame, maxRows int) ([]Row, error) {
	buffer := make([]byte, frame.blockSize())
	records := queryRecords{maxBytes: frame.limit, maxRows: maxRows}
	var recordErr error
	for {
		block := frame.read(ctx, buffer)
		if len(block) == 0 {
			break
		}
		if recordErr != nil {
			continue
		}
		recordErr = records.add(ctx, block)
		if recordErr != nil {
			// Keep reading within the same byte ceiling so HTTP delivery
			// failures and overflow still take precedence over malformed
			// records. Release rows and record scratch as soon as they can
			// no longer contribute to an accepted page.
			records = queryRecords{}
		}
	}
	if err := frame.result(ctx); err != nil {
		return nil, fmt.Errorf("read query page: %w", err)
	}
	if recordErr == nil {
		recordErr = records.finish()
	}
	if recordErr != nil {
		if errors.Is(recordErr, context.Canceled) || errors.Is(recordErr, context.DeadlineExceeded) {
			return nil, recordErr
		}
		return nil, fmt.Errorf("%w: %w", ErrMalformedPage, recordErr)
	}
	return records.rows, nil
}
