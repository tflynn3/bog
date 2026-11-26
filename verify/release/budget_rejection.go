package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"bog/internal/client"
)

const budgetRejectionBody = "query work budget exhausted\n"

func checkBudgetRejection(res *http.Response, err error) error {
	if res == nil {
		return fmt.Errorf("budget rejection has no HTTP response metadata: %w", err)
	}
	if res.StatusCode != http.StatusServiceUnavailable {
		if err != nil {
			return fmt.Errorf("expected HTTP 503 budget rejection: %w", err)
		}
		return fmt.Errorf("expected HTTP 503 budget rejection, got HTTP %d", res.StatusCode)
	}
	// FetchPage owns the body. A joined error can include a close failure;
	// accept only its exact status error with a complete diagnostic body.
	httpErr, statusOnly := err.(*client.HTTPError)
	framed := responseFramed(res)
	_, cursorHeader := res.Header[nextCursor]
	cursorTrailer := hasCursorTrailer(res)
	contentTypes := res.Header.Values("Content-Type")
	encodings := res.Header.Values("Content-Encoding")
	identity := !res.Uncompressed && (len(encodings) == 0 ||
		(len(encodings) == 1 && strings.EqualFold(encodings[0], "identity")))
	lengthMatches := res.ContentLength < 0 || res.ContentLength == int64(len(budgetRejectionBody))
	var ndjson bool
	for _, contentType := range contentTypes {
		ndjson = ndjson || strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "application/x-ndjson")
	}
	if statusOnly && httpErr.StatusCode == res.StatusCode && httpErr.Cause == nil &&
		httpErr.Body == budgetRejectionBody && framed && lengthMatches && identity &&
		!cursorHeader && !cursorTrailer && !ndjson {
		return nil
	}
	if err == nil {
		err = errors.New("missing client HTTP error")
	}
	return fmt.Errorf("unclean HTTP 503 budget rejection (framed=%t, content length=%d, cursor header=%t, cursor trailer=%t, content type=%q, identity encoding=%t): %w",
		framed, res.ContentLength, cursorHeader, cursorTrailer, contentTypes, identity, err)
}

// The budget-1 absent-filter control shares the walk's rejection contract and
// retains the admission report's original evidence fields.
func requireBudgetRejection(ctx context.Context, raw *http.Client, base string) (rejection, error) {
	start := time.Now()
	pager, err := newReleasePager(base, raw, 0)
	if err != nil {
		return rejection{}, err
	}
	page, res, err := pager.fetch(ctx, budgetFilter, 1, "")
	elapsed := time.Since(start)
	evidence := rejection{NDJSONRows: len(page.Rows), ElapsedMS: elapsed.Milliseconds()}
	if res != nil {
		evidence.HTTPStatus = res.StatusCode
		_, evidence.NextCursor = res.Header[nextCursor]
		evidence.NextCursor = evidence.NextCursor || hasCursorTrailer(res)
		var httpErr *client.HTTPError
		if errors.As(err, &httpErr) {
			evidence.NDJSONRows = countNDJSONRows(httpErr.Body, res.Header.Get("Content-Type"))
		}
	}
	if err := checkBudgetRejection(res, err); err != nil {
		return evidence, err
	}
	if elapsed >= queryDeadline {
		return evidence, fmt.Errorf("budget rejection exceeded %s", queryDeadline)
	}
	return evidence, ctx.Err()
}
