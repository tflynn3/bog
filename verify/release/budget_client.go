package main

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	bogclient "bog/internal/client"
)

const (
	budgetRejectionBody = "query work budget exhausted\n"
	budgetPageMediaType = "application/x-ndjson"
)

type budgetPageClient struct {
	client  *bogclient.Client
	timeout time.Duration
}

type budgetObservationKey struct{}

type budgetPageObservation struct {
	response *http.Response
}

type budgetPageTransport struct {
	upstream http.RoundTripper
}

func (t budgetPageTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.upstream.RoundTrip(req)
	if observed, ok := req.Context().Value(budgetObservationKey{}).(*budgetPageObservation); ok {
		observed.response = res
	}
	return res, err
}

func newBudgetPageClient(base string, httpClient *http.Client, byteCap int) (budgetPageClient, error) {
	upstream := httpClient.Transport
	if upstream == nil {
		upstream = http.DefaultTransport
	}
	// The client takes a concrete *http.Transport. Register a local adapter
	// that observes the response and delegates all network I/O to the caller's
	// transport, which the caller continues to own.
	transport := &http.Transport{}
	observer := budgetPageTransport{upstream: upstream}
	transport.RegisterProtocol("http", observer)
	transport.RegisterProtocol("https", observer)
	c, err := bogclient.New(base, transport)
	if err != nil {
		return budgetPageClient{}, fmt.Errorf("create budget page client: %w", err)
	}
	c.MaxPageBytes = int64(byteCap)
	timeout := queryDeadline
	if httpClient.Timeout > 0 {
		timeout = min(timeout, httpClient.Timeout)
	}
	return budgetPageClient{client: c, timeout: timeout}, nil
}

// fetch returns rejected only for a clean initial budget rejection. The Go
// client reads and closes the body, including any trailers, before we inspect it.
func (c budgetPageClient) fetch(ctx context.Context, query bogclient.Query, cursor string) (bogclient.Page, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var observed budgetPageObservation
	ctx = context.WithValue(ctx, budgetObservationKey{}, &observed)
	page, err := c.client.FetchPage(ctx, query, cursor)
	res := observed.response
	if err != nil {
		var status *bogclient.HTTPError
		if cursor != "" || res == nil || !errors.As(err, &status) || status.StatusCode != http.StatusServiceUnavailable {
			return bogclient.Page{}, false, err
		}
		if err := checkBudgetRejection(res, status, err); err != nil {
			return bogclient.Page{}, false, err
		}
		return bogclient.Page{}, true, nil
	}
	if res == nil {
		return bogclient.Page{}, false, errors.New("missing budget page response metadata")
	}
	contentTypes := res.Header.Values("Content-Type")
	if len(contentTypes) != 1 {
		return bogclient.Page{}, false, fmt.Errorf("expected one NDJSON Content-Type on HTTP 200, got %q", contentTypes)
	}
	mediaType, _, err := mime.ParseMediaType(contentTypes[0])
	if err != nil || mediaType != budgetPageMediaType {
		return bogclient.Page{}, false, fmt.Errorf("expected NDJSON Content-Type on HTTP 200, got %q", contentTypes[0])
	}
	if declaresBudgetCursorTrailer(res.Header) {
		return bogclient.Page{}, false, errors.New("unexpected next cursor trailer declaration")
	}
	return page, false, nil
}

func checkBudgetRejection(res *http.Response, status *bogclient.HTTPError, fetchErr error) error {
	_, cursorHeader := res.Header[nextCursor]
	_, cursorTrailer := res.Trailer[nextCursor]
	cursorTrailer = cursorTrailer || declaresBudgetCursorTrailer(res.Header)
	contentTypes := res.Header.Values("Content-Type")
	hasNDJSON := false
	for _, value := range contentTypes {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), budgetPageMediaType) {
			hasNDJSON = true
			break
		}
	}
	// FetchPage returns the status error directly after a clean read and close.
	// A read error sets Cause, and a close error wraps the status error.
	if fetchErr != status || status.Cause != nil || status.Body != budgetRejectionBody || cursorHeader || cursorTrailer || hasNDJSON {
		return fmt.Errorf("unclean HTTP 503 budget rejection (cursor header=%t, cursor trailer=%t, content type=%q): %.200q: %w",
			cursorHeader, cursorTrailer, contentTypes, status.Body, fetchErr)
	}
	return nil
}

// net/http puts chunked trailer declarations in Response.Trailer. A Trailer
// field on a length-delimited response instead remains in Response.Header.
func declaresBudgetCursorTrailer(header http.Header) bool {
	for _, value := range header.Values("Trailer") {
		for name := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(name), nextCursor) {
				return true
			}
		}
	}
	return false
}
