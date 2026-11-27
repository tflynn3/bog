package main

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"bog/internal/client"
)

type releasePager struct {
	client      *client.Client
	query       client.Query
	pageTimeout time.Duration
}

type pageResponseKey struct{}

type pageTransport struct {
	http.RoundTripper
	observe func(*http.Request, *http.Response)
}

func (t pageTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.RoundTripper.RoundTrip(req)
	if capture, ok := req.Context().Value(pageResponseKey{}).(**http.Response); ok {
		*capture = res
	}
	if err == nil && res != nil && t.observe != nil {
		t.observe(req, res)
	}
	return res, err
}

func newReleasePager(base string, raw *http.Client, byteCap int) (*releasePager, error) {
	return newReleasePagerWithObserver(base, raw, byteCap, nil)
}

// Each page uses the earliest of the parent deadline, the verifier page
// timeout, and a positive caller timeout. The same context covers the request
// and body reads. FetchPage owns redirects, page acceptance, and body close.
func newReleasePagerWithObserver(base string, raw *http.Client, byteCap int, observe func(*http.Request, *http.Response)) (*releasePager, error) {
	start, err := time.Parse(time.RFC3339, fixtureStart)
	if err != nil {
		return nil, err
	}
	end, err := time.Parse(time.RFC3339, fixtureEnd)
	if err != nil {
		return nil, err
	}
	underlying := raw.Transport
	if underlying == nil {
		underlying = http.DefaultTransport
	}
	// The candidate listens on loopback HTTP. Observe its response on the
	// same request the client validates, leaving body reads and close to it.
	transport := new(http.Transport)
	transport.RegisterProtocol("http", pageTransport{RoundTripper: underlying, observe: observe})
	production, err := client.New(base, transport)
	if err != nil {
		return nil, err
	}
	production.MaxPageBytes = int64(byteCap)
	pageTimeout := queryDeadline
	if raw.Timeout > 0 {
		pageTimeout = min(pageTimeout, raw.Timeout)
	}
	return &releasePager{
		client: production, query: client.Query{Start: start, End: end}, pageTimeout: pageTimeout,
	}, nil
}

func (p *releasePager) fetch(ctx context.Context, filter string, limit int, cursor string) (client.Page, *http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, p.pageTimeout)
	defer cancel()
	var res *http.Response
	query := p.query
	query.Contains, query.Limit = filter, limit
	page, err := p.client.FetchPage(context.WithValue(ctx, pageResponseKey{}, &res), query, cursor)
	if err != nil {
		return client.Page{}, res, err
	}
	if res == nil {
		return client.Page{}, nil, fmt.Errorf("missing HTTP response metadata")
	}
	mediaType, _, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/x-ndjson" {
		return client.Page{}, res, fmt.Errorf("expected NDJSON content type, got %q", res.Header.Get("Content-Type"))
	}
	if hasCursorTrailer(res) {
		return client.Page{}, res, fmt.Errorf("unexpected next cursor trailer")
	}
	return page, res, nil
}

func checkPageRows(got []client.Row, want []row) error {
	for i, r := range got {
		gotTS := r.TS.UTC().Format(time.RFC3339Nano)
		if i >= len(want) {
			return fmt.Errorf("unexpected row at page offset %d: got %s %.100q past %d expected rows", i, gotTS, r.Line, len(want))
		}
		if gotTS != want[i].TS || r.Line != want[i].Line {
			return fmt.Errorf("unexpected row at page offset %d: got %s %.100q, want %s %.100q", i, gotTS, r.Line, want[i].TS, want[i].Line)
		}
	}
	return nil
}

func checkBudgetPage(page client.Page, want []row, terminal bool, seen map[string]bool) error {
	if len(page.Rows) == 0 && page.NextCursor != "" {
		return fmt.Errorf("empty query page with next cursor")
	}
	if err := checkPageRows(page.Rows, want); err != nil {
		return err
	}
	if n := len(page.Rows); n != len(want) {
		return fmt.Errorf("returned %d rows, want %d", n, len(want))
	}
	if terminal {
		if page.NextCursor != "" {
			return fmt.Errorf("issued a cursor after the final row")
		}
		return nil
	}
	if page.NextCursor == "" {
		return fmt.Errorf("missing continuation cursor")
	}
	if seen[page.NextCursor] {
		return fmt.Errorf("repeated a continuation cursor")
	}
	return nil
}

func responseFramed(res *http.Response) bool {
	return res.ProtoMajor >= 2 || res.ContentLength >= 0 ||
		(len(res.TransferEncoding) == 1 && res.TransferEncoding[0] == "chunked")
}

func hasCursorTrailer(res *http.Response) bool {
	if _, present := res.Trailer[nextCursor]; present {
		return true
	}
	// net/http leaves Trailer declarations in Header on non-chunked HTTP/1.
	for _, declaration := range res.Header.Values("Trailer") {
		for name := range strings.SplitSeq(declaration, ",") {
			if strings.EqualFold(strings.TrimSpace(name), nextCursor) {
				return true
			}
		}
	}
	return false
}
