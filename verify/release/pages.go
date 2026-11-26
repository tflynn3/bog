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
	client *client.Client
	query  client.Query
}

type pageResponseKey struct{}

type pageTransport struct{ http.RoundTripper }

func (t pageTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.RoundTripper.RoundTrip(req)
	if capture, ok := req.Context().Value(pageResponseKey{}).(**http.Response); ok {
		*capture = res
	}
	return res, err
}

func newReleasePager(base string, raw *http.Client, byteCap int) (*releasePager, error) {
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
	transport.RegisterProtocol("http", pageTransport{underlying})
	production, err := client.New(base, transport)
	if err != nil {
		return nil, err
	}
	production.MaxPageBytes = int64(byteCap)
	return &releasePager{production, client.Query{Start: start, End: end}}, nil
}

func (p *releasePager) fetch(ctx context.Context, filter string, limit int, cursor string) (client.Page, *http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, queryDeadline)
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
		if i >= len(want) || r.TS.UTC().Format(time.RFC3339Nano) != want[i].TS || r.Line != want[i].Line {
			return fmt.Errorf("unexpected row at page offset %d", i)
		}
	}
	return nil
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
