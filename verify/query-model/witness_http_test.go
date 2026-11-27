package querymodel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"bog/internal/client"
	model "bog/verify/query-model"
)

type observedQueryKey struct{}

type observedQuery struct {
	response *http.Response
	body     bytes.Buffer
}

type witnessTransport struct{ http.RoundTripper }

type witnessedReadCloser struct {
	io.Reader
	io.Closer
}

func (t witnessTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.RoundTripper.RoundTrip(req)
	if observation, ok := req.Context().Value(observedQueryKey{}).(*observedQuery); ok && err == nil {
		observation.response = res
		if res.StatusCode == http.StatusOK {
			// FetchPage owns the bounded read, framing validation and close.
			res.Body = witnessedReadCloser{io.TeeReader(res.Body, &observation.body), res.Body}
		}
	}
	return res, err
}

func newWitnessClient(base string, transport http.RoundTripper, cap int64) (*client.Client, model.FetchWirePage, error) {
	observed := new(http.Transport)
	observed.RegisterProtocol("http", witnessTransport{transport})
	c, err := client.New(base, observed)
	if err != nil {
		return nil, nil, err
	}
	c.MaxPageBytes = cap
	fetch := func(ctx context.Context, req model.WireRequest) (model.WirePage, error) {
		if req.MaxPageBytes != c.MaxPageBytes {
			return model.WirePage{}, errors.New("configured query byte cap differs from the oracle")
		}
		var observation observedQuery
		q := client.Query{Start: req.Filter.Start, End: req.Filter.End, Contains: req.Filter.Contains, Limit: req.Limit}
		p, err := c.FetchPage(context.WithValue(ctx, observedQueryKey{}, &observation), q, req.Cursor)
		r := observation.response
		if r == nil {
			if err == nil {
				err = errors.New("missing query HTTP response")
			}
			return model.WirePage{}, err
		}
		typ, _, typeErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			failure := &model.FixedBudgetHTTPError{StatusCode: r.StatusCode, Cause: err}
			if cleanQueryFailure(r, typ, typeErr, err) {
				failure.Cause = nil
				failure.CleanBudgetRejection = r.StatusCode == http.StatusServiceUnavailable
			}
			return model.WirePage{}, failure
		}
		body := observation.body.Bytes()
		if r.Header.Get("Content-Length") == "" || r.ContentLength != int64(len(body)) ||
			typeErr != nil || typ != "application/x-ndjson" {
			return model.WirePage{}, &model.FixedBudgetHTTPError{
				StatusCode: r.StatusCode, Cause: errors.New("expected a complete NDJSON body with exact Content-Length"),
			}
		}
		if body == nil {
			body = []byte{}
		}
		page := model.WirePage{NextCursor: p.NextCursor, BodyBytes: int64(len(body)), EncodedBody: body}
		for _, row := range p.Rows {
			page.Rows = append(page.Rows, model.Row{Time: row.TS, Line: row.Line})
		}
		return page, nil
	}
	return c, fetch, nil
}

func cleanQueryFailure(r *http.Response, mediaType string, mediaErr, err error) bool {
	var want string
	switch r.StatusCode {
	case http.StatusServiceUnavailable:
		want = "query work budget exhausted\n"
	case http.StatusGone:
		want = "query capture expired\n"
	case http.StatusBadRequest:
		want = "invalid cursor\n"
	default:
		return false
	}
	status, ok := err.(*client.HTTPError)
	if !ok || status.StatusCode != r.StatusCode || status.Cause != nil || status.Body != want ||
		mediaErr != nil || mediaType != "text/plain" || r.Uncompressed {
		return false
	}
	framed := r.ContentLength == int64(len(want)) ||
		r.ContentLength < 0 && (r.ProtoMajor >= 2 || len(r.TransferEncoding) == 1 && r.TransferEncoding[0] == "chunked")
	enc := r.Header.Values("Content-Encoding")
	if !framed || len(enc) > 1 || len(enc) == 1 && !strings.EqualFold(enc[0], "identity") {
		return false
	}
	if _, present := r.Header[cursorHeader]; present {
		return false
	}
	if _, present := r.Trailer[cursorHeader]; present {
		return false
	}
	for _, declaration := range r.Header.Values("Trailer") {
		for name := range strings.SplitSeq(declaration, ",") {
			if strings.EqualFold(strings.TrimSpace(name), cursorHeader) {
				return false
			}
		}
	}
	return true
}

func sendFixtureBatch(ctx context.Context, c *client.Client, rows []model.Row) (client.IngestOutcome, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := encoder.Encode(httpFixtureRow{Time: row.Time, Line: row.Line}); err != nil {
			return client.IngestRejected, err
		}
	}
	return c.Ingest(ctx, buf.Bytes())
}

func acknowledgeFixtureBatch(ctx context.Context, c *client.Client, rows []model.Row) error {
	outcome, err := sendFixtureBatch(ctx, c, rows)
	if err != nil || outcome == client.IngestCommitted {
		return err
	}
	return fmt.Errorf("unexpected ingest outcome %d", outcome)
}
