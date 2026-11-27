package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"bog/internal/client"
	querymodel "bog/verify/query-model"
)

// fetchOraclePage shares the shipped query checks across release oracle walks.
// Keep the response metadata for fixed-budget HTTP status classification.
func fetchOraclePage(ctx context.Context, pager *releasePager, req querymodel.WireRequest) (querymodel.WirePage, *http.Response, error) {
	if !req.Filter.Start.Equal(pager.query.Start) || !req.Filter.End.Equal(pager.query.End) ||
		req.MaxPageBytes != pager.client.MaxPageBytes {
		return querymodel.WirePage{}, nil, errors.New("oracle bounds differ from the configured release pager")
	}
	page, res, err := pager.fetch(ctx, req.Filter.Contains, req.Limit, req.Cursor)
	if err != nil {
		return querymodel.WirePage{}, res, err
	}
	if res.ContentLength < 0 || res.Header.Get("Content-Length") == "" {
		return querymodel.WirePage{}, res, errors.New("missing exact Content-Length on query response")
	}
	rows := make([]querymodel.Row, len(page.Rows))
	for i, item := range page.Rows {
		rows[i] = querymodel.Row{Time: item.TS, Line: item.Line}
	}
	return querymodel.WirePage{Rows: rows, NextCursor: page.NextCursor, BodyBytes: res.ContentLength}, res, nil
}

type oracleBodyKey struct{}

type oracleBodyTransport struct {
	http.RoundTripper
	byteCap int
}

// Observe the response body consumed by FetchPage on this request. The
// production client still drives every read and owns the close result.
func (t oracleBodyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.RoundTripper.RoundTrip(req)
	if err == nil && res != nil && res.StatusCode == http.StatusOK && res.Body != nil {
		if body, ok := req.Context().Value(oracleBodyKey{}).(*[]byte); ok {
			*body = make([]byte, 0, t.byteCap)
			res.Body = oracleBodyReader{ReadCloser: res.Body, captured: body}
		}
	}
	return res, err
}

type oracleBodyReader struct {
	io.ReadCloser
	captured *[]byte
}

func (r oracleBodyReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 && n <= len(p) {
		body := *r.captured
		*r.captured = append(body, p[:min(n, cap(body)-len(body))]...)
	}
	return n, err
}

func newObservedOraclePager(base string, raw *http.Client, byteCap int) (*releasePager, error) {
	if byteCap <= 0 {
		return nil, errors.New("oracle body observation requires a positive byte cap")
	}
	transport := raw.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return newReleasePager(base, &http.Client{Transport: oracleBodyTransport{transport, byteCap}}, byteCap)
}

type oracleTestBody struct {
	*strings.Reader
	closeErr error
	closes   int
}

func (b *oracleTestBody) Close() error {
	b.closes++
	return b.closeErr
}

func TestOraclePagerObservesClientBody(t *testing.T) {
	t.Parallel()
	const record = "{\"ts\":\"2020-01-01T00:00:00Z\",\"line\":\"a\"}\n"
	closeErr := errors.New("response close failed")
	for _, tc := range []struct {
		name, payload string
		status        int
		chunked       bool
		closeErr      error
		wantErr       error
	}{
		{name: "at cap", payload: record, status: http.StatusOK},
		{name: "empty body", status: http.StatusOK},
		{name: "past cap", payload: record + "x", status: http.StatusOK, chunked: true, wantErr: client.ErrPageBytes},
		{name: "close failure", payload: record, status: http.StatusOK, closeErr: closeErr, wantErr: closeErr},
		{name: "503 close failure", payload: budgetRejectionBody, status: http.StatusServiceUnavailable, closeErr: closeErr, wantErr: closeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := &oracleTestBody{Reader: strings.NewReader(tc.payload), closeErr: tc.closeErr}
			requests := 0
			raw := &http.Client{Transport: budgetRoundTripper(func(*http.Request) (*http.Response, error) {
				requests++
				res := &http.Response{
					StatusCode: tc.status, ProtoMajor: 1, Body: body,
					ContentLength: int64(len(tc.payload)), Header: make(http.Header),
				}
				res.Header.Set("Content-Type", "application/x-ndjson")
				if tc.status != http.StatusOK {
					res.Header.Set("Content-Type", "text/plain")
				}
				if tc.chunked {
					res.ContentLength, res.TransferEncoding = -1, []string{"chunked"}
				} else {
					res.Header.Set("Content-Length", strconv.Itoa(len(tc.payload)))
				}
				return res, nil
			})}
			pager, err := newObservedOraclePager("http://127.0.0.1", raw, len(record))
			if err != nil {
				t.Fatal(err)
			}
			req := querymodel.WireRequest{
				Filter: querymodel.Filter{Start: pager.query.Start, End: pager.query.End},
				Limit:  1, MaxPageBytes: int64(len(record)),
			}
			var captured []byte
			page, err := fetchBackfillPage(context.WithValue(t.Context(), oracleBodyKey{}, &captured), pager, req)
			if !errors.Is(err, tc.wantErr) || (err != nil && (len(page.Rows) != 0 || page.NextCursor != "" || page.BodyBytes != 0)) {
				t.Fatalf("page = %+v, %v, want %v", page, err, tc.wantErr)
			}
			if requests != 1 || body.closes != 1 {
				t.Fatalf("requests=%d closes=%d, want one of each", requests, body.closes)
			}
			if tc.status != http.StatusOK {
				var statusErr *querymodel.FixedBudgetHTTPError
				if captured != nil || !errors.As(err, &statusErr) || statusErr.CleanBudgetRejection {
					t.Fatalf("failed 503 recorded as evidence: captured=%q error=%v", captured, err)
				}
				return
			}
			if want := tc.payload[:min(len(tc.payload), len(record))]; captured == nil || string(captured) != want || cap(captured) != len(record) {
				t.Fatalf("observed body = %q (cap %d), want %q (cap %d)", captured, cap(captured), want, len(record))
			}
		})
	}
}
