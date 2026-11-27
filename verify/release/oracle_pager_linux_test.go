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
	return newReleasePagerWithObserver(base, raw, byteCap, func(req *http.Request, res *http.Response) {
		// FetchPage drives the reads and owns the close result.
		if body, ok := req.Context().Value(oracleBodyKey{}).(*[]byte); ok && res.StatusCode == http.StatusOK && res.Body != nil {
			*body = make([]byte, 0, byteCap)
			res.Body = oracleBodyReader{ReadCloser: res.Body, captured: body}
		}
	})
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

func TestObservedOraclePagerWireBounds(t *testing.T) {
	t.Parallel()
	testReleasePagerWireBounds(t, newObservedOraclePager, func(ctx context.Context, body *[]byte) context.Context {
		return context.WithValue(ctx, oracleBodyKey{}, body)
	})
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
		{name: "clean 503", payload: budgetRejectionBody, status: http.StatusServiceUnavailable},
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
			if err != nil && (len(page.Rows) != 0 || page.NextCursor != "" || page.BodyBytes != 0) {
				t.Fatalf("failed fetch returned a page: %+v, %v", page, err)
			}
			if requests != 1 || body.closes != 1 {
				t.Fatalf("requests=%d closes=%d, want one of each", requests, body.closes)
			}
			if tc.status != http.StatusOK {
				var statusErr *querymodel.FixedBudgetHTTPError
				if captured != nil || !errors.As(err, &statusErr) || statusErr.StatusCode != tc.status ||
					statusErr.CleanBudgetRejection != (tc.closeErr == nil) ||
					(tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
					t.Fatalf("wrong 503 classification: captured=%q error=%v", captured, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("page = %+v, %v, want %v", page, err, tc.wantErr)
			}
			if want := tc.payload[:min(len(tc.payload), len(record))]; captured == nil || string(captured) != want || cap(captured) != len(record) {
				t.Fatalf("observed body = %q (cap %d), want %q (cap %d)", captured, cap(captured), want, len(record))
			}
		})
	}
}
