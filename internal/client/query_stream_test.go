package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestQueryStreamRecordBoundaries(t *testing.T) {
	t.Parallel()

	const special = `{"ts":"2025-01-01T01:00:00.123456789+01:00","line":"日本語\n\r\t\u0000\"\\\uD83D\uDE03"}`
	const specialLine = "日本語\n\r\t\x00\"\\😃"
	long := strings.Repeat("é", queryReadBlock+1)
	wire := special + "\r\n" +
		`{"ts":"2025-01-01T00:00:00.123456789Z","line":"` + long + "\"}\n" +
		special + "\n"

	for _, blockSize := range []int{1, 2, 3, 17, 4096, queryReadBlock - 1, queryReadBlock, len(wire)} {
		t.Run(fmt.Sprint(blockSize), func(t *testing.T) {
			t.Parallel()
			c, err := New("http://bog.test", nil)
			if err != nil {
				t.Fatal(err)
			}
			c.MaxPageBytes = int64(len(wire))
			requests, closed := 0, 0
			c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
				requests++
				if req.URL.Query().Get("limit") != "3" || req.URL.Query().Get("cursor") != fixtureCursor {
					t.Errorf("request %s did not retain cursor and row limit", req.URL)
				}
				body := strings.NewReader(wire)
				resp := retryResponse(http.StatusOK, "", int64(len(wire)), nil, &closed)
				resp.Body = queryResponseBody{
					Reader: queryReadFunc(func(p []byte) (int, error) {
						return body.Read(p[:min(len(p), blockSize)])
					}),
					closeBody: resp.Body.Close,
				}
				resp.Header.Set(nextCursorHeader, fixtureNext)
				return resp, nil
			})
			page, err := c.FetchPage(t.Context(), Query{Limit: 3}, fixtureCursor)
			stamp := time.Date(2025, 1, 1, 0, 0, 0, 123456789, time.UTC)
			want := Page{Rows: []Row{{TS: stamp, Line: specialLine}, {TS: stamp, Line: long}, {TS: stamp, Line: specialLine}}, NextCursor: fixtureNext}
			if err != nil || !reflect.DeepEqual(page, want) {
				t.Fatalf("split %d: got %d rows with cursor %q and error %v", blockSize, len(page.Rows), page.NextCursor, err)
			}
			if requests != 1 || closed != 1 {
				t.Fatalf("%d requests, %d closes, want one of each", requests, closed)
			}
		})
	}
}

func TestQueryStreamLimitsAndFailurePrecedence(t *testing.T) {
	t.Parallel()
	row := contractRow("accepted prefix")
	// Span more than one read even when the underlying reader never fragments
	// a response. The final failure occurs after at least one decoded block.
	prefix := strings.Repeat(row, queryReadBlock/len(row)+1)
	fullRows := len(prefix) / len(row)
	networkFailure := errors.New("late network failure")

	for _, tc := range []struct {
		name        string
		suffix      string
		declared    func(int) int64
		byteDelta   int
		rowDelta    int
		end         error
		want        []error
		wantRetry   bool
		wantTrailer bool
	}{
		{name: "exact fixed length", declared: func(size int) int64 { return int64(size) }},
		{name: "exact chunked"},
		{name: "exact chunked with data and EOF", end: io.EOF},
		{name: "one probe byte", suffix: "Z", byteDelta: -1, want: []error{ErrPageBytes}},
		{name: "overflow with data and read error", suffix: "Z", byteDelta: -1, end: networkFailure, want: []error{ErrPageBytes, ErrPageInterrupted, networkFailure}},
		{name: "read failure at ceiling", end: networkFailure, want: []error{ErrPageInterrupted, networkFailure}, wantRetry: true},
		{name: "short fixed length at row boundary", declared: func(size int) int64 { return int64(size + 1) }, byteDelta: 1,
			want: []error{ErrPageInterrupted, io.ErrUnexpectedEOF}, wantRetry: true},
		{name: "short fixed length within a malformed row", suffix: "{\n", declared: func(size int) int64 { return int64(size + 1) }, byteDelta: 1,
			want: []error{ErrPageInterrupted, io.ErrUnexpectedEOF}, wantRetry: true},
		{name: "longer than declared", declared: func(size int) int64 { return int64(size - 1) }, want: []error{ErrMalformedPage}},
		{name: "truncated chunk after malformed row", suffix: "{\n", end: io.ErrUnexpectedEOF,
			want: []error{ErrPageInterrupted, io.ErrUnexpectedEOF}, wantRetry: true},
		{name: "fully delivered malformed row", suffix: "{\n", want: []error{ErrMalformedPage}},
		{name: "fully delivered unterminated row", suffix: strings.TrimSuffix(row, "\n"), want: []error{ErrMalformedPage}},
		{name: "extra valid row", suffix: row, want: []error{ErrMalformedPage}},
		{name: "malformed UTF-8", suffix: "\xff\n", want: []error{ErrMalformedPage}},
		{name: "overflow after complete malformed row", suffix: "{\n" + strings.Repeat(" ", queryReadBlock), byteDelta: -1, want: []error{ErrPageBytes}},
		{name: "overflow after row limit", suffix: row + strings.Repeat(" ", queryReadBlock), byteDelta: -1, want: []error{ErrPageBytes}},
		{name: "exact row limit plus one row permitted", suffix: row, rowDelta: 1},
		{name: "cursor trailer supplied on EOF", wantTrailer: true, want: []error{ErrMalformedPage}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload := prefix + tc.suffix
			limit := int64(len(payload) + tc.byteDelta)
			c, err := New("http://bog.test", nil)
			if err != nil {
				t.Fatal(err)
			}
			c.MaxPageBytes = limit
			calls, closed, total := 0, 0, 0
			var windows []int
			c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
				length := int64(-1)
				if tc.declared != nil {
					length = tc.declared(len(payload))
				}
				response := retryResponse(http.StatusOK, "", length, nil, &closed)
				if length < 0 {
					response.TransferEncoding = []string{"chunked"}
				}
				response.Header.Set(nextCursorHeader, fixtureNext)
				response.Body = queryResponseBody{
					Reader: queryReadFunc(func(p []byte) (int, error) {
						windows = append(windows, len(p))
						if len(p) > queryReadBlock || int64(total+len(p)) > limit+1 {
							t.Fatalf("asked for %d bytes after reading %d with limit %d", len(p), total, limit)
						}
						calls++
						n := copy(p, payload[total:])
						total += n
						if n == 0 && tc.wantTrailer {
							response.Trailer = http.Header{nextCursorHeader: {fixtureCursor}}
						}
						if tc.end != nil && total == len(payload) {
							return n, tc.end
						}
						if n == 0 {
							return 0, io.EOF
						}
						return n, nil
					}),
					closeBody: response.Body.Close,
				}
				return response, nil
			})
			page, err := c.FetchPage(t.Context(), Query{Limit: fullRows + tc.rowDelta}, fixtureCursor)
			if len(tc.want) == 0 {
				if err != nil || len(page.Rows) != fullRows+tc.rowDelta || page.NextCursor != fixtureNext {
					t.Fatalf("got %d rows, cursor %q, error %v", len(page.Rows), page.NextCursor, err)
				}
			} else {
				assertQueryStreamFailure(t, page, err, tc.want...)
			}
			if retryablePage(err) != tc.wantRetry {
				t.Fatalf("retryablePage(%v) = %v, want %v", err, retryablePage(err), tc.wantRetry)
			}
			if total != len(payload) || calls < 2 || closed != 1 {
				t.Fatalf("read %d bytes in %v, closed %d times", total, windows, closed)
			}
			if tc.byteDelta == 0 && tc.end == nil && windows[len(windows)-1] != 1 {
				t.Fatalf("exact limit did not probe body EOF with one byte: %v", windows)
			}
		})
	}
}

func assertQueryStreamFailure(t *testing.T, page Page, err error, want ...error) {
	t.Helper()
	var response *HTTPError
	if page.Rows != nil || page.NextCursor != "" || !errors.As(err, &response) || response.StatusCode != http.StatusOK {
		t.Fatalf("got %+v and %v, want zero page and HTTP 200 error", page, err)
	}
	for _, cause := range want {
		if !errors.Is(err, cause) {
			t.Errorf("error %v does not preserve %v", err, cause)
		}
	}
	for _, cause := range []error{ErrPageBytes, ErrPageInterrupted, ErrMalformedPage, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, cause) != slices.Contains(want, cause) {
			t.Errorf("classification %v of %v, want %v", cause, err, want)
		}
	}
}

func TestQueryStreamWaitsForFrameEnd(t *testing.T) {
	t.Parallel()
	row := contractRow("first") + contractRow("second")
	for _, cancelRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelRead), func(t *testing.T) {
			t.Parallel()
			deadline, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			ctx, cancel := context.WithCancelCause(deadline)
			defer cancel(nil)
			cause := errors.New("caller canceled at the frame boundary")

			awaitProbe := make(chan struct{})
			finishProbe := make(chan struct{})
			closed := make(chan struct{})
			c, err := New("http://bog.test", nil)
			if err != nil {
				t.Fatal(err)
			}
			c.MaxPageBytes = int64(len(row))
			c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
				body := strings.NewReader(row)
				return &http.Response{
					StatusCode:       http.StatusOK,
					Header:           http.Header{nextCursorHeader: {fixtureNext}},
					TransferEncoding: []string{"chunked"},
					ContentLength:    -1,
					Body: queryResponseBody{
						Reader: queryReadFunc(func(p []byte) (int, error) {
							if body.Len() > 0 {
								return body.Read(p[:min(len(p), 7)])
							}
							close(awaitProbe)
							select {
							case <-req.Context().Done():
								return 0, context.Cause(req.Context())
							case <-finishProbe:
								return 0, io.EOF
							}
						}),
						closeBody: func() error {
							close(closed)
							return nil
						},
					},
				}, nil
			})
			type result struct {
				page Page
				err  error
			}
			got := make(chan result, 1)
			go func() {
				page, err := c.FetchPage(ctx, Query{Limit: 2}, fixtureCursor)
				got <- result{page, err}
			}()

			select {
			case <-awaitProbe:
			case early := <-got:
				t.Fatalf("fetch returned before the termination probe: %+v", early)
			case <-deadline.Done():
				t.Fatal("fetch did not reach the termination probe")
			}
			select {
			case <-closed:
				t.Fatal("body closed before the termination probe finished")
			default:
			}
			if cancelRead {
				cancel(cause)
			} else {
				close(finishProbe)
			}
			select {
			case result := <-got:
				if cancelRead {
					assertQueryStreamFailure(t, result.page, result.err, context.Canceled, cause)
				} else if result.err != nil || len(result.page.Rows) != 2 || result.page.Rows[1].Line != "second" || result.page.NextCursor != fixtureNext {
					t.Fatalf("got %+v and %v", result.page, result.err)
				}
			case <-deadline.Done():
				t.Fatal("fetch did not finish")
			}
			select {
			case <-closed:
			default:
				t.Fatal("fetch returned without closing body")
			}
		})
	}
}

func TestQueryRecordScratchFollowsUnfinishedRecord(t *testing.T) {
	t.Parallel()
	row := contractRow(strings.Repeat("x", queryReadBlock*2))
	records := queryRecords{maxBytes: int64(len(row) * 2), maxRows: 2}
	data := []byte(row + row)
	for i := 0; i < len(data); i += queryReadBlock {
		block := data[i:min(i+queryReadBlock, len(data))]
		if err := records.add(t.Context(), block); err != nil {
			t.Fatal(err)
		}
		wantRows := min(i+queryReadBlock, len(data)) / len(row)
		if len(records.rows) != wantRows {
			t.Fatalf("after block at %d, decoded %d rows, want %d", i, len(records.rows), wantRows)
		}
	}
	if err := records.finish(); err != nil || cap(records.pending) > queryReadBlock {
		t.Fatalf("retained %d bytes of scratch after finish, error %v", cap(records.pending), err)
	}
	// Scratch and transport blocks may be reused as soon as records are parsed.
	for i := range data {
		data[i] = 'z'
	}
	wantLine := strings.Repeat("x", queryReadBlock*2)
	for _, row := range records.rows {
		if row.Line != wantLine {
			t.Fatal("decoded line aliases input")
		}
	}
}

func TestQueryStreamReaderProgress(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	row := []byte(contractRow("first"))
	for _, tc := range []struct {
		name  string
		count int
		want  error
	}{
		{name: "intermittent empty reads", count: 99},
		{name: "stalled body", count: 100, want: io.ErrNoProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			empties := tc.count
			body := bytes.NewReader(row)
			calls := 0
			resp := &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: int64(len(row)),
				Body: io.NopCloser(queryReadFunc(func(p []byte) (int, error) {
					calls++
					if empties > 0 {
						empties--
						return 0, nil
					}
					return body.Read(p)
				})),
			}
			page, err := acceptQueryResponse(ctx, resp, int64(len(row)), 1, "", nil)
			if tc.want != nil {
				assertQueryStreamFailure(t, page, err, ErrPageInterrupted, tc.want)
				if calls != tc.count {
					t.Errorf("stopped after %d empty reads, want %d", calls, tc.count)
				}
			} else if err != nil || len(page.Rows) != 1 || page.Rows[0].Line != "first" {
				t.Fatalf("got %+v and %v", page, err)
			}
		})
	}
}
