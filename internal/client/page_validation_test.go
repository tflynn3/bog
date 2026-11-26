package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const validRecord = "{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":\"a\"}\n"

func TestDecodePageRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		rows int
	}{
		{"empty", "", 0},
		{"complete", validRecord, 1},
		{"escaped newline", "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"a\\nb\"}\n", 1},
		{"multiple", validRecord + validRecord, 2},
		{"partial last row", validRecord + "{\"ts\":", -1},
		{"missing final newline", strings.TrimSuffix(validRecord, "\n"), -1},
		{"blank row", validRecord + "\n", -1},
		{"whitespace row", validRecord + " \n", -1},
		{"invalid utf8", strings.Replace(validRecord, "\"a\"", "\"\xff\"", 1), -1},
		{"unpaired surrogate", strings.Replace(validRecord, "\"a\"", "\"\\ud800\"", 1), -1},
		{"invalid timestamp", strings.Replace(validRecord, "2025", "0000", 1), -1},
		{"missing line", "{\"ts\":\"2025-01-01T00:00:00Z\"}\n", -1},
		{"null line", strings.Replace(validRecord, "\"a\"", "null", 1), -1},
		{"numeric line", strings.Replace(validRecord, "\"a\"", "1", 1), -1},
		{"duplicate member", strings.Replace(validRecord, "\"a\"", "\"a\",\"line\":\"b\"", 1), -1},
		{"unknown member", strings.Replace(validRecord, "\"a\"", "\"a\",\"extra\":0", 1), -1},
		{"same-line objects", strings.TrimSuffix(validRecord, "\n") + validRecord, -1},
		{"non-object", "null\n", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := decodePage([]byte(tc.body), nil)
			if tc.rows < 0 {
				if err == nil || p.Rows != nil || p.NextCursor != "" {
					t.Fatalf("page %+v, error %v", p, err)
				}
				return
			}
			if err != nil || len(p.Rows) != tc.rows || p.NextCursor != "" {
				t.Fatalf("page %+v, error %v", p, err)
			}
		})
	}
}

func TestDecodePageCursor(t *testing.T) {
	encoded := func(size int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, size)) }
	for _, tc := range []struct {
		name   string
		values []string
		valid  bool
	}{
		{"terminal", nil, true},
		{"continuation", []string{fixtureCursor}, true},
		{"maximum token", []string{encoded(2048)}, true},
		{"oversized token", []string{encoded(2049)}, false},
		{"empty header", []string{""}, false},
		{"duplicate headers", []string{fixtureCursor, fixtureCursor}, false},
		{"comma-separated", []string{fixtureCursor + "," + fixtureNext}, false},
		{"too short", []string{encoded(32)}, false},
		{"padded", []string{fixtureCursor + "="}, false},
		{"newline in token", []string{fixtureCursor[:8] + "\n" + fixtureCursor[8:]}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := decodePage([]byte(validRecord), tc.values)
			if !tc.valid {
				if err == nil || p.Rows != nil || p.NextCursor != "" {
					t.Fatalf("page %+v, error %v", p, err)
				}
				return
			}
			want := ""
			if len(tc.values) != 0 {
				want = tc.values[0]
			}
			if err != nil || p.NextCursor != want || len(p.Rows) != 1 {
				t.Fatalf("page %+v, error %v", p, err)
			}
		})
	}
}

func TestReadResponseBody(t *testing.T) {
	for _, tc := range []struct {
		name   string
		length int64
		limit  int64
		err    string
	}{
		{"verified exact limit", 3, 3, ""},
		{"unknown length exact limit", -1, 3, ""},
		{"declared over limit", 4, 3, "exceeds size limit"},
		{"unknown length over limit", -1, 2, "exceeds size limit"},
		{"clean short EOF", 4, 4, io.ErrUnexpectedEOF.Error()},
		{"body exceeds declared length", 2, 4, "exceeds declared Content-Length"},
		{"negative limit", -1, -1, "invalid response size limit"},
		{"overflowing limit", -1, math.MaxInt64, "invalid response size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Response{ContentLength: tc.length, Body: io.NopCloser(strings.NewReader("abc"))}
			b, err := readResponseBody(r, tc.limit)
			if closeErr := r.Body.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if tc.err == "" {
				if err != nil || string(b) != "abc" {
					t.Fatalf("body %q, error %v", b, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.err) || b != nil {
				t.Fatalf("body %q, error %v", b, err)
			}
		})
	}
}

func TestEscapedBasePath(t *testing.T) {
	for _, tc := range []struct {
		base string
		want string
	}{
		{"/", ""},
		{"/a/b///", "/a/b"},
		{"/a%2fb/%2F//", "/a%2fb/%2F"},
		{"/a/%252F/%3F%23", "/a/%252F/%3F%23"},
		{"/a/%2e%2e/%7e", "/a/%2e%2e/%7e"},
	} {
		t.Run(tc.base, func(t *testing.T) {
			batch := []byte(validRecord)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					if r.URL.EscapedPath() != tc.want+"/query" || r.URL.Query().Get("page") != "1" {
						t.Errorf("request target %s", r.RequestURI)
					}
					if _, err := io.WriteString(w, validRecord); err != nil {
						t.Error(err)
					}
				case http.MethodPost:
					b, err := io.ReadAll(r.Body)
					if r.RequestURI != tc.want+"/ingest" || err != nil || !bytes.Equal(b, batch) {
						t.Errorf("request target %s, body %q, error %v", r.RequestURI, b, err)
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected method %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			t.Cleanup(s.Close)
			c, err := New(s.URL+tc.base, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.CloseIdleConnections)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if p, err := c.FetchPage(ctx, Query{}, ""); err != nil || len(p.Rows) != 1 {
				t.Fatalf("page %+v, error %v", p, err)
			}
			if outcome, err := c.Ingest(ctx, batch); err != nil || outcome != IngestCommitted {
				t.Fatalf("ingest outcome %v, error %v", outcome, err)
			}
		})
	}
}

func TestHTTP2StreamFraming(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "complete"
		if interrupted {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ProtoMajor != 2 {
					t.Errorf("protocol %s", r.Proto)
				}
				if _, err := io.WriteString(w, validRecord); err != nil {
					t.Error(err)
					return
				}
				if err := http.NewResponseController(w).Flush(); err != nil {
					t.Error(err)
					return
				}
				if interrupted {
					panic(http.ErrAbortHandler)
				}
			}))
			s.EnableHTTP2 = true
			s.StartTLS()
			t.Cleanup(s.Close)
			transport := s.Client().Transport.(*http.Transport)
			c, err := New(s.URL, transport)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.CloseIdleConnections)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			p, err := c.FetchPage(ctx, Query{}, "")
			var status *HTTPError
			if interrupted {
				if !errors.As(err, &status) || status.StatusCode != http.StatusOK || p.Rows != nil || p.NextCursor != "" {
					t.Fatalf("page %+v, error %v", p, err)
				}
			} else if err != nil || len(p.Rows) != 1 || p.NextCursor != "" {
				t.Fatalf("page %+v, error %v", p, err)
			}
		})
	}
}
