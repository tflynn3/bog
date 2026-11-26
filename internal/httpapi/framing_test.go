package httpapi

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestWriteQueryPageHTTP10(t *testing.T) {
	const row = "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"é\\n\"}\n"
	for _, tc := range []struct {
		name, body, cursor string
	}{
		{name: "empty"},
		{name: "terminal", body: row},
		{name: "continuation larger than automatic length buffer", body: strings.Repeat(row, 1024), cursor: "opaque"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Proto != "HTTP/1.0" {
					t.Errorf("request protocol %q", r.Proto)
				}
				w.Header().Set("Content-Length", "1")
				if err := writeQueryPage(w, []byte(tc.body), tc.cursor); err != nil {
					t.Errorf("write query page: %v", err)
				}
			}))
			defer server.Close()

			conn, err := net.DialTimeout("tcp", server.Listener.Addr().String(), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			const request = "GET /query HTTP/1.0\r\n\r\n"
			if n, err := io.WriteString(conn, request); err != nil || n != len(request) {
				t.Fatalf("write request: %d of %d bytes, %v", n, len(request), err)
			}
			resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			if err := errors.Join(err, resp.Body.Close()); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(tc.body)) ||
				resp.Header.Get("Content-Length") != strconv.Itoa(len(tc.body)) ||
				resp.Header.Get("Content-Type") != "application/x-ndjson" ||
				resp.Header.Get("Bog-Next-Cursor") != tc.cursor ||
				len(resp.TransferEncoding) != 0 || string(body) != tc.body {
				t.Fatalf("status %d headers %v body length %d, want length %d cursor %q",
					resp.StatusCode, resp.Header, len(body), len(tc.body), tc.cursor)
			}
		})
	}
}

type pageWriteStub struct {
	http.ResponseWriter
	n   int
	err error
}

func (w pageWriteStub) Write([]byte) (int, error) {
	return w.n, w.err
}

func TestWriteQueryPageDeliveryError(t *testing.T) {
	disconnect := errors.New("connection closed")
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"interruption", disconnect, disconnect},
		{"short write", nil, io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := pageWriteStub{ResponseWriter: httptest.NewRecorder(), n: 1, err: tc.err}
			body := []byte("first row\nsecond row\n")
			err := writeQueryPage(w, body, "continuation")
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if got := w.Header().Get("Content-Length"); got != fmt.Sprint(len(body)) {
				t.Fatalf("advertised length %s, want %d", got, len(body))
			}
		})
	}
}
