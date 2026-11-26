package httpapi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/client"
	"bog/internal/store"
)

type cancelOnDisconnectWriter struct {
	http.ResponseWriter
	ctx     context.Context
	entered chan<- struct{}
}

func (w cancelOnDisconnectWriter) Write([]byte) (int, error) {
	close(w.entered)
	<-w.ctx.Done()
	return 0, w.ctx.Err()
}

func TestLiveQueryClientDisconnectReleasesSlot(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	row := store.Row{TS: stamp, Line: "initial"}
	s := new(store.Store)
	s.Publish([]store.Row{row})
	api := NewWithQueryLimits(s, publishingAppender{s}, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	entered, returned := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Test-Block-Write") == "yes" {
			// Hold the response write until net/http reports that the client
			// disconnected. All request handling still uses the live route.
			defer close(returned)
			w = cancelOnDisconnectWriter{w, r.Context(), entered}
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	client := server.Client()

	do := func(req *http.Request) (int, http.Header, string, error) {
		res, err := client.Do(req)
		if err != nil {
			return 0, nil, "", err
		}
		body, err := io.ReadAll(res.Body)
		return res.StatusCode, res.Header, string(body), errors.Join(err, res.Body.Close())
	}
	ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+queryBounds, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Test-Block-Write", "yes")
	clientDone := make(chan error, 1)
	go func() {
		_, _, _, err := do(request)
		clientDone <- err
	}()
	waitForAdmission(t, entered, "HTTP client to occupy slot")

	for _, tc := range []struct {
		method, path, body string
		status             int
		want               string
	}{
		{http.MethodGet, queryBounds, "", http.StatusServiceUnavailable, "query capacity exhausted\n"},
		{http.MethodGet, "/healthz", "", http.StatusOK, ""},
		{http.MethodPost, "/ingest", jsonRow(t, row), http.StatusNoContent, ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, tc.method, server.URL+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			status, hdr, body, err := do(req)
			if err != nil || status != tc.status || body != tc.want || hdr.Get("Bog-Next-Cursor") != "" {
				t.Fatalf("status=%d headers=%v body=%q error=%v", status, hdr, body, err)
			}
		})
	}

	cancel()
	if err := waitForAdmission(t, clientDone, "client cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled client returned %v", err)
	}
	waitForAdmission(t, returned, "server to release the slot")
	ctx, cancelRetry := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelRetry()
	retry, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+queryBounds, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body, err := do(retry)
	if err != nil || status != http.StatusOK || body != strings.Repeat(jsonRow(t, row), 2) {
		t.Fatalf("retry: status=%d body=%q error=%v", status, body, err)
	}
}

type cutAfterFirstRecord struct {
	http.ResponseWriter
	wroteRecord bool
}

func (w *cutAfterFirstRecord) Write(p []byte) (int, error) {
	if w.wroteRecord {
		return 0, io.ErrShortWrite
	}
	w.wroteRecord = true
	return w.ResponseWriter.Write(p)
}

func TestInterruptedHTTP10QueryPage(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	stamp := start.Add(time.Second)
	rows := []store.Row{
		{TS: stamp, Line: "initial"},
		{TS: stamp, Line: "tie"},
		{TS: stamp, Line: "tie"},
		{TS: stamp, Line: "last"},
	}
	s := new(store.Store)
	s.Publish(rows)
	api := New(s, nil)
	var continuations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Interrupt the raw HTTP/1.0 request and the client's first attempt.
		if r.URL.Query().Has("cursor") && continuations.Add(1) <= 2 {
			w = &cutAfterFirstRecord{ResponseWriter: w}
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	c, err := client.New(server.URL, transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
	defer cancel()
	q := client.Query{Start: start, End: start.Add(24 * time.Hour), Limit: 1}
	first, err := c.FetchPage(ctx, q, "")
	if err != nil || first.NextCursor == "" || len(first.Rows) != 1 || first.Rows[0] != rows[0] {
		t.Fatalf("first page = %+v, error = %v", first, err)
	}

	address := server.Listener.Addr().String()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close HTTP/1.0 connection: %v", err)
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(admissionWaitTimeout)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "GET %s&cursor=%s&limit=2 HTTP/1.0\r\nHost: %s\r\n\r\n", queryBounds, first.NextCursor, address); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	record := jsonRow(t, rows[1])
	if response.Proto != "HTTP/1.0" || response.StatusCode != http.StatusOK ||
		response.ContentLength != int64(2*len(record)) || response.Header.Get("Bog-Next-Cursor") == "" ||
		string(body) != record || !errors.Is(readErr, io.ErrUnexpectedEOF) {
		t.Fatalf("interrupted page: protocol=%s status=%d headers=%v body=%q error=%v",
			response.Proto, response.StatusCode, response.Header, body, readErr)
	}

	q.Limit = 2
	var got []store.Row
	consume := func(page client.Page) error {
		got = append(got, page.Rows...)
		return nil
	}
	retry, err := c.WalkFrom(ctx, q, first.NextCursor, consume)
	if !errors.Is(err, io.ErrUnexpectedEOF) || retry != first.NextCursor || len(got) != 0 {
		t.Fatalf("interrupted walk: retry=%q rows=%v error=%v", retry, got, err)
	}
	next, err := c.WalkFrom(ctx, q, retry, consume)
	if err != nil || next != "" || len(got) != 3 || got[0] != rows[1] || got[1] != rows[2] || got[2] != rows[3] {
		t.Fatalf("retried walk: next=%q rows=%v error=%v", next, got, err)
	}
}
