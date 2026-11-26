package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
