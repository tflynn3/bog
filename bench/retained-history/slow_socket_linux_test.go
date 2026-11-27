package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestLinuxPausedResponseBackpressuresAndCancelsWriter(t *testing.T) {
	const token = "next"
	f := fixtures("live")[0]
	body, err := f.batch(1, 1+slowPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Bog-Next-Cursor", token)
		w.WriteHeader(http.StatusOK)
		// Match the candidate's per-record writes and default send buffer.
		for record := range bytes.Lines(body) {
			n, err := w.Write(record)
			if err != nil {
				written <- err
				return
			}
			if n != len(record) {
				written <- io.ErrShortWrite
				return
			}
		}
		written <- nil
	}))
	server.Config.WriteTimeout = 8 * time.Second
	server.Start()
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	probe := newService(server.URL)
	pause, err := openLinuxPaused(ctx, probe, os.Getpid(), "/query", "test", pageFraming{len(body), token})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pause.abort(); err != nil {
			t.Error(err)
		}
	}()
	if pressure, err := pause.pressure(ctx); err != nil || pressure.SendQueue == 0 {
		t.Fatalf("no TCP backpressure observed: %+v, %v", pressure, err)
	}
	select {
	case err := <-written:
		t.Fatalf("candidate write already completed: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-written:
		var networkErr net.Error
		if err == nil || !errors.As(err, &networkErr) || networkErr.Timeout() {
			t.Fatalf("writer must return a network error after the reset, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not exit after cancelling paused response")
	}
}
