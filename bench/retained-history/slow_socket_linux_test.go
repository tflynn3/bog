package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLinuxPausedResponseBackpressuresAndCancelsWriter(t *testing.T) {
	const token = "next"
	body := strings.Repeat("x", 1<<20)
	written := make(chan error, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Bog-Next-Cursor", token)
		n, err := io.WriteString(w, body)
		if n < len(body) && err == nil {
			err = io.ErrShortWrite
		}
		written <- err
	}))
	server.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		if err := c.(*net.TCPConn).SetWriteBuffer(4096); err != nil {
			t.Error(err)
		}
		return ctx
	}
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
