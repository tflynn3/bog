package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestBackpressuredContinuationStalledPeer(t *testing.T) {
	t.Parallel()
	rejected := errors.New("reject partial response")
	for _, tc := range []struct {
		name     string
		sendBody bool
		reject   bool
	}{
		{name: "cancel-before-headers"},
		{name: "cancel-during-body", sendBody: true},
		{name: "close-with-unread-body", sendBody: true, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := ln.Close(); err != nil {
					t.Error(err)
				}
			})
			deadline := time.Now().Add(10 * time.Second)
			if err := ln.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			bodyStarted := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				result <- withBackpressuredContinuation(ctx, deadline, "http://"+ln.Addr().String()+"/query", func(_ *net.TCPConn, res *http.Response) error {
					if !tc.sendBody {
						close(bodyStarted)
						return errors.New("callback ran without a response")
					}
					if res.StatusCode != http.StatusOK || res.ContentLength != 2 {
						return fmt.Errorf("unexpected response: HTTP %d, Content-Length %d", res.StatusCode, res.ContentLength)
					}
					var b [1]byte
					if _, err := io.ReadFull(res.Body, b[:]); err != nil {
						return err
					}
					if b[0] != 'x' {
						return fmt.Errorf("first response byte = %q, want x", b[0])
					}
					close(bodyStarted)
					if tc.reject {
						return rejected
					}
					_, err := io.ReadFull(res.Body, b[:])
					return err
				})
			}()
			peer, err := ln.AcceptTCP()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := peer.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := peer.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(peer)
			req, err := http.ReadRequest(reader)
			if err != nil {
				t.Fatal(err)
			}
			if err := req.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if req.Method != http.MethodGet || req.URL.Path != "/query" || !req.Close {
				t.Fatalf("unexpected request: %s %s, close=%t", req.Method, req.URL, req.Close)
			}
			if tc.sendBody {
				if _, err := io.WriteString(peer, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nx"); err != nil {
					t.Fatal(err)
				}
				select {
				case <-bodyStarted:
				case <-time.After(2 * time.Second):
					t.Fatal("continuation did not read the first byte")
				}
			}
			want := rejected
			if !tc.reject {
				want = context.Canceled
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) || (!tc.reject && !errors.Is(err, net.ErrClosed)) {
					t.Fatalf("continuation returned %v, want %v with raw socket closure on cancellation", err, want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("continuation did not return while the peer was stalled")
			}
			if !tc.sendBody {
				select {
				case <-bodyStarted:
					t.Fatal("callback ran before the peer sent headers")
				default:
				}
			}
			if _, err := reader.ReadByte(); err != io.EOF {
				t.Fatalf("peer read after continuation returned = %v, want EOF", err)
			}
		})
	}
}
