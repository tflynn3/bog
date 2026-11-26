package client

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWalkCursorHistory(t *testing.T) {
	third := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("c", 33)))
	for _, tc := range []struct {
		name   string
		path   []string // Request cursors followed by the last response's next cursor.
		cyclic bool
	}{
		{"fresh walk cycles to first token", []string{"", fixtureCursor, fixtureNext, fixtureCursor}, true},
		{"resumed walk cycles to starting token", []string{fixtureCursor, fixtureNext, fixtureCursor}, true},
		{"resumed walk cycles to intermediate token", []string{third, fixtureCursor, fixtureNext, fixtureCursor}, true},
		{"resumed walk reaches terminal page", []string{fixtureCursor, fixtureNext, third, ""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				cursor := r.URL.Query().Get("cursor")
				index := slices.Index(tc.path[:len(tc.path)-1], cursor)
				if index < 0 || (cursor == "" && r.URL.Query().Get("page") != "1") {
					t.Errorf("unexpected request %s", r.RequestURI)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				writeContractPage(t, w, contractRow("input:"+cursor), tc.path[index+1])
			}))
			t.Cleanup(server.Close)
			c, err := New(server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.CloseIdleConnections)
			pageCount := len(tc.path) - 1
			wantDelivered := tc.path[1:]
			if tc.cyclic {
				wantDelivered = wantDelivered[:len(wantDelivered)-1]
			}

			for _, name := range []string{"first walk", "another walk on the same client"} {
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					var delivered []string
					cursor, err := c.WalkFrom(ctx, Query{}, tc.path[0], func(p Page) error {
						delivered = append(delivered, p.NextCursor)
						if len(delivered) > len(wantDelivered) {
							return errors.New("consumer received an extra page")
						}
						wantLine := "input:" + tc.path[len(delivered)-1]
						if len(p.Rows) != 1 || p.Rows[0].Line != wantLine {
							t.Errorf("page %d rows %v, want %q", len(delivered), p.Rows, wantLine)
						}
						return nil
					})
					if tc.cyclic {
						var protocol *HTTPError
						if cursor != tc.path[pageCount-1] || !errors.As(err, &protocol) ||
							protocol.StatusCode != http.StatusOK || protocol.Retryable() || protocol.CursorRejected ||
							!strings.Contains(err.Error(), "cursor cycle") {
							t.Fatalf("walk returned cursor %q and error %v", cursor, err)
						}
					} else if cursor != "" || err != nil {
						t.Fatalf("terminal walk returned cursor %q and error %v", cursor, err)
					}
					if !slices.Equal(delivered, wantDelivered) {
						t.Fatalf("delivered cursors %q, want %q", delivered, wantDelivered)
					}
				})
			}
			t.Cleanup(func() {
				if got := requests.Load(); int(got) != 2*pageCount {
					t.Errorf("made %d requests, want %d", got, 2*pageCount)
				}
			})
		})
	}
}

func TestWalkCancellationPreservesContinuation(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Query().Get("cursor") {
		case "":
			writeContractPage(t, w, contractRow("first"), fixtureCursor)
		case fixtureCursor:
			writeContractPage(t, w, contractRow("last"), "")
		default:
			t.Errorf("unexpected request %s", r.RequestURI)
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	c, err := New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	walkCtx, cancelWalk := context.WithCancel(ctx)
	defer cancelWalk()
	var delivered []string
	cursor, err := c.Walk(walkCtx, Query{}, func(p Page) error {
		delivered = append(delivered, p.NextCursor)
		cancelWalk()
		return nil
	})
	if cursor != fixtureCursor || !errors.Is(err, context.Canceled) ||
		!slices.Equal(delivered, []string{fixtureCursor}) || requests.Load() != 1 {
		t.Fatalf("canceled walk returned cursor %q and error %v after delivering %q in %d requests",
			cursor, err, delivered, requests.Load())
	}
	end, err := c.WalkFrom(ctx, Query{}, cursor, func(p Page) error {
		delivered = append(delivered, p.NextCursor)
		return nil
	})
	if end != "" || err != nil || !slices.Equal(delivered, []string{fixtureCursor, ""}) || requests.Load() != 2 {
		t.Fatalf("resumed walk returned cursor %q and error %v after delivering %q in %d requests",
			end, err, delivered, requests.Load())
	}
}

func TestCloseIdleConnections(t *testing.T) {
	active, release := make(chan struct{}), make(chan struct{})
	closed := make(chan string, 4)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("contains") == "active" {
			close(active)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		writeContractPage(t, w, contractRow(r.RemoteAddr), "")
	}))
	server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- conn.RemoteAddr().String()
		}
	}
	server.Start()
	defer server.Close()
	c, err := New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	activeResult := make(chan error, 1)
	go func() {
		_, err := c.FetchPage(ctx, Query{Contains: "active"}, "")
		activeResult <- err
	}()
	select {
	case <-active:
	case err := <-activeResult:
		t.Fatalf("active request returned early: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	fetchAddr := func() string {
		t.Helper()
		p, err := c.FetchPage(ctx, Query{}, "")
		if err != nil || len(p.Rows) != 1 {
			t.Fatalf("fetch page: %+v, %v", p, err)
		}
		return p.Rows[0].Line
	}
	idleAddr := fetchAddr()
	if reused := fetchAddr(); reused != idleAddr {
		t.Fatalf("request used %s instead of reusing %s", reused, idleAddr)
	}
	c.CloseIdleConnections()
	c.CloseIdleConnections()
	select {
	case addr := <-closed:
		if addr != idleAddr {
			t.Fatalf("closed connection %s, want idle connection %s", addr, idleAddr)
		}
	case <-ctx.Done():
		t.Fatal("idle connection stayed open")
	}
	if addr := fetchAddr(); addr == idleAddr {
		t.Fatalf("request reused closed connection %s", addr)
	}
	close(release)
	select {
	case err := <-activeResult:
		if err != nil {
			t.Fatalf("active request failed during idle cleanup: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("active request failed to finish")
	}
}
