package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestContextBoundedWalkStopsAtCancellation(t *testing.T) {
	t.Parallel()

	for _, phase := range []struct {
		name       string
		status     int
		closeFails bool
	}{
		{name: "headers"},
		{name: "page body", status: http.StatusOK},
		{name: "page body and failed close", status: http.StatusOK, closeFails: true},
		{name: "diagnostic body and failed close", status: http.StatusServiceUnavailable, closeFails: true},
		{name: "retry backoff", status: http.StatusServiceUnavailable},
	} {
		for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(phase.name+"/"+want.Error(), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					const limit = 10 * time.Second
					start := time.Now()
					stop := errors.New("stop this query walk")
					var ctx context.Context
					var cancel context.CancelFunc
					if want == context.DeadlineExceeded {
						ctx, cancel = context.WithTimeoutCause(t.Context(), limit, stop)
					} else {
						var cancelCause context.CancelCauseFunc
						ctx, cancelCause = context.WithCancelCause(t.Context())
						cancel = func() { cancelCause(stop) }
					}
					defer cancel()

					c, err := New("http://bog.test", nil)
					if err != nil {
						t.Fatal(err)
					}
					var requests, closed int
					phaseReady := make(chan struct{})
					closeFailure := errors.New("response body close failed")
					c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
						requests++
						if req.Context() != ctx {
							t.Error("request does not use the original walk context")
						}
						params := req.URL.Query()
						switch requests {
						case 1:
							if params.Get("page") != "1" || params.Has("cursor") {
								t.Errorf("first request = %s", req.URL)
							}
							time.Sleep(3 * time.Second)
							resp := retryResponse(http.StatusOK, contractRow("accepted"), 0, nil, &closed)
							resp.Header.Set(nextCursorHeader, fixtureCursor)
							return resp, nil
						case 2:
							if params.Get("cursor") != fixtureCursor || params.Has("page") {
								t.Errorf("continuation request = %s", req.URL)
							}
						default:
							t.Errorf("unexpected request after cancellation: %s", req.URL)
							return nil, errors.New("unexpected request")
						}

						switch phase.name {
						case "headers":
							close(phaseReady)
							<-req.Context().Done()
							return nil, context.Cause(req.Context())
						case "retry backoff":
							resp := retryResponse(phase.status, "query capacity exhausted\n", 0, nil, &closed)
							body := resp.Body
							resp.Body = queryResponseBody{body, func() error {
								err := body.Close()
								close(phaseReady)
								return err
							}}
							return resp, nil
						default:
							prefix := contractRow("unaccepted")
							resp := retryResponse(phase.status, prefix, int64(len(prefix)*2), nil, &closed)
							resp.Header.Set(nextCursorHeader, fixtureNext)
							reader := strings.NewReader(prefix)
							resp.Body = queryResponseBody{queryReadFunc(func(dst []byte) (int, error) {
								if reader.Len() > 0 {
									return reader.Read(dst)
								}
								close(phaseReady)
								<-req.Context().Done()
								return 0, context.Cause(req.Context())
							}), func() error {
								closed++
								if phase.closeFails {
									return closeFailure
								}
								return nil
							}}
							return resp, nil
						}
					})

					walk, err := c.NewBoundedWalkWithContext(ctx, Query{Limit: 2}, 3)
					if err != nil || requests != 0 {
						t.Fatalf("construct walk: %v, requests = %d", err, requests)
					}
					first, err := walk.NextPage()
					if err != nil || first.NextCursor != fixtureCursor || len(first.Rows) != 1 || first.Rows[0].Line != "accepted" {
						t.Fatalf("first page = %+v, error = %v", first, err)
					}
					type result struct {
						page Page
						err  error
					}
					done := make(chan result, 1)
					policy := PageRetry{MaxRetries: 3, Delay: time.Hour}
					go func() {
						page, err := walk.NextPageWithRetry(policy)
						done <- result{page, err}
					}()
					synctest.Wait()
					// At this point the continuation is blocked in the target
					// phase. For backoff the 503 has already been closed.
					select {
					case <-phaseReady:
					default:
						t.Error("continuation did not reach the cancellation phase")
					}
					if len(done) != 0 {
						t.Error("continuation returned before cancellation")
					}
					cancelAt := time.Now()
					if want == context.Canceled {
						cancel()
					} else {
						cancelAt = start.Add(limit)
					}
					got := <-done
					if !time.Now().Equal(cancelAt) {
						t.Errorf("fetch returned at %v, want %v", time.Now(), cancelAt)
					}
					assertCanceledWalkPage(t, got.page, got.err, want, stop)
					wantInterrupted := phase.status == http.StatusOK && phase.closeFails
					if errors.Is(got.err, ErrPageInterrupted) != wantInterrupted || errors.Is(got.err, ErrPageRetryExhausted) {
						t.Errorf("error = %v, want interrupted = %t and no retry exhaustion", got.err, wantInterrupted)
					}
					if errors.Is(got.err, closeFailure) != phase.closeFails {
						t.Errorf("error = %v, want close failure = %t", got.err, phase.closeFails)
					}
					wantClosed := 1
					if phase.status != 0 {
						wantClosed++
						var status *HTTPError
						if phase.name != "retry backoff" && (!errors.As(got.err, &status) || status.StatusCode != phase.status) {
							t.Errorf("error = %v, want HTTP %d", got.err, phase.status)
						}
					}
					if closed != wantClosed {
						t.Errorf("closed %d bodies by return, want %d", closed, wantClosed)
					}
					assertWalkProgress(t, walk, 1, fixtureCursor)
					page, err := walk.NextPage()
					assertCanceledWalkPage(t, page, err, want, stop)
					page, err = walk.NextPageWithRetry(policy)
					assertCanceledWalkPage(t, page, err, want, stop)
					assertWalkProgress(t, walk, 1, fixtureCursor)
					if requests != 2 {
						t.Errorf("requests = %d, want 2", requests)
					}
				})
			})
		}
	}
}

func assertCanceledWalkPage(t *testing.T, page Page, err, want, cause error) {
	t.Helper()
	if page.Rows != nil || page.NextCursor != "" || !errors.Is(err, want) || !errors.Is(err, cause) {
		t.Fatalf("page = %+v, error = %v, want zero page and errors matching %v and %v", page, err, want, cause)
	}
}

func assertWalkProgress(t *testing.T, walk *ContextBoundedWalk, pages int, cursor string) {
	t.Helper()
	w := walk.walk
	if w.pages != pages || w.cursor != cursor || w.terminal ||
		(pages == 0 && (w.anchor != "" || w.window != 1 || w.distance != 0)) ||
		(pages == 1 && (w.anchor != cursor || w.window != 2 || w.distance != 0)) {
		t.Fatalf("walk advanced after rejected page: %+v", w)
	}
}

func TestContextBoundedWalkDeadlineBetweenCalls(t *testing.T) {
	t.Parallel()
	for _, firstPage := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted first page=%t", firstPage), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				c, err := New("http://bog.test", nil)
				if err != nil {
					t.Fatal(err)
				}
				var requests, closes int
				c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
					requests++
					if req.Context().Err() != nil {
						t.Errorf("dispatched request with expired context: %s", req.URL)
					}
					resp := retryResponse(http.StatusOK, contractRow("first"), 0, nil, &closes)
					resp.Header.Set(nextCursorHeader, fixtureCursor)
					return resp, nil
				})
				// The first page, if read, consumes the full page budget.
				walk, err := c.NewBoundedWalkWithContext(ctx, Query{}, 1)
				if err != nil || requests != 0 {
					t.Fatalf("construct walk: %v, requests = %d", err, requests)
				}
				pages, cursor := 0, ""
				if firstPage {
					page, err := walk.NextPage()
					if err != nil || page.NextCursor != fixtureCursor {
						t.Fatalf("first page = %+v, error = %v", page, err)
					}
					pages, cursor = 1, fixtureCursor
				}
				<-ctx.Done()
				page, err := walk.NextPage()
				assertCanceledWalkPage(t, page, err, context.DeadlineExceeded, context.DeadlineExceeded)
				page, err = walk.NextPageWithRetry(PageRetry{MaxRetries: 1})
				assertCanceledWalkPage(t, page, err, context.DeadlineExceeded, context.DeadlineExceeded)
				assertWalkProgress(t, walk, pages, cursor)
				if requests != pages || closes != pages {
					t.Errorf("requests = %d, closed bodies = %d, want %d each", requests, closes, pages)
				}
			})
		})
	}
}

func TestContextBoundedWalkRetriesAndCompletes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		c, err := New("http://bog.test", nil)
		if err != nil {
			t.Fatal(err)
		}
		var targets []string
		var attempts []time.Duration
		var closed int
		c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
			if req.Context() != ctx {
				t.Error("request context changed during walk")
			}
			if req.URL.Query().Get("limit") != "1" || req.URL.Query().Get("contains") != "match" {
				t.Errorf("query changed: %s", req.URL)
			}
			targets = append(targets, req.URL.RawQuery)
			attempts = append(attempts, time.Since(start))
			switch len(attempts) {
			case 1:
				resp := retryResponse(http.StatusOK, contractRow("match first"), 0, nil, &closed)
				resp.Header.Set(nextCursorHeader, fixtureCursor)
				return resp, nil
			case 2:
				return retryResponse(http.StatusServiceUnavailable, "query capacity exhausted\n", 0, nil, &closed), nil
			case 3:
				body := contractRow("match partial")
				return retryResponse(http.StatusOK, body, int64(len(body)+1), nil, &closed), nil
			case 4:
				return retryResponse(http.StatusOK, contractRow("match last"), 0, nil, &closed), nil
			default:
				t.Errorf("unexpected extra request: %s", req.URL)
				return nil, errors.New("unexpected request")
			}
		})
		walk, err := c.NewBoundedWalkWithContext(ctx, Query{Limit: 1, Contains: "match"}, 2)
		if err != nil {
			t.Fatal(err)
		}
		page, err := walk.NextPage()
		if err != nil || len(page.Rows) != 1 || page.Rows[0].Line != "match first" || page.NextCursor != fixtureCursor {
			t.Fatalf("first page = %+v, error = %v", page, err)
		}
		time.Sleep(time.Second)
		retry := PageRetry{MaxRetries: 2, Delay: 2 * time.Second}
		page, err = walk.NextPageWithRetry(retry)
		if err != nil || len(page.Rows) != 1 || page.Rows[0].Line != "match last" || page.NextCursor != "" {
			t.Fatalf("terminal page = %+v, error = %v", page, err)
		}
		if _, err := walk.NextPageWithRetry(retry); !errors.Is(err, io.EOF) {
			t.Errorf("after terminal page: %v, want EOF", err)
		}
		cancel()
		if _, err := walk.NextPage(); !errors.Is(err, io.EOF) {
			t.Errorf("after completed walk and cancellation: %v, want EOF", err)
		}
		if !slices.Equal(attempts, []time.Duration{0, time.Second, 3 * time.Second, 5 * time.Second}) || closed != 4 {
			t.Errorf("request times = %v, bodies closed = %d", attempts, closed)
		}
		if len(targets) != 4 || !strings.Contains(targets[0], "page=1") || !strings.Contains(targets[1], "cursor="+fixtureCursor) ||
			targets[1] != targets[2] || targets[2] != targets[3] {
			t.Errorf("request targets = %v", targets)
		}
	})
}

func TestContextBoundedWalkOptions(t *testing.T) {
	t.Parallel()
	c, err := New("http://bog.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
		t.Errorf("unexpected request: %s", r.URL)
		return nil, errors.New("unexpected request")
	})
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		maxPages int
	}{
		{name: "nil context", maxPages: 1},
		{name: "zero budget", ctx: t.Context()},
		{name: "negative budget", ctx: t.Context(), maxPages: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if walk, err := c.NewBoundedWalkWithContext(tc.ctx, Query{}, tc.maxPages); walk != nil || err == nil {
				t.Errorf("constructor returned walk = %+v, error = %v", walk, err)
			}
		})
	}
	walk, err := c.NewBoundedWalkWithContext(t.Context(), Query{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, retry := range []PageRetry{{MaxRetries: -1}, {Delay: -time.Second}} {
		if page, err := walk.NextPageWithRetry(retry); page.Rows != nil || page.NextCursor != "" || err == nil {
			t.Errorf("retry %+v returned page = %+v, error = %v", retry, page, err)
		}
	}
	assertWalkProgress(t, walk, 0, "")
}

func TestContextBoundedWalkClosesStalledHTTPBody(t *testing.T) {
	t.Parallel()
	guard, stopGuard := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopGuard()
	ctx, cancel := context.WithCancelCause(guard)
	defer cancel(nil)
	cause := errors.New("stop stalled walk")
	var requests, closes atomic.Int32
	handlerDone := make(chan struct{})
	prefix := contractRow("incomplete")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch n := requests.Add(1); n {
		case 1:
			writeContractPage(t, w, contractRow("first"), fixtureCursor)
		case 2:
			defer close(handlerDone)
			if r.URL.Query().Get("cursor") != fixtureCursor {
				t.Errorf("continuation = %s", r.URL)
			}
			w.Header().Set("Content-Length", strconv.Itoa(2*len(prefix)))
			w.Header().Set(nextCursorHeader, fixtureNext)
			if n, err := io.WriteString(w, prefix); err != nil || n != len(prefix) {
				t.Errorf("write body prefix: %d of %d bytes, %v", n, len(prefix), err)
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Errorf("flush body prefix: %v", err)
				return
			}
			select {
			case <-r.Context().Done():
			case <-guard.Done():
				t.Error("stalled response stayed open after cancellation")
			}
		default:
			t.Errorf("unexpected request %d: %s", n, r.URL)
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	defer cancel(nil)

	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	c, err := New(server.URL, transport)
	if err != nil {
		t.Fatal(err)
	}
	c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
		resp, err := transport.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		body := resp.Body
		resp.Body = queryResponseBody{queryReadFunc(func(dst []byte) (int, error) {
			n, err := body.Read(dst)
			if n > 0 && req.URL.Query().Has("cursor") {
				cancel(cause)
			}
			return n, err
		}), func() error {
			closes.Add(1)
			return body.Close()
		}}
		return resp, nil
	})

	walk, err := c.NewBoundedWalkWithContext(ctx, Query{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	first, err := walk.NextPage()
	if err != nil || first.NextCursor != fixtureCursor {
		t.Fatalf("first page = %+v, error = %v", first, err)
	}
	page, err := walk.NextPageWithRetry(PageRetry{MaxRetries: 2, Delay: time.Hour})
	assertCanceledWalkPage(t, page, err, context.Canceled, cause)
	assertWalkProgress(t, walk, 1, fixtureCursor)
	if closes.Load() != 2 {
		t.Errorf("closed bodies = %d, want 2", closes.Load())
	}
	select {
	case <-handlerDone:
	case <-guard.Done():
		t.Fatal("continuation handler did not exit")
	}
	page, err = walk.NextPage()
	assertCanceledWalkPage(t, page, err, context.Canceled, cause)
	if requests.Load() != 2 {
		t.Errorf("requests = %d, want 2", requests.Load())
	}
}

func TestContextBoundedWalkRejectsPageCanceledAtClose(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c, err := New("http://bog.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	var requests, closes int
	c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
		requests++
		resp := retryResponse(http.StatusOK, contractRow("first"), 0, nil, &closes)
		resp.Header.Set(nextCursorHeader, fixtureCursor)
		body := resp.Body
		resp.Body = queryResponseBody{body, func() error {
			cancel()
			return body.Close()
		}}
		return resp, nil
	})
	walk, err := c.NewBoundedWalkWithContext(ctx, Query{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	page, err := walk.NextPage()
	assertCanceledWalkPage(t, page, err, context.Canceled, context.Canceled)
	page, err = walk.NextPageWithRetry(PageRetry{MaxRetries: 1})
	assertCanceledWalkPage(t, page, err, context.Canceled, context.Canceled)
	assertWalkProgress(t, walk, 0, "")
	if requests != 1 || closes != 1 {
		t.Errorf("requests = %d, closed bodies = %d, want 1 each", requests, closes)
	}
}
