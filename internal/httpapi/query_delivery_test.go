package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
)

type queryBackend struct {
	name   string
	newAPI func(*testing.T, *store.Store, int, uint64) *API
}

var queryBackends = []queryBackend{
	{"legacy", func(t *testing.T, s *store.Store, bytes int, work uint64) *API {
		t.Helper()
		handler := NewWithQueryLimits(s, nil, bytes, 2, work)
		a, ok := handler.(*API)
		if !ok {
			t.Fatalf("legacy handler %T does not expose query lifecycle controls", handler)
		}
		return a
	}},
	{"retained", func(_ *testing.T, s *store.Store, bytes int, work uint64) *API {
		return NewWithPageSource(&legacyPageSource{store: s}, nil, bytes, 2, work)
	}},
}

type openingWriter struct {
	*httptest.ResponseRecorder
	flush    func() error
	writeErr error
	writes   int
}

func (w *openingWriter) FlushError() error { return w.flush() }

func (w *openingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.ResponseRecorder.Write(p)
}

func TestOpeningCaptureDeliveryOutcome(t *testing.T) {
	for _, backend := range queryBackends {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				flushErr error
				writes   int
				live     bool
			}{
				{"confirmed headers unsent", errors.Join(ErrQueryHeadersUnsent, io.ErrClosedPipe), 0, false},
				{"ambiguous header failure", io.ErrClosedPipe, 0, true},
				{"failed body after header flush", nil, 1, true},
				{"failed body without flush support", http.ErrNotSupported, 1, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					a := backend.newAPI(t, captureFixtureSource().store, 4096, DefaultQueryWorkBudget)
					closeRetirementTestAPI(t, a)
					w := &openingWriter{
						ResponseRecorder: httptest.NewRecorder(), writeErr: io.ErrShortWrite,
						flush: func() error { return tc.flushErr },
					}
					a.ServeHTTP(w, httptest.NewRequest(http.MethodGet, queryBounds+"&page=1&limit=1", nil))
					token := w.Header().Get("Bog-Next-Cursor")
					if w.writes != tc.writes || token == "" {
						t.Fatalf("opening wrote %d records, want %d, cursor present=%t", w.writes, tc.writes, token != "")
					}
					for range 2 {
						out := requestCapturePage(a, "&cursor="+token)
						if tc.live {
							assertRetirementTerminalPage(t, out)
						} else {
							assertQueryRejection(t, out, http.StatusGone, "query capture expired")
						}
					}
				})
			}
		})
	}
}

func TestOpeningDeliveryRacesRetirementAndClose(t *testing.T) {
	for _, backend := range queryBackends {
		t.Run(backend.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := backend.newAPI(t, captureFixtureSource().store, 4096, DefaultQueryWorkBudget)
				closeRetirementTestAPI(t, a)
				header := make(chan string, 1)
				proceed, done := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(proceed) })
				defer unblock()
				w := &openingWriter{ResponseRecorder: httptest.NewRecorder()}
				w.flush = func() error {
					header <- w.Header().Get("Bog-Next-Cursor")
					<-proceed
					return nil
				}
				go func() {
					defer close(done)
					a.ServeHTTP(w, httptest.NewRequest(http.MethodGet, queryBounds+"&page=1&limit=1", nil))
				}()
				token := waitForAdmission(t, header, "opening headers")
				if token == "" {
					t.Fatal("opening omitted its cursor")
				}
				retired, closed := make(chan error, 1), make(chan error, 1)
				go func() { retired <- a.RetireQueryCapture(token) }()
				go func() {
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					closed <- a.Close(ctx)
				}()
				if err := waitForAdmission(t, retired, "retirement"); err != nil {
					t.Fatal(err)
				}
				if err := waitForAdmission(t, closed, "close deadline"); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("close during opening delivery: %v", err)
				}
				assertQueryRejection(t, requestCapturePage(a, "&cursor="+token), http.StatusGone, "query capture expired")
				assertQueryRejection(t, requestCapturePage(a, "&page=1"), http.StatusServiceUnavailable, "query capacity exhausted")
				unblock()
				waitForAdmission(t, done, "opening to finish delivery")
				first := store.Row{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "first"}
				assertRetainedPage(t, w.ResponseRecorder, jsonRow(t, first))
				if err := a.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := a.RetireQueryCapture(token); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

type blockingReleaseSource struct {
	*trackedPageSource
	entered chan struct{}
	proceed chan struct{}
}

func (s *blockingReleaseSource) Release(id string) error {
	close(s.entered)
	<-s.proceed
	return s.trackedPageSource.Release(id)
}

func TestCloseDeadlineCoversPendingSourceRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &blockingReleaseSource{
			trackedPageSource: captureFixtureSource(),
			entered:           make(chan struct{}),
			proceed:           make(chan struct{}),
		}
		source.closeErr = errors.New("release failed")
		a := NewWithPageSource(source, nil, 4096, 2, DefaultQueryWorkBudget)
		token := issueCapture(t, a)
		unblock := sync.OnceFunc(func() { close(source.proceed) })
		defer unblock()
		retired := make(chan error, 1)
		go func() { retired <- a.RetireQueryCapture(token) }()
		waitForAdmission(t, source.entered, "source release")

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close during source release: %v", err)
		}
		assertQueryRejection(t, requestCapturePage(a, "&cursor="+token), http.StatusGone, "query capture expired")
		if err := a.RetireQueryCapture(token); err != nil {
			t.Fatalf("repeat retirement during source release: %v", err)
		}
		unblock()
		if err := waitForAdmission(t, retired, "retirement to return"); !errors.Is(err, source.closeErr) {
			t.Fatalf("retirement release error: %v", err)
		}
		if err := a.Close(t.Context()); !errors.Is(err, source.closeErr) {
			t.Fatalf("close release error: %v", err)
		}
		if got := source.releases.Load(); got != 1 {
			t.Fatalf("released %d captures, want one", got)
		}
	})
}
