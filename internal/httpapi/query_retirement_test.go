package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
)

func closeRetirementTestAPI(t *testing.T, a *API) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
		defer cancel()
		if err := a.Close(ctx); err != nil {
			t.Errorf("close API: %v", err)
		}
	})
}

func TestRetireQueryCaptureRejectsInvalidTokens(t *testing.T) {
	source := captureFixtureSource()
	a := NewWithPageSource(source, nil, 4096, 1, DefaultQueryWorkBudget)
	closeRetirementTestAPI(t, a)
	foreign := NewWithPageSource(captureFixtureSource(), nil, 4096, 1, DefaultQueryWorkBudget)
	closeRetirementTestAPI(t, foreign)

	token := issueCapture(t, a)
	foreignToken := issueCapture(t, foreign) // Same capture number, different signing key.
	tampered := []byte(token)
	tampered[16] = 'A'
	if tampered[16] == token[16] {
		tampered[16] = 'B'
	}
	for _, tc := range []struct {
		name, token string
	}{
		{"empty", ""},
		{"short", token[:len(token)-1]},
		{"invalid encoding", "!" + token[1:]},
		{"tampered", string(tampered)},
		{"foreign issuer", foreignToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := a.RetireQueryCapture(tc.token); !errors.Is(err, store.ErrInvalidQueryCursor) {
				t.Fatalf("retire error = %v, want invalid cursor", err)
			}
			if got := source.releases.Load(); got != 0 {
				t.Fatalf("invalid token caused %d releases", got)
			}
			assertRetirementTerminalPage(t, requestCapturePage(a, "&cursor="+token))
		})
	}
}

func assertRetirementTerminalPage(t *testing.T, out *httptest.ResponseRecorder) {
	t.Helper()
	want := jsonRow(t, store.Row{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "second"})
	if next := assertRetainedPage(t, out, want); next != "" {
		t.Fatalf("terminal page returned next cursor %q", next)
	}
}

func TestRetireQueryCaptureKeepsTerminalInputReplayableUntilRetired(t *testing.T) {
	source := captureFixtureSource()
	a := NewWithPageSource(source, nil, 4096, 1, DefaultQueryWorkBudget)
	closeRetirementTestAPI(t, a)
	token := issueCapture(t, a)
	path := "&cursor=" + token
	other := issueCapture(t, a)
	for range 2 {
		assertRetirementTerminalPage(t, requestCapturePage(a, path))
	}
	if got := source.releases.Load(); got != 0 {
		t.Fatalf("terminal requests caused %d releases", got)
	}
	for range 2 {
		if err := a.RetireQueryCapture(token); err != nil {
			t.Fatal(err)
		}
		assertQueryRejection(t, requestCapturePage(a, path), http.StatusGone, "query capture expired")
		if got := source.releases.Load(); got != 1 {
			t.Fatalf("retirement left %d releases, want 1", got)
		}
	}
	assertRetirementTerminalPage(t, requestCapturePage(a, "&cursor="+other))
}

func TestLegacyRetirementAuthenticatesExpiredCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		newLegacy := func() *API {
			a := NewWithQueryLimits(captureFixtureSource().store, nil, 4096, 1, DefaultQueryWorkBudget).(*API)
			closeRetirementTestAPI(t, a)
			return a
		}
		a, foreign := newLegacy(), newLegacy()
		token := issueCapture(t, a)
		foreignToken := issueCapture(t, foreign)
		path := "&cursor=" + token
		assertRetirementTerminalPage(t, requestCapturePage(a, path))
		time.Sleep(queryCaptureLifetime)
		synctest.Wait()

		assertQueryRejection(t, requestCapturePage(a, path), http.StatusGone, "query capture expired")
		assertQueryRejection(t, requestCapturePage(a, path+"&contains=wrong"), http.StatusBadRequest, "invalid cursor")
		tampered := []byte(token)
		tampered[len(tampered)/2] = 'A'
		if string(tampered) == token {
			tampered[len(tampered)/2] = 'B'
		}
		for _, invalid := range []string{foreignToken, string(tampered), token + "A"} {
			if err := a.RetireQueryCapture(invalid); !errors.Is(err, store.ErrInvalidQueryCursor) {
				t.Fatalf("retiring invalid cursor returned %v", err)
			}
		}
		for range 2 {
			if err := a.RetireQueryCapture(token); err != nil {
				t.Fatalf("retire expired cursor: %v", err)
			}
		}
	})
}

func TestRetireQueryCaptureDrainsAdmittedResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &blockingReplaySource{
			trackedPageSource: captureFixtureSource(),
			entered:           make(chan struct{}),
			proceed:           make(chan struct{}),
		}
		a := NewWithPageSource(source, nil, 4096, 2, DefaultQueryWorkBudget)
		closeRetirementTestAPI(t, a)
		token := issueCapture(t, a)
		path := "&cursor=" + token
		for range maxQueryCaptures - 1 {
			issueCapture(t, a)
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		source.block.Store(true)
		result := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			out := httptest.NewRecorder()
			r := httptest.NewRequestWithContext(ctx, http.MethodGet, queryBounds+path, nil)
			a.ServeHTTP(out, r)
			result <- out
		}()
		waitForAdmission(t, source.entered, "admitted resume")
		for range 2 {
			if err := a.RetireQueryCapture(token); err != nil {
				t.Fatal(err)
			}
		}
		assertQueryRejection(t, requestCapturePage(a, path), http.StatusGone, "query capture expired")
		assertQueryRejection(t, requestCapturePage(a, "&page=1"), http.StatusServiceUnavailable, "query capacity exhausted")
		if got := source.releases.Load(); got != 0 {
			t.Fatalf("retirement caused %d releases during admitted resume", got)
		}

		close(source.proceed)
		assertRetirementTerminalPage(t, waitForAdmission(t, result, "admitted resume to finish"))
		if got := source.releases.Load(); got != 1 {
			t.Fatalf("resume drain left %d releases, want 1", got)
		}
		issueCapture(t, a)
	})
}

func TestRetireQueryCaptureReturnsIdleReleaseError(t *testing.T) {
	source := captureFixtureSource()
	source.closeErr = errors.New("release failed")
	a := NewWithPageSource(source, nil, 4096, 1, DefaultQueryWorkBudget)
	closeRetirementTestAPI(t, a)
	token := issueCapture(t, a)
	if err := a.RetireQueryCapture(token); !errors.Is(err, source.closeErr) {
		t.Fatalf("retirement error = %v, want %v", err, source.closeErr)
	}
	if err := a.RetireQueryCapture(token); err != nil {
		t.Fatalf("repeat retirement: %v", err)
	}
	assertQueryRejection(t, requestCapturePage(a, "&cursor="+token), http.StatusGone, "query capture expired")
	if got := source.releases.Load(); got != 1 {
		t.Fatalf("retirement left %d releases, want 1", got)
	}
}
