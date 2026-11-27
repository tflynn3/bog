package httpapi

import (
	"context"
	"errors"
	"io"
	"math/bits"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
)

const planIntBytes = bits.UintSize / 8

func usedQueryBytes(b *queryByteBudget) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func assertLegacyPlanUsage(t *testing.T, a *API, bytes uint64, captures int) {
	t.Helper()
	if got := usedQueryBytes(&a.initResources().scan); got != bytes {
		t.Errorf("scan bytes in use = %d, want %d", got, bytes)
	}
	a.captures.mu.Lock()
	live := len(a.captures.live)
	a.captures.mu.Unlock()
	a.legacy.mu.Lock()
	views := len(a.legacy.views)
	a.legacy.mu.Unlock()
	if live != captures || views != captures {
		t.Errorf("live captures = %d, Store views = %d, want %d of each", live, views, captures)
	}
}

func legacyPlanStore(count int) (*store.Store, store.Row) {
	row := store.Row{TS: time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC), Line: "x"}
	s := new(store.Store)
	s.Publish(slices.Repeat([]store.Row{row}, count))
	return s, row
}

func newLegacyPlanTestAPI(t *testing.T, s *store.Store, scanBytes uint64) *API {
	t.Helper()
	limits := DefaultQueryResourceLimits()
	limits.ScanBytes = scanBytes
	a := NewWithQueryResources(s, nil, 4096, 4, DefaultQueryWorkBudget, limits)
	closeRetirementTestAPI(t, a)
	return a
}

func TestLegacyPlanSelectiveOpeningBesideLargeIdleCursor(t *testing.T) {
	const count = 400_000
	s, row := legacyPlanStore(count)
	a := NewWithQueryLimits(s, nil, 4096, 4, DefaultQueryWorkBudget).(*API)
	closeRetirementTestAPI(t, a)

	token := issueCapture(t, a)
	denseBytes := uint64(legacyPlanCaptureBytes + 2*count*planIntBytes)
	assertLegacyPlanUsage(t, a, denseBytes, 1)
	for _, absent := range []string{"y", "needle"} {
		out := requestCapturePage(a, "&page=1&limit=1&contains="+absent)
		if next := assertRetainedPage(t, out, ""); next != "" {
			t.Fatalf("zero matches issued cursor %q", next)
		}
		assertLegacyPlanUsage(t, a, denseBytes, 1)
	}

	first := store.Row{TS: row.TS, Line: "needle first"}
	second := store.Row{TS: row.TS, Line: "needle second"}
	s.Publish([]store.Row{first, second, second})
	open := requestCapturePage(a, "&page=1&limit=1&contains=needle")
	sparse := assertRetainedPage(t, open, jsonRow(t, first))
	if sparse == "" {
		t.Fatal("sparse walk omitted its cursor")
	}
	// Three ordinals in four slots, plus three exact encoded sizes.
	assertLegacyPlanUsage(t, a, denseBytes+legacyPlanCaptureBytes+7*planIntBytes, 2)
	path := "&cursor=" + sparse + "&contains=needle"
	for range 2 {
		out := requestCapturePage(a, path)
		if next := assertRetainedPage(t, out, jsonRow(t, second)+jsonRow(t, second)); next != "" {
			t.Fatalf("terminal retry issued cursor %q", next)
		}
	}
	if err := a.RetireQueryCapture(sparse); err != nil {
		t.Fatal(err)
	}
	assertLegacyPlanUsage(t, a, denseBytes, 1)

	out := requestCapturePage(a, "&cursor="+token+"&limit=1")
	if next := assertRetainedPage(t, out, jsonRow(t, row)); next == "" {
		t.Fatal("large cursor lost its continuation")
	}
	if err := a.RetireQueryCapture(token); err != nil {
		t.Fatal(err)
	}
	assertLegacyPlanUsage(t, a, 0, 0)
}

// Query work polls the request context before visiting each row. This lets a
// test pause a scan after an observed allocation with no production hook.
type planScanContext struct {
	context.Context
	poll func()
}

func (c planScanContext) Err() error {
	c.poll()
	return c.Context.Err()
}

func TestLegacyPlanConcurrentDenseGrowth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const rows = 4096
		const firstHeld = legacyPlanSetupBytes + 1024*planIntBytes
		const limit = firstHeld + legacyPlanSetupBytes + 512*planIntBytes
		s, row := legacyPlanStore(rows)
		a := newLegacyPlanTestAPI(t, s, limit)
		gate := newResourceTestGate(t)
		defer gate.unblock()
		pause := sync.OnceFunc(func() { gate.wait(t.Context()) })
		ctx := planScanContext{Context: t.Context(), poll: func() {
			if usedQueryBytes(&a.initResources().scan) == firstHeld {
				pause()
			}
		}}
		out := httptest.NewRecorder()
		done := serveResourceRequest(a, out, ctx, "&page=1&limit=1")
		waitForAdmission(t, gate.entered, "first scan with an allocated ordinal array")

		// The second opening has room for setup and some matches. Its next
		// replacement would exceed the limit while the old array is live.
		assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), http.StatusServiceUnavailable, ErrQueryScanCapacityExhausted.Error())
		if got := usedQueryBytes(&a.initResources().scan); got != firstHeld {
			t.Fatalf("after rejecting competitor scan usage = %d, want %d", got, firstHeld)
		}
		gate.unblock()
		waitForAdmission(t, done, "first dense opening")
		token := assertRetainedPage(t, out, jsonRow(t, row))
		if token == "" {
			t.Fatal("admitted scan omitted cursor")
		}
		const retained = legacyPlanCaptureBytes + 2*rows*planIntBytes
		assertLegacyPlanUsage(t, a, retained, 1)
		assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), http.StatusServiceUnavailable, ErrQueryScanCapacityExhausted.Error())
		assertLegacyPlanUsage(t, a, retained, 1)
		if err := a.RetireQueryCapture(token); err != nil {
			t.Fatal(err)
		}
		assertLegacyPlanUsage(t, a, 0, 0)
		issueCapture(t, a)
		assertLegacyPlanUsage(t, a, retained, 1)
	})
}

func TestLegacyPlanRejectsAllocationsBeforeDelivery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		count    int
		capacity uint64
	}{
		{"bounded setup", 0, legacyPlanSetupBytes - 1},
		{"first ordinal", 1, legacyPlanSetupBytes + planIntBytes - 1},
		{"old and new ordinals coexist", 5, legacyPlanSetupBytes + 9*planIntBytes - 1},
		{"encoded sizes", 3, legacyPlanSetupBytes + 6*planIntBytes - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := legacyPlanStore(tc.count)
			a := newLegacyPlanTestAPI(t, s, tc.capacity)
			for range 2 {
				out := requestCapturePage(a, "&page=1&limit=1")
				assertQueryRejection(t, out, http.StatusServiceUnavailable, ErrQueryScanCapacityExhausted.Error())
				assertLegacyPlanUsage(t, a, 0, 0)
				if got := usedQueryBytes(&a.initResources().response); got != 0 {
					t.Fatalf("rejection left %d response bytes charged", got)
				}
			}
			if tc.capacity >= legacyPlanSetupBytes {
				assertRetainedPage(t, requestCapturePage(a, "&page=1&limit=1&contains=missing"), "")
			}
		})
	}
}

func TestLegacyPlanCleanupAfterAllocation(t *testing.T) {
	const rows = 16
	const scanBytes = legacyPlanSetupBytes + 2*rows*planIntBytes
	const heldDuringScan = legacyPlanSetupBytes + 4*planIntBytes
	const retained = legacyPlanCaptureBytes + 2*rows*planIntBytes
	for _, tc := range []struct {
		name    string
		trigger uint64
	}{
		{"cancel during scan", heldDuringScan},
		{"cancel after cursor preparation", retained},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := legacyPlanStore(rows)
			a := newLegacyPlanTestAPI(t, s, scanBytes)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			requestCtx := planScanContext{Context: ctx, poll: func() {
				if usedQueryBytes(&a.initResources().scan) == tc.trigger {
					cancel()
				}
			}}
			out := httptest.NewRecorder()
			a.ServeHTTP(out, httptest.NewRequestWithContext(requestCtx, http.MethodGet, queryBounds+"&page=1&limit=1", nil))
			assertQueryRejection(t, out, http.StatusRequestTimeout, "request cancelled")
			assertLegacyPlanUsage(t, a, 0, 0)
			if got := usedQueryBytes(&a.initResources().response); got != 0 {
				t.Fatalf("cancellation left %d response bytes charged", got)
			}
			issueCapture(t, a)
			assertLegacyPlanUsage(t, a, retained, 1)
		})
	}

	t.Run("confirmed pre-header failure", func(t *testing.T) {
		s, _ := legacyPlanStore(rows)
		a := newLegacyPlanTestAPI(t, s, scanBytes)
		w := &openingWriter{
			ResponseRecorder: httptest.NewRecorder(),
			flush: func() error {
				assertLegacyPlanUsage(t, a, retained, 1)
				return errors.Join(ErrQueryHeadersUnsent, io.ErrClosedPipe)
			},
		}
		a.ServeHTTP(w, httptest.NewRequest(http.MethodGet, queryBounds+"&page=1&limit=1", nil))
		if w.writes != 0 {
			t.Fatalf("attempted %d body writes after header failure", w.writes)
		}
		assertLegacyPlanUsage(t, a, 0, 0)
		issueCapture(t, a)
	})

	t.Run("close prevents publication", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, _ := legacyPlanStore(rows)
			a := newLegacyPlanTestAPI(t, s, scanBytes)
			gate := newResourceTestGate(t)
			defer gate.unblock()
			pause := sync.OnceFunc(func() { gate.wait(t.Context()) })
			ctx := planScanContext{Context: t.Context(), poll: func() {
				if usedQueryBytes(&a.initResources().scan) == heldDuringScan {
					pause()
				}
			}}
			out := httptest.NewRecorder()
			done := serveResourceRequest(a, out, ctx, "&page=1&limit=1")
			waitForAdmission(t, gate.entered, "opening scan with charged ordinals")
			closed := make(chan error, 1)
			go func() { closed <- a.Close(t.Context()) }()
			synctest.Wait()
			if got := usedQueryBytes(&a.initResources().scan); got != heldDuringScan {
				t.Fatalf("close during admitted scan left %d bytes, want %d", got, heldDuringScan)
			}
			gate.unblock()
			waitForAdmission(t, done, "opening rejected after shutdown")
			assertQueryRejection(t, out, http.StatusServiceUnavailable, ErrQueryCapacityExhausted.Error())
			if err := waitForAdmission(t, closed, "shutdown drain"); err != nil {
				t.Fatal(err)
			}
			assertLegacyPlanUsage(t, a, 0, 0)
		})
	})
}

func TestLegacyPlanChargeThroughFinalUse(t *testing.T) {
	for _, action := range []string{"expire", "retire", "close"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const retained = legacyPlanCaptureBytes + 4*planIntBytes
				a := newLegacyPlanTestAPI(t, captureFixtureSource().store, legacyPlanSetupBytes+4*planIntBytes)
				token := issueCapture(t, a)
				path := "&cursor=" + token
				assertRetirementTerminalPage(t, requestCapturePage(a, path))
				gate := newResourceTestGate(t)
				defer gate.unblock()
				w := &resourceBodyWriter{ResponseRecorder: httptest.NewRecorder(), gate: gate, ctx: t.Context()}
				done := serveResourceRequest(a, w, t.Context(), path)
				waitForAdmission(t, gate.entered, "admitted terminal replay")

				switch action {
				case "expire":
					time.Sleep(queryCaptureLifetime)
					synctest.Wait()
				case "retire":
					if err := a.RetireQueryCapture(token); err != nil {
						t.Fatal(err)
					}
				case "close":
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("close with active delivery: %v", err)
					}
				}
				assertLegacyPlanUsage(t, a, retained, 1)
				assertQueryRejection(t, requestCapturePage(a, path), http.StatusGone, ErrCaptureExpired.Error())
				if action != "close" {
					assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), http.StatusServiceUnavailable, ErrQueryScanCapacityExhausted.Error())
				}
				gate.unblock()
				waitForAdmission(t, done, "terminal replay to drain")
				assertRetirementTerminalPage(t, w.ResponseRecorder)
				assertLegacyPlanUsage(t, a, 0, 0)
				if action != "close" {
					issueCapture(t, a)
					assertLegacyPlanUsage(t, a, retained, 1)
				}
			})
		})
	}
}

func TestLegacyPlanByteTrimmedWalkAfterBackfill(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tie := start.Add(time.Second)
	early := store.Row{TS: start, Line: "a"}
	firstTie := store.Row{TS: tie, Line: "<"}
	secondTie := store.Row{TS: tie, Line: "b"}
	last := store.Row{TS: tie.Add(time.Second), Line: "c"}
	var s store.Store
	s.Publish([]store.Row{firstTie, early})
	s.Publish([]store.Row{secondTie, secondTie, last})
	bodyLimit := len(jsonRow(t, firstTie)) + len(jsonRow(t, secondTie))
	a := NewWithQueryLimits(&s, nil, bodyLimit, 4, DefaultQueryWorkBudget).(*API)
	closeRetirementTestAPI(t, a)
	token := assertRetainedPage(t, requestCapturePage(a, "&page=1&limit=1"), jsonRow(t, early))
	s.Publish([]store.Row{{TS: start, Line: "backfill"}, {TS: tie, Line: "new tie"}})

	for _, page := range []struct {
		limit int
		body  string
	}{
		{1000, jsonRow(t, firstTie) + jsonRow(t, secondTie)},
		{1, jsonRow(t, secondTie)},
		{1000, jsonRow(t, last)},
	} {
		if token == "" {
			t.Fatal("walk ended before reaching exhaustion")
		}
		path := "&cursor=" + token + "&limit=" + strconv.Itoa(page.limit)
		first := requestCapturePage(a, path)
		retry := requestCapturePage(a, path)
		token = assertRetainedPage(t, first, page.body)
		if again := assertRetainedPage(t, retry, page.body); again != token {
			t.Fatalf("retry changed next token from %q to %q", token, again)
		}
	}
	if token != "" {
		t.Fatalf("exhausted walk returned next token %q", token)
	}
}

func TestLegacyPlanRetainsSizeArrayCapacityAtOversizedBarrier(t *testing.T) {
	const count = 16
	_, row := legacyPlanStore(0)
	rows := slices.Repeat([]store.Row{row}, count)
	rows[2].Line = strings.Repeat("x", 4096)
	var s store.Store
	s.Publish(rows)
	a := newLegacyPlanTestAPI(t, &s, legacyPlanSetupBytes+2*count*planIntBytes)
	token := issueCapture(t, a)
	const retained = legacyPlanCaptureBytes + 2*count*planIntBytes
	assertLegacyPlanUsage(t, a, retained, 1)
	next := assertRetainedPage(t, requestCapturePage(a, "&cursor="+token), jsonRow(t, row))
	if next == "" {
		t.Fatal("walk stopped before reporting its oversized record")
	}
	err := &oversizedQueryRecord{4096, len(jsonRow(t, rows[2]))}
	assertQueryRejection(t, requestCapturePage(a, "&cursor="+next), http.StatusUnprocessableEntity, err.Error())
	assertLegacyPlanUsage(t, a, retained, 1)
	if err := a.RetireQueryCapture(token); err != nil {
		t.Fatal(err)
	}
	assertLegacyPlanUsage(t, a, 0, 0)
}
