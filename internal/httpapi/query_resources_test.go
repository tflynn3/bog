package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
)

func (p *legacyQueryPlan) prepare(limit, byteCap int, literalWork, budget uint64, work *store.QueryWork) (queryResponse, error) {
	response := queryResponse{maxBytes: byteCap}
	err := p.prepareInto(&response, limit, literalWork, budget, work)
	if err != nil {
		return queryResponse{}, err
	}
	return response, nil
}

func (p *legacyQueryPlan) replay(offset, limit, byteCap int, work *store.QueryWork) (queryResponse, int, error) {
	response := queryResponse{maxBytes: byteCap}
	next, err := p.replayInto(&response, offset, limit, work)
	if err != nil {
		return queryResponse{}, 0, err
	}
	return response, next, nil
}

// Exercise #123's topology with small explicit byte limits: two terminal
// deliveries, two opening scans, and a fifth request probing each resource.
const fixtureScanBytes = 864

type resourceTestGate struct {
	entered chan struct{}
	proceed chan struct{}
	unblock func()
}

func newResourceTestGate(t *testing.T) *resourceTestGate {
	t.Helper()
	g := &resourceTestGate{entered: make(chan struct{}), proceed: make(chan struct{})}
	g.unblock = sync.OnceFunc(func() { close(g.proceed) })
	t.Cleanup(g.unblock)
	return g
}

func (g *resourceTestGate) wait(ctx context.Context) {
	close(g.entered)
	select {
	case <-ctx.Done():
	case <-g.proceed:
	}
}

type resourceScanGateKey struct{}

type resourcePageSource struct {
	rows    []PageRow
	calls   atomic.Int32
	mu      sync.Mutex
	next    uint64
	live    map[string]bool
	expires time.Time
	release *resourceTestGate
}

func newResourcePageSource() *resourcePageSource {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	return &resourcePageSource{
		rows: []PageRow{
			{Row: store.Row{TS: stamp, Line: "opening"}, Position: "1"},
			{Row: store.Row{TS: stamp, Line: strings.Repeat("<", 18)}, Position: "2"},
			{Row: store.Row{TS: stamp, Line: strings.Repeat("z", 62)}, Position: "3"},
		},
		live: make(map[string]bool),
	}
}

// This adapter borrows immutable line and position strings. Its scratch
// and up to three PageRow values coexist during Page, and the result
// retains its backing slice through HTTP consumption.
func (*resourcePageSource) PageScanBytes(PageRequest) uint64 { return fixtureScanBytes }

func (s *resourcePageSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	s.calls.Add(1)
	if err := work.Charge(1); err != nil {
		return PageResult{}, err
	}
	start := 0
	if req.After != "" {
		var err error
		start, err = strconv.Atoi(string(req.After))
		if err != nil {
			return PageResult{}, err
		}
	}
	if start < 0 || start > len(s.rows) {
		return PageResult{}, fmt.Errorf("fixture position %d out of range", start)
	}
	scratch := make([]PageRow, 0, len(s.rows))
	more := false
	for _, r := range s.rows[start:] {
		if !strings.Contains(r.Row.Line, req.Filter.Contains) {
			continue
		}
		if len(scratch) == req.Limit {
			more = req.Snapshot
			break
		}
		scratch = append(scratch, r)
	}
	id := req.Capture
	if req.Snapshot {
		s.mu.Lock()
		switch {
		case id == "":
			s.next++
			id = strconv.FormatUint(s.next, 10)
			s.live[id] = true
		case !s.live[id]:
			s.mu.Unlock()
			return PageResult{}, ErrCaptureExpired
		}
		s.mu.Unlock()
	}
	out := PageResult{Capture: id, ExpiresAt: s.expires, More: more, Rows: make([]PageRow, len(scratch))}
	if g, ok := ctx.Value(resourceScanGateKey{}).(*resourceTestGate); ok {
		g.wait(ctx)
	}
	copy(out.Rows, scratch)
	return out, nil
}

func (s *resourcePageSource) Release(id string) error {
	if s.release != nil && id == "1" {
		s.release.wait(context.Background())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.live[id] {
		return errors.New("fixture received duplicate release")
	}
	delete(s.live, id)
	return nil
}

func (s *resourcePageSource) pinned() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.live)
}

type resourceBodyWriter struct {
	*httptest.ResponseRecorder
	gate *resourceTestGate
	ctx  context.Context
	err  error
}

func (w *resourceBodyWriter) Write(p []byte) (int, error) {
	if w.gate != nil {
		w.gate.wait(w.ctx)
		w.gate = nil
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if w.err != nil {
		return 0, w.err
	}
	return w.ResponseRecorder.Write(p)
}

func serveResourceRequest(a *API, w http.ResponseWriter, ctx context.Context, suffix string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.ServeHTTP(w, httptest.NewRequest(http.MethodGet, queryBounds+suffix, nil).WithContext(ctx))
	}()
	return done
}

func TestQueryResourcesRequireSourceScanBound(t *testing.T) {
	source := newResourcePageSource()
	unbounded := struct{ PageSource }{source}
	a := NewWithPageSourceAndQueryResources(unbounded, nil, 256, 1, 32768, DefaultQueryResourceLimits())
	closeRetirementTestAPI(t, a)
	for _, suffix := range []string{"", "&page=1&limit=1"} {
		assertQueryRejection(t, requestCapturePage(a, suffix), http.StatusServiceUnavailable, ErrQueryScanCapacityExhausted.Error())
	}
	if source.calls.Load() != 0 || len(a.captures.live) != 0 {
		t.Fatal("scan refusal reached Page or left an opening capture reserved")
	}

	compat := NewWithPageSource(unbounded, nil, 256, 1, 32768)
	closeRetirementTestAPI(t, compat)
	issueCapture(t, compat)
}

type resourceCountingWriter struct {
	*httptest.ResponseRecorder
	bytes int
}

func (w *resourceCountingWriter) Write(p []byte) (int, error) {
	w.bytes += len(p)
	return len(p), nil
}

func TestQueryResourcesImplicitResponseLimitFollowsPageCap(t *testing.T) {
	const pageCap = 64 << 20
	s := new(store.Store)
	line := strings.Repeat("a", 1<<20)
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]store.Row, 40)
	for i := range rows {
		rows[i] = store.Row{TS: stamp, Line: line}
	}
	s.Publish(rows)
	const wantBytes = 40 * ((1 << 20) + len("{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"\"}\n"))

	a := NewWithQueryLimits(s, nil, pageCap, 1, DefaultQueryWorkBudget).(*API)
	closeRetirementTestAPI(t, a)
	out := &resourceCountingWriter{ResponseRecorder: httptest.NewRecorder()}
	a.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds, nil))
	if out.Code != http.StatusOK || out.bytes != wantBytes || out.Header().Get("Content-Length") != strconv.Itoa(wantBytes) {
		t.Fatalf("large page: status=%d bytes=%d headers=%v", out.Code, out.bytes, out.Header())
	}

	explicit := NewWithQueryResources(s, nil, pageCap, 1, DefaultQueryWorkBudget, DefaultQueryResourceLimits())
	closeRetirementTestAPI(t, explicit)
	assertQueryRejection(t, requestCapturePage(explicit, ""), http.StatusServiceUnavailable, ErrQueryResponseCapacityExhausted.Error())
}

func TestQueryResourceContention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := newResourcePageSource()
		terminal := jsonRow(t, source.rows[1].Row) + jsonRow(t, source.rows[2].Row)
		a := NewWithPageSourceAndQueryResources(source, nil, 256, 6, 32768, QueryResourceLimits{
			ResponseBytes: uint64(2 * len(terminal)), ScanBytes: 2 * fixtureScanBytes, Captures: 4,
		})
		closeRetirementTestAPI(t, a)
		tokens := []string{issueCapture(t, a), issueCapture(t, a)}
		var resumeGates, scanGates [2]*resourceTestGate
		var resumeDone, scanDone [2]<-chan struct{}
		var resumes [2]*resourceBodyWriter
		var scans [2]*httptest.ResponseRecorder
		var cancelResume, cancelScan [2]context.CancelFunc

		for i := range tokens {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelResume[i] = cancel
			resumeGates[i] = newResourceTestGate(t)
			resumes[i] = &resourceBodyWriter{ResponseRecorder: httptest.NewRecorder(), gate: resumeGates[i], ctx: ctx}
			resumeDone[i] = serveResourceRequest(a, resumes[i], ctx, "&cursor="+tokens[i])
			waitForAdmission(t, resumeGates[i].entered, "terminal delivery")
		}
		// Scans completed before delivery, so this request reaches the
		// response allocator, which is exactly full.
		assertQueryRejection(t, requestCapturePage(a, "&limit=1"), 503, ErrQueryResponseCapacityExhausted.Error())

		for i := range tokens {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelScan[i] = cancel
			scanGates[i] = newResourceTestGate(t)
			ctx = context.WithValue(ctx, resourceScanGateKey{}, scanGates[i])
			scans[i] = httptest.NewRecorder()
			scanDone[i] = serveResourceRequest(a, scans[i], ctx, "&page=1&limit=1")
			waitForAdmission(t, scanGates[i].entered, "opening scan")
		}
		if got := source.pinned(); got != 4 {
			t.Fatalf("pinned %d captures, want 4", got)
		}
		calls := source.calls.Load()
		assertQueryRejection(t, requestCapturePage(a, "&limit=1"), 503, ErrQueryScanCapacityExhausted.Error())
		assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), 503, ErrQueryCaptureCapacityExhausted.Error())
		assertQueryRejection(t, requestCapturePage(a, "&cursor="+tokens[0]), 503, ErrQueryScanCapacityExhausted.Error())
		if source.calls.Load() != calls {
			t.Fatal("refused request called Page")
		}

		for i, cancel := range cancelScan {
			cancel()
			waitForAdmission(t, scanDone[i], "cancelled opening scan")
			assertQueryRejection(t, scans[i], 408, "request cancelled")
		}
		if got := source.pinned(); got != 2 {
			t.Fatalf("after cancelled scans pinned %d captures, want 2", got)
		}
		cancelResume[0]()
		waitForAdmission(t, resumeDone[0], "cancelled delivery")
		for _, token := range tokens {
			for range 2 {
				out := requestCapturePage(a, "&cursor="+token)
				if got := assertRetainedPage(t, out, terminal); got != "" {
					t.Fatal("retry changed terminal cursor")
				}
			}
		}
		for _, token := range tokens {
			if err := a.RetireQueryCapture(token); err != nil {
				t.Fatal(err)
			}
		}
		if got := source.pinned(); got != 1 {
			t.Fatalf("after retirement pinned %d captures, want 1", got)
		}
		resumeGates[1].unblock()
		waitForAdmission(t, resumeDone[1], "final delivery")
		assertRetainedPage(t, resumes[1].ResponseRecorder, terminal)
		if got := source.pinned(); got != 0 {
			t.Fatalf("after delivery pinned %d captures, want 0", got)
		}
	})
}

func TestQueryResourcePartialResponseRollback(t *testing.T) {
	for _, name := range []string{"legacy", "retained"} {
		t.Run(name, func(t *testing.T) {
			const work = 32768
			source := captureFixtureSource()
			first := jsonRow(t, store.Row{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "first"})
			limits := QueryResourceLimits{
				ResponseBytes: uint64(len(first) + 1), ScanBytes: 2 << 20, Captures: 1,
			}
			var a *API
			if name == "retained" {
				a = NewWithPageSourceAndQueryResources(source, nil, 256, 2, work, limits)
			} else {
				a = NewWithQueryResources(source.store, nil, 256, 2, work, limits)
			}
			closeRetirementTestAPI(t, a)

			for _, suffix := range []string{"", "&page=1"} {
				assertQueryRejection(t, requestCapturePage(a, suffix), 503, ErrQueryResponseCapacityExhausted.Error())
			}
			token := issueCapture(t, a)
			path := "&cursor=" + token
			for range 2 {
				out := requestCapturePage(a, path)
				assertRetirementTerminalPage(t, out)
			}
			assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), 503, ErrQueryCaptureCapacityExhausted.Error())
			if err := a.RetireQueryCapture(token); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueryResourceCaptureChargeThroughRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := newResourcePageSource()
		source.expires = time.Now().Add(time.Second)
		source.release = newResourceTestGate(t)
		defer source.release.unblock()
		a := NewWithPageSourceAndQueryResources(source, nil, 256, 3, 32768, QueryResourceLimits{256, fixtureScanBytes, 1})
		token := issueCapture(t, a)

		// Expiry retires the capture, but the source still holds its pin.
		waitForAdmission(t, source.release.entered, "expired capture release")
		assertQueryRejection(t, requestCapturePage(a, "&cursor="+token), 410, "query capture expired")
		assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), 503, ErrQueryCaptureCapacityExhausted.Error())
		if out := requestCapturePage(a, "&limit=1"); out.Code != 200 {
			t.Fatalf("ordinary query during capture release: %d %s", out.Code, out.Body)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close during release: %v", err)
		}
		source.release.unblock()
		if err := a.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if source.pinned() != 0 {
			t.Fatal("Close returned before source released capture")
		}
	})
}

func TestQueryResourceScanChargeThroughConsumption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := newResourcePageSource()
		a := NewWithPageSourceAndQueryResources(source, nil, 256, 2, 32768, QueryResourceLimits{256, fixtureScanBytes, 1})
		closeRetirementTestAPI(t, a)
		gate := newResourceTestGate(t)
		defer gate.unblock()
		done := make(chan error, 1)
		go func() {
			done <- a.withPage(t.Context(), PageRequest{Limit: 1}, nil, func(out PageResult) error {
				gate.wait(t.Context())
				if len(out.Rows) != 1 {
					return errors.New("fixture missing borrowed result")
				}
				return io.ErrUnexpectedEOF
			})
		}()
		waitForAdmission(t, gate.entered, "scan consumer")
		assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), 503, ErrQueryScanCapacityExhausted.Error())
		if source.calls.Load() != 1 || source.pinned() != 0 {
			t.Fatal("refused opening attempted source acquisition")
		}
		gate.unblock()
		if err := waitForAdmission(t, done, "scan consumer error"); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
		// The failed opening rolled back its capture slot as well.
		issueCapture(t, a)
	})
}

func TestQueryResourcesCloseDrainsOrdinaryDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := newResourcePageSource()
		limits := QueryResourceLimits{256, fixtureScanBytes, 1}
		a := NewWithPageSourceAndQueryResources(source, nil, 256, 2, 32768, limits)
		other := NewWithPageSourceAndQueryResources(source, nil, 256, 2, 32768, limits)
		closeRetirementTestAPI(t, other)
		g := newResourceTestGate(t)
		defer g.unblock()
		w := &resourceBodyWriter{ResponseRecorder: httptest.NewRecorder(), gate: g, ctx: t.Context(), err: io.ErrShortWrite}
		done := serveResourceRequest(a, w, t.Context(), "")
		waitForAdmission(t, g.entered, "ordinary delivery")
		if out := requestCapturePage(other, ""); out.Code != http.StatusOK {
			t.Fatalf("different issuer: %d %s", out.Code, out.Body)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close during ordinary delivery: %v", err)
		}
		assertQueryRejection(t, requestCapturePage(a, ""), 503, ErrQueryCapacityExhausted.Error())
		g.unblock()
		waitForAdmission(t, done, "failed ordinary delivery")
		if err := a.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := a.initResources().response.used; got != 0 {
			t.Fatalf("failed delivery leaked %d response bytes", got)
		}
	})
}

func TestQueryReservationCheckedGrowth(t *testing.T) {
	for _, tc := range []struct {
		name string
		cap  uint64
		a, b uint64
		fail bool
	}{
		{"empty", 0, 0, 0, false},
		{"disabled", 0, 0, 1, true},
		{"exact", 10, 2, 8, false},
		{"oversubscribed", 10, 2, 9, true},
		{"overflow", math.MaxUint64, math.MaxUint64 - 1, 2, true},
		{"maximum", math.MaxUint64, 0, math.MaxUint64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := &queryByteBudget{limit: tc.cap, err: ErrQueryScanCapacityExhausted}
			first := queryReservation{budget: budget}
			if err := first.grow(tc.a); err != nil {
				t.Fatal(err)
			}
			second := queryReservation{budget: budget}
			err := second.grow(tc.b)
			if tc.fail {
				if !errors.Is(err, ErrQueryScanCapacityExhausted) || budget.used != tc.a {
					t.Fatalf("failed growth: err=%v used=%d want=%d", err, budget.used, tc.a)
				}
			} else if err != nil || budget.used != tc.a+tc.b {
				t.Fatalf("inclusive growth: err=%v used=%d", err, budget.used)
			}
			first.release()
			second.release()
			second.release()
			if budget.used != 0 {
				t.Fatalf("released budget still charged %d", budget.used)
			}
		})
	}
}

func TestQueryResourceResumeAfterResponsePressure(t *testing.T) {
	for _, name := range []string{"legacy", "retained"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const work = 32768
				source := captureFixtureSource()
				row := store.Row{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "second"}
				limits := QueryResourceLimits{
					ResponseBytes: uint64(len(jsonRow(t, row))), ScanBytes: 2 << 20, Captures: 1,
				}
				var a *API
				if name == "retained" {
					a = NewWithPageSourceAndQueryResources(source, nil, 256, 2, work, limits)
				} else {
					a = NewWithQueryResources(source.store, nil, 256, 2, work, limits)
				}
				closeRetirementTestAPI(t, a)
				token := issueCapture(t, a)
				path := "&cursor=" + token
				gate := newResourceTestGate(t)
				defer gate.unblock()
				w := &resourceBodyWriter{ResponseRecorder: httptest.NewRecorder(), gate: gate, ctx: t.Context()}
				done := serveResourceRequest(a, w, t.Context(), path)
				waitForAdmission(t, gate.entered, "resume delivery")
				assertQueryRejection(t, requestCapturePage(a, path), 503, ErrQueryResponseCapacityExhausted.Error())
				gate.unblock()
				waitForAdmission(t, done, "resume body")
				assertRetirementTerminalPage(t, w.ResponseRecorder)
				for range 2 {
					retry := requestCapturePage(a, path)
					assertRetirementTerminalPage(t, retry)
					if retry.Body.String() != w.Body.String() {
						t.Fatal("retry changed terminal bytes")
					}
				}
			})
		})
	}
}

func TestQueryResourceFailedOpeningDelivery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flushErr error
		keep     bool
	}{
		{"confirmed pre-header failure", errors.Join(ErrQueryHeadersUnsent, io.ErrClosedPipe), false},
		{"ambiguous header failure", io.ErrClosedPipe, true},
		{"headers flushed with failed body", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := newResourcePageSource()
			terminal := jsonRow(t, source.rows[1].Row) + jsonRow(t, source.rows[2].Row)
			limits := QueryResourceLimits{ResponseBytes: uint64(len(terminal)), ScanBytes: fixtureScanBytes, Captures: 1}
			a := NewWithPageSourceAndQueryResources(source, nil, 256, 2, 32768, limits)
			closeRetirementTestAPI(t, a)
			w := &openingWriter{
				ResponseRecorder: httptest.NewRecorder(), writeErr: io.ErrShortWrite,
				flush: func() error { return tc.flushErr },
			}
			a.ServeHTTP(w, httptest.NewRequest(http.MethodGet, queryBounds+"&page=1&limit=1", nil))
			token := w.Header().Get("Bog-Next-Cursor")
			if token == "" || w.Code != 200 {
				t.Fatalf("opening status=%d cursor=%q", w.Code, token)
			}
			path := "&cursor=" + token
			if tc.keep {
				assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), 503, ErrQueryCaptureCapacityExhausted.Error())
				// The retry needs the entire response allowance.
				assertRetainedPage(t, requestCapturePage(a, path), terminal)
				if err := a.RetireQueryCapture(token); err != nil {
					t.Fatal(err)
				}
			} else {
				assertQueryRejection(t, requestCapturePage(a, path), 410, "query capture expired")
			}
			issueCapture(t, a)
		})
	}
}
