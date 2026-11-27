package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"bog/internal/client"
	"bog/internal/httpapi"
	"bog/internal/store"
	model "bog/verify/query-model"
)

func TestSeededQueryTraceBothPageSources(t *testing.T) {
	t.Parallel()
	for _, seed := range []int64{-17, 0, 2709} {
		for _, retained := range []bool{false, true} {
			name := "legacy"
			if retained {
				name = "retained"
			}
			t.Run(fmt.Sprintf("%s/%d", name, seed), func(t *testing.T) {
				t.Parallel()
				cfg := seededTraceConfig(seed)
				cfg.MaxRecoveryWork, cfg.MaxRecoveryBytes = 1<<16, 1<<16
				result, err := model.VerifyQueryTrace(t.Context(), cfg, func(ctx context.Context, budget int, run func(model.QueryTraceAdapter) error) (err error) {
					fixture := &runtimeQueryFixture{retained: retained, budget: budget}
					if err := fixture.start(); err != nil {
						return errors.Join(err, fixture.stop())
					}
					defer func() { err = errors.Join(err, fixture.stop()) }()
					return run(fixture.adapter())
				})
				if err != nil {
					t.Fatalf("result=%+v: %v", result, err)
				}
				if result.MinimumWorkBudget != result.RejectedBudget+1 ||
					result.BoundaryHeld.Rows != 5 || result.BoundaryHeld.ByteTrimmedPages == 0 ||
					result.Held.Rows != 5 || result.Fresh.Rows != 8 || result.Restarted.Rows <= result.Fresh.Rows || result.Recovery == nil {
					t.Fatalf("incomplete query trace: %+v", result)
				}
				t.Logf("boundary=%d/%d held=%+v fresh=%+v restart=%+v recovery=%+v",
					result.RejectedBudget, result.MinimumWorkBudget, result.Held, result.Fresh, result.Restarted, *result.Recovery)
			})
		}
	}
}

type fixtureGeneration struct {
	store     *store.Store
	serial    int
	scanIndex []store.PositionedRow
}

type fixtureCapture struct {
	generation int
	view       *store.PositionedSnapshot
	scanIndex  []store.PositionedRow
}

// runtimeQueryFixture owns acknowledgement, activation and acquisition under
// one mutex. It exercises the real HTTP PageSource seam with owned immutable
// snapshot views. The journal here is a fixture WAL, retained across issuer
// replacement, with encoded batches preserved to exercise decode on restart.
type runtimeQueryFixture struct {
	mu          sync.Mutex
	retained    bool
	budget      int
	concurrency int
	demand      *queryDemandMeter
	meterScans  bool
	journal     [][]byte
	checkpoint  []store.Row // decoded prefix for the tail-only replacement fixture
	covered     int
	active      fixtureGeneration
	captures    map[string]fixtureCapture
	nextCapture int
	failAppend  bool
	rejections  int

	api     *httpapi.API
	handler http.Handler
	server  *httptest.Server
	client  *client.Client
	fetch   model.FetchWirePage
}

func (f *runtimeQueryFixture) Append(body []byte, rows []store.Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAppend {
		f.failAppend = false
		f.rejections++
		return errors.New("injected whole-batch append failure")
	}
	f.journal = append(f.journal, append([]byte(nil), body...))
	f.active.store.Publish(rows)
	if f.meterScans {
		f.active.scanIndex = indexFixtureScanRows(f.active.scanIndex, rows)
	}
	return nil
}

func (f *runtimeQueryFixture) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	if !req.Snapshot {
		return httpapi.PageResult{}, errors.New("fixture expects a snapshot request")
	}
	if !f.meterScans {
		if err := work.Charge(1); err != nil {
			return httpapi.PageResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return httpapi.PageResult{}, err
	}
	rows, more, id, err := f.scanPage(ctx, req, work)
	if err != nil {
		return httpapi.PageResult{}, err
	}
	if f.demand != nil && cap(rows) != 0 {
		size := int64(cap(rows)) * int64(unsafe.Sizeof(store.PositionedRow{}))
		f.demand.enter(model.ScanBufferReady, size)
		defer f.demand.leave(model.ScanBufferReady, size)
	}
	if gate, ok := ctx.Value(scanGateKey{}).(*resumeGate); ok && req.Capture == "" {
		gate.wait(ctx)
		// Return ownership of a successful acquisition even on cancellation.
		// The HTTP issuer observes cancellation and releases it.
	}
	result := httpapi.PageResult{Capture: id, Rows: make([]httpapi.PageRow, 0, len(rows)), More: more}
	for _, row := range rows {
		pos := row.Position.Time.UTC().Format(time.RFC3339Nano) + "/" + strconv.FormatUint(row.Position.Ordinal, 10)
		result.Rows = append(result.Rows, httpapi.PageRow{Row: row.Row, Position: httpapi.PagePosition(pos)})
	}
	return result, nil
}

func (f *runtimeQueryFixture) scanPage(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) ([]store.PositionedRow, bool, string, error) {
	id := req.Capture
	capture, err := f.captureView(id)
	if err != nil {
		return nil, false, "", err
	}
	var after *store.TailPosition
	if id != "" {
		stamp, ordinal, found := strings.Cut(string(req.After), "/")
		t, timeErr := time.Parse(time.RFC3339Nano, stamp)
		i, ordinalErr := strconv.ParseUint(ordinal, 10, 64)
		if !found || timeErr != nil || ordinalErr != nil {
			return nil, false, "", fmt.Errorf("invalid fixture position: %w", errors.Join(timeErr, ordinalErr))
		}
		after = &store.TailPosition{Time: t, Ordinal: i}
	}
	var rows []store.PositionedRow
	var more bool
	if f.meterScans {
		meter, ok := ctx.Value(fixtureScanMeterKey{}).(*fixtureScanMeter)
		if !ok {
			return nil, false, "", errors.New("missing fixture scan meter")
		}
		rows, more, err = meter.page(capture.scanIndex, req, after, work)
	} else {
		rows, more, err = capture.view.Page(req.Filter.Start, req.Filter.End, req.Filter.Contains, req.Limit, 0, after, work)
		if err == nil {
			err = work.Charge(uint64(len(rows)))
		}
	}
	if err != nil {
		return nil, false, "", err
	}
	if id == "" {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.nextCapture++
		id = strconv.Itoa(f.nextCapture)
		f.captures[id] = capture
		if f.demand != nil {
			f.demand.acquire(id)
		}
	}
	return rows, more, id, nil
}

func (f *runtimeQueryFixture) captureView(id string) (fixtureCapture, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == "" {
		capture := fixtureCapture{generation: f.active.serial, scanIndex: f.active.scanIndex}
		if !f.meterScans {
			capture.view = f.active.store.CapturePositionedSnapshot()
		}
		return capture, nil
	}
	capture, ok := f.captures[id]
	if !ok {
		return fixtureCapture{}, httpapi.ErrCaptureExpired
	}
	return capture, nil
}

func (f *runtimeQueryFixture) Release(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.demand != nil {
		f.demand.release(id)
	}
	delete(f.captures, id)
	return nil
}

func (f *runtimeQueryFixture) startIssuer() {
	if f.active.store == nil {
		f.active.store = new(store.Store)
	}
	f.captures = make(map[string]fixtureCapture)
	if f.retained {
		f.api = httpapi.NewWithPageSource(f, f, 256, max(1, f.concurrency), uint64(f.budget))
		f.handler = f.api
	} else {
		f.handler = httpapi.NewWithQueryLimits(f.active.store, f, 256, max(1, f.concurrency), uint64(f.budget))
	}
}

func (f *runtimeQueryFixture) start() error {
	f.startIssuer()
	handler := f.handler
	if f.meterScans {
		handler = instrumentFixtureScans(handler, uint64(f.budget))
	}
	f.server = httptest.NewServer(handler)
	var transport http.RoundTripper = f.server.Client().Transport
	if f.meterScans {
		transport = fixtureScanTransport{transport}
	}
	var err error
	f.client, f.fetch, err = newWitnessClient(f.server.URL, transport, 256)
	return err
}

func (f *runtimeQueryFixture) stop() error {
	if f.server == nil {
		return nil
	}
	f.server.Close()
	f.server = nil
	return f.closeIssuer()
}

func (f *runtimeQueryFixture) closeIssuer() error {
	closer, ok := f.handler.(interface{ Close(context.Context) error })
	if !ok {
		return fmt.Errorf("query issuer %T does not expose Close", f.handler)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := closer.Close(ctx)
	f.api = nil
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.captures) != 0 {
		err = errors.Join(err, fmt.Errorf("%d captures remained after HTTP drain", len(f.captures)))
	}
	return err
}

func (f *runtimeQueryFixture) replay(ctx context.Context, checkpoint []store.Row, journal [][]byte) (fixtureGeneration, model.QueryRecovery, error) {
	var generation fixtureGeneration
	var input []store.Row
	var collect func([]store.Row)
	if f.meterScans {
		collect = func(rows []store.Row) { input = append(input, rows...) }
	}
	s, stats, err := replayFixtureJournal(ctx, checkpoint, journal, collect)
	if err == nil {
		generation.store = s
		generation.scanIndex = indexFixtureScanRows(nil, input)
	}
	return generation, stats, err
}

func replayFixtureJournal(ctx context.Context, checkpoint []store.Row, journal [][]byte, collect func([]store.Row)) (*store.Store, model.QueryRecovery, error) {
	s := new(store.Store)
	s.Publish(checkpoint)
	if collect != nil {
		collect(checkpoint)
	}
	stats := model.QueryRecovery{CommittedRows: len(checkpoint)}
	for i, body := range journal {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		rows, err := store.DecodeIngest(body)
		if err != nil {
			return nil, stats, fmt.Errorf("recover batch %d: %w", i+1, err)
		}
		// Work counts encoded bytes decoded. Memory is peak encoded replay
		// input per batch, the scratch working set of this fixture meter.
		stats.Work += uint64(len(body))
		stats.PeakBytes = max(stats.PeakBytes, uint64(len(body)))
		stats.CommittedRows += len(rows)
		s.Publish(rows)
		if collect != nil {
			collect(rows)
		}
	}
	return s, stats, ctx.Err()
}

// activateTail consumes only whole frames from the current journal. The
// successor is reconstructed from its new checkpoint and the carried suffix.
// No covered journal frame is available to the next replacement.
func (f *runtimeQueryFixture) activateTail(ctx context.Context, cutoff int) (model.ReplacementGeneration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	committed := f.active.store.PublishedCount()
	if cutoff <= f.covered || cutoff > committed || len(f.checkpoint) != f.covered {
		return model.ReplacementGeneration{}, fmt.Errorf("invalid tail cutoff %d (covered=%d committed=%d)", cutoff, f.covered, committed)
	}
	checkpoint := slices.Clone(f.checkpoint)
	consumed := 0
	for len(checkpoint) < cutoff && consumed < len(f.journal) {
		if err := ctx.Err(); err != nil {
			return model.ReplacementGeneration{}, err
		}
		rows, err := store.DecodeIngest(f.journal[consumed])
		if err != nil {
			return model.ReplacementGeneration{}, fmt.Errorf("decode tail frame %d: %w", consumed+1, err)
		}
		checkpoint = append(checkpoint, rows...)
		consumed++
	}
	if len(checkpoint) != cutoff {
		return model.ReplacementGeneration{}, fmt.Errorf("tail cutoff %d does not match a whole frame boundary", cutoff)
	}
	tail := slices.Clone(f.journal[consumed:])
	next, stats, err := f.replay(ctx, checkpoint, tail)
	if err != nil {
		return model.ReplacementGeneration{}, err
	}
	if stats.CommittedRows != committed {
		return model.ReplacementGeneration{}, fmt.Errorf("tail activation recovered %d committed rows, want %d", stats.CommittedRows, committed)
	}
	f.checkpoint, f.journal, f.covered = checkpoint, tail, cutoff
	next.serial = f.active.serial + 1
	f.active = next
	return f.replacementPointLocked(), nil
}

func (f *runtimeQueryFixture) replacementPointLocked() model.ReplacementGeneration {
	return model.ReplacementGeneration{
		Generation: uint64(f.active.serial), CoveredRows: f.covered,
		CommittedRows: f.active.store.PublishedCount(), Complete: true,
	}
}

func (f *runtimeQueryFixture) retiredPins(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var live uint64
	for _, capture := range f.captures {
		if capture.generation < f.active.serial {
			live++
		}
	}
	return live, ctx.Err()
}

func (f *runtimeQueryFixture) publish(ctx context.Context, cutoff int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cutoff <= f.covered || cutoff != f.active.store.PublishedCount() {
		return fmt.Errorf("invalid publication cutoff %d after %d", cutoff, f.covered)
	}
	if f.covered > 0 && len(f.captures) != 1 {
		return fmt.Errorf("replacement needs one held capture, got %d", len(f.captures))
	}
	next, stats, err := f.replay(ctx, f.checkpoint, f.journal)
	if err != nil {
		return err
	}
	if stats.CommittedRows != cutoff {
		return fmt.Errorf("publication replay covered %d rows, want %d", stats.CommittedRows, cutoff)
	}
	next.serial = f.active.serial + 1
	f.active = next
	f.covered = cutoff
	return nil
}

func (f *runtimeQueryFixture) retire(ctx context.Context, cursor string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.api.RetireQueryCapture(cursor); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, capture := range f.captures {
		if capture.generation < f.active.serial {
			return errors.New("retired generation retained its capture after the retirement response")
		}
	}
	if len(f.captures) != 1 {
		return fmt.Errorf("expected only the fresh capture to remain, got %d", len(f.captures))
	}
	return ctx.Err()
}

func (f *runtimeQueryFixture) restart(ctx context.Context) (model.QueryRecovery, error) {
	if err := f.stop(); err != nil {
		return model.QueryRecovery{}, err
	}
	next, stats, err := f.replay(ctx, f.checkpoint, f.journal)
	if err != nil {
		return stats, err
	}
	next.serial = f.active.serial
	f.active = next
	return stats, f.start()
}

func (f *runtimeQueryFixture) reject(ctx context.Context, rows []model.Row) error {
	f.mu.Lock()
	f.failAppend = true
	rejections := f.rejections
	f.mu.Unlock()
	outcome, err := sendFixtureBatch(ctx, f.client, rows)
	f.mu.Lock()
	defer f.mu.Unlock()
	status, ok := err.(*client.HTTPError)
	if outcome != client.IngestUncommitted || !ok || status.StatusCode != http.StatusServiceUnavailable ||
		status.Body != "WAL unavailable\n" || status.Cause != nil || f.rejections != rejections+1 {
		return fmt.Errorf("expected injected uncommitted WAL refusal, outcome=%d error=%v", outcome, err)
	}
	return ctx.Err()
}

func (f *runtimeQueryFixture) adapter() model.QueryTraceAdapter {
	adapter := model.QueryTraceAdapter{
		Acknowledge: func(ctx context.Context, rows []model.Row) error { return acknowledgeFixtureBatch(ctx, f.client, rows) },
		Fetch:       func(ctx context.Context, req model.WireRequest) (model.WirePage, error) { return f.fetch(ctx, req) },
		Lifecycle:   &model.QueryTraceLifecycle{RejectAppend: f.reject, Restart: f.restart},
	}
	if f.retained {
		adapter.Publish, adapter.Lifecycle.Retire = f.publish, f.retire
	}
	if f.meterScans {
		adapter.FetchObserved = f.fetchObserved
	}
	return adapter
}
