package querymodel_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	model "bog/verify/query-model"
)

const (
	fixtureScanBytesSource = "retained fixture line bytes passed to literal matching"
	fixtureScanWorkSource  = "retained fixture successful PageSource scan charges"
	fixtureScanBytesHeader = "X-Fixture-Scan-Bytes"
	fixtureScanWorkHeader  = "X-Fixture-Scan-Work"
)

// Build the fixture's scan input at acknowledgement and recovery. An opening
// holds the immutable index for its generation even after replacement.
func indexFixtureScanRows(prior []store.PositionedRow, batch []store.Row) []store.PositionedRow {
	rows := slices.Clone(prior)
	for i, row := range batch {
		rows = append(rows, store.PositionedRow{Row: row, Position: store.TailPosition{
			Time: row.TS.UTC(), Ordinal: uint64(len(prior) + i),
		}})
	}
	slices.SortFunc(rows, func(a, b store.PositionedRow) int {
		if order := a.Position.Time.Compare(b.Position.Time); order != 0 {
			return order
		}
		return cmp.Compare(a.Position.Ordinal, b.Position.Ordinal)
	})
	return rows
}

type fixtureScanMeterKey struct{}

type fixtureScanMeter struct {
	budget, charged, scanned uint64
}

func (m *fixtureScanMeter) charge(work *store.QueryWork, units uint64) error {
	if units > m.budget-m.charged {
		return store.ErrQueryWorkExhausted
	}
	if err := work.Charge(units); err != nil {
		return err
	}
	m.charged += units
	return nil
}

func (m *fixtureScanMeter) page(input []store.PositionedRow, req httpapi.PageRequest, after *store.TailPosition, work *store.QueryWork) ([]store.PositionedRow, bool, error) {
	// One setup unit and the worst-case number of seek comparisons.
	if err := m.charge(work, 1+uint64(bits.Len(uint(len(input))))); err != nil {
		return nil, false, err
	}
	start := sort.Search(len(input), func(i int) bool {
		pos := input[i].Position
		return !pos.Time.Before(req.Filter.Start) && (after == nil || pos.Time.After(after.Time) ||
			(pos.Time.Equal(after.Time) && pos.Ordinal > after.Ordinal))
	})
	var rows []store.PositionedRow
	for _, candidate := range input[start:] {
		if err := m.charge(work, 1); err != nil {
			return nil, false, err
		}
		if !candidate.Position.Time.Before(req.Filter.End) {
			break
		}
		n := uint64(len(candidate.Row.Line))
		if err := m.charge(work, n); err != nil {
			return nil, false, err
		}
		m.scanned += n
		if strings.Contains(candidate.Row.Line, req.Filter.Contains) {
			if len(rows) == req.Limit {
				return rows, true, nil
			}
			if err := m.charge(work, 1); err != nil {
				return nil, false, err
			}
			rows = append(rows, candidate)
		}
	}
	return rows, false, nil
}

// The real query handler runs all PageSource calls before it writes the
// response headers. Send the final request-local counters through fixture-only
// headers so cancellation and subsequent requests cannot mix observations.
func instrumentFixtureScans(handler http.Handler, budget uint64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/query" {
			handler.ServeHTTP(w, r)
			return
		}
		meter := &fixtureScanMeter{budget: budget}
		handler.ServeHTTP(fixtureScanWriter{w, meter}, r.WithContext(context.WithValue(r.Context(), fixtureScanMeterKey{}, meter)))
	})
}

type fixtureScanWriter struct {
	http.ResponseWriter
	meter *fixtureScanMeter
}

func (w fixtureScanWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w fixtureScanWriter) WriteHeader(status int) {
	if status == http.StatusOK {
		w.Header().Set(fixtureScanBytesHeader, strconv.FormatUint(w.meter.scanned, 10))
		w.Header().Set(fixtureScanWorkHeader, strconv.FormatUint(w.meter.charged, 10))
	}
	w.ResponseWriter.WriteHeader(status)
}

type fixtureScanWitnessKey struct{}

type fixtureScanTransport struct{ http.RoundTripper }

func (t fixtureScanTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.RoundTripper.RoundTrip(req)
	if witness, ok := req.Context().Value(fixtureScanWitnessKey{}).(*http.Header); ok && err == nil {
		*witness = res.Header
	}
	return res, err
}

func (f *runtimeQueryFixture) fetchObserved(ctx context.Context, req model.WireRequest) (model.WirePage, model.QueryScanObservation, error) {
	var scan model.QueryScanObservation
	var header http.Header
	page, err := f.fetch(context.WithValue(ctx, fixtureScanWitnessKey{}, &header), req)
	if err != nil {
		return page, scan, err
	}
	input, inputErr := strconv.ParseUint(header.Get(fixtureScanBytesHeader), 10, 64)
	work, workErr := strconv.ParseUint(header.Get(fixtureScanWorkHeader), 10, 64)
	if err := errors.Join(inputErr, workErr); err != nil {
		return page, scan, fmt.Errorf("read fixture scan counters: %w", err)
	}
	if work == 0 || work > uint64(f.budget) || input > work {
		return page, scan, fmt.Errorf("invalid fixture counters: scanned=%d charged=%d budget=%d", input, work, f.budget)
	}
	return page, model.QueryScanObservation{
		ScannedBytes:     &model.QueryCostCounter{Source: fixtureScanBytesSource, Value: input},
		ChargedWorkUnits: &model.QueryCostCounter{Source: fixtureScanWorkSource, Value: work},
	}, nil
}

func TestQueryCostsFixtureScanInputAndBudget(t *testing.T) {
	t.Parallel()
	epoch := time.Unix(1, 0).UTC()
	end := epoch.Add(time.Second)
	index := indexFixtureScanRows(nil, []store.Row{
		{TS: end, Line: "界 outside end"},
		{TS: epoch, Line: "界"},
		{TS: epoch, Line: "miss"},
		{TS: epoch.Add(-time.Second), Line: "界 before start"},
		{TS: epoch, Line: "界"},
	})
	for _, tc := range []struct {
		name, literal string
		budget, http  uint64
		cancel        bool
		wantBytes     uint64
		wantWork      uint64
		wantOrdinals  []uint64
		wantMore      bool
		wantErr       error
	}{
		{name: "tied match and lookahead", literal: "界", budget: 18, http: 18, wantBytes: 10, wantWork: 18, wantOrdinals: []uint64{1}, wantMore: true},
		{name: "absent matches still scan input", literal: "absent", budget: 18, http: 18, wantBytes: 10, wantWork: 18},
		{name: "local bound", literal: "界", budget: 17, http: 18, wantBytes: 7, wantWork: 15, wantErr: store.ErrQueryWorkExhausted},
		{name: "shared HTTP bound", literal: "界", budget: 18, http: 17, wantBytes: 7, wantWork: 15, wantErr: store.ErrQueryWorkExhausted},
		{name: "cancellation", literal: "界", budget: 18, http: 18, cancel: true, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			meter := fixtureScanMeter{budget: tc.budget}
			work := store.NewQueryWork(ctx, tc.http)
			req := httpapi.PageRequest{Filter: httpapi.PageFilter{Start: epoch, End: end, Contains: tc.literal}, Limit: 1}
			rows, more, err := meter.page(index, req, nil, work)
			if !errors.Is(err, tc.wantErr) || meter.scanned != tc.wantBytes || meter.charged != tc.wantWork || more != tc.wantMore {
				t.Fatalf("bytes=%d work=%d more=%t err=%v", meter.scanned, meter.charged, more, err)
			}
			var ordinals []uint64
			for _, row := range rows {
				ordinals = append(ordinals, row.Position.Ordinal)
			}
			if !slices.Equal(ordinals, tc.wantOrdinals) {
				t.Fatalf("ordinals=%v, want %v", ordinals, tc.wantOrdinals)
			}
			if err == nil {
				if err := work.Charge(1); !errors.Is(err, store.ErrQueryWorkExhausted) {
					t.Fatalf("fixture charges did not reach the HTTP work meter: %v", err)
				}
			}
		})
	}
}

func TestQueryCostsFixtureConcurrentObservations(t *testing.T) {
	t.Parallel()
	fixture := &runtimeQueryFixture{retained: true, meterScans: true, budget: 1024, concurrency: 2}
	t.Cleanup(func() {
		if err := fixture.stop(); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.start(); err != nil {
		t.Fatal(err)
	}
	adapter := fixture.adapter()
	epoch := time.Unix(1, 0).UTC()
	rows := []model.Row{{Time: epoch, Line: "x"}, {Time: epoch.Add(time.Second), Line: "界 distinct payload"}}
	if err := adapter.Acknowledge(t.Context(), rows); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Publish(t.Context(), len(rows)); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		t.Run(row.Line, func(t *testing.T) {
			t.Parallel()
			req := model.WireRequest{
				Filter: model.Filter{Start: row.Time, End: row.Time.Add(time.Second), Contains: row.Line},
				Limit:  1, MaxPageBytes: 256,
			}
			page, scan, err := adapter.FetchObserved(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Rows) != 1 || page.Rows[0].Line != row.Line || page.NextCursor != "" ||
				scan.ScannedBytes == nil || scan.ScannedBytes.Value != uint64(len(row.Line)) {
				t.Fatalf("request did not retain its own scan evidence: page=%+v scan=%+v", page, scan.ScannedBytes)
			}
		})
	}
}
