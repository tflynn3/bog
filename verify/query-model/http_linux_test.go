package querymodel_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	model "bog/verify/query-model"
)

type httpSnapshotPage struct {
	rows  []model.Row
	token string
}

func snapshotQuery(filter model.Filter, limit int, token string) string {
	q := url.Values{
		"start": {filter.Start.Format(time.RFC3339Nano)},
		"end":   {filter.End.Format(time.RFC3339Nano)},
		"limit": {strconv.Itoa(limit)},
	}
	if filter.Contains != "" {
		q.Set("contains", filter.Contains)
	}
	if token == "" {
		q.Set("page", "1")
	} else {
		q.Set("cursor", token)
	}
	return "/query?" + q.Encode()
}

func requestSnapshot(ctx context.Context, handler http.Handler, path string) (page httpSnapshotPage, err error) {
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil))
	res := out.Result()
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	if res.StatusCode != http.StatusOK {
		return httpSnapshotPage{}, fmt.Errorf("status %d: %s", res.StatusCode, out.Body.String())
	}
	if res.ContentLength != int64(out.Body.Len()) || res.Header.Get("Content-Type") != "application/x-ndjson" {
		return httpSnapshotPage{}, fmt.Errorf("unexpected framing: %v for %d bytes", res.Header, out.Body.Len())
	}
	values := res.Header.Values("Bog-Next-Cursor")
	if len(values) > 1 || len(values) == 1 && values[0] == "" {
		return httpSnapshotPage{}, fmt.Errorf("unexpected cursor header: %v", values)
	}
	page = httpSnapshotPage{token: res.Header.Get("Bog-Next-Cursor")}
	decoder := json.NewDecoder(res.Body)
	for {
		var row struct {
			TS   time.Time `json:"ts"`
			Line string    `json:"line"`
		}
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				return page, nil
			}
			return httpSnapshotPage{}, err
		}
		page.rows = append(page.rows, model.Row{Time: row.TS, Line: row.Line})
	}
}

func TestHTTPSnapshotWalkWorkBudgets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  int
		budget uint64
		limits []int
		tied   bool
	}{
		{"96 tied rows", 96, 128, []int{1}, true},
		{"8192 interleaved backfill rows", 8192, 1000, []int{5, 7}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial, late := make([]model.Row, tc.count), make([]model.Row, tc.count)
			for i := range initial {
				stamp := epoch
				line := fmt.Sprintf("hit%02d", i)
				if !tc.tied {
					stamp = epoch.Add(2 * time.Duration(i) * time.Microsecond)
					line = fmt.Sprintf("hit%04d %s", i, strings.Repeat("x", 27))
				}
				initial[i] = model.Row{Time: stamp, Line: line}
				lateStamp := stamp
				if !tc.tied {
					lateStamp = stamp.Add(time.Microsecond)
				}
				late[i] = model.Row{Time: lateStamp, Line: "hit after capture"}
			}
			s, log := loadHistory([][]model.Row{initial})
			h := httpapi.NewWithQueryLimits(&s.store, nil, httpapi.DefaultQueryMaxResponseBytes, 2, tc.budget)
			filter := model.Filter{Start: epoch, End: epoch.Add(time.Second), Contains: "hit"}
			highWater := log.HighWater()
			want, err := log.Match(highWater, filter)
			if err != nil {
				t.Fatal(err)
			}
			token := ""
			for page, offset := 0, 0; offset < len(want); page++ {
				limit := tc.limits[page%len(tc.limits)]
				var writer sync.WaitGroup
				if page == 1 {
					started := make(chan struct{})
					writer.Go(func() {
						close(started)
						s.publish(late)
					})
					<-started
				}
				got, err := requestSnapshot(t.Context(), h, snapshotQuery(filter, limit, token))
				writer.Wait()
				if page == 1 {
					log.Append(late)
				}
				if err != nil {
					t.Fatalf("page %d at offset %d (limit %d, budget %d, cursor length %d): %v",
						page+1, offset, limit, tc.budget, len(token), err)
				}
				n := min(limit, len(want)-offset)
				checkSnapshotRows(t, got, want[offset:offset+n], offset)
				offset += n
				if (got.token != "") != (offset < len(want)) || (got.token != "" && got.token == token) {
					t.Fatalf("unexpected continuation at offset %d: %q", offset, got.token)
				}
				token = got.token
			}
		})
	}
}

func checkSnapshotRows(t *testing.T, page httpSnapshotPage, want []model.Entry, offset int) {
	t.Helper()
	if len(page.rows) != len(want) {
		t.Fatalf("offset %d: got %d rows, want %d", offset, len(page.rows), len(want))
	}
	for i, row := range page.rows {
		if !row.Time.Equal(want[i].Time) || row.Line != want[i].Line {
			t.Fatalf("offset %d: got %+v, want %+v", offset+i, row, want[i])
		}
	}
}

func TestHTTPSnapshotByteCapAndIssuerReplacement(t *testing.T) {
	initial := make([]model.Row, 5)
	for i := range initial {
		initial[i] = model.Row{Time: epoch, Line: fmt.Sprintf("match %d", i)}
	}
	s, log := loadHistory([][]model.Row{initial})
	record, err := json.Marshal(store.Row{TS: epoch, Line: initial[0].Line})
	if err != nil {
		t.Fatal(err)
	}
	rowBytes := len(record) + 1
	newHandler := func(maxBytes int) http.Handler {
		return httpapi.NewWithQueryMaxResponseBytes(&s.store, nil, maxBytes)
	}
	issuer, replacement := newHandler(2*rowBytes), newHandler(2*rowBytes)
	filter := model.Filter{Start: epoch, End: epoch.Add(time.Second), Contains: "match"}
	want, err := log.Match(log.HighWater(), filter)
	if err != nil {
		t.Fatal(err)
	}
	firstPath := snapshotQuery(filter, 5, "")
	first, err := requestSnapshot(t.Context(), issuer, firstPath)
	if err != nil {
		t.Fatal(err)
	}
	checkSnapshotRows(t, first, want[:2], 0)
	if first.token == "" {
		t.Fatal("missing cursor after byte truncation")
	}

	continuation := snapshotQuery(filter, 5, first.token)
	tampered := []byte(first.token)
	middle := len(tampered) / 2
	if tampered[middle] == 'A' {
		tampered[middle] = 'B'
	} else {
		tampered[middle] = 'A'
	}
	changedFilter, changedBounds := filter, filter
	changedFilter.Contains = "mat"
	changedBounds.End = changedBounds.End.Add(time.Nanosecond)
	for _, tc := range []struct {
		name    string
		handler http.Handler
		path    string
	}{
		{"replacement", replacement, continuation},
		{"tampered", issuer, snapshotQuery(filter, 5, string(tampered))},
		{"changed filter", issuer, snapshotQuery(changedFilter, 5, first.token)},
		{"changed bounds", issuer, snapshotQuery(changedBounds, 5, first.token)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := httptest.NewRecorder()
			tc.handler.ServeHTTP(out, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if out.Code != http.StatusBadRequest || out.Body.String() != "invalid cursor\n" ||
				out.Header().Get("Bog-Next-Cursor") != "" {
				t.Fatalf("rejection status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
			}
		})
	}

	late := []model.Row{{Time: epoch, Line: "match 5"}}
	s.publish(late)
	log.Append(late)
	for i := 0; i < 2; i++ {
		second, err := requestSnapshot(t.Context(), issuer, continuation)
		if err != nil {
			t.Fatal(err)
		}
		checkSnapshotRows(t, second, want[2:4], 2)
		if second.token == "" {
			t.Fatal("missing second cursor after byte truncation")
		}
		last, err := requestSnapshot(t.Context(), issuer, snapshotQuery(filter, 5, second.token))
		if err != nil {
			t.Fatal(err)
		}
		checkSnapshotRows(t, last, want[4:], 4)
		if last.token != "" {
			t.Fatal("terminal page returned a cursor")
		}
	}
	fresh, err := log.Match(log.HighWater(), filter)
	if err != nil {
		t.Fatal(err)
	}
	token := ""
	for offset := 0; offset < len(fresh); {
		restart, err := requestSnapshot(t.Context(), replacement, snapshotQuery(filter, 5, token))
		if err != nil {
			t.Fatal(err)
		}
		n := min(2, len(fresh)-offset)
		checkSnapshotRows(t, restart, fresh[offset:offset+n], offset)
		offset += n
		if (restart.token != "") != (offset < len(fresh)) {
			t.Fatalf("restart at offset %d: unexpected continuation %q", offset, restart.token)
		}
		token = restart.token
	}

	out := httptest.NewRecorder()
	newHandler(rowBytes-1).ServeHTTP(out, httptest.NewRequest(http.MethodGet, firstPath, nil))
	if out.Code != http.StatusUnprocessableEntity || out.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("oversize status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
	}
}
