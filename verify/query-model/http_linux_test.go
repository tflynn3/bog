package querymodel_test

import (
	"encoding/json"
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

const nextCursorHeader = "Bog-Next-Cursor"

func snapshotRequest(h http.Handler, filter model.Filter, limit int, cursor string) *httptest.ResponseRecorder {
	params := url.Values{
		"start": {filter.Start.Format(time.RFC3339Nano)},
		"end":   {filter.End.Format(time.RFC3339Nano)},
		"limit": {strconv.Itoa(limit)},
	}
	if filter.Contains != "" {
		params.Set("contains", filter.Contains)
	}
	if cursor == "" {
		params.Set("page", "1")
	} else {
		params.Set("cursor", cursor)
	}
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, "/query?"+params.Encode(), nil))
	return out
}

func snapshotRows(out *httptest.ResponseRecorder) ([]store.Row, error) {
	if out.Code != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", out.Code, strings.TrimSpace(out.Body.String()))
	}
	if want := strconv.Itoa(out.Body.Len()); out.Header().Get("Content-Length") != want {
		return nil, fmt.Errorf("Content-Length = %q, want %s", out.Header().Get("Content-Length"), want)
	}
	var rows []store.Row
	decoder := json.NewDecoder(out.Body)
	for {
		var row store.Row
		switch err := decoder.Decode(&row); err {
		case io.EOF:
			return rows, nil
		case nil:
			rows = append(rows, row)
		default:
			return nil, err
		}
	}
}

func TestHTTPSnapshotWalkConcurrentBackfill(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  int
		limits []int
		tied   bool
	}{
		{"96 tied rows", 96, []int{5, 5, 2}, true},
		{"8192 rows with backfill", 8192, []int{5, 7}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial := make([]model.Row, tc.count)
			backfill := make([]model.Row, tc.count)
			for i := range initial {
				stamp := epoch
				if !tc.tied {
					stamp = stamp.Add(time.Duration(i) * time.Millisecond)
				}
				line := strconv.FormatInt(int64(i), 36)
				if !tc.tied {
					line = fmt.Sprintf("needle %030d", i)
				}
				initial[i] = model.Row{Time: stamp, Line: line}
				backfill[i] = model.Row{Time: stamp.Add(time.Nanosecond), Line: "needle late"}
			}
			if !tc.tied {
				// The first continuation starts after row five, just before a
				// long invisible gap. The remaining backfill interleaves.
				for i := range backfill[:len(backfill)/2] {
					backfill[i].Time = initial[4].Time.Add(time.Nanosecond)
				}
			}

			// Publication races continuations against changing run layouts.
			s, log := loadHistory([][]model.Row{initial})
			h := httpapi.New(&s.store, nil)
			filter := model.Filter{Start: epoch, End: epoch.Add(time.Hour)}
			if !tc.tied {
				filter.Contains = "needle"
			}
			want, err := log.Match(log.HighWater(), filter)
			if err != nil {
				t.Fatal(err)
			}

			cursor := ""
			for offset, page := 0, 0; offset < len(want); page++ {
				limit := tc.limits[page%len(tc.limits)]
				var writer sync.WaitGroup
				if page == 1 {
					s.publish(backfill[:len(backfill)/2])
					writer.Go(func() { s.publish(backfill[len(backfill)/2:]) })
				}
				out := snapshotRequest(h, filter, limit, cursor)
				writer.Wait()
				next := out.Header().Get(nextCursorHeader)
				got, err := snapshotRows(out)
				if err != nil {
					t.Fatalf("offset %d limit %d: %v", offset, limit, err)
				}
				n := min(limit, len(want)-offset)
				if len(got) != n {
					t.Fatalf("offset %d: got %d rows, want %d", offset, len(got), n)
				}
				for i, row := range got {
					wantRow := want[offset+i]
					if !row.TS.Equal(wantRow.Time) || row.Line != wantRow.Line {
						t.Fatalf("offset %d: got %+v, want %+v", offset+i, row, wantRow)
					}
				}
				offset += n
				if (next != "") != (offset < len(want)) || (next != "" && next == cursor) {
					t.Fatalf("offset %d: cursor did not advance or terminate", offset)
				}
				cursor = next
			}
		})
	}
}

func TestHTTPSnapshotCursorRoutingAndByteCap(t *testing.T) {
	filter := model.Filter{Start: epoch, End: epoch.Add(time.Second)}
	s, log := loadHistory([][]model.Row{{
		{Time: epoch, Line: "a"},
		{Time: epoch, Line: "b"},
		{Time: epoch, Line: "c"},
	}})
	record, err := json.Marshal(store.Row{TS: epoch, Line: "a"})
	if err != nil {
		t.Fatal(err)
	}
	// A limit of three rows is cut down to two by the encoded byte cap.
	maxBytes := 2 * (len(record) + 1)
	issuer := httpapi.NewWithQueryMaxResponseBytes(&s.store, nil, maxBytes)
	replacement := httpapi.NewWithQueryMaxResponseBytes(&s.store, nil, maxBytes)
	want, err := log.Match(log.HighWater(), filter)
	if err != nil {
		t.Fatal(err)
	}
	checkPage := func(h http.Handler, token string, offset, n int, more bool) string {
		t.Helper()
		out := snapshotRequest(h, filter, 3, token)
		next := out.Header().Get(nextCursorHeader)
		rows, err := snapshotRows(out)
		if err != nil || len(rows) != n || (next != "") != more {
			t.Fatalf("offset %d: rows=%+v cursor=%t error=%v", offset, rows, next != "", err)
		}
		for i, row := range rows {
			w := want[offset+i]
			if !row.TS.Equal(w.Time) || row.Line != w.Line {
				t.Fatalf("row %d = %+v, want %+v", offset+i, row, w)
			}
		}
		return next
	}
	token := checkPage(issuer, "", 0, 2, true)
	modified := "A"
	if token[0] == 'A' {
		modified = "B"
	}
	modified += token[1:]
	for _, tc := range []struct {
		name   string
		server http.Handler
		filter model.Filter
		token  string
	}{
		{"replacement", replacement, filter, token},
		{"tampered", issuer, filter, modified},
		{"changed literal", issuer, model.Filter{Start: filter.Start, End: filter.End, Contains: "a"}, token},
		{"changed start", issuer, model.Filter{Start: filter.Start.Add(time.Nanosecond), End: filter.End}, token},
		{"changed end", issuer, model.Filter{Start: filter.Start, End: filter.End.Add(time.Second)}, token},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := snapshotRequest(tc.server, tc.filter, 3, tc.token)
			if out.Code != http.StatusBadRequest || out.Header().Get(nextCursorHeader) != "" || out.Body.String() != "invalid cursor\n" {
				t.Fatalf("status %d headers %v body %q", out.Code, out.Header(), out.Body.String())
			}
		})
	}
	checkPage(issuer, token, 2, 1, false)
	checkPage(replacement, "", 0, 2, true)
}
