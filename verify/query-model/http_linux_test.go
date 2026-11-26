package querymodel_test

import (
	"encoding/json"
	"fmt"
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

func httpQuery(handler http.Handler, filter model.Filter, limit int, token string) *httptest.ResponseRecorder {
	params := url.Values{
		"start":    {filter.Start.Format(time.RFC3339Nano)},
		"end":      {filter.End.Format(time.RFC3339Nano)},
		"contains": {filter.Contains},
		"limit":    {strconv.Itoa(limit)},
	}
	if token == "" {
		params.Set("page", "1")
	} else {
		params.Set("cursor", token)
	}
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, httptest.NewRequest(http.MethodGet, "/query?"+params.Encode(), nil))
	return out
}

func assertHTTPPage(t *testing.T, out *httptest.ResponseRecorder, want []model.Entry, more bool) string {
	t.Helper()
	if out.Code != http.StatusOK {
		t.Fatalf("status %d: %s", out.Code, out.Body.String())
	}
	body := out.Body.String()
	if out.Header().Get("Content-Length") != strconv.Itoa(len(body)) ||
		out.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("headers %v for %d body bytes", out.Header(), len(body))
	}
	for i, entry := range want {
		line, rest, ok := strings.Cut(body, "\n")
		var row store.Row
		if !ok {
			t.Fatalf("missing complete row %d", i)
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("decode row %d: %v", i, err)
		}
		if !row.TS.Equal(entry.Time) || row.Line != entry.Line {
			t.Fatalf("row %d: got %+v, want %+v", i, row, entry)
		}
		body = rest
	}
	token := out.Header().Get("Bog-Next-Cursor")
	if body != "" || (token != "") != more {
		t.Fatalf("remaining body %q, cursor %q, want more=%t", body, token, more)
	}
	return token
}

func TestHTTPSnapshotWorkBudget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		size     int
		budget   uint64
		limit    int
		backfill bool
	}{
		{"96 tied rows", 96, 128, 5, false},
		{"8192 backfilled rows", 8192, 1000, 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := make([]model.Row, tc.size)
			for i := range batch {
				stamp := epoch
				if tc.backfill {
					stamp = stamp.Add(time.Duration((i*4051)%tc.size/2) * time.Millisecond)
				}
				line := fmt.Sprintf("%02x", i%256)
				if tc.backfill {
					line += strings.Repeat(" backfill", 6)
				}
				batch[i] = model.Row{Time: stamp, Line: line}
			}
			s, log := loadHistory([][]model.Row{batch})
			filter := model.Filter{Start: epoch, End: epoch.Add(10 * time.Second)}
			want, err := log.Match(log.HighWater(), filter)
			if err != nil {
				t.Fatal(err)
			}
			handler := httpapi.NewWithQueryLimits(&s.store, nil, 32<<20, 1, tc.budget)
			limit := 5
			token := assertHTTPPage(t, httpQuery(handler, filter, limit, ""), want[:limit], true)
			// Force a large merged explicit run ahead of the offset-5 request.
			// Its new timestamps fall between original snapshot timestamps.
			if tc.backfill {
				late := make([]model.Row, tc.size)
				for i := range late {
					stamp := epoch.Add(time.Duration((i*31)%tc.size/2)*time.Millisecond + time.Nanosecond)
					late[i] = model.Row{Time: stamp, Line: "late backfill"}
				}
				s.publish(late)
				log.Append(late)
			}
			for offset := limit; offset < len(want); offset += tc.limit {
				last := min(offset+tc.limit, len(want))
				t.Run(fmt.Sprintf("offset=%d", offset), func(t *testing.T) {
					var writer sync.WaitGroup
					begin := make(chan struct{})
					batch := []model.Row{{Time: epoch.Add(time.Duration(offset%1000) * time.Millisecond), Line: "late"}}
					writer.Go(func() {
						<-begin
						s.publish(batch)
					})
					close(begin)
					out := httpQuery(handler, filter, tc.limit, token)
					writer.Wait()
					log.Append(batch)
					token = assertHTTPPage(t, out, want[offset:last], last < len(want))
				})
				if t.Failed() {
					break
				}
			}
		})
	}
}

func TestHTTPSnapshotRouting(t *testing.T) {
	rows := []model.Row{{Time: epoch, Line: "hit1"}, {Time: epoch, Line: "hit2"}, {Time: epoch, Line: "hit3"}}
	s, log := loadHistory([][]model.Row{rows})
	filter := model.Filter{Start: epoch, End: epoch.Add(time.Second), Contains: "hit"}
	want, err := log.Match(log.HighWater(), filter)
	if err != nil {
		t.Fatal(err)
	}
	size := len(`{"ts":"2025-01-02T00:00:00.000000123Z","line":"hit1"}` + "\n")
	issuer := httpapi.NewWithQueryMaxResponseBytes(&s.store, nil, size)
	replacement := httpapi.NewWithQueryMaxResponseBytes(&s.store, nil, size)
	token := assertHTTPPage(t, httpQuery(issuer, filter, 3, ""), want[:1], true)
	withOffset := filter
	withOffset.Start = filter.Start.In(time.FixedZone("offset", -5*60*60))
	next := assertHTTPPage(t, httpQuery(issuer, withOffset, 3, token), want[1:2], true)
	assertHTTPPage(t, httpQuery(issuer, filter, 3, next), want[2:], false)

	differentBounds := filter
	differentBounds.End = filter.End.Add(time.Nanosecond)
	differentLiteral := filter
	differentLiteral.Contains = "hit2"
	changed := "A"
	if token[0] == 'A' {
		changed = "B"
	}
	for _, tc := range []struct {
		name    string
		handler http.Handler
		filter  model.Filter
		token   string
	}{
		{"replacement", replacement, filter, token},
		{"tampered", issuer, filter, changed + token[1:]},
		{"malformed", issuer, filter, token + "%"},
		{"different bounds", issuer, differentBounds, token},
		{"different literal", issuer, differentLiteral, token},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := httpQuery(tc.handler, tc.filter, 3, tc.token)
			if out.Code != http.StatusBadRequest || out.Header().Get("Bog-Next-Cursor") != "" ||
				!strings.Contains(out.Body.String(), "invalid cursor") {
				t.Fatalf("status %d, headers %v, body %q", out.Code, out.Header(), out.Body.String())
			}
		})
	}
	late := []model.Row{{Time: epoch, Line: "hit4"}}
	s.publish(late)
	log.Append(late)
	want, err = log.Match(log.HighWater(), filter)
	if err != nil {
		t.Fatal(err)
	}
	token = ""
	for i, entry := range want {
		token = assertHTTPPage(t, httpQuery(replacement, filter, 3, token), []model.Entry{entry}, i+1 < len(want))
	}
}
