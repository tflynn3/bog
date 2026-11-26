package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

func TestCompactSnapshotCursor(t *testing.T) {
	a := newAPI(nil, nil, DefaultQueryMaxResponseBytes, DefaultQueryWorkBudget)
	stamp := time.Date(1, 1, 1, 0, 0, 0, 7, time.UTC)
	query := snapshotQuery{stamp, stamp.Add(time.Second), "needle"}
	key, err := a.snapshotKey(query, store.NewQueryWork(t.Context(), snapshotBounds+uint64(len(query.contains))))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []store.SnapshotCursor{
		{Count: 1, Ordinal: 0},
		{Count: 96, Ordinal: 95},
		{Count: 8192, Ordinal: 4096},
		{Count: int(^uint(0) >> 1), Ordinal: int(^uint(0)>>1) - 1},
	} {
		token := encodeSnapshotCursor(key, want)
		units := 1 + uint64(len(token)) + uint64(snapshotEncoding.DecodedLen(len(token)))
		for _, budget := range []uint64{units - 1, units} {
			got, err := decodeSnapshotCursor(key, token, store.NewQueryWork(t.Context(), budget))
			if budget < units {
				if !errors.Is(err, store.ErrQueryWorkExhausted) {
					t.Fatalf("%v at %d units: %v", want, budget, err)
				}
				continue
			}
			if err != nil || got != want {
				t.Fatalf("decoded %v, %v, want %v", got, err, want)
			}
		}
	}

	token := encodeSnapshotCursor(key, store.SnapshotCursor{Count: 2, Ordinal: 1})
	signed := func(payload []byte) string {
		return snapshotEncoding.EncodeToString(append(payload, snapshotTag(key, payload)...))
	}
	for _, invalid := range []string{
		"", token[:len(token)-1], token + "A", "%" + token[1:],
		signed([]byte{2, 2}), signed([]byte{0, 0}), signed([]byte{2, 1, 0}),
		signed([]byte{0x82, 0, 1}), signed([]byte{2, 0x81, 0}),
	} {
		if _, err := decodeSnapshotCursor(key, invalid, nil); !errors.Is(err, store.ErrInvalidQueryCursor) {
			t.Fatalf("invalid token %q: %v", invalid, err)
		}
	}
	for _, changed := range []snapshotQuery{
		{query.start.Add(time.Nanosecond), query.end, query.contains},
		{query.start, query.end.Add(time.Nanosecond), query.contains},
		{query.start, query.end, query.contains + "\x00"},
	} {
		other, err := a.snapshotKey(changed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeSnapshotCursor(other, token, nil); !errors.Is(err, store.ErrInvalidQueryCursor) {
			t.Fatalf("changed query %+v: %v", changed, err)
		}
	}
}

func TestParsedQueryTruncationWorkBoundary(t *testing.T) {
	s := new(store.Store)
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	row := store.Row{TS: stamp, Line: "same"}
	s.Publish([]store.Row{row, row, row, row, row})
	line := jsonRow(t, row)
	api := newAPI(s, nil, 2*len(line), DefaultQueryWorkBudget)
	query := snapshotQuery{stamp, stamp.Add(time.Second), ""}
	// One run and three seek units, then five row-and-line charges. The
	// omitted row is detected from its raw length before escaping its line.
	first := uint64(1 + 3 + 5*(1+len(row.Line)) + 2*len(row.Line) + snapshotBounds)
	delivery := uint64((2*len(line) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit)
	for _, tc := range []struct {
		name   string
		budget uint64
		status int
	}{
		{"before scope binding", first - 1, http.StatusServiceUnavailable},
		{"before delivery commit", first + delivery - 1, http.StatusServiceUnavailable},
		{"exact", first + delivery, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := httptest.NewRecorder()
			api.executeParsedQuery(out, query, 5, true, "", store.NewQueryWork(t.Context(), tc.budget))
			if tc.status != http.StatusOK {
				assertQueryRejection(t, out, tc.status, "query work budget exhausted")
				return
			}
			key, err := api.snapshotKey(query, nil)
			if err != nil {
				t.Fatal(err)
			}
			token := out.Header().Get("Bog-Next-Cursor")
			decoded, err := decodeSnapshotCursor(key, token, nil)
			if out.Code != http.StatusOK || out.Body.String() != strings.Repeat(line, 2) || err != nil ||
				decoded != (store.SnapshotCursor{Count: 5, Ordinal: 1}) {
				t.Fatalf("status %d body %q cursor %+v err %v", out.Code, out.Body.String(), decoded, err)
			}
		})
	}
}

func TestParsedQueryWorkBudgetWalk(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 123, time.UTC)
	query := snapshotQuery{stamp, stamp.Add(10 * time.Second), ""}
	for _, tc := range []struct {
		name       string
		size       int
		limit      int
		budget     uint64
		interleave bool
	}{
		{"tied", 96, 5, 128, false},
		{"backfill", 8192, 7, 1000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := new(store.Store)
			batch := make([]store.Row, tc.size)
			for i := range batch {
				at, line := stamp, "ab"
				if tc.interleave {
					at = at.Add(time.Duration((i*4051)%tc.size/2) * time.Millisecond)
					line = strings.Repeat(" backfill", 6) + line
				}
				batch[i] = store.Row{TS: at, Line: line}
			}
			s.Publish(batch)
			want := s.Query(query.start, query.end, "", tc.size)
			a := newAPI(s, nil, DefaultQueryMaxResponseBytes, tc.budget)
			token := ""
			for offset := 0; offset < len(want); {
				limit := tc.limit
				if offset == 0 {
					limit = 5
				}
				out := httptest.NewRecorder()
				a.executeParsedQuery(out, query, limit, token == "", token, store.NewQueryWork(t.Context(), tc.budget))
				last := min(offset+limit, len(want))
				var body strings.Builder
				for _, row := range want[offset:last] {
					body.WriteString(jsonRow(t, row))
				}
				token = out.Header().Get("Bog-Next-Cursor")
				if out.Code != http.StatusOK || out.Body.String() != body.String() || (token != "") != (last < len(want)) {
					t.Fatalf("offset %d, limit %d: status %d, body %q, cursor %q", offset, limit, out.Code, out.Body.String(), token)
				}
				if tc.interleave && offset == 0 {
					late := make([]store.Row, tc.size)
					for i := range late {
						at := stamp.Add(time.Duration((i*31)%tc.size/2)*time.Millisecond + time.Nanosecond)
						late[i] = store.Row{TS: at, Line: "late"}
					}
					s.Publish(late)
				}
				offset = last
			}
		})
	}
}
