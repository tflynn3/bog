package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMixedLoadKeepsOriginalCutoff(t *testing.T) {
	f := fixture{2048, 32, 64, 8, 1024}
	var mu sync.Mutex
	var appended, snapshot []row
	queryInFlight := make(chan struct{})
	overlappingAppend := make(chan struct{})
	var generation uint64
	covered := 0
	mixedActive := false
	var secondMixedPage, resumedAfterPublication bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		switch req.URL.Path {
		case "/compact":
			mu.Lock()
			defer mu.Unlock()
			cutoff, err := strconv.Atoi(q.Get("cutoff"))
			if err != nil {
				t.Error(err)
			}
			state := "complete"
			if q.Get("action") == "start" && cutoff == f.SeedRows+f.BatchRows {
				mixedActive = true
			}
			switch {
			case mixedActive && len(appended) < f.SeedRows+f.TailRows:
				state = "running"
			case cutoff > covered:
				generation++
				covered = cutoff
				mixedActive = false
			}
			if err := json.NewEncoder(w).Encode(compaction{generation, covered, len(appended), state}); err != nil {
				t.Error(err)
			}
		case "/ingest":
			var batch []row
			d := json.NewDecoder(req.Body)
			for {
				var r row
				err := d.Decode(&r)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				batch = append(batch, r)
			}
			mu.Lock()
			ordinal := len(appended)
			mu.Unlock()
			switch ordinal {
			case f.SeedRows + f.BatchRows:
				select {
				case <-queryInFlight:
				case <-req.Context().Done():
					return
				}
			case f.SeedRows + f.TailRows - f.BatchRows:
				// Delay the last commit until the runner has witnessed running
				// compaction after the overlapping pair.
				for {
					mu.Lock()
					observed := secondMixedPage
					mu.Unlock()
					if observed {
						break
					}
					select {
					case <-time.After(time.Millisecond):
					case <-req.Context().Done():
						return
					}
				}
			}
			mu.Lock()
			appended = append(appended, batch...)
			mu.Unlock()
			if ordinal == f.SeedRows+f.BatchRows {
				close(overlappingAppend)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/query":
			start, e1 := time.Parse(time.RFC3339Nano, q.Get("start"))
			end, e2 := time.Parse(time.RFC3339Nano, q.Get("end"))
			limit, e3 := strconv.Atoi(q.Get("limit"))
			offset := 0
			if q.Get("cursor") != "" {
				var err error
				offset, err = strconv.Atoi(q.Get("cursor"))
				if err != nil {
					t.Error(err)
				}
			}
			if e1 != nil || e2 != nil || e3 != nil {
				t.Error("invalid query")
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			active := mixedActive
			mu.Unlock()
			if offset == 1 && active {
				close(queryInFlight)
				select {
				case <-overlappingAppend:
				case <-req.Context().Done():
					return
				}
			}
			mu.Lock()
			if offset == 0 {
				snapshot = nil
				for _, r := range appended {
					if !r.TS.Before(start) && r.TS.Before(end) && strings.Contains(r.Line, q.Get("contains")) {
						snapshot = append(snapshot, r)
					}
				}
				slices.SortStableFunc(snapshot, func(a, b row) int { return a.TS.Compare(b.TS) })
			}
			if offset > 1 && mixedActive {
				secondMixedPage = true
			}
			if offset > 1 && !mixedActive && generation == 2 {
				resumedAfterPublication = true
			}
			if offset < 0 || offset > len(snapshot) {
				mu.Unlock()
				t.Error("query offset outside snapshot")
				w.WriteHeader(400)
				return
			}
			stop := min(offset+limit, len(snapshot))
			page := slices.Clone(snapshot[offset:stop])
			if stop < len(snapshot) {
				w.Header().Set("Bog-Next-Cursor", fmt.Sprint(stop))
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
			enc := json.NewEncoder(w)
			for _, r := range page {
				if err := enc.Encode(r); err != nil {
					t.Error(err)
				}
			}
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	s := newService(srv.URL)
	defer s.client.CloseIdleConnections()
	var r result
	if err := f.load(t.Context(), s, options{mode: "live", control: "/compact"}, &r); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	resumed := resumedAfterPublication
	mu.Unlock()
	if !resumed {
		t.Fatal("captured cursor did not survive compaction publication")
	}
	if n, err := f.recover(t.Context(), s); err != nil || n != f.SeedRows+f.TailRows {
		t.Fatalf("recover: %d %v", n, err)
	}
	if r.IngestSamples != f.TailRows/f.BatchRows || r.QuerySamples < 2 || r.Compaction == nil || r.Compaction.Generation != 2 {
		t.Fatalf("incomplete mixed evidence: %+v", r)
	}
}
