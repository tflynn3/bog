package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/client"
	"bog/internal/store"
)

func TestColdConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args []string
		ok   bool
	}{
		{"cold", []string{"-cold", "-smoke"}, true},
		{"candidate", []string{"-cold", "-bog-bin", "bog", "-bog-revision", "candidate"}, true},
		{"partial candidate", []string{"-cold", "-bog-bin", "bog"}, false},
		{"competing modes", []string{"-cold", "-incremental"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(tc.args, "test", io.Discard)
			if (err == nil) != tc.ok || (tc.ok && !cfg.cold) {
				t.Fatalf("parseConfig = %+v, %v", cfg, err)
			}
		})
	}
}

func TestColdProcessReadCounters(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, before, after string
		ok                  bool
	}{
		{"logical and storage", "rchar: 100\nread_bytes: 0\n", "rchar: 154\nread_bytes: 4096\n", true},
		{"logical only due to cache", "rchar: 100\nread_bytes: 0\n", "rchar: 154\nread_bytes: 0\n", true},
		{"bad integer", "rchar: bad\nread_bytes: 0\n", "rchar: 154\nread_bytes: 4096\n", false},
		{"absent field", "rchar: 100\n", "rchar: 154\nread_bytes: 4096\n", false},
		{"duplicate reads", "rchar: 100\nread_bytes: 0\n", "rchar: 154\nread_bytes: 4096\nread_bytes: 0\n", false},
		{"negative storage", "rchar: 100\nread_bytes: 0\n", "rchar: 154\nread_bytes: -1\n", false},
		{"cannot subtract observer", "rchar: 100\nread_bytes: 0\n", "rchar: 101\nread_bytes: 4096\n", false},
		{"decreasing read bytes", "rchar: 100\nread_bytes: 5000\n", "rchar: 154\nread_bytes: 4096\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, e1 := parseReadIO([]byte(tc.before))
			after, e2 := parseReadIO([]byte(tc.after))
			logical, storage, e3 := after.delta(before)
			if (e1 == nil && e2 == nil && e3 == nil) != tc.ok {
				t.Fatalf("counters parsed as %+v, %+v, errors: %v, %v, %v", before, after, e1, e2, e3)
			}
			if tc.ok && (logical != 54-int64(len(tc.before)) || storage != after.storage) {
				t.Fatalf("incorrect delta: %d, %d", logical, storage)
			}
		})
	}
}

func TestColdReportValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*coldReport)
	}{
		{"valid", nil},
		{"cache policy", func(r *coldReport) { r.Environment["cache_policy"] = "" }},
		{"missing parity", func(r *coldReport) { r.Oracle = "" }},
		{"duplicate layout", func(r *coldReport) { r.Experimental[1].Name = r.Experimental[0].Name }},
		{"wrong phase", func(r *coldReport) { r.Production.Phases[2].Phase = "clustered" }},
		{"missing sparse sample", func(r *coldReport) { r.Production.Phases[0].Queries[0].Samples = nil }},
		{"wrong p95", func(r *coldReport) { r.Experimental[0].Phases[0].Queries[0].P95NS++ }},
		{"wrong memory peak", func(r *coldReport) { r.Production.Phases[0].Queries[0].PeakDecodeHeapUpperBoundBytes++ }},
		{"missing decode bytes", func(r *coldReport) { r.Experimental[0].Phases[0].Queries[0].Samples[0].DecodeBytes = nil }},
		{"introduced row", func(r *coldReport) { r.Experimental[3].Phases[1].Queries[0].Matches++ }},
		{"Bloom filters a short literal", func(r *coldReport) {
			r.Experimental[3].Phases[0].Queries[3].Samples[0].Payload.Bytes--
		}},
		{"unmatched WAL denominator", func(r *coldReport) {
			a := r.Experimental[0].Phases[1].Rewrite
			*a = writeAmplification(a.WrittenBytes, a.AppendedWALBytes+1, a.Boundary)
		}},
		{"invalid live", func(r *coldReport) { r.Live = liveReport{Status: "measured"} }},
		{"valid live", func(r *coldReport) {
			r.Live = liveReport{Status: "measured", Revision: strings.Repeat("a", 40), BinarySHA256: strings.Repeat("b", 64)}
			for _, p := range r.Production.Phases[1:] {
				r.Live.Measurements = append(r.Live.Measurements, publicationMeasurement{
					Scenario: p.Phase, CoveredRows: p.Rows, ElapsedNS: 1, HTTPQueryParity: true,
					WriteAmplification: writeAmplification(4096, p.Rewrite.AppendedWALBytes, liveBoundary),
				})
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			report := testColdReport()
			if tc.edit != nil {
				tc.edit(&report)
			}
			data, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			ok := tc.edit == nil || tc.name == "valid live"
			if err := validateBenchmarkReport(bytes.NewReader(data)); (err == nil) != ok {
				t.Fatalf("validateBenchmarkReport = %v, want success %t", err, ok)
			}
			if ok && validateBenchmarkReport(bytes.NewReader(append(data, []byte("{}")...))) == nil {
				t.Fatal("accepted trailing report data")
			}
		})
	}
}

func testColdReport() coldReport {
	report := coldReport{
		SchemaVersion: 5, Mode: "cold", Revision: "test", Oracle: coldOracle,
		Fixture:     coldFixture{"mixed", "custom", seed, 1, 2, blockTarget},
		Environment: map[string]any{"os": "linux", "cache_policy": coldCachePolicy},
		Live:        liveReport{Status: "unavailable", Reason: "no pinned binary supplied"},
	}
	makeLayout := func(name string, production, bloom bool) coldLayout {
		result := coldLayout{Name: name}
		for i, phase := range coldPhases {
			p := coldPhase{Phase: phase, Rows: 6, Segments: 2, ManifestBytes: 120}
			if bloom {
				p.BloomBytes = 2 * int64(len(summary{}))
			}
			p.RetainedBytes = 8000 + p.BloomBytes
			p.RewrittenBytes = p.RetainedBytes
			if i > 0 {
				p.Rows += 4
				p.ReusedBytes = 4000 + p.BloomBytes/2
				p.RewrittenBytes -= p.ReusedBytes
				p.RetiredBytes = p.RewrittenBytes
				boundary := coldRewriteBoundary
				if production {
					boundary = componentBoundary
				}
				a := writeAmplification(uint64(p.RewrittenBytes+p.ManifestBytes), 300, boundary)
				p.Rewrite = &a
			}
			for _, q := range coldQueries() {
				m := coldQueryReport{Query: q.name, Literal: q.literal, Start: q.start, End: q.end, Matches: 1}
				if q.name == "all" {
					m.Matches = p.Rows
				} else if q.name == "absent" {
					m.Matches = 0
				}
				for run := range coldRuns(q, report.Fixture.SparseRuns) {
					s := coldSample{ElapsedNS: int64(run+1) * 10, ReadBytes: 8000, StorageReadBytes: int64(run) * 4096,
						GoAllocationBytes: 10000, PeakRSSBytes: 40000}
					if !production {
						s.Payload = &payloadReads{Blocks: 2, Bytes: 5000}
						s.DecodeBytes = new(6000)
					}
					m.Samples = append(m.Samples, s)
				}
				m.summarize()
				p.Queries = append(p.Queries, m)
			}
			result.Phases = append(result.Phases, p)
		}
		return result
	}
	report.Production = makeLayout(productionV1, true, false)
	for _, l := range layouts {
		report.Experimental = append(report.Experimental, makeLayout(l.name(), false, l.prune))
	}
	return report
}

func TestColdLiveQueryOrderedParity(t *testing.T) {
	t.Parallel()
	at := epoch.Format(time.RFC3339)
	cursor := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("c", 40)))
	first := fmt.Sprintf("{\"ts\":%q,\"line\":\"é-one\"}\n", at)
	second := fmt.Sprintf("{\"ts\":%q,\"line\":\"é-two\"}\n", at)
	oracle := []checkpoint.BlockRow{
		{Row: store.Row{TS: epoch, Line: "é-one"}, Ordinal: 2},
		{Row: store.Row{TS: epoch, Line: "é-two"}, Ordinal: 3},
	}
	for _, tc := range []struct {
		name, opening, terminal string
		ok                      bool
	}{
		{"exact pages", first, second, true},
		{"tie reordered", second, first, false},
		{"short literal false positive", first, strings.Replace(second, "é", "e", 1), false},
		{"extra duplicate", first, second + second, false},
		{"missing row", first, "", false},
		{"missing newline", first, strings.TrimSuffix(second, "\n"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				params := r.URL.Query()
				if r.URL.Path != "/query" || params.Get("contains") != "é" {
					t.Errorf("unexpected request: %s", r.URL)
					http.Error(w, "bad query", http.StatusBadRequest)
					return
				}
				data := tc.terminal
				if params.Get("cursor") == "" {
					w.Header().Set("Bog-Next-Cursor", cursor)
					data = tc.opening
				}
				w.Header().Set("Content-Type", "application/x-ndjson")
				w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				if _, err := io.WriteString(w, data); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			c, err := client.New(server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = checkLiveQuery(ctx, c, client.Query{Start: epoch, End: epoch.Add(time.Second), Contains: "é"}, oracle)
			if (err == nil) != tc.ok {
				t.Fatalf("checkLiveQuery = %v, want success %t", err, tc.ok)
			}
		})
	}
}

func TestBloomTailReplacementKeepsPredecessorReadable(t *testing.T) {
	for _, l := range layouts {
		t.Run(l.name(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			rows := []checkpoint.BlockRow{
				{Row: store.Row{TS: epoch.Add(time.Second), Line: strings.Repeat("sealed", 80)}, Ordinal: 0},
				{Row: store.Row{TS: epoch, Line: "é duplicate"}, Ordinal: 1},
			}
			path, err := publishSegments(t.Context(), dir, "initial", snapshot{}, rows, l, 560)
			if err != nil {
				t.Fatal(err)
			}
			before, err := readSnapshot(path)
			if err != nil {
				t.Fatal(err)
			}
			duplicate := rows[1]
			duplicate.Ordinal++
			path, err = publishSegments(t.Context(), dir, "late", before, []checkpoint.BlockRow{duplicate}, l, 560)
			if err != nil {
				t.Fatal(err)
			}
			after, err := readSnapshot(path)
			if err != nil {
				t.Fatal(err)
			}
			bytes, err := accountSegments(before, after)
			if err != nil || len(before.Segments) != 2 || len(after.Segments) != 2 ||
				bytes.ReusedBytes != before.Segments[0].FileBytes || bytes.RetiredBytes != before.Segments[1].FileBytes ||
				after.Segments[1].Bloom != l.prune {
				t.Fatalf("replacement accounting = %+v, %v, before %+v after %+v", bytes, err, before, after)
			}
			for _, trial := range []struct {
				snap snapshot
				want []checkpoint.BlockRow
			}{{before, rows}, {after, append(rows, duplicate)}} {
				q := query{start: firstTime, end: endTime, literal: "é"}
				got, err := querySnapshot(t.Context(), dir, trial.snap, 560, q)
				if err != nil {
					t.Fatal(err)
				}
				if err := checkSnapshotRows(got, reference(trial.want, q)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
