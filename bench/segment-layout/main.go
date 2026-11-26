// segment-layout compares file-backed checkpoint block layouts.
package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

var (
	first = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	end   = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	epoch = time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)
)

type probe struct {
	Name    string    `json:"name"`
	Literal string    `json:"literal"`
	Start   time.Time `json:"start_inclusive"`
	End     time.Time `json:"end_exclusive"`
}

var probes = []probe{
	{"rare", "RARE[42]", first, end},
	{"absent", "BOG_ABSENT", first, end},
	{"one_byte", "G", first, end},
	{"two_bytes", "é", first, end},
	{"three_bytes", "GET", first, end},
	{"oversized", "LARGE_END", first, end},
	{"half_open", "GET", epoch, epoch.Add(time.Second)},
	{"all", "", first, end},
}

func fixture(n int) ([]checkpoint.BlockRow, error) {
	rows := make([]checkpoint.BlockRow, 0, n+1)
	zone := time.FixedZone("fixture offset", -7*3600)
	for i := range n {
		ts := epoch.Add(time.Duration(i%997*313%997) * time.Second)
		line := fmt.Sprintf("level=info GET /api/%c status=200 café elapsed=17ms %s",
			'a'+i%8, strings.Repeat("request complete ", 6))
		switch i {
		case 0:
			ts, line = epoch.In(zone), "level=warn GET RARE[42]"
		case n - 1:
			ts, line = epoch, "level=warn GET RARE[42]"
		case 1:
			ts = first
		case 2:
			ts = end.Add(-time.Nanosecond)
		}
		rows = append(rows, checkpoint.BlockRow{Row: store.Row{TS: ts, Line: line}, Ordinal: uint64(i)})
	}
	// One complete legal MaxBatch frame. The high-cardinality prefix triggers
	// summary overflow while the suffix tests exact matching near the frame limit.
	var prefix []byte
	for i := range 512 {
		prefix = fmt.Appendf(prefix, "#%04x", i)
	}
	const head, tail, marker = `{"ts":"2025-03-01T12:00:00Z","line":"`, `"}`, "LARGE_END"
	line := string(prefix) + strings.Repeat("x", store.MaxBatch-len(head)-len(tail)-len(marker)-len(prefix)) + marker
	decoded, err := store.Decode([]byte(head + line + tail))
	if err != nil {
		return nil, fmt.Errorf("decode oversized fixture: %w", err)
	}
	return append(rows, checkpoint.BlockRow{Row: decoded[0], Ordinal: uint64(n)}), nil
}

func partition(rows []checkpoint.BlockRow, blockRows int) [][]checkpoint.BlockRow {
	var blocks [][]checkpoint.BlockRow
	for len(rows) > 0 {
		size, count := checkpointHead, 0
		for count < min(blockRows, len(rows)) {
			next := checkpointRow + len(rows[count].Row.Line)
			if size+next > checkpoint.MaxBlockSize {
				break
			}
			size += next
			count++
		}
		// Pass even an invalid singleton to EncodeBlock so it reports the error.
		count = max(1, count)
		blocks = append(blocks, rows[:count])
		rows = rows[count:]
	}
	return blocks
}

func oracle(rows []checkpoint.BlockRow, p probe) []checkpoint.BlockRow {
	var matches []checkpoint.BlockRow
	for _, r := range rows {
		ts := r.Row.TS.UTC()
		if ts.Compare(p.Start.UTC()) >= 0 && ts.Compare(p.End.UTC()) < 0 && strings.Index(r.Row.Line, p.Literal) >= 0 {
			matches = append(matches, r)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		a, b := matches[i], matches[j]
		if a.Row.TS.Equal(b.Row.TS) {
			return a.Ordinal < b.Ordinal
		}
		return a.Row.TS.Before(b.Row.TS)
	})
	return matches
}

func equalRow(a, b checkpoint.BlockRow) bool {
	return a.Ordinal == b.Ordinal && a.Row.TS.Equal(b.Row.TS) && a.Row.Line == b.Row.Line
}

type queryResult struct {
	readStats
	Probe       string `json:"probe"`
	Cache       string `json:"cache"`
	FirstReadNS int64  `json:"first_read_ns"`
	Matches     int    `json:"matches"`
}

func query(f *os.File, kind layout, p probe) ([]checkpoint.BlockRow, readStats, error) {
	var matches []checkpoint.BlockRow
	stats, err := scan(f, kind, p.Literal, false, func(_ int, rows []checkpoint.BlockRow) error {
		for _, r := range rows {
			if !r.Row.TS.Before(p.Start) && r.Row.TS.Before(p.End) && strings.Contains(r.Row.Line, p.Literal) {
				matches = append(matches, r)
			}
		}
		return nil
	})
	if err != nil {
		return nil, stats, err
	}
	slices.SortFunc(matches, func(a, b checkpoint.BlockRow) int {
		if c := a.Row.TS.Compare(b.Row.TS); c != 0 {
			return c
		}
		return cmp.Compare(a.Ordinal, b.Ordinal)
	})
	return matches, stats, nil
}

type layoutResult struct {
	Layout   string        `json:"layout"`
	Retained int64         `json:"retained_bytes"`
	Rewrite  int64         `json:"rewrite_bytes"`
	Verified readStats     `json:"full_verifier"`
	Queries  []queryResult `json:"queries"`
}

func measure(path string, kind layout, blocks [][]checkpoint.BlockRow, expected [][]checkpoint.BlockRow) (result layoutResult, err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	result.Layout = layouts[kind]
	result.Rewrite, err = writeLayout(f, kind, blocks)
	if err != nil {
		return result, err
	}
	if err := f.Sync(); err != nil {
		return result, err
	}
	st, err := f.Stat()
	if err != nil {
		return result, err
	}
	result.Retained = st.Size()
	for i, p := range probes {
		cache := prepareCache(f)
		start := time.Now()
		matches, stats, err := query(f, kind, p)
		elapsed := time.Since(start)
		if err != nil {
			return result, fmt.Errorf("query %s: %w", p.Name, err)
		}
		if !slices.EqualFunc(matches, expected[i], equalRow) {
			return result, fmt.Errorf("query %s differs from fixture scan (got %d rows, want %d)", p.Name, len(matches), len(expected[i]))
		}
		result.Queries = append(result.Queries, queryResult{stats, p.Name, cache, elapsed.Nanoseconds(), len(matches)})
	}
	result.Verified, err = scan(f, kind, "", true, func(i int, rows []checkpoint.BlockRow) error {
		if i >= len(blocks) || !slices.EqualFunc(rows, blocks[i], equalRow) {
			return fmt.Errorf("verified block %d differs from fixture", i)
		}
		return nil
	})
	if err == nil && result.Verified.BlocksRead != len(blocks) {
		err = errors.New("verified block count differs from fixture")
	}
	return result, err
}

func run(w io.Writer, parent string, n, blockRows int) (err error) {
	if n < 4 || n > 1_000_000 || blockRows < 1 {
		return errors.New("rows must be in [4, 1000000] and block-rows must be positive")
	}
	rows, err := fixture(n)
	if err != nil {
		return err
	}
	blocks := partition(rows, blockRows)
	expected := make([][]checkpoint.BlockRow, len(probes))
	for i, p := range probes {
		expected[i] = oracle(rows, p)
	}
	dir, err := os.MkdirTemp(parent, "bog-segment-layout-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()

	report := struct {
		Fixture       string         `json:"fixture"`
		Rows          int            `json:"rows"`
		BlockRows     int            `json:"target_block_rows"`
		Blocks        int            `json:"blocks"`
		Oversized     int            `json:"oversized_line_bytes"`
		GOOS          string         `json:"goos"`
		GOARCH        string         `json:"goarch"`
		GoVersion     string         `json:"go_version"`
		Threads       int            `json:"gomaxprocs"`
		Directory     string         `json:"benchmark_directory"`
		LatencyMethod string         `json:"latency_method"`
		MemoryMethod  string         `json:"memory_method"`
		RewriteMethod string         `json:"rewrite_method"`
		Probes        []probe        `json:"probes"`
		Layouts       []layoutResult `json:"layouts"`
	}{
		Fixture: "synthetic-retained-log-v1", Rows: len(rows), BlockRows: blockRows, Blocks: len(blocks),
		Oversized: len(rows[n].Row.Line), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GoVersion: runtime.Version(), Threads: runtime.GOMAXPROCS(0), Directory: dir,
		LatencyMethod: "one file query per probe after reported cache preparation, including metadata, decode, exact matching and result sort",
		MemoryMethod:  "max over decoded blocks of stored buffer + distinct uncompressed buffer + decoded row structs and line bytes; excludes metadata, zlib workspace, retained query results, allocator overhead and OS cache",
		RewriteMethod: "bytes accepted by writes for one complete snapshot rewrite into a fresh file, including framing and summaries",
		Probes:        probes,
	}
	for kind, name := range layouts {
		result, err := measure(filepath.Join(dir, name+".bin"), layout(kind), blocks, expected)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		report.Layouts = append(report.Layouts, result)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func main() {
	n := flag.Int("rows", 50_000, "number of ordinary fixture rows, plus one oversized row")
	blockRows := flag.Int("block-rows", 512, "maximum rows per block")
	dir := flag.String("dir", "", "parent directory for temporary benchmark files (default OS temp)")
	flag.Parse()
	if err := run(os.Stdout, *dir, *n, *blockRows); err != nil {
		log.Fatal(err)
	}
}
