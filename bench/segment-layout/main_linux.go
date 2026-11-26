package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"
	"unsafe"

	"bog/internal/checkpoint"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

type measurement struct {
	Name            string    `json:"query"`
	Literal         string    `json:"literal"`
	Start           time.Time `json:"start_inclusive"`
	End             time.Time `json:"end_exclusive"`
	MatchingRows    int       `json:"matching_rows"`
	BlocksRead      int       `json:"logical_blocks_read"`
	PeakDecodeBytes int       `json:"peak_decode_bytes"`
	ColdSamplesNS   []int64   `json:"cold_samples_ns"`
	ColdP95NS       int64     `json:"cold_p95_ns"`
}

type layoutReport struct {
	Name          string        `json:"layout"`
	Blocks        int           `json:"blocks"`
	RetainedBytes int64         `json:"retained_bytes"`
	RewriteBytes  int64         `json:"rewrite_bytes"`
	Queries       []measurement `json:"queries"`
}

func writeGeneration(path string, blocks [][]checkpoint.BlockRow, l layout) (_ int64, err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := writeLayout(f, l, len(blocks), func(i int) ([]byte, summary, error) {
		rows := slices.Clone(blocks[i])
		slices.SortFunc(rows, ordered)
		var filter summary
		if l.prune {
			for _, row := range rows {
				filter.add(row.Row.Line)
			}
		}
		raw, err := checkpoint.EncodeBlock(rows)
		return raw, filter, err
	}); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func queryFile(r io.ReaderAt, size int64, q query) (matches []checkpoint.BlockRow, blocks, peak int, err error) {
	blocks, err = scanBlocks(r, size, q, func(raw []byte, buffers int) error {
		rows, err := checkpoint.DecodeBlock(raw)
		if err != nil {
			return err
		}
		working := buffers + len(rows)*int(unsafe.Sizeof(checkpoint.BlockRow{}))
		for _, row := range rows {
			working += len(row.Row.Line)
			if !row.Row.TS.Before(q.start) && row.Row.TS.Before(q.end) && strings.Contains(row.Row.Line, q.literal) {
				matches = append(matches, row)
			}
		}
		peak = max(peak, working)
		return nil
	})
	if err != nil {
		return nil, 0, 0, err
	}
	slices.SortFunc(matches, ordered)
	return matches, blocks, peak, nil
}

func measure(path string, size int64, q query, want []checkpoint.BlockRow, runs int) (m measurement, err error) {
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	fd := int(f.Fd())
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_RANDOM); err != nil {
		return m, fmt.Errorf("disable readahead: %w", err)
	}
	m = measurement{Name: q.name, Literal: q.literal, Start: q.start, End: q.end, MatchingRows: len(want)}
	for range runs {
		if err := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
			return m, fmt.Errorf("evict file pages: %w", err)
		}
		start := time.Now()
		got, blocks, peak, err := queryFile(f, size, q)
		m.ColdSamplesNS = append(m.ColdSamplesNS, time.Since(start).Nanoseconds())
		if err != nil {
			return m, err
		}
		if !slices.Equal(got, want) {
			return m, fmt.Errorf("ordered results differ from reference (%d rows vs %d)", len(got), len(want))
		}
		m.BlocksRead, m.PeakDecodeBytes = blocks, max(m.PeakDecodeBytes, peak)
	}
	samples := slices.Clone(m.ColdSamplesNS)
	slices.Sort(samples)
	m.ColdP95NS = samples[(len(samples)*95+99)/100-1]
	return m, nil
}

func run(parent, revision, profile string, batches, runs int, out io.Writer) (err error) {
	dir, err := os.MkdirTemp(parent, "bog-segment-layout-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return err
	}
	var kernel unix.Utsname
	if err := unix.Uname(&kernel); err != nil {
		return err
	}
	rows := fixture(batches, profile == "entropy")
	blocks := partition(rows)
	rewrite := append(slices.Clone(rows), checkpoint.BlockRow{Row: rows[len(rows)-2].Row, Ordinal: uint64(len(rows))})
	rewriteBlocks := partition(rewrite)
	qs := queries()
	want := make([][]checkpoint.BlockRow, len(qs))
	for i, q := range qs {
		want[i] = reference(rows, q)
	}
	var results []layoutReport
	for _, l := range layouts {
		path := filepath.Join(dir, l.name())
		retained, err := writeGeneration(path, blocks, l)
		if err != nil {
			return fmt.Errorf("%s initial generation: %w", l.name(), err)
		}
		item := layoutReport{Name: l.name(), Blocks: len(blocks), RetainedBytes: retained}
		for i, q := range qs {
			m, err := measure(path, retained, q, want[i], runs)
			if err != nil {
				return fmt.Errorf("%s/%s: %w", l.name(), q.name, err)
			}
			item.Queries = append(item.Queries, m)
		}
		item.RewriteBytes, err = writeGeneration(path+".rewrite", rewriteBlocks, l)
		if err != nil {
			return fmt.Errorf("%s rewritten generation: %w", l.name(), err)
		}
		if _, err := measure(path+".rewrite", item.RewriteBytes, qs[0], reference(rewrite, qs[0]), 1); err != nil {
			return fmt.Errorf("%s rewritten generation: %w", l.name(), err)
		}
		results = append(results, item)
	}
	return json.NewEncoder(out).Encode(map[string]any{
		"schema_version": 1, "revision": revision, "layouts": results,
		"fixture": map[string]any{
			"name": profile + "-log-v1", "seed": seed, "batches": batches, "batch_size_pattern": batchPattern,
			"rows": len(rows), "rewritten_rows": len(rewrite), "target_block_bytes": blockTarget, "samples_per_query": runs,
			"max_line_bytes": largeLineSize,
		},
		"environment": map[string]any{
			"os": runtime.GOOS, "arch": runtime.GOARCH, "go": runtime.Version(), "cpus": runtime.NumCPU(),
			"gomaxprocs": runtime.GOMAXPROCS(0), "kernel": unix.ByteSliceToString(kernel.Release[:]),
			"filesystem_magic": fmt.Sprintf("%#x", fs.Type), "filesystem_block_bytes": fs.Bsize,
			"cache_policy": "FADV_RANDOM + FADV_DONTNEED before each query", "work_directory": filepath.Dir(dir),
			"zstd_level": zstd.SpeedDefault.String(), "zstd_window_bytes": zstdWindow,
		},
	})
}

func sourceRevision() string {
	if build, ok := debug.ReadBuildInfo(); ok {
		var revision string
		var dirty bool
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				dirty = setting.Value == "true"
			}
		}
		if revision != "" {
			if dirty {
				return revision + "-dirty"
			}
			return revision
		}
	}
	return "unknown"
}

func main() {
	smoke := flag.Bool("smoke", false, "use the fixed smoke workload and one sample per query")
	batches := flag.Int("batches", 2048, "ordinary fixture batches")
	runs := flag.Int("runs", 21, "cold samples per query and layout")
	profile := flag.String("fixture", "mixed", "fixture profile (mixed or entropy)")
	dir := flag.String("dir", "", "parent for temporary benchmark files (defaults to OS temp directory)")
	revision := flag.String("revision", sourceRevision(), "source revision to record")
	flag.Parse()
	log.SetFlags(0)
	if *smoke {
		*batches, *runs = 64, 1
	}
	if *batches < 1 || *batches > 10000 || *runs < 1 || *runs > 1000 || flag.NArg() != 0 {
		log.Print("require 1–10000 batches, 1–1000 runs and no positional arguments")
		os.Exit(2)
	}
	if *profile != "mixed" && *profile != "entropy" {
		log.Print("require -fixture mixed or -fixture entropy")
		os.Exit(2)
	}
	if err := run(*dir, *revision, *profile, *batches, *runs, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
