// Command segment-layout compares experimental checkpoint block layouts.
package main

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

const (
	seed         = 17044
	summaryBytes = 256
	oversized    = 4096
	rowsPerBlock = 128
)

type variant struct {
	name                string
	compressed, summary bool
}
type block struct {
	offset     int64
	size       uint32
	summary    [summaryBytes]byte
	incomplete bool
}
type layout struct {
	variant  variant
	path     string
	blocks   []block
	retained int64
}

// Every four-byte gram sets one bit. Collisions only cause extra reads. A line
// over the indexing limit marks its whole block incomplete, so it is never pruned.
func addGram(s *[summaryBytes]byte, gram string) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(gram))
	n := h.Sum32() % (summaryBytes * 8)
	s[n/8] |= 1 << (n % 8)
}
func summarize(rows []checkpoint.BlockRow) (s [summaryBytes]byte, incomplete bool) {
	for _, r := range rows {
		if len(r.Row.Line) > oversized {
			incomplete = true
			continue
		}
		for i := 0; i+4 <= len(r.Row.Line); i++ {
			addGram(&s, r.Row.Line[i:i+4])
		}
	}
	return
}
func mayContain(b block, literal string) bool {
	if len(literal) < 4 || b.incomplete {
		return true
	}
	for i := 0; i+4 <= len(literal); i++ {
		var bit [summaryBytes]byte
		addGram(&bit, literal[i:i+4])
		for j := range bit {
			if bit[j]&b.summary[j] != bit[j] {
				return false
			}
		}
	}
	return true
}
func fixture(n int) [][]checkpoint.BlockRow {
	rng := rand.New(rand.NewSource(seed))
	rows := make([]checkpoint.BlockRow, 0, n+4)
	for i := 0; i < n; i++ {
		line := fmt.Sprintf("service=api region=%02d level=info request=%08x message=steady-state", rng.Intn(16), rng.Uint32())
		if i == n/3 {
			line += " rare-needle-17"
		}
		if i == n/2 {
			line = strings.Repeat("oversized-data-", 400) + " giant-needle-17"
		}
		rows = append(rows, checkpoint.BlockRow{Row: store.Row{TS: time.Unix(1700000000+int64(i), 0).UTC(), Line: line}, Ordinal: uint64(i)})
	}
	// Identical rows and identities ensure the oracle catches accidental deduplication.
	rows = append(rows, rows[n/3], rows[n/3])
	var blocks [][]checkpoint.BlockRow
	for len(rows) > 0 {
		k := rowsPerBlock
		if k > len(rows) {
			k = len(rows)
		}
		blocks = append(blocks, rows[:k])
		rows = rows[k:]
	}
	return blocks
}
func encode(rows []checkpoint.BlockRow, compressed bool) ([]byte, error) {
	raw, err := checkpoint.EncodeBlock(rows)
	if err != nil {
		return nil, err
	}
	if !compressed {
		return raw, nil
	}
	var out bytes.Buffer
	w, err := flate.NewWriter(&out, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err = w.Write(raw); err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
func decode(data []byte, compressed bool) ([]checkpoint.BlockRow, error) {
	if compressed {
		r := flate.NewReader(bytes.NewReader(data))
		defer r.Close()
		var err error
		data, err = io.ReadAll(io.LimitReader(r, int64(checkpoint.MaxBlockSize)+1))
		if err != nil {
			return nil, err
		}
		if len(data) > checkpoint.MaxBlockSize {
			return nil, fmt.Errorf("decompressed block too large")
		}
	}
	return checkpoint.DecodeBlock(data)
}

// Records are length, optional incomplete flag and bitmap, then encoded block.
func writeLayout(path string, v variant, rows [][]checkpoint.BlockRow) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, group := range rows {
		data, e := encode(group, v.compressed)
		if e != nil {
			_ = f.Close()
			return 0, e
		}
		var header [4]byte
		binary.LittleEndian.PutUint32(header[:], uint32(len(data)))
		parts := [][]byte{header[:]}
		if v.summary {
			s, incomplete := summarize(group)
			flag := byte(0)
			if incomplete {
				flag = 1
			}
			parts = append(parts, []byte{flag}, s[:])
		}
		parts = append(parts, data)
		for _, part := range parts {
			n, e := f.Write(part)
			total += int64(n)
			if e != nil {
				_ = f.Close()
				return 0, e
			}
			if n != len(part) {
				_ = f.Close()
				return 0, io.ErrShortWrite
			}
		}
	}
	if err = f.Close(); err != nil {
		return 0, err
	}
	return total, nil
}
func openLayout(path string, v variant) (layout, error) {
	f, err := os.Open(path)
	if err != nil {
		return layout{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return layout{}, err
	}
	l := layout{variant: v, path: path, retained: info.Size()}
	for offset := int64(0); offset < info.Size(); {
		var header [4]byte
		if _, err = f.ReadAt(header[:], offset); err != nil {
			return l, err
		}
		offset += 4
		b := block{size: binary.LittleEndian.Uint32(header[:])}
		if v.summary {
			var metadata [1 + summaryBytes]byte
			if _, err = f.ReadAt(metadata[:], offset); err != nil {
				return l, err
			}
			offset += int64(len(metadata))
			b.incomplete = metadata[0] != 0
			copy(b.summary[:], metadata[1:])
		}
		b.offset = offset
		offset += int64(b.size)
		if b.size == 0 || offset > info.Size() {
			return l, fmt.Errorf("invalid block size")
		}
		l.blocks = append(l.blocks, b)
	}
	return l, nil
}

type result struct {
	rows  []checkpoint.BlockRow
	reads int
	peak  uint64
}

func query(l layout, literal string, measureMemory bool) (result, error) {
	// Reopen and reload the persisted directory for each query; no decoded block cache.
	l, err := openLayout(l.path, l.variant)
	if err != nil {
		return result{}, err
	}
	f, err := os.Open(l.path)
	if err != nil {
		return result{}, err
	}
	defer f.Close()
	var out result
	for _, b := range l.blocks {
		if l.variant.summary && !mayContain(b, literal) {
			continue
		}
		data := make([]byte, b.size)
		if _, err = f.ReadAt(data, b.offset); err != nil {
			return out, err
		}
		out.reads++
		var before, after runtime.MemStats
		if measureMemory {
			runtime.ReadMemStats(&before)
		}
		rows, err := decode(data, l.variant.compressed)
		if err != nil {
			return out, err
		}
		if measureMemory {
			runtime.ReadMemStats(&after)
			if after.HeapAlloc >= before.HeapAlloc && after.HeapAlloc-before.HeapAlloc > out.peak {
				out.peak = after.HeapAlloc - before.HeapAlloc
			}
		}
		for _, row := range rows {
			if strings.Contains(row.Row.Line, literal) {
				out.rows = append(out.rows, row)
			}
		}
	}
	return out, nil
}
func reference(groups [][]checkpoint.BlockRow, literal string) []checkpoint.BlockRow {
	var found []checkpoint.BlockRow
	for _, group := range groups {
		for _, row := range group {
			if strings.Contains(row.Row.Line, literal) {
				found = append(found, row)
			}
		}
	}
	return found
}
func equalRows(a, b []checkpoint.BlockRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Ordinal != b[i].Ordinal || a[i].Row.Line != b[i].Row.Line || !a[i].Row.TS.Equal(b[i].Row.TS) {
			return false
		}
	}
	return true
}
func run() error {
	smoke := flag.Bool("smoke", false, "small deterministic fixture and one timed query")
	flag.Parse()
	n, repeats := 16384, 10
	if *smoke {
		n, repeats = 512, 1
	}
	groups := fixture(n)
	variants := []variant{{"raw", false, false}, {"flate", true, false}, {"summary", false, true}, {"flate+summary", true, true}}
	dir, err := os.MkdirTemp("", "bog-segment-layout-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	layouts := make([]layout, 0, len(variants))
	for _, v := range variants {
		path := dir + "/" + v.name
		written, err := writeLayout(path, v, groups)
		if err != nil {
			return err
		}
		l, err := openLayout(path, v)
		if err != nil {
			return err
		}
		if written != l.retained {
			return fmt.Errorf("write size mismatch")
		}
		layouts = append(layouts, l)
	}
	literals := []string{"rare-needle-17", "absent-needle-17", "a", "ap", "api", "giant-needle-17", "oversized-data", "steady-state"}
	for _, l := range layouts {
		for _, lit := range literals {
			got, err := query(l, lit, false)
			if err != nil {
				return err
			}
			want := reference(groups, lit)
			if !equalRows(got.rows, want) {
				return fmt.Errorf("%s literal %q differs from ordered reference: got %d want %d", l.variant.name, lit, len(got.rows), len(want))
			}
		}
	}
	fmt.Printf("seed=%d rows=%d blocks=%d rows_per_block=%d oversized_threshold=%d summary=%d bytes/block flate=BestSpeed\n", seed, n+2, len(groups), rowsPerBlock, oversized, summaryBytes+1)
	fmt.Printf("runtime=%s os=%s arch=%s cpus=%d smoke=%t\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), *smoke)
	fmt.Println("Cold query: reopen files and reload persisted metadata each time; no application block cache. OS page cache is uncontrolled and likely warm; fixture creation and validation precede timing.")
	fmt.Println("Decode memory: maximum HeapAlloc increase during one decode, GC disabled; includes transient allocations, excludes input buffer and retained query results. Measured separately from latency.")
	fmt.Println("variant literal median_query_us logical_blocks_read peak_decode_heap_delta_bytes retained_bytes rewrite_bytes")
	for _, l := range layouts {
		rewrite, err := writeLayout(dir+"/rewrite", l.variant, groups)
		if err != nil {
			return err
		}
		if rewrite != l.retained {
			return fmt.Errorf("rewrite size differs for %s", l.variant.name)
		}
		for _, lit := range literals {
			times := make([]time.Duration, 0, repeats)
			reads := 0
			for i := 0; i < repeats; i++ {
				start := time.Now()
				got, err := query(l, lit, false)
				elapsed := time.Since(start)
				if err != nil {
					return err
				}
				reads = got.reads
				times = append(times, elapsed)
			}
			sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
			runtime.GC()
			old := debug.SetGCPercent(-1)
			measured, err := query(l, lit, true)
			debug.SetGCPercent(old)
			if err != nil {
				return err
			}
			fmt.Printf("%s %q %.1f %d %d %d %d\n", l.variant.name, lit, float64(times[len(times)/2].Nanoseconds())/1000, reads, measured.peak, l.retained, rewrite)
		}
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
