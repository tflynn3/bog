package store

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const MaxBatch = 4 << 20

type Row struct {
	TS   time.Time `json:"ts"`
	Line string    `json:"line"`
}

var timestamp = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)$`)

func ParseTime(s string) (time.Time, error) {
	if !timestamp.MatchString(s) {
		return time.Time{}, errors.New("invalid timestamp")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	// Go's parser accepts out-of-range zone components; RFC3339 does not.
	if s[len(s)-1] != 'Z' {
		z := s[len(s)-5:]
		if z[:2] > "23" || z[3:] > "59" {
			return time.Time{}, errors.New("invalid zone")
		}
	}
	t = t.UTC()
	if t.Year() < 1 || t.Year() > 9999 {
		return time.Time{}, errors.New("timestamp outside supported range")
	}
	return t, nil
}
func Decode(data []byte) ([]Row, error) {
	if len(data) == 0 || len(data) > MaxBatch || !utf8.Valid(data) {
		return nil, errors.New("invalid batch")
	}
	var rows []Row
	for len(data) > 0 {
		line, rest, found := bytes.Cut(data, []byte{'\n'})
		if found {
			data = rest
		} else {
			data = nil
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		d := json.NewDecoder(bytes.NewReader(line))
		tok, err := d.Token()
		if err != nil || tok != json.Delim('{') {
			return nil, errors.New("expected object")
		}
		var ts, text string
		seen := map[string]bool{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return nil, e
			}
			key, ok := k.(string)
			if !ok || seen[key] || (key != "ts" && key != "line") {
				return nil, errors.New("invalid field")
			}
			seen[key] = true
			var v json.RawMessage
			if e = d.Decode(&v); e != nil {
				return nil, e
			}
			var s string
			if len(v) == 0 || v[0] != '"' || json.Unmarshal(v, &s) != nil {
				return nil, errors.New("field must be string")
			}
			if key == "ts" {
				ts = s
			} else {
				text = s
			}
		}
		if _, err = d.Token(); err != nil {
			return nil, err
		}
		var extra any
		if err = d.Decode(&extra); err != io.EOF {
			return nil, errors.New("trailing data")
		}
		if !seen["ts"] || !seen["line"] {
			return nil, errors.New("missing field")
		}
		t, err := ParseTime(ts)
		if err != nil {
			return nil, fmt.Errorf("timestamp: %w", err)
		}
		if t.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
			return nil, errors.New("timestamp reserved for exclusive query end")
		}
		rows = append(rows, Row{t, text})
	}
	if len(rows) == 0 {
		return nil, errors.New("empty batch")
	}
	return rows, nil
}

type Store struct {
	publishMu sync.Mutex
	mu        sync.RWMutex
	rows      []Row
	restored  []int          // immutable run for the recovered WAL prefix
	runs      [][]int        // immutable sorted ordinal runs, one per occupied level
	blocks    []*blockFilter // immutable summaries of sealed append-ordinal blocks
}

// RecoveryBuilder collects validated WAL frames before the store becomes ready.
type RecoveryBuilder struct {
	rows    []Row
	blocks  []*blockFilter
	ordered bool
}

func NewRecoveryBuilder() *RecoveryBuilder { return &RecoveryBuilder{ordered: true} }

// Add appends decoded rows in WAL order and builds summaries of sealed blocks.
func (b *RecoveryBuilder) Add(rows []Row) {
	for _, row := range rows {
		if len(b.rows) > 0 && row.TS.Before(b.rows[len(b.rows)-1].TS) {
			b.ordered = false
		}
		b.rows = append(b.rows, row)
		if len(b.rows)%blockSize == 0 {
			b.blocks = append(b.blocks, buildBlock(b.rows[len(b.rows)-blockSize:]))
		}
	}
}

// Install publishes the complete recovered prefix. It must be called before live writes.
func (b *RecoveryBuilder) Install(s *Store) {
	run := make([]int, len(b.rows))
	for i := range run {
		run[i] = i
	}
	if !b.ordered {
		sort.Slice(run, func(i, j int) bool {
			a, c := run[i], run[j]
			if b.rows[a].TS.Equal(b.rows[c].TS) {
				return a < c
			}
			return b.rows[a].TS.Before(b.rows[c].TS)
		})
	}
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows, s.restored, s.runs, s.blocks = b.rows, run, nil, b.blocks
}

// Publish serializes writers, but leaves readers free to use the previous snapshot
// while the new run is sorted and merged.
func (s *Store) Publish(batch []Row) {
	if len(batch) == 0 {
		return
	}
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	s.mu.RLock()
	rows := s.rows
	runs := append([][]int(nil), s.runs...)
	// The captured length is the immutable published prefix. Only the serialized
	// writer appends beyond it, so readers may retain this slice without copying.
	blocks := s.blocks
	s.mu.RUnlock()
	base := len(rows)
	rows = append(rows, batch...)
	for len(blocks)*blockSize+blockSize <= len(rows) {
		start := len(blocks) * blockSize
		blocks = append(blocks, buildBlock(rows[start:start+blockSize]))
	}
	run := make([]int, len(batch))
	for i := range run {
		run[i] = base + i
	}
	less := func(a, b int) bool {
		if rows[a].TS.Equal(rows[b].TS) {
			return a < b
		}
		return rows[a].TS.Before(rows[b].TS)
	}
	sort.Slice(run, func(i, j int) bool { return less(run[i], run[j]) })
	for level := 0; ; level++ {
		if level == len(runs) {
			runs = append(runs, run)
			break
		}
		if runs[level] == nil {
			runs[level] = run
			break
		}
		old := runs[level]
		merged := make([]int, 0, len(old)+len(run))
		i, j := 0, 0
		for i < len(old) && j < len(run) {
			if less(old[i], run[j]) {
				merged = append(merged, old[i])
				i++
			} else {
				merged = append(merged, run[j])
				j++
			}
		}
		merged = append(merged, old[i:]...)
		merged = append(merged, run[j:]...)
		runs[level] = nil
		run = merged
	}
	s.mu.Lock()
	s.rows, s.runs, s.blocks = rows, runs, blocks
	s.mu.Unlock()
}

type candidate struct{ ordinal, run, offset int }
type candidates struct {
	rows  []Row
	items []candidate
}

func (h candidates) Len() int { return len(h.items) }
func (h candidates) Less(i, j int) bool {
	a, b := h.items[i].ordinal, h.items[j].ordinal
	if h.rows[a].TS.Equal(h.rows[b].TS) {
		return a < b
	}
	return h.rows[a].TS.Before(h.rows[b].TS)
}
func (h candidates) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *candidates) Push(x any)   { h.items = append(h.items, x.(candidate)) }
func (h *candidates) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

// Position identifies a row by timestamp and zero-based append ordinal.
type Position struct {
	Time    time.Time `json:"time"`
	Ordinal int       `json:"ordinal"`
}

// QueryPage captures the published row count when snapshot is nil. A continuation
// is valid only when its position still identifies a matching row in that snapshot.
func (s *Store) QueryPage(start, end time.Time, contains string, limit int, snapshot *int, after *Position) ([]Row, int, *Position, bool) {
	return s.query(start, end, contains, limit, snapshot, after, true)
}

func (s *Store) query(start, end time.Time, contains string, limit int, snapshot *int, after *Position, lookahead bool) ([]Row, int, *Position, bool) {
	s.mu.RLock()
	rows, runs, blocks := s.rows, s.runs, s.blocks
	if len(s.restored) > 0 {
		combined := make([][]int, 0, len(runs)+1)
		combined = append(combined, s.restored)
		runs = append(combined, runs...)
	}
	count := len(rows)
	if snapshot != nil {
		count = *snapshot
		if count < 1 || count > len(rows) || after == nil || after.Ordinal < 0 || after.Ordinal >= count {
			s.mu.RUnlock()
			return nil, 0, nil, false
		}
		row := rows[after.Ordinal]
		if !row.TS.Equal(after.Time) || row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, contains) {
			s.mu.RUnlock()
			return nil, 0, nil, false
		}
	}
	s.mu.RUnlock()
	if !start.Before(end) || limit <= 0 {
		return []Row{}, count, nil, true
	}
	// Retain the full timestamp range for Bloom selectivity, while using the
	// cursor-relative offset for scans. Both share the initial lower seek.
	offsets := make([]int, len(runs))
	bound := 0
	for i, run := range runs {
		lo := sort.Search(len(run), func(j int) bool { return !rows[run[j]].TS.Before(start) })
		if len(contains) >= 4 {
			hi := lo + sort.Search(len(run)-lo, func(j int) bool { return !rows[run[lo+j]].TS.Before(end) })
			bound += hi - lo
		}
		if after != nil {
			lo += sort.Search(len(run)-lo, func(j int) bool {
				ordinal := run[lo+j]
				t := rows[ordinal].TS
				return t.After(after.Time) || (t.Equal(after.Time) && ordinal > after.Ordinal)
			})
		}
		offsets[i] = lo
	}
	wide := len(contains) >= 4 && bound >= 32768
	ordered := func(budget int) ([]Row, *Position, bool) {
		h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
		for i, offset := range offsets {
			if offset < len(runs[i]) && rows[runs[i][offset]].TS.Before(end) {
				h.items = append(h.items, candidate{runs[i][offset], i, offset})
			}
		}
		heap.Init(h)
		var result []Row
		var last candidate
		examined := 0
		for h.Len() > 0 {
			if budget > 0 && examined >= budget {
				return nil, nil, false
			}
			current := h.items[0]
			examined++
			row := rows[current.ordinal]
			if current.ordinal < count && strings.Contains(row.Line, contains) {
				if len(result) == limit {
					return result, &Position{rows[last.ordinal].TS, last.ordinal}, true
				}
				if result == nil {
					result = make([]Row, 0, min(limit, count))
				}
				result = append(result, row)
				last = current
				if !lookahead && len(result) == limit {
					return result, nil, true
				}
			}
			next := current.offset + 1
			run := runs[current.run]
			if next < len(run) && rows[run[next]].TS.Before(end) {
				h.items[0] = candidate{run[next], current.run, next}
				heap.Fix(h, 0)
			} else {
				heap.Pop(h)
			}
		}
		if result == nil {
			result = []Row{}
		}
		return result, nil, true
	}
	if wide {
		// Bound work even for absent literals and runs containing newer rows.
		budget := max(512, limit)
		if limit <= int(^uint(0)>>1)/2 {
			budget = max(512, 2*limit)
		} else {
			budget = int(^uint(0) >> 1)
		}
		if result, next, complete := ordered(budget); complete {
			return result, count, next, true
		}
		probes := literalProbes(contains)
		possible := make([]int, 0)
		possibleRows := 0
		for first := 0; first < count; first += blockSize {
			block := first / blockSize
			if block < len(blocks) && !blocks[block].mayContainProbes(probes) {
				continue
			}
			n := blockSize
			if count-first < n {
				n = count - first
			}
			possibleRows += n
			if possibleRows > bound/4 {
				break
			}
			possible = append(possible, first)
		}
		if possibleRows <= bound/4 {
			result, next := queryBlocks(rows, possible, count, start, end, contains, limit, after, lookahead)
			return result, count, next, true
		}
	}
	// The unbounded merge skips non-matches within each run before touching the
	// heap, avoiding heap maintenance for every rejected row.
	h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
	advance := func(runIndex, offset int) {
		run := runs[runIndex]
		for offset < len(run) {
			ordinal := run[offset]
			row := rows[ordinal]
			if !row.TS.Before(end) {
				return
			}
			if ordinal < count && strings.Contains(row.Line, contains) {
				heap.Push(h, candidate{ordinal, runIndex, offset})
				return
			}
			offset++
		}
	}
	for i, offset := range offsets {
		advance(i, offset)
	}
	result := []Row{}
	var last candidate
	for h.Len() > 0 && len(result) < limit {
		last = heap.Pop(h).(candidate)
		if len(result) == 0 {
			result = make([]Row, 0, min(limit, count))
		}
		result = append(result, rows[last.ordinal])
		if lookahead || len(result) < limit {
			advance(last.run, last.offset+1)
		}
	}
	var next *Position
	if h.Len() > 0 {
		next = &Position{rows[last.ordinal].TS, last.ordinal}
	}
	return result, count, next, true
}

func (s *Store) Query(start, end time.Time, contains string, limit int) []Row {
	rows, _, _, _ := s.query(start, end, contains, limit, nil, nil, false)
	return rows
}
