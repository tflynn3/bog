package store

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
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
		if ts, text, ok := plainRow(line); ok {
			t, err := checkedTime(ts)
			if err != nil {
				return nil, err
			}
			rows = append(rows, Row{t, text})
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
		t, err := checkedTime(ts)
		if err != nil {
			return nil, err
		}
		rows = append(rows, Row{t, text})
	}
	if len(rows) == 0 {
		return nil, errors.New("empty batch")
	}
	return rows, nil
}

// plainRow accepts only the unescaped, two-field form. All other forms go
// through the JSON decoder, including malformed input and escaped keys.
func plainRow(b []byte) (ts, line string, ok bool) {
	i := 0
	space := func() {
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\r' || b[i] == '\n') {
			i++
		}
	}
	take := func(c byte) bool {
		space()
		if i >= len(b) || b[i] != c {
			return false
		}
		i++
		return true
	}
	str := func() (string, bool) {
		space()
		if i >= len(b) || b[i] != '"' {
			return "", false
		}
		i++
		start := i
		for i < len(b) && b[i] != '"' {
			if b[i] < 0x20 || b[i] == '\\' {
				return "", false
			}
			i++
		}
		if i == len(b) {
			return "", false
		}
		v := string(b[start:i])
		i++
		return v, true
	}
	if !take('{') {
		return "", "", false
	}
	var seen uint8
	for n := 0; n < 2; n++ {
		if n > 0 && !take(',') {
			return "", "", false
		}
		key, valid := str()
		if !valid || !take(':') {
			return "", "", false
		}
		value, valid := str()
		if !valid {
			return "", "", false
		}
		switch key {
		case "ts":
			if seen&1 != 0 {
				return "", "", false
			}
			seen |= 1
			ts = value
		case "line":
			if seen&2 != 0 {
				return "", "", false
			}
			seen |= 2
			line = value
		default:
			return "", "", false
		}
	}
	if seen != 3 || !take('}') {
		return "", "", false
	}
	space()
	return ts, line, i == len(b)
}

func checkedTime(ts string) (time.Time, error) {
	t, err := ParseTime(ts)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp: %w", err)
	}
	if t.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
		return time.Time{}, errors.New("timestamp reserved for exclusive query end")
	}
	return t, nil
}

type Store struct {
	publishMu sync.Mutex
	mu        sync.RWMutex
	rows      []Row
	restored  indexRun       // immutable run for the recovered WAL prefix
	runs      [][]int        // immutable sorted ordinal runs, one per occupied level
	blocks    []*blockFilter // immutable summaries of sealed append-ordinal blocks
}

// indexRun maps sorted ranks to append ordinals. A nil ordinal slice denotes
// the ordered recovered prefix, whose rank is its ordinal.
type indexRun struct {
	ordinals []int
	length   int
}

func (r indexRun) ordinal(rank int) int {
	if r.ordinals == nil {
		return rank
	}
	return r.ordinals[rank]
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
	oldLen := len(b.rows)
	if b.ordered && len(rows) > 0 {
		if oldLen > 0 && rows[0].TS.Before(b.rows[oldLen-1].TS) {
			b.ordered = false
		}
		for i := 1; i < len(rows) && b.ordered; i++ {
			if rows[i].TS.Before(rows[i-1].TS) {
				b.ordered = false
			}
		}
	}
	b.rows = append(b.rows, rows...)
	for len(b.blocks)*blockSize+blockSize <= len(b.rows) {
		start := len(b.blocks) * blockSize
		b.blocks = append(b.blocks, buildBlock(b.rows[start:start+blockSize]))
	}
}

// Install publishes the complete recovered prefix. It must be called before live writes.
func (b *RecoveryBuilder) Install(s *Store) {
	run := indexRun{length: len(b.rows)}
	if !b.ordered {
		run.ordinals = make([]int, len(b.rows))
		for i := range run.ordinals {
			run.ordinals[i] = i
		}
		slices.SortFunc(run.ordinals, func(a, c int) int {
			if b.rows[a].TS.Before(b.rows[c].TS) {
				return -1
			}
			if b.rows[c].TS.Before(b.rows[a].TS) {
				return 1
			}
			return a - c
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
	rows, blocks := s.rows, s.blocks
	runs := make([]indexRun, 0, len(s.runs)+1)
	if s.restored.length > 0 {
		runs = append(runs, s.restored)
	}
	for _, run := range s.runs {
		runs = append(runs, indexRun{ordinals: run, length: len(run)})
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
	// Seek once to the start and cursor for each run. Upper bounds are deferred
	// until a bounded scan has had a chance to answer the query.
	offsets := make([]int, len(runs))
	lower := make([]int, len(runs))
	for i, run := range runs {
		lo := sort.Search(run.length, func(j int) bool { return !rows[run.ordinal(j)].TS.Before(start) })
		lower[i] = lo
		if after != nil {
			lo += sort.Search(run.length-lo, func(j int) bool {
				ordinal := run.ordinal(lo + j)
				t := rows[ordinal].TS
				return t.After(after.Time) || (t.Equal(after.Time) && ordinal > after.Ordinal)
			})
		}
		offsets[i] = lo
	}
	active := -1
	for i, offset := range offsets {
		if offset < runs[i].length && rows[runs[i].ordinal(offset)].TS.Before(end) {
			if active >= 0 {
				active = -2
				break
			}
			active = i
		}
	}
	if active == -1 {
		return []Row{}, count, nil, true
	}
	var directResult []Row
	var directLast int
	directOffset := 0
	if active >= 0 {
		directOffset = offsets[active]
	}
	direct := func(budget int) ([]Row, *Position, bool) {
		run := runs[active]
		examined := 0
		for directOffset < run.length {
			ordinal := run.ordinal(directOffset)
			row := rows[ordinal]
			if !row.TS.Before(end) {
				break
			}
			if budget > 0 && examined >= budget {
				return nil, nil, false
			}
			directOffset++
			examined++
			if ordinal >= count || !strings.Contains(row.Line, contains) {
				continue
			}
			if len(directResult) == limit {
				return directResult, &Position{rows[directLast].TS, directLast}, true
			}
			if directResult == nil {
				directResult = make([]Row, 0, min(limit, count))
			}
			directResult = append(directResult, row)
			directLast = ordinal
			if !lookahead && len(directResult) == limit {
				return directResult, nil, true
			}
		}
		if directResult == nil {
			directResult = []Row{}
		}
		return directResult, nil, true
	}
	ordered := func(budget int) ([]Row, *Position, bool) {
		h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
		for i, offset := range offsets {
			if offset < runs[i].length && rows[runs[i].ordinal(offset)].TS.Before(end) {
				h.items = append(h.items, candidate{runs[i].ordinal(offset), i, offset})
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
			if next < run.length && rows[run.ordinal(next)].TS.Before(end) {
				h.items[0] = candidate{run.ordinal(next), current.run, next}
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
	if len(contains) >= 4 {
		// Bound work even for absent literals and runs containing newer rows.
		budget := max(512, limit)
		if limit <= int(^uint(0)>>1)/2 {
			budget = max(512, 2*limit)
		} else {
			budget = int(^uint(0) >> 1)
		}
		// The multi-run policy uses the full-window estimate before attempting
		// the merge. A single run can answer dense pages without that estimate.
		bound := 0
		intervalStart, intervalEnd := 0, count
		implicit := active >= 0 && runs[active].ordinals == nil
		if active >= 0 {
			if result, next, complete := direct(budget); complete {
				return result, count, next, true
			}
		}
		if implicit {
			// The recovered run is already in append order. Resume the bounded
			// scan and inspect only blocks that may contain the literal.
			run := runs[active]
			hi := sort.Search(run.length, func(i int) bool { return !rows[i].TS.Before(end) })
			hi = min(hi, count)
			probes := literalProbes(contains)
			for directOffset < hi {
				block := directOffset / blockSize
				blockEnd := min((block+1)*blockSize, hi)
				if block < len(blocks) && !blocks[block].mayContainProbes(probes) {
					directOffset = blockEnd
					continue
				}
				for directOffset < blockEnd {
					ordinal := directOffset
					directOffset++
					row := rows[ordinal]
					if !strings.Contains(row.Line, contains) {
						continue
					}
					if len(directResult) == limit {
						return directResult, count, &Position{rows[directLast].TS, directLast}, true
					}
					if directResult == nil {
						directResult = make([]Row, 0, min(limit, count))
					}
					directResult = append(directResult, row)
					directLast = ordinal
					if !lookahead && len(directResult) == limit {
						return directResult, count, nil, true
					}
				}
			}
			if directResult == nil {
				directResult = []Row{}
			}
			return directResult, count, nil, true
		}
		for i, run := range runs {
			hi := lower[i] + sort.Search(run.length-lower[i], func(j int) bool { return !rows[run.ordinal(lower[i]+j)].TS.Before(end) })
			bound += hi - lower[i]
		}
		if bound >= 32768 {
			if active == -2 {
				if result, next, complete := ordered(budget); complete {
					return result, count, next, true
				}
			}
			probes := literalProbes(contains)
			possible := make([]int, 0)
			possibleRows := 0
			for first := intervalStart / blockSize * blockSize; first < intervalEnd; first += blockSize {
				block := first / blockSize
				if block < len(blocks) && !blocks[block].mayContainProbes(probes) {
					continue
				}
				possibleRows += min(first+blockSize, intervalEnd) - max(first, intervalStart)
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
	}
	if active >= 0 {
		result, next, _ := direct(0)
		return result, count, next, true
	}
	// The unbounded merge skips non-matches within each run before touching the
	// heap, avoiding heap maintenance for every rejected row.
	h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
	advance := func(runIndex, offset int) {
		run := runs[runIndex]
		for offset < run.length {
			ordinal := run.ordinal(offset)
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
