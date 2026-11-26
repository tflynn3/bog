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
	return decodeAppend(nil, data)
}

// decodeAppend appends a complete batch to rows, retaining Decode validation.
func decodeAppend(rows []Row, data []byte) ([]Row, error) {
	initialLen := len(rows)
	if len(data) == 0 || len(data) > MaxBatch || !utf8.Valid(data) {
		return nil, errors.New("invalid batch")
	}
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
	if len(rows) == initialLen {
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
	runs      []indexRun     // immutable sorted ordinal runs, one per occupied level
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
	oldLen := len(b.rows)
	b.rows = append(b.rows, rows...)
	b.finishFrame(oldLen)
}

// DecodeFrame appends a validated WAL payload only if the entire batch decodes.
func (b *RecoveryBuilder) DecodeFrame(payload []byte) error {
	oldLen := len(b.rows)
	rows, err := decodeAppend(b.rows, payload)
	if err != nil {
		return err
	}
	b.rows = rows
	b.finishFrame(oldLen)
	return nil
}

func (b *RecoveryBuilder) finishFrame(oldLen int) {
	if b.ordered {
		for i := max(1, oldLen); i < len(b.rows); i++ {
			if b.rows[i].TS.Before(b.rows[i-1].TS) {
				b.ordered = false
				break
			}
		}
	}
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
		run.buildVisibility()
	}
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	// The builder can keep appending to its backing arrays. Force the first
	// live append to allocate its own arrays, preserving the shared prefix.
	s.rows = b.rows[:len(b.rows):len(b.rows)]
	s.blocks = b.blocks[:len(b.blocks):len(b.blocks)]
	s.restored, s.runs = run, nil
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
	runs := append([]indexRun(nil), s.runs...)
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
	run := indexRun{base: base, length: len(batch)}
	less := func(a, b int) bool {
		if rows[a].TS.Equal(rows[b].TS) {
			return a < b
		}
		return rows[a].TS.Before(rows[b].TS)
	}
	for i := 1; i < len(batch); i++ {
		if batch[i].TS.Before(batch[i-1].TS) {
			run.ordinals = make([]int, len(batch))
			for j := range run.ordinals {
				run.ordinals[j] = base + j
			}
			sort.Slice(run.ordinals, func(i, j int) bool { return less(run.ordinals[i], run.ordinals[j]) })
			break
		}
	}
	level := 0
	for ; level < len(runs) && runs[level].length != 0; level++ {
		old := runs[level]
		if old.ordinals == nil && run.ordinals == nil && old.base+old.length == run.base &&
			!rows[run.base].TS.Before(rows[old.base+old.length-1].TS) {
			run = indexRun{base: old.base, length: old.length + run.length}
		} else {
			merged := make([]int, 0, old.length+run.length)
			i, j := 0, 0
			for i < old.length && j < run.length {
				if less(old.ordinal(i), run.ordinal(j)) {
					merged = append(merged, old.ordinal(i))
					i++
				} else {
					merged = append(merged, run.ordinal(j))
					j++
				}
			}
			for ; i < old.length; i++ {
				merged = append(merged, old.ordinal(i))
			}
			for ; j < run.length; j++ {
				merged = append(merged, run.ordinal(j))
			}
			run = indexRun{ordinals: merged, base: old.base, length: len(merged)}
		}
		runs[level] = indexRun{}
	}
	if run.ordinals != nil {
		run.buildVisibility()
	}
	if level == len(runs) {
		runs = append(runs, run)
	} else {
		runs[level] = run
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

// QueryPage captures the published row count when both snapshot and after are nil.
// A continuation requires both values, and after must identify a matching row
// in the supplied snapshot.
func (s *Store) QueryPage(start, end time.Time, contains string, limit int, snapshot *int, after *Position) ([]Row, int, *Position, bool) {
	rows, count, next, err := s.QueryPageWithWork(start, end, contains, limit, snapshot, after, nil)
	return rows, count, next, err == nil
}

// QueryPageWithWork is QueryPage with cancellation and work accounting.
func (s *Store) QueryPageWithWork(start, end time.Time, contains string, limit int, snapshot *int, after *Position, work *QueryWork) ([]Row, int, *Position, error) {
	matched, count, err := s.query(start, end, contains, limit, snapshot, after, true, false, work)
	var next *Position
	if matched.more {
		next = &Position{matched.rows[len(matched.rows)-1].TS, matched.last}
	}
	return matched.rows, count, next, err
}

func (s *Store) query(start, end time.Time, contains string, limit int, snapshot *int, after *Position, lookahead, collectOrdinals bool, work *QueryWork) (queryMatches, int, error) {
	if err := work.Charge(); err != nil {
		return queryMatches{}, 0, err
	}
	if (snapshot == nil) != (after == nil) {
		return queryMatches{}, 0, ErrInvalidQueryCursor
	}
	s.mu.RLock()
	rows, blocks, restored, levels := s.rows, s.blocks, s.restored, s.runs
	s.mu.RUnlock()
	count := len(rows)
	if snapshot != nil {
		count = *snapshot
		if count < 1 || count > len(rows) || after.Ordinal < 0 || after.Ordinal >= count {
			return queryMatches{}, 0, ErrInvalidQueryCursor
		}
		row := rows[after.Ordinal]
		if err := work.row(row.Line, true); err != nil {
			return queryMatches{}, 0, err
		}
		if (!collectOrdinals && !row.TS.Equal(after.Time)) || row.TS.Before(start) || !row.TS.Before(end) || !strings.Contains(row.Line, contains) {
			return queryMatches{}, 0, ErrInvalidQueryCursor
		}
		if collectOrdinals {
			after = &Position{row.TS, after.Ordinal}
		}
	}
	if !start.Before(end) || limit <= 0 {
		return queryMatches{rows: []Row{}}, count, nil
	}
	// Publication replaces the run levels, so the captured slice is immutable.
	runs := make([]indexRun, 0, len(levels)+1)
	if restored.length > 0 {
		runs = append(runs, restored)
	}
	for _, run := range levels {
		if run.length > 0 {
			runs = append(runs, run)
		}
	}
	// Seek once to the start and cursor for each run. Upper bounds are deferred
	// until a bounded scan has had a chance to answer the query.
	offsets := make([]int, 0, len(runs))
	lower := make([]int, 0, len(runs))
	visibleRuns := runs[:0]
	for _, run := range runs {
		if err := work.Charge(1); err != nil {
			return queryMatches{}, 0, err
		}
		if run.base >= count {
			continue
		}
		if run.ordinals == nil {
			run.length = min(run.length, count-run.base)
		}
		lo, next, err := run.seekStart(rows, start, after, collectOrdinals, work)
		if err != nil {
			return queryMatches{}, 0, err
		}
		lower = append(lower, lo)
		offset, err := run.nextVisible(next, count, work)
		if err != nil {
			return queryMatches{}, 0, err
		}
		offsets = append(offsets, offset)
		visibleRuns = append(visibleRuns, run)
	}
	runs = visibleRuns
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
		return queryMatches{rows: []Row{}}, count, nil
	}
	directResult := queryMatches{rows: []Row{}}
	directOffset := 0
	if active >= 0 {
		directOffset = offsets[active]
	}
	direct := func(scanThreshold int) (bool, error) {
		run := runs[active]
		examined := 0
		for directOffset < run.length {
			ordinal := run.ordinal(directOffset)
			row := rows[ordinal]
			if !row.TS.Before(end) {
				break
			}
			if scanThreshold > 0 && examined >= scanThreshold {
				return false, nil
			}
			examined++
			if err := work.row(row.Line, true); err != nil {
				return false, err
			}
			if strings.Contains(row.Line, contains) {
				if directResult.add(row, ordinal, limit, count, lookahead, collectOrdinals) {
					return true, nil
				}
			}
			next, err := run.nextVisible(directOffset+1, count, work)
			if err != nil {
				return false, err
			}
			directOffset = next
		}
		return true, nil
	}
	ordered := func(scanThreshold int) (queryMatches, bool, error) {
		h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
		for i, offset := range offsets {
			if offset < runs[i].length && rows[runs[i].ordinal(offset)].TS.Before(end) {
				h.items = append(h.items, candidate{runs[i].ordinal(offset), i, offset})
			}
		}
		heap.Init(h)
		result := queryMatches{rows: []Row{}}
		examined := 0
		for h.Len() > 0 {
			if scanThreshold > 0 && examined >= scanThreshold {
				return queryMatches{}, false, nil
			}
			current := h.items[0]
			examined++
			row := rows[current.ordinal]
			if err := work.row(row.Line, true); err != nil {
				return queryMatches{}, false, err
			}
			if strings.Contains(row.Line, contains) {
				if result.add(row, current.ordinal, limit, count, lookahead, collectOrdinals) {
					return result, true, nil
				}
			}
			run := runs[current.run]
			next, err := run.nextVisible(current.offset+1, count, work)
			if err != nil {
				return queryMatches{}, false, err
			}
			if next < run.length && rows[run.ordinal(next)].TS.Before(end) {
				h.items[0] = candidate{run.ordinal(next), current.run, next}
				heap.Fix(h, 0)
			} else {
				heap.Pop(h)
			}
		}
		return result, true, nil
	}
	var runProbes []uint64
	var runEnds []int
	if len(contains) >= 4 {
		// Try a bounded scan before paying for the Bloom selection paths.
		scanThreshold := max(512, limit)
		if limit <= int(^uint(0)>>1)/2 {
			scanThreshold = max(512, 2*limit)
		} else {
			scanThreshold = int(^uint(0) >> 1)
		}
		// The multi-run policy uses the full-window estimate before attempting
		// the merge. A single run can answer dense pages without that estimate.
		bound := 0
		intervalStart, intervalEnd := 0, count
		implicit := active >= 0 && runs[active].ordinals == nil
		if active >= 0 {
			complete, err := direct(scanThreshold)
			if err != nil {
				return queryMatches{}, 0, err
			}
			if complete {
				return directResult, count, nil
			}
		}
		if implicit {
			// An implicit run is already in append order. Resume the bounded
			// scan and inspect only blocks that may contain the literal.
			run := runs[active]
			if err := work.seek(run.length); err != nil {
				return queryMatches{}, 0, err
			}
			hi := run.base + sort.Search(run.length, func(i int) bool { return !rows[run.base+i].TS.Before(end) })
			hi = min(hi, count)
			directOffset += run.base
			probes := literalProbes(contains)
			for directOffset < hi {
				block := directOffset / blockSize
				blockEnd := min((block+1)*blockSize, hi)
				if err := work.Charge(1); err != nil {
					return queryMatches{}, 0, err
				}
				if block < len(blocks) && !blocks[block].mayContainProbes(probes) {
					directOffset = blockEnd
					continue
				}
				for directOffset < blockEnd {
					ordinal := directOffset
					directOffset++
					row := rows[ordinal]
					if err := work.row(row.Line, true); err != nil {
						return queryMatches{}, 0, err
					}
					if !strings.Contains(row.Line, contains) {
						continue
					}
					if directResult.add(row, ordinal, limit, count, lookahead, collectOrdinals) {
						return directResult, count, nil
					}
				}
			}
			return directResult, count, nil
		}
		for i, run := range runs {
			if err := work.seek(run.length - lower[i]); err != nil {
				return queryMatches{}, 0, err
			}
			hi := lower[i] + sort.Search(run.length-lower[i], func(j int) bool { return !rows[run.ordinal(lower[i]+j)].TS.Before(end) })
			bound += min(hi-lower[i], count-run.base)
		}
		if bound >= 32768 {
			if active == -2 {
				result, complete, err := ordered(scanThreshold)
				if err != nil {
					return queryMatches{}, 0, err
				}
				if complete {
					return result, count, nil
				}
			}
			allImplicit := active == -2
			for i, run := range runs {
				if offsets[i] < run.length && rows[run.ordinal(offsets[i])].TS.Before(end) && run.ordinals != nil {
					allImplicit = false
					break
				}
			}
			if allImplicit {
				runProbes = literalProbes(contains)
				runEnds = make([]int, len(runs))
				for i, run := range runs {
					if run.ordinals == nil {
						if err := work.seek(run.length); err != nil {
							return queryMatches{}, 0, err
						}
						hi := sort.Search(run.length, func(j int) bool { return !rows[run.base+j].TS.Before(end) })
						runEnds[i] = max(0, min(hi, count-run.base))
					}
				}
			} else {
				probes := literalProbes(contains)
				possible := make([]int, 0)
				possibleRows := 0
				for first := intervalStart / blockSize * blockSize; first < intervalEnd; first += blockSize {
					block := first / blockSize
					if err := work.Charge(1); err != nil {
						return queryMatches{}, 0, err
					}
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
					result, err := queryBlocks(rows, possible, count, start, end, contains, limit, after, lookahead, collectOrdinals, work)
					if err != nil {
						return queryMatches{}, 0, err
					}
					return result, count, nil
				}
			}
		}
	}
	if active >= 0 {
		_, err := direct(0)
		if err != nil {
			return queryMatches{}, 0, err
		}
		return directResult, count, nil
	}
	// The unbounded merge skips non-matches within each run before touching the
	// heap, avoiding heap maintenance for every rejected row.
	h := &candidates{rows: rows, items: make([]candidate, 0, len(runs))}
	advance := func(runIndex, offset int) error {
		run := runs[runIndex]
		if runProbes != nil && run.ordinals == nil {
			for offset < runEnds[runIndex] {
				ordinal := run.base + offset
				block := ordinal / blockSize
				blockEnd := min((block+1)*blockSize-run.base, runEnds[runIndex])
				if err := work.Charge(1); err != nil {
					return err
				}
				if block < len(blocks) && !blocks[block].mayContainProbes(runProbes) {
					offset = blockEnd
					continue
				}
				for offset < blockEnd {
					ordinal = run.base + offset
					line := rows[ordinal].Line
					if err := work.row(line, true); err != nil {
						return err
					}
					if strings.Contains(line, contains) {
						heap.Push(h, candidate{ordinal, runIndex, offset})
						return nil
					}
					offset++
				}
			}
			return nil
		}
		for offset < run.length {
			ordinal := run.ordinal(offset)
			row := rows[ordinal]
			if !row.TS.Before(end) {
				return nil
			}
			if err := work.row(row.Line, true); err != nil {
				return err
			}
			if strings.Contains(row.Line, contains) {
				heap.Push(h, candidate{ordinal, runIndex, offset})
				return nil
			}
			next, err := run.nextVisible(offset+1, count, work)
			if err != nil {
				return err
			}
			offset = next
		}
		return nil
	}
	for i, offset := range offsets {
		if err := advance(i, offset); err != nil {
			return queryMatches{}, 0, err
		}
	}
	result := queryMatches{rows: []Row{}}
	for h.Len() > 0 && len(result.rows) < limit {
		last := heap.Pop(h).(candidate)
		if result.add(rows[last.ordinal], last.ordinal, limit, count, lookahead, collectOrdinals) {
			break
		}
		if collectOrdinals && len(result.rows) == limit && h.Len() > 0 {
			// Another run already supplied a matching successor. The
			// next page will seek from the last returned ordinal.
			break
		}
		offset, err := runs[last.run].nextVisible(last.offset+1, count, work)
		if err != nil {
			return queryMatches{}, 0, err
		}
		if err := advance(last.run, offset); err != nil {
			return queryMatches{}, 0, err
		}
	}
	result.more = lookahead && h.Len() > 0
	return result, count, nil
}

func (s *Store) Query(start, end time.Time, contains string, limit int) []Row {
	rows, _ := s.QueryWithWork(start, end, contains, limit, nil)
	return rows
}

// QueryWithWork is Query with cancellation and work accounting.
func (s *Store) QueryWithWork(start, end time.Time, contains string, limit int, work *QueryWork) ([]Row, error) {
	matched, _, err := s.query(start, end, contains, limit, nil, nil, false, false, work)
	return matched.rows, err
}
