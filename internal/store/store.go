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
	runs      [][]int // immutable sorted ordinal runs, one per occupied level
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
	s.mu.RUnlock()
	base := len(rows)
	rows = append(rows, batch...)
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
	s.rows, s.runs = rows, runs
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
	rows, runs := s.rows, s.runs
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
	h := &candidates{rows: rows}
	// Each run is sorted, so skip nonmatches before placing its next row in the heap.
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
	for i, run := range runs {
		offset := sort.Search(len(run), func(j int) bool {
			ordinal := run[j]
			t := rows[ordinal].TS
			if t.Before(start) {
				return false
			}
			return after == nil || t.After(after.Time) || (t.Equal(after.Time) && ordinal > after.Ordinal)
		})
		advance(i, offset)
	}
	result := make([]Row, 0, limit)
	var last candidate
	for h.Len() > 0 && len(result) < limit {
		last = heap.Pop(h).(candidate)
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
