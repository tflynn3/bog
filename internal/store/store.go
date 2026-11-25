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
	mu      sync.RWMutex
	rows    []Row
	buckets map[int64][]int
}

// minute returns the UTC epoch minute, rounding toward negative infinity.
func minute(t time.Time) int64 {
	seconds := t.Unix()
	m := seconds / 60
	if seconds < 0 && seconds%60 != 0 {
		m--
	}
	return m
}

func (s *Store) Publish(rows []Row) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets == nil {
		s.buckets = make(map[int64][]int)
	}
	base := len(s.rows)
	s.rows = append(s.rows, rows...)
	for i, row := range rows {
		m := minute(row.TS)
		s.buckets[m] = append(s.buckets[m], base+i)
	}
}

type match struct {
	row   Row
	order int
}
type matches []match

func (m matches) Len() int { return len(m) }
func (m matches) Less(i, j int) bool {
	if m[i].row.TS.Equal(m[j].row.TS) {
		return m[i].order > m[j].order
	}
	return m[j].row.TS.Before(m[i].row.TS)
}
func (m matches) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m *matches) Push(x any)   { *m = append(*m, x.(match)) }
func (m *matches) Pop() any     { old := *m; x := old[len(old)-1]; *m = old[:len(old)-1]; return x }
func (s *Store) Query(start, end time.Time, contains string, limit int) []Row {
	if !start.Before(end) {
		return []Row{}
	}
	first, last := minute(start), minute(end.Add(-time.Nanosecond))
	narrow := last >= first && last-first < 32
	s.mu.RLock()
	rowsSnapshot := s.rows
	var bucketSnapshots [][]int
	if narrow {
		bucketSnapshots = make([][]int, 0, last-first+1)
		for m := first; m <= last; m++ {
			bucketSnapshots = append(bucketSnapshots, s.buckets[m])
		}
	}
	s.mu.RUnlock()
	result := make(matches, 0, limit)
	consider := func(i int) {
		r := rowsSnapshot[i]
		if !r.TS.Before(start) && r.TS.Before(end) && strings.Contains(r.Line, contains) {
			candidate := match{r, i}
			if len(result) < limit {
				heap.Push(&result, candidate)
			} else if candidate.row.TS.Before(result[0].row.TS) || (candidate.row.TS.Equal(result[0].row.TS) && candidate.order < result[0].order) {
				result[0] = candidate
				heap.Fix(&result, 0)
			}
		}
	}
	if narrow {
		for _, bucket := range bucketSnapshots {
			for _, i := range bucket {
				consider(i)
			}
		}
	} else {
		for i := range rowsSnapshot {
			consider(i)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].row.TS.Equal(result[j].row.TS) {
			return result[i].order < result[j].order
		}
		return result[i].row.TS.Before(result[j].row.TS)
	})
	rows := make([]Row, len(result))
	for i, m := range result {
		rows[i] = m.row
	}
	return rows
}
