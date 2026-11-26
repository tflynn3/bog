package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
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
