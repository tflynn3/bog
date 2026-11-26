package store

import (
	"slices"
	"sort"
	"sync"
	"time"
)

type Row struct {
	TS   time.Time `json:"ts"`
	Line string    `json:"line"`
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
