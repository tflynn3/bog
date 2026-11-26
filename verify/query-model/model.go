// Package querymodel provides append-log fixtures and a snapshot pagination oracle.
package querymodel

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Row is the timestamp and line as supplied by a fixture or returned by a query.
type Row struct {
	Time time.Time
	Line string
}

// Position identifies a row by UTC instant and zero-based append ordinal.
type Position struct {
	Time    time.Time
	Ordinal int
}

// Entry is a matching row and its original position in the append log.
type Entry struct {
	Row
	Ordinal int
}

// Filter selects rows in [Start, End) containing the case-sensitive literal.
type Filter struct {
	Start, End time.Time
	Contains   string
}

// Log records successful appends in fixture order. Its zero value is empty.
// Callers synchronize access if they share a Log between goroutines.
type Log struct {
	rows []Row
}

// Append records a batch and returns the new high-water mark.
func (l *Log) Append(rows []Row) int {
	l.rows = append(l.rows, rows...)
	return l.HighWater()
}

// HighWater returns the current append count, including nonmatching rows.
func (l *Log) HighWater() int {
	return len(l.rows)
}

// Match scans a fixed prefix of the log and returns matching entries in query order.
func (l *Log) Match(highWater int, filter Filter) ([]Entry, error) {
	if highWater < 0 || highWater > l.HighWater() {
		return nil, fmt.Errorf("high-water mark %d outside append log of length %d", highWater, l.HighWater())
	}
	start, end := filter.Start.UTC(), filter.End.UTC()
	var matches []Entry
	for ordinal, row := range l.rows[:highWater] {
		row.Time = row.Time.UTC()
		if !row.Time.Before(start) && row.Time.Before(end) && strings.Contains(row.Line, filter.Contains) {
			matches = append(matches, Entry{Row: row, Ordinal: ordinal})
		}
	}
	slices.SortFunc(matches, func(a, b Entry) int {
		return cmp.Or(a.Time.Compare(b.Time), cmp.Compare(a.Ordinal, b.Ordinal))
	})
	return matches, nil
}
