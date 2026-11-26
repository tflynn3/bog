package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

type row struct {
	TS   time.Time `json:"ts"`
	Line string    `json:"line"`
}

type disposition uint8

const (
	acknowledged disposition = iota
	rejected
	unknown
)

type batch struct {
	rows  []row
	state disposition
}

// The ledger records rows at submission time. Unique fixture lines let it
// distinguish duplicate delivery, partial batches and rows never submitted.
type ledger []batch

type reconciliation struct {
	AcknowledgedRows int `json:"acknowledged_rows"`
	RejectedRows     int `json:"rejected_rows"`
	UnknownRows      int `json:"unknown_rows"`
	RecoveredUnknown int `json:"recovered_unknown_rows"`
}

func (l ledger) reconcile(got []row) (proof reconciliation, err error) {
	seen := make(map[string]bool, len(got))
	for _, r := range got {
		if seen[r.Line] {
			return proof, fmt.Errorf("duplicate recovered row %.100q", r.Line)
		}
		seen[r.Line] = true
	}
	var want []row
	for i, b := range l {
		switch b.state {
		case acknowledged:
			proof.AcknowledgedRows += len(b.rows)
		case rejected:
			proof.RejectedRows += len(b.rows)
		case unknown:
			proof.UnknownRows += len(b.rows)
			var present int
			for _, r := range b.rows {
				if seen[r.Line] {
					present++
				}
			}
			if present != 0 && present != len(b.rows) {
				return proof, fmt.Errorf("interrupted batch %d recovered %d of %d rows", i, present, len(b.rows))
			}
			proof.RecoveredUnknown += present
			if present == 0 {
				continue
			}
		}
		if b.state != rejected {
			want = append(want, b.rows...)
		}
	}
	slices.SortStableFunc(want, func(a, b row) int { return a.TS.Compare(b.TS) })
	if len(got) != len(want) {
		return proof, fmt.Errorf("recovered %d rows, expected %d", len(got), len(want))
	}
	for i, r := range got {
		if r.Line != want[i].Line || !r.TS.Equal(want[i].TS) {
			return proof, fmt.Errorf("row %d got (%s, %.100q), expected (%s, %.100q)", i, r.TS, r.Line, want[i].TS, want[i].Line)
		}
	}
	return proof, nil
}

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func fixtureRows(prefix string, n, second int, padding string) []row {
	rows := make([]row, n)
	for i := range rows {
		rows[i] = row{epoch.Add(time.Duration(second) * time.Second), fmt.Sprintf("%s-%04d%s", prefix, i, padding)}
	}
	return rows
}

func encodeRows(rows []row) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
