package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"bog/internal/checkpoint"
)

// The framing also serves the worker protocol. It distinguishes timestamp
// ties, duplicate lines with new ordinals, and different line boundaries.
func rowHeader(row checkpoint.BlockRow) [24]byte {
	var h [24]byte
	le.PutUint64(h[:8], uint64(row.Row.TS.Unix()))
	le.PutUint32(h[8:12], uint32(row.Row.TS.Nanosecond()))
	le.PutUint64(h[12:20], row.Ordinal)
	le.PutUint32(h[20:], uint32(len(row.Row.Line)))
	return h
}

func writeQuerySample(out io.Writer, metrics any, rows []checkpoint.BlockRow) error {
	w := bufio.NewWriter(out)
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		return err
	}
	for _, row := range rows {
		h := rowHeader(row)
		if _, err := w.Write(h[:]); err != nil {
			return err
		}
		if _, err := w.WriteString(row.Row.Line); err != nil {
			return err
		}
	}
	return w.Flush()
}

func readQuerySample(r io.Reader, want []checkpoint.BlockRow, sample any) error {
	input := bufio.NewReader(r)
	header, err := input.ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("sample metrics: %w", err)
	}
	if err := json.Unmarshal(header, sample); err != nil {
		return err
	}
	var h [24]byte
	var chunk [4096]byte
	for i, row := range want {
		if _, err := io.ReadFull(input, h[:]); err != nil {
			return fmt.Errorf("row %d header: %w", i, err)
		}
		if h != rowHeader(row) {
			return fmt.Errorf("exact oracle mismatch in timestamp, ordinal or length at row %d", i)
		}
		for line := row.Row.Line; len(line) > 0; {
			part := chunk[:min(len(line), len(chunk))]
			if _, err := io.ReadFull(input, part); err != nil {
				return fmt.Errorf("row %d line: %w", i, err)
			}
			if string(part) != line[:len(part)] {
				return fmt.Errorf("exact oracle mismatch in complete line at row %d", i)
			}
			line = line[len(part):]
		}
	}
	if _, err := input.ReadByte(); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("exact oracle row count: extra data after final row")
	}
	return nil
}
