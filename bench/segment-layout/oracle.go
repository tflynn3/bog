package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"bog/internal/checkpoint"
)

const (
	oracleMagic      = "BOGORC01"
	oracleHeaderSize = 20
	oracleRowSize    = 24
	oracleBufferSize = 32 << 10
	// The largest ordinary batch has 256 rows. Include the special rows
	// and either backfill in the bound on header counts.
	maxFixtureRows = maxBatches*256 + 8
)

// The controller writes the independent oracle's ordered rows once per query.
// The worker compares them byte for byte outside the timed interval, using
// bounded scratch space even for the oversized line.
func writeOracle(ctx context.Context, path string, total int, rows []checkpoint.BlockRow, fingerprint io.Writer) (err error) {
	if total < 1 || total > maxFixtureRows || len(rows) > total {
		return errors.New("invalid oracle row counts")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	var dst io.Writer = f
	if fingerprint != nil {
		dst = io.MultiWriter(f, fingerprint)
	}
	return encodeOracle(ctx, dst, total, rows)
}

func encodeOracle(ctx context.Context, dst io.Writer, total int, rows []checkpoint.BlockRow) error {
	w := bufio.NewWriterSize(dst, oracleBufferSize)
	var header [oracleHeaderSize]byte
	copy(header[:8], oracleMagic)
	le.PutUint32(header[8:12], uint32(total))
	le.PutUint32(header[12:16], uint32(len(rows)))
	seal(header[:])
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	var fields [oracleRowSize]byte
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := row.Row.Line
		if len(line) > largeLineSize {
			return errors.New("oversized oracle line")
		}
		le.PutUint64(fields[:8], uint64(row.Row.TS.Unix()))
		le.PutUint32(fields[8:12], uint32(row.Row.TS.Nanosecond()))
		le.PutUint64(fields[12:20], row.Ordinal)
		le.PutUint32(fields[20:24], uint32(len(line)))
		if _, err := w.Write(fields[:]); err != nil {
			return err
		}
		if _, err := w.WriteString(line); err != nil {
			return err
		}
	}
	return w.Flush()
}

type oracleReader struct {
	input              io.ReaderAt
	reader             *bufio.Reader
	totalRows, matches int
	size               int64
	scratch            [oracleBufferSize]byte
}

func readOracle(input io.ReaderAt, size int64) (*oracleReader, error) {
	var header [oracleHeaderSize]byte
	if size < oracleHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	if err := checkedRead(input, header[:], 0); err != nil {
		return nil, fmt.Errorf("oracle header: %w", err)
	}
	total, matches := le.Uint32(header[8:12]), le.Uint32(header[12:16])
	if string(header[:8]) != oracleMagic || total == 0 || total > maxFixtureRows || matches > total ||
		int64(matches)*oracleRowSize > size-oracleHeaderSize {
		return nil, errors.New("invalid oracle header")
	}
	return &oracleReader{
		input: input, reader: bufio.NewReaderSize(nil, oracleBufferSize),
		totalRows: int(total), matches: int(matches), size: size,
	}, nil
}

func (o *oracleReader) verify(got []checkpoint.BlockRow) error {
	if len(got) != o.matches {
		return fmt.Errorf("exact oracle row count: got %d, want %d", len(got), o.matches)
	}
	o.reader.Reset(io.NewSectionReader(o.input, oracleHeaderSize, o.size-oracleHeaderSize))
	var fields [oracleRowSize]byte
	for i, row := range got {
		if _, err := io.ReadFull(o.reader, fields[:]); err != nil {
			return fmt.Errorf("oracle row %d: %w", i, err)
		}
		size := le.Uint32(fields[20:24])
		if uint64(len(row.Row.Line)) != uint64(size) || size > uint32(largeLineSize) ||
			uint64(row.Row.TS.Unix()) != le.Uint64(fields[:8]) ||
			uint32(row.Row.TS.Nanosecond()) != le.Uint32(fields[8:12]) ||
			row.Ordinal != le.Uint64(fields[12:20]) {
			return fmt.Errorf("exact oracle fields differ at row %d", i)
		}
		for line := row.Row.Line; len(line) > 0; {
			n := min(len(line), len(o.scratch))
			if _, err := io.ReadFull(o.reader, o.scratch[:n]); err != nil {
				return fmt.Errorf("oracle line %d: %w", i, err)
			}
			if line[:n] != string(o.scratch[:n]) {
				return fmt.Errorf("exact oracle line differs at row %d", i)
			}
			line = line[n:]
		}
	}
	if _, err := o.reader.ReadByte(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("oracle end: %w", err)
		}
		return errors.New("trailing oracle data")
	}
	return nil
}
