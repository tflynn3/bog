package backup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"bog/internal/store"
)

const (
	referenceRunBytes = 8 << 20
	// Charge line bytes and space for the decoded row and slice growth.
	referenceRowCharge = 128
	referenceFanIn     = 8
	// There are fewer than 2^63 WAL bytes, hence fewer than 2^63 rows and
	// initial runs. Every level groups eight runs from the level below.
	referenceLevels = 1 + 63/3
)

type referenceSorter struct {
	ctx       context.Context
	directory string
	runID     uint64
	levels    [referenceLevels][]string
	rows      []Row
	bytes     int
}

func (s *referenceSorter) add(row Row) error {
	charge := referenceRowCharge + len(row.Line)
	if s.bytes+charge > referenceRunBytes {
		if err := s.flush(); err != nil {
			return err
		}
	}
	s.rows = append(s.rows, row)
	s.bytes += charge
	return nil
}

func (s *referenceSorter) flush() error {
	if len(s.rows) == 0 {
		return nil
	}
	if s.directory == "" {
		dir, err := os.MkdirTemp("", "bog-verify-")
		if err != nil {
			return fmt.Errorf("create reference scratch directory: %w", err)
		}
		s.directory = dir
		if err := os.Chmod(dir, 0700); err != nil {
			return fmt.Errorf("set reference scratch permissions: %w", err)
		}
	}
	slices.SortFunc(s.rows, func(a, b Row) int {
		return comparePositions(a.Position(), b.Position())
	})
	path, err := s.writeRun(func(out *bufio.Writer) error {
		for _, row := range s.rows {
			if err := s.ctx.Err(); err != nil {
				return err
			}
			key := row.Position()
			var header [rowHeader]byte
			little.PutUint64(header[:8], uint64(key.Time.Unix()))
			little.PutUint32(header[8:12], uint32(key.Time.Nanosecond()))
			little.PutUint64(header[12:20], key.Ordinal)
			little.PutUint32(header[20:], uint32(len(row.Line)))
			if _, err := out.Write(header[:]); err != nil {
				return err
			}
			if _, err := out.WriteString(row.Line); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	clear(s.rows)
	s.rows = s.rows[:0]
	s.bytes = 0

	for level := range s.levels {
		runs := append(s.levels[level], path)
		if len(runs) < referenceFanIn {
			s.levels[level] = runs
			return nil
		}
		path, err = s.merge(runs)
		if err != nil {
			return err
		}
		s.levels[level] = nil
	}
	return errors.New("too many reference sort runs")
}

func (s *referenceSorter) finish() (*referenceRun, error) {
	if err := s.flush(); err != nil {
		return nil, err
	}
	var path string
	for _, runs := range s.levels {
		if len(runs) == 0 {
			continue
		}
		if path != "" {
			runs = append(runs, path)
		}
		if len(runs) == 1 {
			path = runs[0]
			continue
		}
		var err error
		path, err = s.merge(runs)
		if err != nil {
			return nil, err
		}
	}
	if path == "" {
		return nil, nil
	}
	return openReferenceRun(path)
}

func (s *referenceSorter) writeRun(write func(*bufio.Writer) error) (_ string, err error) {
	if err := s.ctx.Err(); err != nil {
		return "", err
	}
	s.runID++
	path := filepath.Join(s.directory, strconv.FormatUint(s.runID, 10))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", fmt.Errorf("create reference sort run: %w", err)
	}
	defer func() {
		if err = errors.Join(err, f.Close()); err != nil {
			err = fmt.Errorf("write reference sort run: %w", err)
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return "", err
	}
	out := bufio.NewWriterSize(f, 64<<10)
	if err := write(out); err != nil {
		return "", err
	}
	return path, out.Flush()
}

func (s *referenceSorter) merge(paths []string) (string, error) {
	path, err := s.writeRun(func(out *bufio.Writer) error {
		return s.mergeInto(out, paths)
	})
	if err != nil {
		return "", err
	}
	for _, old := range paths {
		if err := os.Remove(old); err != nil {
			return "", fmt.Errorf("remove merged reference sort run: %w", err)
		}
	}
	return path, nil
}

func (s *referenceSorter) mergeInto(out *bufio.Writer, paths []string) (err error) {
	runs := make([]*referenceRun, 0, len(paths))
	defer func() {
		for _, run := range runs {
			err = errors.Join(err, run.file.Close())
		}
	}()
	for _, path := range paths {
		run, err := openReferenceRun(path)
		if err != nil {
			return err
		}
		runs = append(runs, run)
	}
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		var next *referenceRun
		for _, run := range runs {
			if run.valid && (next == nil || run.key.less(next.key)) {
				next = run
			}
		}
		if next == nil {
			return nil
		}
		if _, err := out.Write(next.header[:]); err != nil {
			return err
		}
		if _, err := io.CopyN(out, next.input, next.size); err != nil {
			return err
		}
		if err := next.advance(); err != nil {
			return err
		}
	}
}

type referenceRun struct {
	file   *os.File
	input  *bufio.Reader
	header [rowHeader]byte
	key    Position
	size   int64
	valid  bool
}

func openReferenceRun(path string) (*referenceRun, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open reference sort run: %w", err)
	}
	r := &referenceRun{file: f, input: bufio.NewReaderSize(f, 64<<10)}
	if err := r.advance(); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return r, nil
}

func (r *referenceRun) advance() error {
	r.valid = false
	if _, err := io.ReadFull(r.input, r.header[:]); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("read reference sort key: %w", err)
	}
	r.key, r.size, r.valid = rowKey(r.header[:], store.MaxBatch)
	if !r.valid {
		return errors.New("invalid reference sort key")
	}
	return nil
}
