package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"bog/internal/checkpoint"
	"golang.org/x/sys/unix"
)

type workerRequest struct {
	Path, Literal string
	Start, End    time.Time
	Component     *componentRequest
	Decode        *decodeRequest
	Qualification *qualificationRequest
	Cold          *coldRequest
	ColdPrepare   *componentRequest
}

type querySample struct {
	payloadReads
	ElapsedNS    int64
	DecodeBytes  int
	PeakRSSBytes uint64
}

func worker(ctx context.Context, arg string, out io.Writer) error {
	var req workerRequest
	if err := json.Unmarshal([]byte(arg), &req); err != nil {
		return err
	}
	if req.Component != nil {
		m, err := measureComponent(ctx, *req.Component)
		if err != nil {
			return err
		}
		m.PeakRSSBytes, err = selfPeakRSS()
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(m)
	}
	if req.Qualification != nil {
		return qualificationWorker(ctx, *req.Qualification, out)
	}
	if req.Decode != nil {
		return decodeWorker(ctx, *req.Decode, out)
	}
	if req.Cold != nil {
		return coldWorker(ctx, *req.Cold, out)
	}
	if req.ColdPrepare != nil {
		return prepareColdProduction(ctx, *req.ColdPrepare, out)
	}
	return queryWorker(req, out)
}

func queryWorker(req workerRequest, out io.Writer) (err error) {
	f, err := os.Open(req.Path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	for _, advice := range []int{unix.FADV_RANDOM, unix.FADV_DONTNEED} {
		if err := unix.Fadvise(int(f.Fd()), 0, 0, advice); err != nil {
			return fmt.Errorf("file cache advice %d: %w", advice, err)
		}
	}
	q := query{literal: req.Literal, start: req.Start, end: req.End}
	start := time.Now()
	rows, reads, peak, err := queryFile(f, info.Size(), q)
	elapsed := time.Since(start)
	if err != nil {
		return err
	}
	rss, err := selfPeakRSS()
	if err != nil {
		return err
	}
	return writeQuerySample(out, querySample{reads, elapsed.Nanoseconds(), peak, rss}, rows)
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

func rowHeader(row checkpoint.BlockRow) [24]byte {
	var h [24]byte
	le.PutUint64(h[:8], uint64(row.Row.TS.Unix()))
	le.PutUint32(h[8:12], uint32(row.Row.TS.Nanosecond()))
	le.PutUint64(h[12:20], row.Ordinal)
	le.PutUint32(h[20:], uint32(len(row.Row.Line)))
	return h
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

// VmHWM measures the executed worker after fork/exec.
func selfPeakRSS() (uint64, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(data)) {
		if value, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			fields := strings.Fields(value)
			if len(fields) != 2 || fields[1] != "kB" {
				break
			}
			kib, err := strconv.ParseUint(fields[0], 10, 54)
			if err != nil || kib == 0 {
				return 0, fmt.Errorf("invalid Linux peak RSS %q", fields[0])
			}
			return kib * 1024, nil
		}
	}
	return 0, errors.New("missing Linux process peak RSS")
}

func measureIsolated(ctx context.Context, cfg config, path string, q query, want []checkpoint.BlockRow, total int) (measurement, error) {
	req := workerRequest{Path: path, Literal: q.literal, Start: q.start, End: q.end}
	arg, err := json.Marshal(req)
	if err != nil {
		return measurement{}, err
	}
	m := newMeasurement(q, len(want), total)
	for i := range cfg.runs {
		var sample querySample
		sampleCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		_, err := runWorker(sampleCtx, []string{workerFlag, string(arg)}, func(r io.Reader) error {
			return readQuerySample(r, want, &sample)
		})
		cancel()
		if err != nil {
			return measurement{}, fmt.Errorf("sample %d: %w", i+1, err)
		}
		if err := m.addSample(sample.payloadReads, sample.ElapsedNS, sample.DecodeBytes, sample.PeakRSSBytes); err != nil {
			return measurement{}, err
		}
	}
	m.ColdP95NS = p95(m.ColdSamplesNS)
	return m, nil
}

func queryFile(r io.ReaderAt, size int64, q query) (matches []checkpoint.BlockRow, reads payloadReads, peak int, err error) {
	reads, err = scanBlocks(r, size, q, func(raw []byte, buffers int) error {
		return matchBlock(raw, buffers, q, &matches, &peak)
	})
	if err != nil {
		return nil, payloadReads{}, 0, err
	}
	slices.SortFunc(matches, ordered)
	return matches, reads, peak, nil
}

func matchBlock(raw []byte, buffers int, q query, matches *[]checkpoint.BlockRow, peak *int) error {
	rows, err := checkpoint.DecodeBlock(raw)
	if err != nil {
		return err
	}
	working := buffers + len(rows)*decodedRowBytes
	for _, row := range rows {
		working += len(row.Row.Line)
		if !row.Row.TS.Before(q.start) && row.Row.TS.Before(q.end) && strings.Contains(row.Row.Line, q.literal) {
			*matches = append(*matches, row)
		}
	}
	*peak = max(*peak, working)
	return nil
}
