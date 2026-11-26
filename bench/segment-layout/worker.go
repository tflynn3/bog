package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"time"
)

const (
	workerArgument = "--layout-worker"
	// At maxRuns there are 45,000 int64 samples. Leave room for
	// their JSON representation, query identities and counters.
	maxWorkerOutput     = 2 << 20
	maxWorkerDiagnostic = 32 << 10
)

var errWorkerOutputLimit = errors.New("worker output exceeded its size limit")

type workerOutput struct {
	buf      bytes.Buffer
	limit    int
	exceeded bool
}

func (w *workerOutput) Write(data []byte) (int, error) {
	if len(data) > w.limit-w.buf.Len() {
		w.exceeded = true
		return 0, errWorkerOutputLimit
	}
	return w.buf.Write(data)
}

type workerReport struct {
	Layout       string          `json:"layout"`
	PeakRSSBytes int64           `json:"peak_worker_rss_bytes"`
	Generations  [][]measurement `json:"generations"`
}

func workerArgs(dir string, l layout, runs int) []string {
	return []string{workerArgument, dir, l.name(), strconv.Itoa(runs)}
}

func parseWorkerArgs(args []string) (string, layout, int, error) {
	if len(args) != 4 || args[0] != workerArgument || args[1] == "" {
		return "", layout{}, 0, errors.New("invalid worker arguments")
	}
	runs, err := strconv.Atoi(args[3])
	if err != nil || runs < 1 || runs > maxRuns {
		return "", layout{}, 0, errors.New("invalid worker sample count")
	}
	for _, l := range layouts {
		if args[2] == l.name() {
			return args[1], l, runs, nil
		}
	}
	return "", layout{}, 0, fmt.Errorf("unknown worker layout %q", args[2])
}

func invokeWorker(ctx context.Context, executable string, args []string) (workerReport, error) {
	// CommandContext kills on cancellation. Run drains the streams and
	// waits for the child to exit before the workspace can be removed.
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.WaitDelay = 2 * time.Second
	out := workerOutput{limit: maxWorkerOutput}
	diagnostic := workerOutput{limit: maxWorkerDiagnostic}
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	if err := cmd.Run(); err != nil {
		if out.exceeded {
			err = errors.Join(err, fmt.Errorf("stdout: %w", errWorkerOutputLimit))
		}
		if diagnostic.exceeded {
			err = errors.Join(err, fmt.Errorf("stderr: %w", errWorkerOutputLimit))
		}
		if diagnostic.buf.Len() != 0 {
			err = fmt.Errorf("%w: %s", err, diagnostic.buf.String())
		}
		return workerReport{}, errors.Join(ctx.Err(), err)
	}
	if err := ctx.Err(); err != nil {
		return workerReport{}, err
	}
	var result workerReport
	if err := json.Unmarshal(out.buf.Bytes(), &result); err != nil {
		return workerReport{}, fmt.Errorf("decode worker report: %w", err)
	}
	return result, nil
}

type cachePolicy func(*os.File) (beforeSample func() error, err error)

func measureLayout(dir string, l layout, runs int, cache cachePolicy) (workerReport, error) {
	result := workerReport{Layout: l.name(), Generations: make([][]measurement, len(phases))}
	for i, phase := range phases {
		for _, q := range queries() {
			m, err := measurePrepared(generationPath(dir, phase, l), oraclePath(dir, phase, q), q, runs, cache)
			if err != nil {
				return workerReport{}, fmt.Errorf("%s/%s: %w", phase, q.name, err)
			}
			result.Generations[i] = append(result.Generations[i], m)
		}
	}
	return result, nil
}

func measurePrepared(path, expectedPath string, q query, runs int, cache cachePolicy) (_ measurement, err error) {
	f, err := os.Open(path)
	if err != nil {
		return measurement{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return measurement{}, err
	}
	expectedFile, err := os.Open(expectedPath)
	if err != nil {
		return measurement{}, err
	}
	defer func() { err = errors.Join(err, expectedFile.Close()) }()
	expectedInfo, err := expectedFile.Stat()
	if err != nil {
		return measurement{}, err
	}
	expected, err := readOracle(expectedFile, expectedInfo.Size())
	if err != nil {
		return measurement{}, err
	}
	beforeSample, err := cache(f)
	if err != nil {
		return measurement{}, err
	}
	return measureQuery(f, info.Size(), q, expected, runs, beforeSample)
}

func workerMain(args []string, out io.Writer, cache cachePolicy, peakRSS func() (int64, error)) error {
	dir, l, runs, err := parseWorkerArgs(args)
	if err != nil {
		return err
	}
	result, err := measureLayout(dir, l, runs, cache)
	if err != nil {
		return err
	}
	result.PeakRSSBytes, err = peakRSS()
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}

func collectWorkers(ctx context.Context, executable, dir string, results []layoutReport, runs int) error {
	for i, l := range layouts {
		worker, err := invokeWorker(ctx, executable, workerArgs(dir, l, runs))
		if err != nil {
			return fmt.Errorf("worker %s: %w", l.name(), err)
		}
		if err := results[i].recordWorker(worker, runs); err != nil {
			return fmt.Errorf("worker %s report: %w", l.name(), err)
		}
	}
	// The unpruned variants precede the pruned variants in the matrix.
	baselines := make(map[codec]int, len(layouts)/2)
	for i, l := range layouts {
		if !l.prune {
			baselines[l.codec] = i
		}
		for j, phase := range phases {
			gen := results[i].generation(j)
			if !l.prune {
				if err := gen.compareUnpruned(results[0].generation(j)); err != nil {
					return fmt.Errorf("%s: %s: %w", phase, l.name(), err)
				}
			}
			baseline := results[baselines[l.codec]].generation(j)
			if err := gen.compareBaseline(baseline); err != nil {
				return fmt.Errorf("%s: %s: %w", phase, l.name(), err)
			}
		}
	}
	return nil
}

func (r *layoutReport) generation(phase int) *generationReport {
	if phase == 0 {
		return &r.generationReport
	}
	return &r.Backfills[phase-1].generationReport
}

func (r *layoutReport) recordWorker(worker workerReport, runs int) error {
	if runs < 1 || runs > maxRuns || worker.Layout != r.Name || worker.PeakRSSBytes <= 0 || len(worker.Generations) != len(phases) {
		return errors.New("invalid worker report identity, RSS or generation count")
	}
	for i, rows := range worker.Generations {
		gen := r.generation(i)
		expected := queries()
		if len(rows) != len(expected) {
			return fmt.Errorf("%s: wrong worker query count", phases[i])
		}
		for j, m := range rows {
			q := expected[j]
			if m.Name != q.name || m.Literal != q.literal || !m.Start.Equal(q.start) || !m.End.Equal(q.end) ||
				!m.OrderedParity || len(m.ColdSamplesNS) != runs || m.ColdP95NS != p95(m.ColdSamplesNS) || slices.Min(m.ColdSamplesNS) < 0 ||
				m.MatchingRows < 0 || m.MatchingRows > gen.Rows ||
				m.MatchingFraction != float64(m.MatchingRows)/float64(gen.Rows) ||
				m.Blocks < 0 || m.Blocks > gen.Blocks || m.Bytes < 0 || m.Bytes > gen.RetainedBytes ||
				m.DecodedBytes < m.Bytes || m.DecodedBytes > int64(m.Blocks)*maxBlock ||
				m.PeakDecodeBytes < 0 || (m.PeakDecodeBytes == 0) != (m.Blocks == 0) ||
				(q.name == "all" && (m.MatchingRows != gen.Rows || m.Blocks != gen.Blocks)) {
				return fmt.Errorf("%s/%s: invalid worker query metrics", phases[i], q.name)
			}
		}
		gen.Queries = rows
	}
	r.PeakWorkerRSSBytes = worker.PeakRSSBytes
	return nil
}

func (g *generationReport) compareBaseline(baseline *generationReport) error {
	if g.RetainedBytes-baseline.RetainedBytes != g.BloomMetadataBytes {
		return errors.New("retained-byte delta differs from Bloom metadata size")
	}
	if g.OracleSHA256 != baseline.OracleSHA256 || g.Rows != baseline.Rows || g.Blocks != baseline.Blocks {
		return errors.New("layout and baseline have different fixture identities")
	}
	for i := range g.Queries {
		m, b := &g.Queries[i], baseline.Queries[i]
		if err := m.compareReads(b); err != nil {
			return err
		}
		if len(m.Literal) < 4 && m.payloadReads != b.payloadReads {
			return fmt.Errorf("%s pruned payload reads for a short literal", m.Name)
		}
	}
	return nil
}

// Compression must preserve which blocks a time range selects and their
// total decoded size. Stored bytes may differ between codecs.
func (g *generationReport) compareUnpruned(raw *generationReport) error {
	for i, m := range g.Queries {
		b := raw.Queries[i]
		if m.Blocks != b.Blocks || m.DecodedBytes != b.DecodedBytes || m.MatchingRows != b.MatchingRows {
			return fmt.Errorf("%s selected different rows or blocks from raw", m.Name)
		}
	}
	return nil
}
