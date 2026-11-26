package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"
)

const benchmarkSeed = 33003
const benchmarkRows = 128_000
const benchmarkBatch = 128
const benchmarkPage = 256

type benchmarkEvidence struct {
	Host              string  `json:"host"`
	Kernel            string  `json:"kernel"`
	Architecture      string  `json:"architecture"`
	FilesystemMagic   string  `json:"filesystem_magic"`
	Seed              int     `json:"seed"`
	Rows              int     `json:"rows"`
	RowsPerBatch      int     `json:"rows_per_batch"`
	IngestRowsPerSec  float64 `json:"durable_ingest_rows_per_second"`
	IngestP99MS       float64 `json:"ingest_p99_ms"`
	QueryP95MS        float64 `json:"query_page_p95_ms"`
	RestartToHealthMS float64 `json:"restart_to_health_ms"`
	PeakRSSKiB        int64   `json:"peak_candidate_rss_kib"`
}

func benchmarkCase(ctx context.Context, s *server) (any, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return nil, err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &fs); err != nil {
		return nil, err
	}
	proof := benchmarkEvidence{
		Host: host, Kernel: strings.TrimSpace(string(kernel)), Architecture: runtime.GOARCH,
		FilesystemMagic: fmt.Sprintf("%#x", fs.Type), Seed: benchmarkSeed, Rows: benchmarkRows, RowsPerBatch: benchmarkBatch,
	}

	rng := rand.New(rand.NewPCG(benchmarkSeed, 0))
	var l ledger
	var payloads [][]byte
	for offset := 0; offset < benchmarkRows; offset += benchmarkBatch {
		rows := make([]row, benchmarkBatch)
		for i := range rows {
			n := offset + i
			rows[i] = row{epoch.Add(time.Duration(n/4) * time.Millisecond), fmt.Sprintf("%06d-%016x", n, rng.Uint64())}
		}
		payload, err := encodeRows(rows)
		if err != nil {
			return proof, err
		}
		l = append(l, batch{rows, acknowledged})
		payloads = append(payloads, payload)
	}
	var ingest []time.Duration
	start := time.Now()
	for _, payload := range payloads {
		sent := time.Now()
		if _, _, err := s.request(ctx, http.MethodPost, "/ingest", payload, http.StatusNoContent); err != nil {
			return proof, err
		}
		ingest = append(ingest, time.Since(sent))
	}
	proof.IngestRowsPerSec = benchmarkRows / time.Since(start).Seconds()
	proof.IngestP99MS = percentileMS(ingest, 99)
	cursor, err := s.capture(ctx)
	if err != nil {
		return proof, err
	}
	if err := s.process.stop(syscall.SIGKILL); err != nil {
		return proof, err
	}
	proof.PeakRSSKiB = peakRSS(s.process)
	start = time.Now()
	if err := s.start(ctx); err != nil {
		return proof, err
	}
	proof.RestartToHealthMS = milliseconds(time.Since(start))
	if err := s.rejectCursor(ctx, cursor); err != nil {
		return proof, err
	}

	var got []row
	var query []time.Duration
	cursor = ""
	for i := 0; i < benchmarkRows/benchmarkPage; i++ {
		sent := time.Now()
		page, next, err := s.page(ctx, cursor, benchmarkPage)
		if err != nil {
			return proof, err
		}
		query = append(query, time.Since(sent))
		got = append(got, page...)
		terminal := i == benchmarkRows/benchmarkPage-1
		if len(page) != benchmarkPage || (next == "") != terminal || (!terminal && next == cursor) {
			return proof, fmt.Errorf("invalid benchmark page %d", i+1)
		}
		cursor = next
	}
	proof.QueryP95MS = percentileMS(query, 95)
	if _, err := l.reconcile(got); err != nil {
		return proof, err
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return proof, err
	}
	proof.PeakRSSKiB = max(proof.PeakRSSKiB, peakRSS(s.process))
	if proof.PeakRSSKiB <= 0 {
		return proof, errors.New("candidate peak RSS unavailable")
	}
	return proof, nil
}

func percentileMS(samples []time.Duration, percentile int) float64 {
	slices.Sort(samples)
	return milliseconds(samples[(len(samples)*percentile+99)/100-1])
}

func milliseconds(d time.Duration) float64 { return d.Seconds() * 1000 }

func peakRSS(p *child) int64 {
	if usage, ok := p.cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		return usage.Maxrss
	}
	return 0
}
