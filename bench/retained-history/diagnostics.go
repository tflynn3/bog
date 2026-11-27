package main

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type queryAttempt struct {
	Admission string         `json:"admission"`
	Phase     string         `json:"phase"`
	Status    *int           `json:"http_status"`
	Headers   *time.Duration `json:"response_headers_ns"`
	Body      *time.Duration `json:"response_body_complete_ns"`
	Budget    time.Duration  `json:"request_budget_ns"`
	Elapsed   time.Duration  `json:"elapsed_ns"`
	BodyBytes int            `json:"body_bytes"`
	HasCursor bool           `json:"has_cursor_header"`
	Detail    string         `json:"detail,omitempty"`
}

type queryPhase struct {
	Attempts   int            `json:"attempts"`
	Admissions map[string]int `json:"admissions"`
	MaxHeaders *time.Duration `json:"max_response_headers_ns"`
	MaxBody    *time.Duration `json:"max_response_body_complete_ns"`
	Last       queryAttempt   `json:"last"`
	Failure    *queryAttempt  `json:"first_failure,omitempty"`
}

func recordQuery(phases map[string]*queryPhase, name string, q queryAttempt, err error) {
	if phases == nil {
		return
	}
	p := phases[name]
	if p == nil {
		p = &queryPhase{Admissions: make(map[string]int)}
		phases[name] = p
	}
	p.Attempts++
	p.Admissions[q.Admission]++
	recordMaximum(&p.MaxHeaders, q.Headers)
	recordMaximum(&p.MaxBody, q.Body)
	if err != nil {
		q.Detail = diagnosticError(err)
		if p.Failure == nil {
			p.Failure = &q
		}
	} else {
		q.Phase = "complete"
	}
	p.Last = q
}

func recordMaximum(dst **time.Duration, sample *time.Duration) {
	if sample != nil && (*dst == nil || *sample > **dst) {
		*dst = sample
	}
}

func diagnosticError(err error) string {
	var requestErr *url.Error
	if errors.As(err, &requestErr) {
		err = requestErr.Err // The URL can contain an opaque cursor.
	}
	return truncate(err.Error(), 512)
}

func truncate(s string, n int) string { return s[:min(len(s), n)] }

func (q *queryAttempt) observeHeaders(res *http.Response, elapsed time.Duration) {
	q.Headers, q.Status = &elapsed, &res.StatusCode
	q.HasCursor = len(res.Header.Values("Bog-Next-Cursor")) > 0
	q.Phase = "body"
	switch {
	case res.StatusCode == http.StatusOK && res.Header.Get("Content-Type") == "application/x-ndjson":
		q.Admission = "ndjson_response"
	case res.StatusCode >= 400:
		q.Admission = "http_rejection"
	default:
		q.Admission = "unexpected_response"
	}
}

func (q *queryAttempt) observeRejection(body []byte) {
	if q.Status == nil || *q.Status != http.StatusServiceUnavailable || q.HasCursor {
		return
	}
	switch strings.TrimSpace(string(body)) {
	case "query capacity exhausted":
		q.Admission = "capacity_rejection"
	case "query work budget exhausted":
		q.Admission = "work_budget_rejection"
	}
}

type candidateEvidence struct {
	Lifetime            string         `json:"lifetime"`
	Ready               *time.Duration `json:"start_to_ready_ns"`
	StartupError        string         `json:"startup_error,omitempty"`
	ExitedBeforeCleanup bool           `json:"exited_before_cleanup"`
	Reaped              bool           `json:"reaped"`
	ExitCode            *int           `json:"exit_code"`
	Signal              string         `json:"exit_signal,omitempty"`
	WaitError           string         `json:"wait_error,omitempty"`
	LogTail             string         `json:"log_tail"`
	LogDrainError       string         `json:"log_drain_error,omitempty"`
	LogTruncated        bool           `json:"log_truncated"`
}

type capacityDiagnostics struct {
	Backfills         []*backfillTimeline    `json:"backfill_timelines,omitempty"`
	Queries           map[string]*queryPhase `json:"queries"`
	Candidates        []*candidateEvidence   `json:"candidates"`
	Restart           *restartEvidence       `json:"restart,omitempty"`
	ReplayDuration    *time.Duration         `json:"replay_duration_ns"`
	StagingBytes      *int64                 `json:"peak_sampled_checkpoint_staging_bytes"`
	PinCount          *int                   `json:"peak_sampled_pin_records"`
	OldestPin         *time.Duration         `json:"oldest_sampled_pin_record_age_ns"`
	RunningIngestP99  *time.Duration         `json:"compaction_running_ingest_p99_ns"`
	RunningSamples    int                    `json:"compaction_running_ingest_samples"`
	OpeningCompaction *compaction            `json:"opening_compaction,omitempty"`
	OverlapCompaction *compaction            `json:"overlap_compaction,omitempty"`
	MemoryEvents      map[string]uint64      `json:"cgroup_memory_events"`
	Unavailable       map[string]string      `json:"unavailable"`
}

const logLimit = 32 << 10

type candidateLog struct {
	mu        sync.Mutex
	tail      []byte
	truncated bool
}

func (l *candidateLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if drop := len(l.tail) + len(p) - logLimit; drop > 0 {
		l.truncated = true
		if drop >= len(l.tail) {
			prefix := drop - len(l.tail)
			l.tail = append(l.tail[:0], p[prefix:]...)
			return len(p), nil
		}
		l.tail = l.tail[:copy(l.tail, l.tail[drop:])]
	}
	l.tail = append(l.tail, p...)
	return len(p), nil
}

func (d *capacityDiagnostics) sampleStorage(dir string) error {
	sample, err := observeStorage(dir, time.Now())
	if sample.StagingBytes != nil && (d.StagingBytes == nil || *sample.StagingBytes > *d.StagingBytes) {
		d.StagingBytes = sample.StagingBytes
	}
	if sample.PinRecords != nil && (d.PinCount == nil || *sample.PinRecords > *d.PinCount) {
		d.PinCount = sample.PinRecords
	}
	recordMaximum(&d.OldestPin, sample.OldestRecord)
	return err
}

func stagingBytes(root *os.Root) (int64, error) {
	const name = ".bog-checkpoint-stage"
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.IsDir() {
		return 0, errors.New("checkpoint staging path is not a directory")
	}
	entries, err := fs.ReadDir(root.FS(), name)
	if err != nil {
		return 0, err
	}
	var bytes int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		if !info.Mode().IsRegular() {
			return 0, errors.New("non-regular checkpoint staging entry")
		}
		bytes += info.Size()
	}
	return bytes, nil
}

func pinRecords(root *os.Root, now time.Time) (int, *time.Duration, error) {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return 0, nil, err
	}
	count := 0
	var oldest *time.Duration
	for _, entry := range entries {
		token, ok := strings.CutPrefix(entry.Name(), ".bog-history-pin-")
		if !ok || len(token) != 32 || strings.Trim(token, "0123456789abcdef") != "" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, nil, err
		}
		if !info.Mode().IsRegular() {
			return 0, nil, errors.New("non-regular generation pin record")
		}
		count++
		age := max(now.Sub(info.ModTime()), 0)
		recordMaximum(&oldest, &age)
	}
	return count, oldest, nil
}

func memoryEvents(data string) (map[string]uint64, error) {
	stats := make(map[string]uint64)
	for line := range strings.SplitSeq(strings.TrimSpace(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid memory.events line %q", line)
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
		if _, duplicate := stats[fields[0]]; duplicate {
			return nil, fmt.Errorf("duplicate memory.events counter %q", fields[0])
		}
		stats[fields[0]] = n
	}
	if _, ok := stats["oom_kill"]; !ok {
		return nil, errors.New("missing oom_kill in memory.events")
	}
	return stats, nil
}

func (d *capacityDiagnostics) markMissing(mode string) {
	for _, phase := range []string{"seed_capture", "mixed_continuation", "after_publication_continuation", "fresh_backfill", "restart_walk"} {
		if d.Queries[phase] == nil {
			d.Unavailable["query."+phase] = "phase did not issue a request"
		}
	}
	for _, measurement := range []struct {
		name, reason      string
		missing, liveOnly bool
	}{
		{"replay_duration", "candidate does not expose replay-only timing", d.ReplayDuration == nil, false},
		{"peak_sampled_checkpoint_staging_bytes", "no valid checkpoint staging sample", d.StagingBytes == nil, true},
		{"peak_sampled_pin_records", "no valid pin record sample", d.PinCount == nil, true},
		{"oldest_sampled_pin_record_age", "no published pin record observed", d.OldestPin == nil, true},
		{"compaction_running_ingest_p99", "no acknowledgement bracketed by running compaction observations", d.RunningIngestP99 == nil, true},
		{"cgroup_memory_events", "no cgroup memory events sample", d.MemoryEvents == nil, false},
	} {
		if measurement.missing && d.Unavailable[measurement.name] == "" {
			reason := measurement.reason
			if mode == "legacy" && measurement.liveOnly {
				reason = "live compaction is disabled in legacy mode"
			}
			d.Unavailable[measurement.name] = reason
		}
	}
}
