package main

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type caseDiagnostics struct {
	Candidates     []*processEvidence        `json:"candidates"`
	Queries        map[string]*queryEvidence `json:"queries"`
	ReplayDuration *time.Duration            `json:"replay_duration_ns"`
	Unavailable    map[string]string         `json:"unavailable"`
}

func newCaseDiagnostics() *caseDiagnostics {
	return &caseDiagnostics{
		Queries: make(map[string]*queryEvidence),
		Unavailable: map[string]string{
			"replay_duration":       "candidate does not expose replay-only timing",
			"storage_metrics":       "checkpoint staging and pin records are sampled by the live capacity runner",
			"compaction_ingest_p99": "durability fixtures do not drive live compaction",
		},
	}
}

type processEvidence struct {
	StartupProbe string         `json:"startup_probe"`
	Health       *time.Duration `json:"start_to_health_ns"`
	StartupError string         `json:"startup_error,omitempty"`
	Reaped       bool           `json:"reaped"`
	ExitCode     *int           `json:"exit_code"`
	Signal       string         `json:"exit_signal,omitempty"`
	WaitError    string         `json:"wait_error,omitempty"`
	PeakRSSKiB   *int64         `json:"peak_rss_kib"`
	Log          string         `json:"log_prefix"`
	LogTruncated bool           `json:"log_truncated"`
	start        time.Time
}

type queryEvidence struct {
	Attempts   int            `json:"attempts"`
	Admissions map[string]int `json:"admissions"`
	MaxHeaders *time.Duration `json:"max_response_headers_ns"`
	MaxBody    *time.Duration `json:"max_response_body_complete_ns"`
	Last       queryResponse  `json:"last"`
	Failure    *queryResponse `json:"first_failure,omitempty"`
}

type queryResponse struct {
	Lifetime  int            `json:"lifetime"`
	Status    *int           `json:"http_status"`
	Admission string         `json:"admission"`
	Phase     string         `json:"phase"`
	Headers   *time.Duration `json:"response_headers_ns"`
	Body      *time.Duration `json:"response_body_complete_ns"`
	Elapsed   time.Duration  `json:"elapsed_ns"`
	Bytes     int            `json:"body_bytes"`
	Cursor    bool           `json:"has_cursor_header"`
	Error     string         `json:"detail,omitempty"`
}

func (d *caseDiagnostics) observeQuery(kind string, q queryResponse, err error) {
	e := d.Queries[kind]
	if e == nil {
		e = &queryEvidence{Admissions: make(map[string]int)}
		d.Queries[kind] = e
	}
	e.Attempts++
	e.Admissions[q.Admission]++
	if q.Headers != nil && (e.MaxHeaders == nil || *q.Headers > *e.MaxHeaders) {
		e.MaxHeaders = q.Headers
	}
	if q.Body != nil && (e.MaxBody == nil || *q.Body > *e.MaxBody) {
		e.MaxBody = q.Body
	}
	if err != nil {
		q.Error = diagnosticError(err)
		if e.Failure == nil {
			e.Failure = &q
		}
	} else {
		q.Phase = "complete"
	}
	e.Last = q
}

func (q *queryResponse) observeHeaders(resp *http.Response, elapsed time.Duration) {
	q.Status, q.Headers = &resp.StatusCode, &elapsed
	q.Cursor = len(resp.Header.Values("Bog-Next-Cursor")) > 0
	q.Phase = "body"
	switch {
	case resp.StatusCode == http.StatusOK && resp.Header.Get("Content-Type") == "application/x-ndjson":
		q.Admission = "ndjson_response"
	case resp.StatusCode >= 400:
		q.Admission = "http_rejection"
	default:
		q.Admission = "unexpected_response"
	}
}

func (q *queryResponse) observeRejection(body []byte) {
	if q.Status == nil || *q.Status != http.StatusServiceUnavailable || q.Cursor {
		return
	}
	switch strings.TrimSpace(string(body)) {
	case "query capacity exhausted":
		q.Admission = "capacity_rejection"
	case "query work budget exhausted":
		q.Admission = "work_budget_rejection"
	}
}

func diagnosticError(err error) string {
	var requestErr *url.Error
	if errors.As(err, &requestErr) {
		err = requestErr.Err
	}
	s := err.Error()
	return s[:min(len(s), 512)]
}
