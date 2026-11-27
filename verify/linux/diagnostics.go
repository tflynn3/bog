package main

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type caseDiagnostics struct {
	Candidates     []*processEvidence                               `json:"candidates"`
	Queries        map[string]*queryEvidence                        `json:"queries"`
	Readiness      map[string]*readinessEvidence                    `json:"readiness,omitempty"`
	Ingest         map[string]*observationSeries[ingestObservation] `json:"ingest,omitempty"`
	UncertainWAL   *readinessObservation                            `json:"uncertain_wal_outcome,omitempty"`
	ReplayDuration *time.Duration                                   `json:"replay_duration_ns"`
	Unavailable    map[string]string                                `json:"unavailable"`
}

func newCaseDiagnostics() *caseDiagnostics {
	return &caseDiagnostics{
		Queries:   make(map[string]*queryEvidence),
		Readiness: make(map[string]*readinessEvidence),
		Ingest:    make(map[string]*observationSeries[ingestObservation]),
		Unavailable: map[string]string{
			"replay_duration":              "candidate does not expose replay-only timing",
			"storage_metrics":              "checkpoint staging and pin records are sampled by the live capacity runner",
			"uncertain_wal_outcome":        "no validated recovery_required readiness response was observed",
			"cgroup_peak_and_retired_pins": "observed separately by the live capacity runner in capacity.json",
			"compaction_ingest_p99":        "durability fixtures do not drive live compaction",
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

func (q *queryResponse) observeHeaders(resp *http.Response, elapsed *time.Duration) {
	q.Status, q.Headers = &resp.StatusCode, elapsed
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

func (q *queryResponse) observeBody(body []byte, elapsed *time.Duration, transportErr error) {
	q.Bytes, q.Body = len(body), elapsed
	if elapsed == nil {
		return
	}
	q.observeRejection(body)
	q.Phase = "validation"
	if transportErr != nil {
		q.Phase = "close"
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

// Probe keys are fixed by the fixtures. Store a constant number of samples
// per key even when polling stability or submitting benchmark batches.
type observationSeries[T any] struct {
	Attempts     int `json:"attempts"`
	First        *T  `json:"first,omitempty"`
	Last         *T  `json:"last,omitempty"`
	FirstFailure *T  `json:"first_failure,omitempty"`
}

func (s *observationSeries[T]) record(obs T, failed bool) {
	s.Attempts++
	if s.First == nil {
		s.First = &obs
	}
	s.Last = &obs
	if failed && s.FirstFailure == nil {
		s.FirstFailure = &obs
	}
}

type readinessEvidence struct {
	ExpectedReason string        `json:"expected_reason"`
	StableFor      time.Duration `json:"required_stable_ns"`
	observationSeries[readinessObservation]
	Error string `json:"detail,omitempty"`
}

func (d *caseDiagnostics) observeReadiness(e *readinessEvidence, obs readinessObservation, err error) {
	if err != nil {
		obs.Error = diagnosticError(err)
	}
	e.record(obs, err != nil)
	if obs.Ready != nil && !*obs.Ready && obs.Reason == "recovery_required" && d.UncertainWAL == nil {
		d.UncertainWAL = &obs
		delete(d.Unavailable, "uncertain_wal_outcome")
	}
}

func (d *caseDiagnostics) observeIngest(key string, obs ingestObservation, err error) {
	if err != nil {
		obs.Error = diagnosticError(err)
	}
	e := d.Ingest[key]
	if e == nil {
		e = &observationSeries[ingestObservation]{}
		d.Ingest[key] = e
	}
	e.record(obs, err != nil)
}

func (d *caseDiagnostics) confirmRollback(key string, obs readinessObservation) {
	e := d.Ingest[key]
	if e != nil && e.Last.Outcome == ingestWALRefused && e.Last.Lifetime == obs.Lifetime && obs.state() == diskRollback {
		e.Last.Outcome = ingestRolledBack
		e.Last.Rollback = &obs
	}
}
