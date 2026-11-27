package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	ingestNotSubmitted       = "not_submitted"
	ingestAcknowledged       = "acknowledged_durable"
	ingestHTTPRejected       = "http_rejected"
	ingestWALRefused         = "wal_refused"
	ingestRolledBack         = "rollback_observed"
	ingestTransportAmbiguous = "transport_ambiguous"
	ingestUnexpectedResponse = "unexpected_response"
)

type ingestObservation struct {
	Lifetime       int                   `json:"lifetime"`
	Rows           int                   `json:"batch_rows"`
	ExpectedStatus *int                  `json:"expected_http_status,omitempty"`
	Status         *int                  `json:"http_status"`
	ContinueStatus *int                  `json:"continue_http_status,omitempty"`
	Outcome        string                `json:"outcome"`
	Elapsed        time.Duration         `json:"elapsed_ns"`
	Headers        *time.Duration        `json:"response_headers_ns"`
	Body           *time.Duration        `json:"response_body_complete_ns"`
	Rollback       *readinessObservation `json:"rollback_readiness,omitempty"`
	Error          string                `json:"detail,omitempty"`
}

func probeIngest(ctx context.Context, client *http.Client, addr string, payload []byte, rows, want int, start time.Time) (ingestObservation, error) {
	const path = "/ingest"
	const maxIngestResponse = 4096
	result, err := exchange(ctx, client, addr, http.MethodPost, path, payload, maxIngestResponse, start)
	obs := ingestObservation{
		Rows: rows, ExpectedStatus: &want, Outcome: ingestNotSubmitted,
		Elapsed: time.Since(start), Headers: result.headersAt, Body: result.bodyAt,
	}
	if result.response == nil {
		if result.attempted {
			obs.Outcome = ingestTransportAmbiguous
		}
		return obs, err
	}
	status := result.response.StatusCode
	obs.Status = &status
	switch {
	case status == http.StatusNoContent:
		obs.Outcome = ingestAcknowledged
	case status == http.StatusServiceUnavailable && result.bodyAt != nil && strings.TrimSpace(string(result.body)) == "WAL unavailable":
		obs.Outcome = ingestWALRefused
	case status >= 400 && status < 500 || status == http.StatusServiceUnavailable:
		obs.Outcome = ingestHTTPRejected
	default:
		obs.Outcome = ingestUnexpectedResponse
	}
	if err != nil {
		return obs, err
	}
	if err := result.expectStatus(http.MethodPost, path, want); err != nil {
		return obs, err
	}
	if want == http.StatusServiceUnavailable && obs.Outcome != ingestWALRefused {
		return obs, fmt.Errorf("expected WAL allocation refusal, got %.200q", result.body)
	}
	return obs, nil
}
