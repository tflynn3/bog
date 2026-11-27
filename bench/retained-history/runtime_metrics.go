package main

import (
	"bytes"
	"encoding/json"

	"bog/verify/capacityevidence"
)

const (
	pendingCompactionsMetric     = capacityevidence.PendingCompactionsMetric
	oldestPinnedGenerationMetric = capacityevidence.OldestPinnedGenerationMetric
)

type runtimeSample = capacityevidence.RuntimeMetrics

func readRuntimeMetrics(raw json.RawMessage, statusErr error) runtimeSample {
	var fields map[string]json.RawMessage
	var absent string
	switch {
	case statusErr != nil:
		absent = "compaction status unavailable: " + diagnosticError(statusErr)
	case len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")):
		absent = "candidate did not expose metrics"
	case json.Unmarshal(raw, &fields) != nil:
		absent = "candidate metrics must be a JSON object"
		// A decode error can leave a partially populated map.
		fields = nil
	}
	m, issues := capacityevidence.DecodeRuntimeMetrics(fields)
	if len(issues) == 0 {
		return m
	}
	m.Unavailable = make(map[string]string, len(issues))
	for field, issue := range issues {
		reason := absent
		if reason == "" {
			reason = runtimeMetricReason(field, issue)
		}
		m.Unavailable[field] = reason
	}
	return m
}

func runtimeMetricReason(field string, issue capacityevidence.RuntimeMetricIssue) string {
	switch field {
	case pendingCompactionsMetric, oldestPinnedGenerationMetric:
		if issue == capacityevidence.RuntimeMetricMissing {
			return "candidate did not expose metric"
		}
		if field == pendingCompactionsMetric {
			return "candidate pending compactions must be a nonnegative integer"
		}
		return "candidate generation ID must be a lowercase hex SHA-256 digest or null"
	default:
		return "candidate did not supply a nonnegative integer"
	}
}
