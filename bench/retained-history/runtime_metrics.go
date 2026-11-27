package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

const (
	pendingCompactionsMetric     = "pending_compactions"
	oldestPinnedGenerationMetric = "oldest_pinned_generation"
)

type runtimeSample struct {
	PublicationLag     *uint64 `json:"publication_lag_ns"`
	Reserved           *uint64 `json:"staging_reserved_bytes"`
	Used               *uint64 `json:"staging_used_bytes"`
	OldestPin          *uint64 `json:"oldest_pin_age_ns"`
	Reclaimable        *uint64 `json:"reclaimable_bytes"`
	RetiredPins        *uint64 `json:"retired_generation_pins"`
	PendingCompactions *uint64 `json:"pending_compactions"`
	// For a decoded snapshot, nil without an unavailable reason means no live pins.
	OldestPinnedGeneration *string           `json:"oldest_pinned_generation"`
	Unavailable            map[string]string `json:"unavailable,omitempty"`
}

func readRuntimeMetrics(raw json.RawMessage, statusErr error) runtimeSample {
	var m runtimeSample
	var fields map[string]json.RawMessage
	var absent string
	switch {
	case statusErr != nil:
		absent = "compaction status unavailable: " + diagnosticError(statusErr)
	case len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")):
		absent = "candidate did not expose metrics"
	case json.Unmarshal(raw, &fields) != nil:
		absent = "candidate metrics must be a JSON object"
	}
	for _, field := range []struct {
		name string
		dest **uint64
	}{
		{"publication_lag_ns", &m.PublicationLag},
		{"staging_reserved_bytes", &m.Reserved},
		{"staging_used_bytes", &m.Used},
		{"oldest_pin_age_ns", &m.OldestPin},
		{"reclaimable_bytes", &m.Reclaimable},
		{"retired_generation_pins", &m.RetiredPins},
	} {
		reason := absent
		if reason == "" {
			if err := json.Unmarshal(fields[field.name], field.dest); err != nil || *field.dest == nil {
				*field.dest = nil
				reason = "candidate did not supply a nonnegative integer"
			}
		}
		if reason != "" {
			m.markUnavailable(field.name, reason)
		}
	}
	if absent != "" {
		m.markUnavailable(pendingCompactionsMetric, absent)
		m.markUnavailable(oldestPinnedGenerationMetric, absent)
		return m
	}
	m.readCompactionAndPins(fields)
	return m
}

func (m *runtimeSample) markUnavailable(field, reason string) {
	if m.Unavailable == nil {
		m.Unavailable = make(map[string]string)
	}
	m.Unavailable[field] = reason
}

func (m *runtimeSample) readCompactionAndPins(fields map[string]json.RawMessage) {
	const missing = "candidate did not expose metric"
	switch raw := fields[pendingCompactionsMetric]; {
	case len(raw) == 0:
		m.markUnavailable(pendingCompactionsMetric, missing)
	case json.Unmarshal(raw, &m.PendingCompactions) != nil || m.PendingCompactions == nil:
		m.PendingCompactions = nil
		m.markUnavailable(pendingCompactionsMetric, "candidate pending compactions must be a nonnegative integer")
	}

	raw := fields[oldestPinnedGenerationMetric]
	var id string
	switch {
	case len(raw) == 0:
		m.markUnavailable(oldestPinnedGenerationMetric, missing)
	case bytes.Equal(raw, []byte("null")):
		// The producer observed that no generation had a live pin.
	case json.Unmarshal(raw, &id) != nil || !validGenerationID(id):
		m.markUnavailable(oldestPinnedGenerationMetric, "candidate generation ID must be a lowercase hex SHA-256 digest or null")
	default:
		m.OldestPinnedGeneration = &id
	}
}

func validGenerationID(id string) bool {
	if len(id) != hex.EncodedLen(sha256.Size) || strings.ToLower(id) != id {
		return false
	}
	var digest [sha256.Size]byte
	_, err := hex.Decode(digest[:], []byte(id))
	return err == nil
}
