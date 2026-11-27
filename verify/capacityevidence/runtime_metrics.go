package capacityevidence

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
)

const (
	PendingCompactionsMetric     = "pending_compactions"
	OldestPinnedGenerationMetric = "oldest_pinned_generation"
)

// RuntimeMetrics is the schema-v1 candidate_status_metrics object emitted by
// the benchmark. A nil counter records no measurement. A nil generation with
// a successful snapshot and no matching unavailable reason records no live pins.
type RuntimeMetrics struct {
	PublicationLag         *uint64           `json:"publication_lag_ns"`
	Reserved               *uint64           `json:"staging_reserved_bytes"`
	Used                   *uint64           `json:"staging_used_bytes"`
	OldestPin              *uint64           `json:"oldest_pin_age_ns"`
	Reclaimable            *uint64           `json:"reclaimable_bytes"`
	RetiredPins            *uint64           `json:"retired_generation_pins"`
	PendingCompactions     *uint64           `json:"pending_compactions"`
	OldestPinnedGeneration *string           `json:"oldest_pinned_generation"`
	Unavailable            map[string]string `json:"unavailable,omitempty"`
}

func (m *RuntimeMetrics) counters() [7]struct {
	name string
	dest **uint64
} {
	return [7]struct {
		name string
		dest **uint64
	}{
		{"publication_lag_ns", &m.PublicationLag},
		{"staging_reserved_bytes", &m.Reserved},
		{"staging_used_bytes", &m.Used},
		{"oldest_pin_age_ns", &m.OldestPin},
		{"reclaimable_bytes", &m.Reclaimable},
		{"retired_generation_pins", &m.RetiredPins},
		{PendingCompactionsMetric, &m.PendingCompactions},
	}
}

// RuntimeMetricIssue describes why a field in a live metrics object did
// not supply an observation. The benchmark supplies the diagnostic wording.
type RuntimeMetricIssue uint8

const (
	RuntimeMetricMissing RuntimeMetricIssue = iota + 1
	RuntimeMetricMalformed
)

// DecodeRuntimeMetrics decodes each known member independently so a malformed
// member does not discard valid observations from the same snapshot.
// Unknown members are ignored. Live diagnostics for missing or malformed
// members are returned separately from the values.
func DecodeRuntimeMetrics(fields map[string]json.RawMessage) (RuntimeMetrics, map[string]RuntimeMetricIssue) {
	var m RuntimeMetrics
	issues := make(map[string]RuntimeMetricIssue)
	for _, field := range m.counters() {
		raw, ok := fields[field.name]
		if !ok {
			issues[field.name] = RuntimeMetricMissing
			continue
		}
		if err := jsonv2.Unmarshal(raw, field.dest); err != nil || *field.dest == nil {
			*field.dest = nil
			issues[field.name] = RuntimeMetricMalformed
		}
	}
	raw, ok := fields[OldestPinnedGenerationMetric]
	if !ok {
		issues[OldestPinnedGenerationMetric] = RuntimeMetricMissing
	} else if id, err := decodePinnedGeneration(raw); err != nil {
		issues[OldestPinnedGenerationMetric] = RuntimeMetricMalformed
	} else {
		m.OldestPinnedGeneration = id
	}
	return m, issues
}

var errPinnedGeneration = errors.New("must be a lowercase hexadecimal SHA-256 digest or null")

func decodePinnedGeneration(raw []byte) (*string, error) {
	var id *string
	if err := jsonv2.Unmarshal(raw, &id); err != nil {
		return nil, errPinnedGeneration
	}
	if id != nil && (len(*id) != 2*sha256.Size || strings.Trim(*id, "0123456789abcdef") != "") {
		return nil, errPinnedGeneration
	}
	return id, nil
}

// The report reader uses the same counter schema and retains the generation
// member's presence so omission and explicit null remain distinguishable.
type runtimeObservations struct {
	RuntimeMetrics
	OldestPinnedGeneration jsontext.Value `json:"oldest_pinned_generation,omitzero"`
}

func (m *runtimeObservations) validate(hasStatus bool) error {
	for _, field := range m.counters() {
		if *field.dest != nil {
			if err := m.validateObservation(field.name, hasStatus); err != nil {
				return err
			}
		}
	}

	if len(m.OldestPinnedGeneration) == 0 {
		return nil
	}
	id, err := decodePinnedGeneration(m.OldestPinnedGeneration)
	if err != nil {
		return fmt.Errorf("%s %w", OldestPinnedGenerationMetric, err)
	}
	if id == nil {
		// Explicit null records an empty live pin set only on a successful
		// snapshot with no matching reason. It also serves as a historical
		// placeholder on skipped snapshots.
		return nil
	}
	return m.validateObservation(OldestPinnedGenerationMetric, hasStatus)
}

func (m *runtimeObservations) validateObservation(name string, hasStatus bool) error {
	if m.Unavailable[name] != "" {
		return fmt.Errorf("%s contradicts unavailable status", name)
	}
	if !hasStatus {
		return fmt.Errorf("%s requires a successful candidate status snapshot", name)
	}
	return nil
}
