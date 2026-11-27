package capacityevidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	PendingCompactionsMetric       = "pending_compactions"
	OldestPinnedGenerationMetric   = "oldest_pinned_generation"
	ReplayTailBytesMetric          = "replay_tail_bytes"
	PredecessorPinAgeMetric        = "oldest_predecessor_pin_age_ns"
	PredecessorPinGenerationMetric = "predecessor_pin_generation"
	ReclaimedBytesMetric           = "reclaimed_bytes"
	ReclaimedPredecessorMetric     = "reclaimed_predecessor_generation"
	ReclaimedReplacementMetric     = "reclaimed_replacement_generation"
)

// RuntimeMetrics is the schema-v1 candidate_status_metrics object emitted by
// the benchmark. A nil counter records no measurement. A nil generation with
// a successful snapshot and no matching unavailable reason records no live pins.
//
// The correlation fields describe the same snapshot as the compaction status.
// ReplayTailBytes measures committed suffix bytes eligible for replay from the
// status generation and covered boundary. PredecessorPinAge measures the oldest
// live reference to PredecessorPinGeneration. These generation numbers use the
// control route's compaction sequence. A completed reclamation reports the bytes
// actually credited to capacity and both generations in the requested replacement.
// The runtime omits the reclamation tuple until the credit is complete.
type RuntimeMetrics struct {
	PublicationLag           *uint64           `json:"publication_lag_ns"`
	Reserved                 *uint64           `json:"staging_reserved_bytes"`
	Used                     *uint64           `json:"staging_used_bytes"`
	OldestPin                *uint64           `json:"oldest_pin_age_ns"`
	Reclaimable              *uint64           `json:"reclaimable_bytes"`
	RetiredPins              *uint64           `json:"retired_generation_pins"`
	PendingCompactions       *uint64           `json:"pending_compactions"`
	OldestPinnedGeneration   *string           `json:"oldest_pinned_generation"`
	ReplayTailBytes          *uint64           `json:"replay_tail_bytes,omitempty"`
	PredecessorPinAge        *uint64           `json:"oldest_predecessor_pin_age_ns,omitempty"`
	PredecessorPinGeneration *uint64           `json:"predecessor_pin_generation,omitempty"`
	ReclaimedBytes           *uint64           `json:"reclaimed_bytes,omitempty"`
	ReclaimedPredecessor     *uint64           `json:"reclaimed_predecessor_generation,omitempty"`
	ReclaimedReplacement     *uint64           `json:"reclaimed_replacement_generation,omitempty"`
	Unavailable              map[string]string `json:"unavailable,omitempty"`
}

func (m *RuntimeMetrics) counters() [13]struct {
	name string
	dest **uint64
} {
	return [13]struct {
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
		{ReplayTailBytesMetric, &m.ReplayTailBytes},
		{PredecessorPinAgeMetric, &m.PredecessorPinAge},
		{PredecessorPinGenerationMetric, &m.PredecessorPinGeneration},
		{ReclaimedBytesMetric, &m.ReclaimedBytes},
		{ReclaimedPredecessorMetric, &m.ReclaimedPredecessor},
		{ReclaimedReplacementMetric, &m.ReclaimedReplacement},
	}
}

// RuntimeMetricIssue describes why a field in a live metrics object did
// not supply an observation. The benchmark supplies the diagnostic wording.
type RuntimeMetricIssue uint8

const (
	RuntimeMetricMissing RuntimeMetricIssue = iota + 1
	RuntimeMetricMalformed
	RuntimeMetricDuplicate
)

// ErrRuntimeMetricsAbsent indicates an omitted or null live metrics object.
var ErrRuntimeMetricsAbsent = errors.New("live metrics object is absent")

// DecodeRuntimeMetrics reads the raw live metrics object before any map
// conversion can collapse repeated names. Each known member supplies an
// observation only if it occurs once and has a valid value. Unknown members
// are ignored. An absent or invalid object returns an error and marks every
// known member missing.
func DecodeRuntimeMetrics(raw []byte) (RuntimeMetrics, map[string]RuntimeMetricIssue, error) {
	var m RuntimeMetrics
	counters := m.counters()
	members := make(map[string]runtimeMetricMember, len(counters)+1)
	for _, field := range counters {
		members[field.name] = runtimeMetricMember{}
	}
	members[OldestPinnedGenerationMetric] = runtimeMetricMember{}
	err := readRuntimeMetricMembers(raw, members)
	if err != nil {
		clear(members)
	}

	issues := make(map[string]RuntimeMetricIssue)
	for _, field := range counters {
		member := members[field.name]
		if issue := member.issue(); issue != 0 {
			issues[field.name] = issue
			continue
		}
		if err := jsonv2.Unmarshal(member.raw, field.dest); err != nil || *field.dest == nil {
			*field.dest = nil
			issues[field.name] = RuntimeMetricMalformed
		}
	}
	member := members[OldestPinnedGenerationMetric]
	if issue := member.issue(); issue != 0 {
		issues[OldestPinnedGenerationMetric] = issue
	} else if id, err := decodePinnedGeneration(member.raw); err != nil {
		issues[OldestPinnedGenerationMetric] = RuntimeMetricMalformed
	} else {
		m.OldestPinnedGeneration = id
	}
	return m, issues, err
}

type runtimeMetricMember struct {
	raw       jsontext.Value
	duplicate bool
}

func (m runtimeMetricMember) issue() RuntimeMetricIssue {
	switch {
	case m.duplicate:
		return RuntimeMetricDuplicate
	case m.raw == nil:
		return RuntimeMetricMissing
	default:
		return 0
	}
}

func readRuntimeMetricMembers(raw []byte, members map[string]runtimeMetricMember) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ErrRuntimeMetricsAbsent
	}
	// Match the compaction envelope's permissive syntax reader. Known values
	// still use strict typed decoding after their member counts are established.
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	start, err := dec.ReadToken()
	if err != nil {
		return err
	}
	if start.Kind() != '{' {
		return errors.New("runtime metrics must be a JSON object")
	}
	for {
		name, err := dec.ReadToken()
		if err != nil {
			return err
		}
		if name.Kind() == '}' {
			break
		}
		key := name.String()
		member, known := members[key]
		if !known || member.raw != nil || member.duplicate {
			if err := dec.SkipValue(); err != nil {
				return err
			}
			if known {
				members[key] = runtimeMetricMember{duplicate: true}
			}
			continue
		}
		value, err := dec.ReadValue()
		if err != nil {
			return err
		}
		members[key] = runtimeMetricMember{raw: value.Clone()}
	}
	switch _, err := dec.ReadToken(); err {
	case io.EOF:
		return nil
	case nil:
		return errors.New("unexpected trailing data after runtime metrics")
	default:
		return err
	}
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
