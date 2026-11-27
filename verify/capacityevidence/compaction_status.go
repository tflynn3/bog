package capacityevidence

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
)

// DecodeCompactionStatus decodes one live control reply. The caller bounds the
// input and decides whether the observation proves publication or pin drain.
// Metrics are returned as received, with nil for omission and raw null for an
// explicit null. An invalid envelope returns no observation or metrics.
func DecodeCompactionStatus(data []byte) (CompactionObservation, json.RawMessage, error) {
	// Read envelope members individually so duplicate checks stop at this
	// level. The metric consumers interpret the contents of the raw value.
	dec := jsontext.NewDecoder(bytes.NewReader(data), jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	start, err := dec.ReadToken()
	if err != nil {
		return CompactionObservation{}, nil, err
	}
	if start.Kind() != '{' {
		return CompactionObservation{}, nil, errors.New("expected compaction status JSON object")
	}

	var observation CompactionObservation
	fields := [...]struct {
		name string
		dest any
	}{
		{"state", &observation.State},
		{"generation", &observation.Generation},
		{"covered_rows", &observation.CoveredRows},
		{"committed_rows", &observation.CommittedRows},
	}
	seen := make(map[string]jsontext.Value)
	for {
		name, err := dec.ReadToken()
		if err != nil {
			return CompactionObservation{}, nil, err
		}
		if name.Kind() == '}' {
			break
		}
		key := name.String()
		if _, exists := seen[key]; exists {
			return CompactionObservation{}, nil, fmt.Errorf("duplicate compaction status member %q", key)
		}
		switch key {
		case "state", "generation", "covered_rows", "committed_rows", "metrics":
			value, err := dec.ReadValue()
			if err != nil {
				return CompactionObservation{}, nil, err
			}
			seen[key] = value.Clone()
		default:
			if err := dec.SkipValue(); err != nil {
				return CompactionObservation{}, nil, err
			}
			seen[key] = nil
		}
	}
	switch _, err := dec.ReadToken(); err {
	case io.EOF:
	case nil:
		return CompactionObservation{}, nil, errors.New("unexpected trailing data after compaction status")
	default:
		return CompactionObservation{}, nil, err
	}

	for _, field := range fields {
		raw := seen[field.name]
		if raw == nil || raw.Kind() == 'n' {
			return CompactionObservation{}, nil, fmt.Errorf("missing or null %q", field.name)
		}
		if err := jsonv2.Unmarshal(raw, field.dest); err != nil {
			return CompactionObservation{}, nil, fmt.Errorf("%s: %w", field.name, err)
		}
	}
	if observation.State != "running" && observation.State != "complete" {
		return CompactionObservation{}, nil, fmt.Errorf("unexpected state %q", observation.State)
	}
	if observation.CoveredRows < 0 || observation.CommittedRows < observation.CoveredRows {
		return CompactionObservation{}, nil, fmt.Errorf("invalid row counters: covered=%d committed=%d", observation.CoveredRows, observation.CommittedRows)
	}
	return observation, json.RawMessage(seen["metrics"]), nil
}
