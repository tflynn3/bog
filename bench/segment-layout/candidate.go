package main

import (
	"errors"
	"fmt"
	"strings"
)

type candidateIdentity struct {
	Revision string
	SHA256   string
	Arch     string
}

func lowerHex(s string, length int) bool {
	return len(s) == length && strings.Trim(s, "0123456789abcdef") == ""
}

func (c candidateIdentity) valid() bool {
	return lowerHex(c.Revision, 40) && lowerHex(c.SHA256, 64) && (c.Arch == "amd64" || c.Arch == "arm64")
}

type livePublication struct {
	Before          controlReply `json:"before_backfill"`
	After           controlReply `json:"after_compaction"`
	PersistedParity bool         `json:"persisted_row_parity"`
}

func (r coldMatrixReport) requireCandidate(candidate candidateIdentity) error {
	if !candidate.valid() || r.Environment["os"] != "linux" || r.Environment["arch"] != candidate.Arch {
		return errors.New("cold matrix does not match the expected native Linux candidate architecture")
	}
	for _, profile := range r.Profiles {
		if err := profile.requireCandidate(candidate); err != nil {
			return fmt.Errorf("%s: %w", profile.Fixture.Profile, err)
		}
	}
	return nil
}

func (r coldReport) requireCandidate(candidate candidateIdentity) error {
	live := r.Live
	if live.Status != "measured" {
		return fmt.Errorf("live candidate control unavailable: %s", live.Reason)
	}
	if live.Revision != candidate.Revision || live.BinarySHA256 != candidate.SHA256 {
		return errors.New("live candidate revision or binary SHA-256 differs from the expected identity")
	}
	if err := live.validate(r.Production.Phases); err != nil {
		return err
	}
	initialRows := r.Production.Phases[0].Rows
	for _, m := range live.Measurements {
		p := m.Publication
		if p == nil || !p.PersistedParity ||
			!p.Before.atCutoff("complete", initialRows) || !p.After.atCutoff("complete", m.CoveredRows) ||
			*p.After.Generation <= *p.Before.Generation {
			return fmt.Errorf("%s: missing or invalid ordered live publication and persisted row parity", m.Scenario)
		}
	}
	return nil
}
