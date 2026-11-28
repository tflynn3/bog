package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
)

type compactionObservation struct {
	State      string            `json:"state"`
	Generation uint64            `json:"generation"`
	Covered    uint64            `json:"covered_rows"`
	Committed  uint64            `json:"committed_rows"`
	Metrics    compactionMetrics `json:"metrics"`
}

type compactionMetrics struct {
	Pending              int     `json:"pending_compactions"`
	Pinned               *string `json:"oldest_pinned_generation"`
	Retired              int     `json:"retired_generation_pins"`
	PinAge               uint64  `json:"oldest_pin_age_ns"`
	StagingReserved      uint64  `json:"staging_reserved_bytes"`
	StagingUsed          uint64  `json:"staging_used_bytes"`
	Replay               uint64  `json:"replay_tail_bytes"`
	Reclaimable          uint64  `json:"reclaimable_bytes"`
	PublicationLag       uint64  `json:"publication_lag_ns"`
	PredecessorAge       *uint64 `json:"oldest_predecessor_pin_age_ns,omitempty"`
	Predecessor          *uint64 `json:"predecessor_pin_generation,omitempty"`
	Reclaimed            uint64  `json:"reclaimed_bytes,omitempty"`
	ReclaimedPredecessor *uint64 `json:"reclaimed_predecessor_generation,omitempty"`
	ReclaimedReplacement *uint64 `json:"reclaimed_replacement_generation,omitempty"`
}

func (r *retainedRuntime) observationLocked() compactionObservation {
	p, _ := r.wal.PublishedAuthority()
	status := compactionObservation{State: "complete", Generation: p.Sequence, Covered: p.First, Committed: p.Next}
	m := &status.Metrics
	m.Replay = uint64(p.End - p.Covered)
	m.StagingReserved = r.budget.Usage().InUse[checkpoint.StagingBytes]
	m.StagingUsed = m.StagingReserved
	m.PublicationLag = r.publicationLag
	if r.running {
		status.State, m.Pending = "running", 1
	}
	now := time.Now()
	var oldest, predecessor time.Time
	for _, view := range r.captures {
		if oldest.IsZero() || view.at.Before(oldest) {
			oldest = view.at
			number := strconv.FormatUint(view.point.Sequence, 10)
			m.Pinned = &number
			m.PinAge = uint64(now.Sub(view.at))
		}
		if view.point.Sequence < p.Sequence {
			m.Retired++
			if predecessor.IsZero() || view.at.Before(predecessor) {
				predecessor = view.at
				age := uint64(now.Sub(view.at))
				m.PredecessorAge, m.Predecessor = &age, &view.point.Sequence
			}
		}
	}
	for _, retired := range r.retired {
		m.Reclaimable += retired.bytes
	}
	if r.reclaimed.bytes != 0 {
		m.Reclaimed = r.reclaimed.bytes
		m.ReclaimedPredecessor = &r.reclaimed.sequence
		m.ReclaimedReplacement = &r.reclaimed.replacement
	}
	return status
}

func (r *retainedRuntime) capacityControl(api *httpapi.API) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		params := req.URL.Query()
		action := params.Get("action")
		method := http.MethodPost
		if action == "status" {
			method = http.MethodGet
		}
		if action != "status" && action != "start" && action != "retire" {
			http.Error(w, "invalid compaction action", http.StatusBadRequest)
			return
		}
		if req.Method != method {
			w.Header().Set("Allow", method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if action == "retire" {
			if err := api.RetireQueryCapture(params.Get("cursor")); err != nil {
				if errors.Is(err, store.ErrInvalidQueryCursor) {
					http.Error(w, "invalid cursor", http.StatusBadRequest)
				} else {
					http.Error(w, "capture release failed", http.StatusServiceUnavailable)
				}
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		cutoff, err := strconv.ParseUint(params.Get("cutoff"), 10, 64)
		if err != nil {
			http.Error(w, "invalid compaction cutoff", http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		status := r.observationLocked()
		if action == "start" && cutoff != status.Covered {
			if cutoff < status.Covered || cutoff > status.Committed {
				r.mu.Unlock()
				http.Error(w, "cutoff outside the committed suffix", http.StatusBadRequest)
				return
			}
			if r.running || r.stopped.Load() {
				r.mu.Unlock()
				http.Error(w, "compaction unavailable", http.StatusServiceUnavailable)
				return
			}
			r.running, r.maintenanceErr = true, nil
			r.jobs <- cutoff
			status.State, status.Metrics.Pending = "running", 1
		} else if action == "status" && cutoff > status.Covered && !r.running && r.maintenanceErr != nil {
			r.mu.Unlock()
			http.Error(w, "compaction failed", http.StatusServiceUnavailable)
			return
		}
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}
