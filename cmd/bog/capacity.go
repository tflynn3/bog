package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
)

type capacityStatus struct {
	State         string          `json:"state"`
	Generation    uint64          `json:"generation"`
	CoveredRows   uint64          `json:"covered_rows"`
	CommittedRows uint64          `json:"committed_rows"`
	Metrics       capacityMetrics `json:"metrics"`
}

type capacityMetrics struct {
	Pending uint64  `json:"pending_compactions"`
	Oldest  *string `json:"oldest_pinned_generation"`
	Retired uint64  `json:"retired_generation_pins"`
	Staging uint64  `json:"staging_used_bytes"`
	Tail    uint64  `json:"replay_tail_used_bytes"`
}

func (r *retained) capacityControl(api *httpapi.API) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		values := req.URL.Query()
		action := values.Get("action")
		switch {
		case action == "retire" && req.Method == http.MethodPost:
			if err := api.RetireQueryCapture(values.Get("cursor")); err != nil {
				code := http.StatusBadRequest
				if errors.Is(err, httpapi.ErrCaptureExpired) {
					code = http.StatusGone
				}
				http.Error(w, err.Error(), code)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		case action == "start" && req.Method == http.MethodPost,
			action == "status" && req.Method == http.MethodGet:
		default:
			http.Error(w, "invalid compaction operation", http.StatusBadRequest)
			return
		}
		cutoff, err := strconv.ParseUint(values.Get("cutoff"), 10, 64)
		if err != nil {
			http.Error(w, "invalid cutoff", http.StatusBadRequest)
			return
		}
		status, err := r.controlStatus(action, cutoff)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			log.Printf("write compaction status: %v", err)
		}
	}
}

func (r *retained) controlStatus(action string, cutoff uint64) (capacityStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	covered := r.point.NextOrdinal
	if action == "start" && cutoff != covered {
		if err := r.startLocked(cutoff, true); err != nil {
			return capacityStatus{}, err
		}
	}
	if r.maintErr != nil && r.cutoff == cutoff && cutoff != covered {
		return capacityStatus{}, r.maintErr
	}
	status := capacityStatus{State: "complete", Generation: r.sequence, CoveredRows: covered, CommittedRows: r.point.CommittedOrdinal}
	if r.busy != nil {
		status.State = "running"
		status.Metrics.Pending = 1
	}
	usage := r.budget.Usage()
	status.Metrics.Staging = usage.InUse[checkpoint.StagingBytes]
	status.Metrics.Tail = usage.InUse[checkpoint.ReplayTailBytes]
	var earliest uint64
	for id, n := range r.pins {
		if id != r.point.Generation {
			status.Metrics.Retired += uint64(n)
		}
	}
	for _, view := range r.views {
		if status.Metrics.Oldest == nil || view.point.NextOrdinal < earliest {
			id := hexSum(view.point.Generation)
			status.Metrics.Oldest, earliest = &id, view.point.NextOrdinal
		}
	}
	return status, nil
}
