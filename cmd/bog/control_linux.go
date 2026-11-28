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

func (s *retained) capacityControl(api http.Handler, issuer *httpapi.API) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", api)
	mux.HandleFunc("/_capacity/compaction", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		action := q.Get("action")
		if action == "retire" {
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			err := issuer.RetireQueryCapture(q.Get("cursor"))
			switch {
			case errors.Is(err, store.ErrInvalidQueryCursor):
				http.Error(w, "invalid cursor", http.StatusBadRequest)
			case err != nil:
				http.Error(w, "capture release failed", http.StatusInternalServerError)
			default:
				w.WriteHeader(http.StatusNoContent)
			}
			return
		}
		var method string
		switch action {
		case "start":
			method = http.MethodPost
		case "status":
			method = http.MethodGet
		default:
			http.Error(w, "invalid compaction action", http.StatusBadRequest)
			return
		}
		if r.Method != method {
			w.Header().Set("Allow", method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		cut, err := strconv.ParseUint(q.Get("cutoff"), 10, 64)
		if err != nil {
			http.Error(w, "invalid compaction cutoff", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		if action == "start" {
			err = s.startLocked(cut)
		} else if s.maintenance == nil && s.point.NextOrdinal < cut {
			err = s.maintenanceErr
		}
		reply := s.controlStatusLocked()
		s.mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			panic(http.ErrAbortHandler)
		}
	})
	return mux
}

type compactionStatus struct {
	State         string         `json:"state"`
	Generation    uint64         `json:"generation"`
	CoveredRows   uint64         `json:"covered_rows"`
	CommittedRows uint64         `json:"committed_rows"`
	Metrics       map[string]any `json:"metrics"`
}

func (s *retained) controlStatusLocked() compactionStatus {
	p := s.point
	staging := s.budget.Usage().InUse[checkpoint.StagingBytes]
	metrics := map[string]any{
		"publication_lag_ns":       s.publicationLag.Nanoseconds(),
		"staging_reserved_bytes":   staging,
		"staging_used_bytes":       staging,
		"pending_compactions":      0,
		"oldest_pin_age_ns":        0,
		"oldest_pinned_generation": nil,
		"replay_tail_bytes":        p.CommittedEnd - p.CoveredOffset,
	}
	status := compactionStatus{"complete", s.selector.Sequence, p.NextOrdinal, p.CommittedOrdinal, metrics}
	if s.maintenance != nil {
		status.State = "running"
		metrics["pending_compactions"] = 1
	}
	var pins int
	var reclaimable int64
	for _, e := range s.retired {
		pins += e.pins
		reclaimable += e.size
		if e.pins > 0 && metrics["oldest_pinned_generation"] == nil {
			age := time.Since(e.retiredAt).Nanoseconds()
			id := hexDigest(e.gen.ID)
			metrics["oldest_pinned_generation"] = id
			metrics["oldest_pin_age_ns"] = age
			metrics["oldest_predecessor_pin_age_ns"] = age
			metrics["predecessor_pin_generation"] = e.number
		}
	}
	metrics["retired_generation_pins"] = pins
	metrics["reclaimable_bytes"] = reclaimable
	if s.reclaimedBytes > 0 && s.reclaimedReplacement == s.selector.Sequence {
		metrics["reclaimed_bytes"] = s.reclaimedBytes
		metrics["reclaimed_predecessor_generation"] = s.reclaimedPredecessor
		metrics["reclaimed_replacement_generation"] = s.reclaimedReplacement
	}
	return status
}
