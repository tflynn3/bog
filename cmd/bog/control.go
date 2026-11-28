package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
)

// Keep explicitly requested maintenance observable even when staging
// finishes in less than a status polling interval.
const manualPublicationDelay = 150 * time.Millisecond

type compactionStatus struct {
	State         string            `json:"state"`
	Generation    uint64            `json:"generation"`
	CoveredRows   uint64            `json:"covered_rows"`
	CommittedRows uint64            `json:"committed_rows"`
	Metrics       compactionMetrics `json:"metrics"`
}

type compactionMetrics struct {
	Pending  uint64  `json:"pending_compactions"`
	Retired  uint64  `json:"retired_generation_pins"`
	Oldest   *string `json:"oldest_pinned_generation"`
	Reserved uint64  `json:"staging_reserved_bytes"`
	Used     uint64  `json:"staging_used_bytes"`
	Replay   uint64  `json:"replay_tail_bytes"`
}

func (s *retained) compactionHandler(api *httpapi.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		action := r.URL.Query().Get("action")
		method := http.MethodPost
		if action == "status" {
			method = http.MethodGet
		}
		if r.Method != method {
			w.Header().Set("Allow", method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if action == "retire" {
			if err := api.RetireQueryCapture(r.URL.Query().Get("cursor")); err != nil {
				code := http.StatusInternalServerError
				if errors.Is(err, store.ErrInvalidQueryCursor) {
					code = http.StatusBadRequest
				}
				http.Error(w, err.Error(), code)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		cutoff, err := strconv.ParseUint(r.URL.Query().Get("cutoff"), 10, 64)
		if err != nil || action != "status" && action != "start" {
			http.Error(w, "invalid compaction request", http.StatusBadRequest)
			return
		}
		status, code, err := s.observeCompaction(r, action, cutoff)
		if err != nil {
			http.Error(w, err.Error(), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}

func (s *retained) observeCompaction(r *http.Request, action string, cutoff uint64) (status compactionStatus, code int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.wal.Capture(r.Context())
	if err != nil {
		return status, http.StatusServiceUnavailable, err
	}
	committed, end := c.NextOrdinal(), c.CommittedEnd()
	_, whole := c.Cutoff(cutoff)
	if err := c.Release(); err != nil {
		return status, http.StatusServiceUnavailable, err
	}
	if action == "start" {
		switch {
		case cutoff == s.current.Boundary.NextOrdinal:
			// The requested cutoff has already been durably selected.
		case !whole || cutoff < s.current.Boundary.NextOrdinal:
			return status, http.StatusBadRequest, errors.New("cutoff must be a committed frame boundary")
		case s.running || s.stopping.Load():
			return status, http.StatusServiceUnavailable, errors.New("compaction unavailable")
		default:
			s.running, s.failed, s.pending, s.requested = true, nil, compactionJob{cutoff, manualPublicationDelay}, cutoff
			s.signalMaintenance()
		}
	} else if !s.running && cutoff > s.current.Boundary.NextOrdinal && s.failed != nil {
		return status, http.StatusServiceUnavailable, s.failed
	}
	stage := s.budget.Usage().InUse[checkpoint.StagingBytes]
	status = compactionStatus{
		State: "complete", Generation: s.ordinal, CoveredRows: s.current.Boundary.NextOrdinal, CommittedRows: committed,
		Metrics: compactionMetrics{Reserved: stage, Used: stage, Replay: uint64(end - s.current.Boundary.CoveredOffset)},
	}
	if s.running {
		status.State = "running"
		status.Metrics.Pending = 1
	}
	oldest := s.current.Boundary.NextOrdinal
	for _, v := range s.views {
		if v.point.Generation != [32]byte(s.current.ID) {
			status.Metrics.Retired++
		}
		if status.Metrics.Oldest == nil || v.point.NextOrdinal < oldest {
			id := fmt.Sprintf("%x", v.point.Generation)
			status.Metrics.Oldest = &id
			oldest = v.point.NextOrdinal
		}
	}
	return status, http.StatusOK, nil
}
