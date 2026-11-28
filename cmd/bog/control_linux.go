package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
)

const compactionPath = "/_capacity/compaction"

func loopback(listener net.Listener) bool {
	address, ok := listener.Addr().(*net.TCPAddr)
	return ok && address.IP.To4() != nil && address.IP.IsLoopback()
}

type compactionStatus struct {
	State      string            `json:"state"`
	Generation uint64            `json:"generation"`
	Covered    uint64            `json:"covered_rows"`
	Committed  uint64            `json:"committed_rows"`
	Metrics    compactionMetrics `json:"metrics"`
}

type compactionMetrics struct {
	Pending         int     `json:"pending_compactions"`
	Retired         int     `json:"retired_generation_pins"`
	Oldest          *string `json:"oldest_pinned_generation"`
	OldestAge       uint64  `json:"oldest_pin_age_ns"`
	OldestRevision  uint64  `json:"predecessor_pin_generation"`
	TailBytes       uint64  `json:"replay_tail_bytes"`
	StagingUsed     uint64  `json:"staging_used_bytes"`
	StagingReserved uint64  `json:"staging_reserved_bytes"`
	Reclaimable     uint64  `json:"reclaimable_bytes"`
	PublicationLag  uint64  `json:"publication_lag_ns"`
}

func (s *retainedStorage) statusLocked() compactionStatus {
	a := s.owner.Selected()
	status := compactionStatus{State: "complete", Generation: a.Revision,
		Covered: s.active.Boundary.NextOrdinal, Committed: a.CommittedOrdinal}
	if s.job != nil || s.switching != nil {
		status.State, status.Metrics.Pending = "running", 1
	}
	if a.Generation != [32]byte(s.active.ID) && a.Revision > 1 {
		status.Generation--
	}
	if s.fenced != nil || s.failedCutoff != 0 {
		status.State = "failed"
	}
	status.Metrics.PublicationLag = uint64(s.publicationLag)
	status.Metrics.TailBytes = uint64(a.CommittedEnd - a.CoveredOffset)
	usage := s.budget.Usage()
	status.Metrics.StagingUsed = usage.InUse[checkpoint.StagingBytes]
	status.Metrics.StagingReserved = status.Metrics.StagingUsed
	for _, old := range s.retired {
		a := old.switches.Before
		size := uint64(a.CommittedEnd - a.Base)
		select {
		case <-old.drained:
			status.Metrics.Reclaimable += size
		default:
			status.Metrics.Retired++
			if status.Metrics.Oldest == nil {
				id := hex.EncodeToString(a.Generation[:])
				status.Metrics.Oldest = &id
				status.Metrics.OldestRevision = a.Revision
				status.Metrics.OldestAge = uint64(time.Since(old.at))
			}
		}
	}
	return status
}

func (s *retainedStorage) capacityHandler(api *httpapi.API, next http.Handler) http.Handler {
	if !s.control {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != compactionPath {
			next.ServeHTTP(w, r)
			return
		}
		q := r.URL.Query()
		method := http.MethodPost
		if q.Get("action") == "status" {
			method = http.MethodGet
		}
		if r.Method != method {
			w.Header().Set("Allow", method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch q.Get("action") {
		case "retire":
			if err := api.RetireQueryCapture(q.Get("cursor")); err != nil {
				code := http.StatusInternalServerError
				if errors.Is(err, store.ErrInvalidQueryCursor) {
					code = http.StatusBadRequest
				}
				http.Error(w, "retire query capture failed", code)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "start", "status":
			cutoff, err := strconv.ParseUint(q.Get("cutoff"), 10, 64)
			if err != nil {
				http.Error(w, "invalid cutoff", http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			if q.Get("action") == "start" {
				if s.stopping.Load() || s.fenced != nil || s.job != nil {
					s.mu.Unlock()
					http.Error(w, "compaction unavailable", http.StatusServiceUnavailable)
					return
				}
				if cutoff != s.active.Boundary.NextOrdinal {
					if s.cutoffLocked(cutoff) == 0 {
						s.mu.Unlock()
						http.Error(w, "cutoff must end an uncovered committed batch", http.StatusBadRequest)
						return
					}
					s.scheduleLocked(cutoff)
				}
			}
			reply := s.statusLocked()
			s.mu.Unlock()
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(reply); err != nil {
				panic(http.ErrAbortHandler)
			}
		default:
			http.Error(w, "unknown compaction action", http.StatusBadRequest)
		}
	})
}
