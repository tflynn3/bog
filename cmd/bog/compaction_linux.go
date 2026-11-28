package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

type compactionStatus struct {
	State      string         `json:"state"`
	Generation uint64         `json:"generation"`
	Covered    uint64         `json:"covered_rows"`
	Committed  uint64         `json:"committed_rows"`
	Metrics    map[string]any `json:"metrics"`
}

var errCompactionConflict = errors.New("compaction cutoff is unavailable")

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
				if errors.Is(err, store.ErrInvalidQueryCursor) {
					http.Error(w, "invalid cursor", http.StatusBadRequest)
				} else {
					log.Printf("retire query capture: %v", err)
					http.Error(w, "capture retirement failed", http.StatusServiceUnavailable)
				}
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if action != "status" && action != "start" {
			http.Error(w, "invalid compaction action", http.StatusBadRequest)
			return
		}
		cutoff, err := strconv.ParseUint(r.URL.Query().Get("cutoff"), 10, 64)
		if err != nil {
			http.Error(w, "invalid compaction cutoff", http.StatusBadRequest)
			return
		}
		var status compactionStatus
		err = s.owner.WithLock(func(l *wal.Locked) error {
			p, _ := l.State()
			if action == "start" && cutoff != p.First {
				if s.stopping.Load() {
					return wal.ErrCapacity
				}
				if s.job != nil {
					if s.job.ordinal != cutoff {
						return errCompactionConflict
					}
				} else {
					found := false
					for _, batch := range s.active.batches {
						if batch.next == cutoff {
							s.schedule(p, batch.end, cutoff)
							found = true
							break
						}
					}
					if !found {
						return errCompactionConflict
					}
				}
			}
			if action == "status" && s.lastFailure != nil && s.failedCutoff == cutoff {
				return s.lastFailure
			}
			status = s.compactionStatus(p)
			return nil
		})
		if err != nil {
			code := http.StatusServiceUnavailable
			if errors.Is(err, errCompactionConflict) {
				code = http.StatusConflict
			}
			http.Error(w, "compaction unavailable", code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			log.Printf("write compaction status: %v", err)
		}
	}
}

func (s *retained) compactionStatus(p wal.Point) compactionStatus {
	now := time.Now()
	state, pending := "complete", 0
	if s.job != nil {
		state, pending = "running", 1
	}
	var pins int
	var oldest *pageCapture
	for _, pin := range s.captures {
		if pin.epoch == s.active {
			continue
		}
		pins++
		if oldest == nil || pin.at.Before(oldest.at) {
			oldest = pin
		}
	}
	usage := s.budget.Usage()
	metrics := map[string]any{
		"publication_lag_ns":       s.lag.Nanoseconds(),
		"staging_reserved_bytes":   usage.InUse[checkpoint.StagingBytes],
		"staging_used_bytes":       usage.InUse[checkpoint.StagingBytes],
		"retired_generation_pins":  pins,
		"pending_compactions":      pending,
		"replay_tail_bytes":        s.replayBytes,
		"oldest_pin_age_ns":        0,
		"reclaimable_bytes":        0,
		"oldest_pinned_generation": nil,
	}
	if oldest != nil {
		age := now.Sub(oldest.at).Nanoseconds()
		metrics["oldest_pin_age_ns"] = age
		metrics["oldest_pinned_generation"] = strconv.FormatUint(oldest.epoch.sequence, 10)
		metrics["oldest_predecessor_pin_age_ns"] = age
		metrics["predecessor_pin_generation"] = oldest.epoch.sequence
		metrics["reclaimable_bytes"] = oldest.epoch.tailBytes
	}
	if s.reclaimedFrom != 0 {
		metrics["reclaimed_bytes"] = s.reclaimed
		metrics["reclaimed_predecessor_generation"] = s.reclaimedFrom
		metrics["reclaimed_replacement_generation"] = s.reclaimedTo
	}
	return compactionStatus{state, s.active.sequence, p.First, p.Next, metrics}
}
