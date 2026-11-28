package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/httpapi"
	"bog/internal/wal"
)

func (r *retained) compact(job *advance) (err error) {
	ctx := context.Background()
	view := job.view
	if view.epoch.manifest == nil {
		if err := r.prepareLegacyAnchor(ctx, view.epoch); err != nil {
			return err
		}
	}
	view.point.Generation = [32]byte(view.epoch.id)
	if err := r.recoverStaging(ctx, view); err != nil {
		return err
	}
	if view.point.SuffixSHA256 == ([32]byte{}) {
		view.point.SuffixSHA256, err = digestRange(view.epoch.tail, view.point.CoveredOffset, view.point.CommittedEnd)
		if err != nil {
			return err
		}
	}
	ticket, err := r.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()

	var selected *epoch
	var activationErr error
	m, err := r.publisher.PublishRecoveryView(ctx, &view, job.cutoff, job.ordinal, func(ctx context.Context, p checkpoint.RecoveryPoint) (err error) {
		prepared, err := ticket.PrepareSelected(ctx, r.owner.Directory(), history.GenerationID(p.Generation))
		if err != nil {
			return err
		}
		defer func() {
			// A failure to release a rejected pin is a cleanup error. Once
			// selected, Activate owns that pin.
			err = errors.Join(err, prepared.Close())
		}()
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.fenced {
			return errors.New("WAL owner fenced during staging")
		}
		old := r.current
		a := wal.Authority{
			Sequence:   old.sequence + 1,
			Generation: hex.EncodeToString(p.Generation[:]), CoveredOffset: p.CoveredOffset, NextOrdinal: p.NextOrdinal,
			End: old.tail.End(), Ordinal: r.ordinal,
		}
		if a.End == p.CommittedEnd {
			a.SuffixSHA256 = hex.EncodeToString(p.SuffixSHA256[:])
		}
		overlap, err := r.budget.Reserve(ctx, checkpoint.StagingBytes, uint64(a.End-a.CoveredOffset))
		if err != nil {
			return err
		}
		defer func() {
			if old.overlap != overlap {
				overlap.Release()
			}
		}()
		tail, err := r.owner.NewTail(old.tail, a)
		if err != nil {
			return err
		}
		if err := r.owner.Select(tail); err != nil {
			if errors.Is(err, wal.ErrUncertain) {
				r.fenced = true
				return errors.Join(err, tail.Close())
			}
			return errors.Join(err, r.owner.Retire(tail))
		}
		gen, err := prepared.Activate()
		if gen.Drained == nil {
			r.fenced = true
			return errors.Join(wal.ErrUncertain, err, tail.Close())
		}
		activationErr = err
		selected = &epoch{tail: tail, name: checkpoint.GenerationManifestName(p.Generation), id: gen.ID,
			covered: p.CoveredOffset, first: p.NextOrdinal, sequence: a.Sequence}
		r.current = selected
		old.retired, old.overlap = true, overlap
		r.retired = append(r.retired, old)
		n := 0
		for n < len(r.batches) && r.batches[n].End <= job.cutoff {
			r.batches[n].Reservation.Release()
			n++
		}
		clear(r.batches[:n])
		r.batches = r.batches[n:]
		r.rebuildTail()
		return nil
	})
	r.mu.Lock()
	if m != nil {
		selected.manifest = m
	}
	fenced, current := r.fenced, r.viewLocked()
	r.mu.Unlock()
	if err != nil && !fenced {
		// Selector preparation can fail cleanly on a full disk. Resolve
		// staging against the generation that actually won.
		err = errors.Join(err, r.recoverStaging(ctx, current))
	}
	return errors.Join(err, activationErr)
}

func (r *retained) prepareLegacyAnchor(ctx context.Context, e *epoch) error {
	// An empty v1 manifest anchors the first v2 advance while the full
	// legacy WAL keeps its commit marker through the selection.
	m, id, _, err := r.readManifest(canonicalManifest)
	if errors.Is(err, os.ErrNotExist) {
		header, hashErr := digestRange(e.tail, 0, 8)
		if hashErr != nil {
			return hashErr
		}
		if _, err := r.publisher.PublishCaptured(ctx, e.tail, 8, nil, 0, hex.EncodeToString(header[:])); err != nil {
			recoverErr := r.publisher.RecoverCaptured(func() (*checkpoint.Manifest, error) {
				return history.ValidatePinnedAuthority(ctx, r.owner.Directory())
			})
			if !errors.Is(recoverErr, os.ErrNotExist) {
				return errors.Join(err, recoverErr)
			}
			return err
		}
		m, id, _, err = r.readManifest(canonicalManifest)
	}
	if err != nil {
		return err
	}
	if m.CoveredOffset != 8 || m.NextOrdinal != 0 || m.Version != 1 {
		return errors.New("unexpected initial legacy checkpoint")
	}
	r.mu.Lock()
	e.name, e.manifest, e.id = canonicalManifest, m, id
	r.mu.Unlock()
	return nil
}

func (r *retained) reclaim(ctx context.Context) error {
	r.mu.Lock()
	if r.fenced {
		r.mu.Unlock()
		return nil
	}
	currentName, currentID := r.current.name, r.current.id
	var drained []*epoch
	keep := r.retired[:0]
	for _, old := range r.retired {
		if old.pins == 0 {
			drained = append(drained, old)
		} else {
			keep = append(keep, old)
		}
	}
	clear(r.retired[len(keep):])
	r.retired = keep
	r.mu.Unlock()
	var err error
	for _, old := range drained {
		if closeErr := r.owner.Retire(old.tail); closeErr != nil {
			err = errors.Join(err, closeErr)
			r.mu.Lock()
			r.retired = append(r.retired, old)
			r.mu.Unlock()
			continue
		}
		if old.overlap != nil {
			old.overlap.Release()
		}
		if old.name != "" && old.name != canonicalManifest {
			if removeErr := r.owner.Root().Remove(old.name); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, removeErr)
			}
			err = errors.Join(err, r.owner.Directory().Sync())
		}
	}
	if currentName != "" && currentName != canonicalManifest {
		err = errors.Join(err, r.catalog.ReclaimSelectedPinned(ctx, r.owner.Directory(), currentID))
	}
	return err
}

type compactStatus struct {
	State      string         `json:"state"`
	Generation uint64         `json:"generation"`
	Covered    uint64         `json:"covered_rows"`
	Committed  uint64         `json:"committed_rows"`
	Metrics    compactMetrics `json:"metrics"`
}

type compactMetrics struct {
	RetiredPins  int     `json:"retired_generation_pins"`
	Pending      int     `json:"pending_compactions"`
	ReplayBytes  uint64  `json:"replay_tail_bytes"`
	StagingBytes uint64  `json:"staging_reserved_bytes"`
	Oldest       *uint64 `json:"oldest_pinned_generation"`
}

func (r *retained) control(api *httpapi.API, w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	if q.Get("action") == "retire" && req.Method == http.MethodPost {
		if err := api.RetireQueryCapture(q.Get("cursor")); err != nil {
			http.Error(w, "invalid or expired query cursor", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	cutoff, err := strconv.ParseUint(q.Get("cutoff"), 10, 64)
	if err != nil {
		http.Error(w, "invalid cutoff", http.StatusBadRequest)
		return
	}
	var start bool
	switch {
	case q.Get("action") == "start" && req.Method == http.MethodPost:
		start = true
	case q.Get("action") == "status" && req.Method == http.MethodGet:
	default:
		http.Error(w, "invalid compaction operation", http.StatusBadRequest)
		return
	}
	status, err := r.compactionStatus(start, cutoff)
	if err != nil {
		http.Error(w, "compaction unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (r *retained) compactionStatus(start bool, cutoff uint64) (compactStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.current
	if start && cutoff != e.first {
		if err := r.startLocked(cutoff); err != nil {
			return compactStatus{}, err
		}
	}
	status := compactStatus{State: "complete", Generation: e.sequence, Covered: e.first, Committed: r.ordinal}
	if r.job != nil {
		status.Metrics.Pending = 1
		if e.first < r.job.ordinal {
			status.State = "running"
		}
	} else if r.failed != nil && cutoff > e.first {
		return compactStatus{}, fmt.Errorf("checkpoint failed: %w", r.failed)
	}
	status.Metrics.ReplayBytes = uint64(e.tail.End() - e.covered)
	status.Metrics.StagingBytes = r.budget.Usage().InUse[checkpoint.StagingBytes]
	for _, old := range r.retired {
		if old.pins > 0 {
			status.Metrics.RetiredPins += old.pins
			if status.Metrics.Oldest == nil {
				status.Metrics.Oldest = &old.sequence
			}
		}
	}
	return status, nil
}
