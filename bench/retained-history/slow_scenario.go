package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"bog/verify/capacityevidence"
)

const slowPageLimit = 1000

type slowScenario struct {
	*slowResponseEvidence
	probe     *backfillProbe
	fixture   fixture
	broad     walk
	selective *walk
	paused    []pausedResponse
	original  []byte
	next      string
	before    map[string]os.FileInfo
	published map[string]os.FileInfo
}

func (s *slowScenario) close() error {
	var err error
	for _, p := range s.paused {
		err = errors.Join(err, p.abort())
	}
	return err
}

func (s *slowScenario) pause(ctx context.Context, phase string) (pausedResponse, error) {
	if s.probe.openPaused == nil {
		return nil, errors.New("paused query transport unavailable")
	}
	p, err := s.probe.openPaused(ctx, s.broad.path(slowPageLimit), phase, pageFraming{len(s.original), s.next})
	if err == nil {
		s.paused = append(s.paused, p)
	}
	return p, err
}

func (s *slowScenario) observePressure(ctx context.Context, p pausedResponse, event string) error {
	m, err := p.pressure(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", event, err)
	}
	s.record(slowEvent{Name: event, Pressure: &m})
	return nil
}

func (s *slowScenario) open(ctx context.Context, held *walk) error {
	p := s.probe
	ledger := p.acknowledged
	if ledger == nil || ledger.rows() != p.HeldCutoff {
		return errors.New("slow response requires the acknowledged predecessor ledger")
	}
	s.broad = *held
	s.broad.pageLimit, s.broad.phase = slowPageLimit, "slow_reference_page"
	reference := s.broad.fork()
	_, body, err := reference.readPage(ctx, p.service, s.fixture)
	if err != nil {
		return fmt.Errorf("read reference continuation: %w", err)
	}
	if reference.cursor == "" || len(body) < 64<<10 {
		return errors.New("fixture reference page cannot sustain a paused response")
	}
	s.original, s.next = body, reference.cursor
	s.before, err = checkpointManifests(p.dir)
	if err != nil {
		return fmt.Errorf("observe checkpoint before replacement: %w", err)
	}
	s.record(slowEvent{Name: "broad_cursor_live", Rows: held.offset})

	first, err := s.pause(ctx, "slow_resume")
	if err != nil {
		return err
	}
	if err := s.observePressure(ctx, first, "first_write_backpressured"); err != nil {
		return err
	}
	s.selective = ledger.selectiveWalk(s.fixture, "slow_selective_open", 1)
	if _, err := s.selective.page(ctx, p.service, s.fixture); err != nil {
		return fmt.Errorf("open selective cursor during paused write: %w", err)
	}
	if s.selective.cursor == "" {
		return errors.New("selective opening did not leave a live cursor")
	}
	s.record(slowEvent{Name: "selective_cursor_live", Rows: s.selective.offset})
	return nil
}

func (s *slowScenario) replace(ctx context.Context, published compaction) error {
	p := s.probe
	s.record(slowEvent{Name: "replacement_published", Compaction: &published})
	metrics, _, err := p.observePublishedPins(ctx, "slow_overlap_pins")
	if err != nil {
		return err
	}
	if metrics.RetiredPins == nil || *metrics.RetiredPins == 0 {
		return errors.New("replacement did not expose live predecessor pins")
	}
	s.published, err = checkpointManifests(p.dir)
	if err != nil {
		return fmt.Errorf("observe checkpoint after replacement: %w", err)
	}
	s.PredecessorManifest, s.SuccessorManifest, err = observeManifestReplacement(s.before, s.published, metrics.OldestPinnedGeneration)
	if err != nil {
		return fmt.Errorf("observe pinned manifest: %w", err)
	}
	other, err := s.pause(ctx, "slow_parallel_resume")
	if err != nil {
		return err
	}
	if err := s.observePressure(ctx, other, "parallel_write_backpressured"); err != nil {
		return err
	}
	if err := s.observePressure(ctx, s.paused[0], "first_write_spans_publication"); err != nil {
		return err
	}
	// Both slots must be occupied. A full TCP send queue alone could
	// contain a response that the candidate already finished writing.
	if err := p.requireRejection(ctx, s.broad, "slow_saturation", "capacity_rejection"); err != nil {
		return err
	}
	s.record(slowEvent{Name: "both_query_slots_occupied", Pins: metrics.RetiredPins})
	return nil
}

func (s *slowScenario) cancelAndRetry(ctx context.Context) error {
	p := s.probe
	if err := s.paused[0].abort(); err != nil {
		return fmt.Errorf("cancel first paused continuation: %w", err)
	}
	s.record(slowEvent{Name: "first_request_cancelled"})
	err := waitSlowObservation(ctx, drainBudget, func(ctx context.Context) (bool, error) {
		retry := s.broad.fork()
		retry.phase = "slow_same_cursor_retry"
		_, body, err := retry.readPage(ctx, p.service, s.fixture)
		if errors.Is(err, errQuerySlotsFull) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !bytes.Equal(body, s.original) || retry.cursor != s.next {
			return false, errors.New("same-cursor retry changed the complete body or next cursor")
		}
		s.record(slowEvent{Name: "same_cursor_retry_exact", Rows: retry.offset - s.broad.offset})
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("recover query capacity and replay the cancelled cursor: %w", err)
	}
	if err := s.observePressure(ctx, s.paused[1], "parallel_write_survived_first_cancellation"); err != nil {
		return err
	}
	return s.paused[1].abort()
}

func (s *slowScenario) verifySelectiveHeld(ctx context.Context) error {
	p := s.probe
	s.selective.phase, s.selective.pageLimit = "slow_selective_held", slowPageLimit
	request := *s.selective
	if _, err := s.selective.exhaust(ctx, p.service, s.fixture); err != nil {
		return fmt.Errorf("held selective ledger parity: %w", err)
	}
	s.record(slowEvent{Name: "held_selective_exact", Rows: s.selective.offset})
	if err := p.retireCapture(ctx, p.control, request.cursor); err != nil {
		return fmt.Errorf("retire selective capture: %w", err)
	}
	return p.requireRejection(ctx, request, "slow_selective_retired_cursor", "retired_or_expired_capture")
}

func (s *slowScenario) retire(ctx context.Context, terminal terminalPage) error {
	p := s.probe
	pause, err := s.pause(ctx, "slow_retiring_resume")
	if err != nil {
		return err
	}
	if err := s.observePressure(ctx, pause, "retirement_write_backpressured"); err != nil {
		return err
	}
	if err := p.observeHeldPin(ctx); err != nil {
		return err
	}
	if err := p.retireCapture(ctx, p.control, terminal.request.cursor); err != nil {
		return fmt.Errorf("retire broad capture during paused write: %w", err)
	}
	p.CaptureRetired = true
	retiredAt := p.boundary("capture_retired")
	if err := p.requireRejection(ctx, s.broad, "slow_retired_cursor", "retired_or_expired_capture"); err != nil {
		return err
	}
	metrics, _, err := p.observePublishedPins(ctx, "slow_retired_while_writing")
	if err != nil {
		return err
	}
	if metrics.RetiredPins == nil || *metrics.RetiredPins == 0 {
		return errors.New("retirement released predecessor while the admitted write was paused")
	}
	path := filepath.Join(p.dir, s.PredecessorManifest)
	if err := requireManifest(p.dir, s.PredecessorManifest, s.before[s.PredecessorManifest]); err != nil {
		return fmt.Errorf("observe predecessor before cancellation: %w", err)
	}
	s.record(slowEvent{Name: "retired_cursor_barred_with_live_pin", Pins: metrics.RetiredPins})
	if err := pause.abort(); err != nil {
		return fmt.Errorf("cancel retired paused continuation: %w", err)
	}
	s.record(slowEvent{Name: "retired_request_cancelled"})
	if err := p.observeDrain(ctx, retiredAt, drainBudget); err != nil {
		return err
	}
	s.record(slowEvent{Name: "predecessor_pins_drained", Pins: p.PinDrain.After})
	err = waitSlowObservation(ctx, capacityevidence.ReclamationBudget, func(ctx context.Context) (bool, error) {
		_, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	})
	if err != nil {
		return fmt.Errorf("observe predecessor manifest reclamation: %w", err)
	}
	if err := requireManifest(p.dir, s.SuccessorManifest, s.published[s.SuccessorManifest]); err != nil {
		return fmt.Errorf("observe successor after reclamation: %w", err)
	}
	s.record(slowEvent{Name: "predecessor_manifest_reclaimed"})
	return nil
}

func (a *acknowledgedRows) selectiveWalk(f fixture, phase string, limit int) *walk {
	// Include the first scattered backfill and a narrow set of seed
	// timestamp ties, using the same case-sensitive literal as the broad walk.
	q := filter{epoch.Add(time.Millisecond), epoch.Add(17 * time.Millisecond), "needle .*"}
	w := f.walkFor(a.rows(), q)
	w.phase, w.pageLimit = phase, limit
	return w
}

func (w walk) fork() walk {
	w.seen = map[string]bool{w.cursor: true}
	return w
}

func (e *slowResponseEvidence) verifyRestart(ctx context.Context, s service, control string, f fixture, trial *restartTrial) error {
	err := e.measurePhase(ctx, s, "restart_parity", func() error {
		if err := trial.verify(ctx, s, control, f); err != nil {
			return err
		}
		w := trial.ledger.selectiveWalk(f, "slow_selective_restart", slowPageLimit)
		if _, err := w.exhaust(ctx, s, f); err != nil {
			return fmt.Errorf("restarted selective ledger parity: %w", err)
		}
		e.record(slowEvent{Name: "restart_exact", Rows: trial.evidence.Ledger.Matched})
		e.record(slowEvent{Name: "restart_selective_exact", Rows: w.offset})
		return nil
	})
	if err != nil {
		e.fail(err)
	} else {
		e.Status, e.Phase = "pass", "complete"
	}
	return err
}
