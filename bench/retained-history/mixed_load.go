package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"bog/verify/capacityevidence"
)

// The workload owns candidate lifetimes and the durable fixture directory.
// Restart must return a ready service over the same data and with no writes.
type mixedLoadWorkload interface {
	Service() service
	Restart(context.Context) (service, error)
}

// Observations must honor cancellation. Sample can run alongside Stalled
// and workload operations. Stalled must identify backpressure
// on the candidate's end of this exact connection.
type mixedLoadObserver interface {
	Sample(context.Context) (mixedLoadSample, error)
	Stalled(context.Context, *net.TCPConn) (bool, error)
}

type mixedLoadSample struct {
	RSSBytes    uint64 `json:"rss_bytes"`
	Descriptors uint64 `json:"descriptors"`
	// Candidate-only cgroup high-water mark through this sample.
	CgroupPeak    *uint64 `json:"cgroup_peak_to_date_bytes,omitempty"`
	CgroupCurrent *uint64 `json:"cgroup_current_bytes,omitempty"`
}

type mixedLoadMemory struct {
	Samples           int     `json:"samples"`
	PeakRSS           uint64  `json:"sampled_peak_rss_bytes"`
	CgroupPeak        *uint64 `json:"cgroup_peak_to_date_bytes,omitempty"`
	PeakCgroupCurrent *uint64 `json:"sampled_peak_cgroup_current_bytes,omitempty"`
	PeakDescriptors   uint64  `json:"peak_descriptors"`
}

type mixedLoadReport struct {
	Phases              map[string]mixedLoadMemory `json:"phases"`
	Queries             map[string]*queryPhase     `json:"queries"`
	FailurePhase        string                     `json:"failure_phase,omitempty"`
	Failure             string                     `json:"failure,omitempty"`
	CleanupFailure      string                     `json:"cleanup_failure,omitempty"`
	Publication         *compaction                `json:"publication,omitempty"`
	Overlap             *mixedLoadOverlap          `json:"overlap,omitempty"`
	AcknowledgedRows    int                        `json:"acknowledged_rows"`
	HeldRows            int                        `json:"held_rows"`
	FreshRows           int                        `json:"fresh_rows"`
	RestartRows         int                        `json:"restart_rows"`
	BaselineDescriptors *uint64                    `json:"baseline_descriptors,omitempty"`
	DrainedDescriptors  *uint64                    `json:"drained_descriptors,omitempty"`
	PinsBeforeCancel    *uint64                    `json:"pins_before_cancel,omitempty"`
	DrainedResources    *runtimeSample             `json:"drained_resources,omitempty"`
	CancelToPinDrain    *time.Duration             `json:"cancel_to_pin_drain_ns,omitempty"`
	PinDrainToReclaim   *time.Duration             `json:"pin_drain_to_reclamation_ns,omitempty"`
	ReclaimedBytes      *uint64                    `json:"reclaimed_bytes,omitempty"`
}

type mixedLoadOverlap struct {
	Memory        *mixedLoadSample `json:"memory_during_overlap,omitempty"`
	StallsBefore  int              `json:"stalls_before"`
	StallsAfter   int              `json:"stalls_after"`
	RunningBefore compaction       `json:"running_before"`
	RunningAfter  compaction       `json:"running_after"`
	AckThrough    int              `json:"acknowledged_through"`
	AckLatency    time.Duration    `json:"ack_latency_ns"`
	Window        time.Duration    `json:"window_ns"`
}

type mixedLoadRun struct {
	fixture
	service
	lifetime context.Context
	observer mixedLoadObserver
	report   *mixedLoadReport
	control  string
	ledger   acknowledgedRows
	cursors  map[*walk]string
	slow     []*mixedLoadResponse
}

// runMixedLoad starts with a ready, empty retained candidate. It owns query
// bodies and captures, while the caller owns process and directory teardown.
func runMixedLoad(ctx context.Context, workload mixedLoadWorkload, observer mixedLoadObserver, f fixture, control string) (report mixedLoadReport, err error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	report = mixedLoadReport{Phases: make(map[string]mixedLoadMemory), Queries: make(map[string]*queryPhase)}
	run := mixedLoadRun{fixture: f, lifetime: ctx, observer: observer, report: &report, control: control, cursors: make(map[*walk]string)}
	defer func() {
		if err != nil {
			report.Failure = diagnosticError(err)
		}
	}()
	phase := "configuration"
	defer func() {
		report.AcknowledgedRows = run.ledger.rows()
		if cleanupErr := run.cleanup(context.WithoutCancel(ctx)); cleanupErr != nil {
			report.CleanupFailure = diagnosticError(cleanupErr)
			if err == nil {
				phase = "cleanup"
			}
			err = errors.Join(err, cleanupErr)
		}
		if err != nil {
			report.FailurePhase = phase
		}
	}()
	if workload == nil || observer == nil || control == "" || f.SeedRows < 16 || f.SeedRows%4 != 0 ||
		f.BatchRows <= 0 || f.TailRows <= f.BatchRows || f.TailRows > int(^uint(0)>>1)-f.SeedRows ||
		f.LineBytes < 32 || f.LineBytes > 60<<10 || f.BatchRows > (4<<20)/(f.LineBytes+128) || f.MemoryBytes == 0 {
		return report, errors.New("invalid mixed-load workload, observer, fixture, or control")
	}
	if err := run.useService(workload.Service()); err != nil {
		return report, err
	}
	do := func(name string, work func(context.Context) error) error {
		phase = name
		return run.measure(ctx, name, work)
	}
	var predecessor uint64
	cutoff, final := f.SeedRows+f.BatchRows, f.SeedRows+f.TailRows
	if err := do("prepare", func(ctx context.Context) (err error) {
		predecessor, err = run.prepare(ctx, cutoff)
		return err
	}); err != nil {
		return report, err
	}
	// Both cursors capture the predecessor plus the acknowledged prefix up
	// to the replacement cutoff. The concurrent suffix must stay invisible.
	broad := f.newWalk(cutoff, false)
	selective := f.newWalk(cutoff, true)
	selective.filter.end = epoch.Add(time.Duration(f.SeedRows/16+1) * time.Millisecond)
	selective = f.walkFor(cutoff, selective.filter)
	if err := do("cursor_open", func(ctx context.Context) error {
		for i, w := range []*walk{selective, broad} {
			w.phase = [2]string{"selective_open", "broad_open"}[i]
			if err := run.open(ctx, w); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return report, err
	}
	if err := do("stalled_overlap", func(ctx context.Context) error {
		return run.overlap(ctx, broad, predecessor, cutoff, final)
	}); err != nil {
		return report, err
	}
	if err := do("publication", func(ctx context.Context) error {
		return run.publish(ctx, predecessor, cutoff, final)
	}); err != nil {
		return report, err
	}
	if err := do("held_response_parity", func(ctx context.Context) error {
		if err := run.requireStalls(ctx, 0); err != nil {
			return err
		}
		if err := run.drainResponse(ctx, broad); err != nil {
			return err
		}
		for i, w := range []*walk{selective, broad} {
			w.phase = [2]string{"selective_held", "broad_held"}[i]
			if w.cursor != "" {
				if _, err := w.exhaust(ctx, run.service, f); err != nil {
					return err
				}
			}
		}
		report.HeldRows = broad.offset
		return run.retire(ctx, selective)
	}); err != nil {
		return report, err
	}
	if err := do("cancel_and_reclaim", func(ctx context.Context) error {
		return run.cancelAndReclaim(ctx, broad, predecessor, cutoff, final)
	}); err != nil {
		return report, err
	}
	if err := do("fresh_response_parity", func(ctx context.Context) error {
		return run.verifyLedger(ctx, "fresh", &report.FreshRows)
	}); err != nil {
		return report, err
	}
	phase = "restart_startup"
	s, err := workload.Restart(ctx)
	if err != nil {
		return report, err
	}
	if err := run.useService(s); err != nil {
		return report, err
	}
	err = do("restart_parity", func(ctx context.Context) error {
		current, err := run.compact(ctx, control, "status", cutoff)
		if err != nil {
			return err
		}
		if current.compaction != *report.Publication {
			return fmt.Errorf("restart checkpoint differs from publication: %+v", current.compaction)
		}
		return run.verifyLedger(ctx, "restart", &report.RestartRows)
	})
	return report, err
}

func (r *mixedLoadRun) prepare(ctx context.Context, cutoff int) (uint64, error) {
	initial, err := r.compact(ctx, r.control, "status", 0)
	if err != nil {
		return 0, err
	}
	if initial.State != "complete" || initial.CoveredRows != 0 || initial.CommittedRows != 0 {
		return 0, fmt.Errorf("mixed load needs an empty candidate: %+v", initial.compaction)
	}
	generation := initial.Generation
	for lo := 0; lo < r.SeedRows; {
		hi := lo + min(8192, r.SeedRows-lo)
		if err := r.appendRange(ctx, r.service, lo, hi, nil); err != nil {
			return 0, err
		}
		generation, err = r.prepareCheckpoint(ctx, r.control, hi, generation)
		if err != nil {
			return 0, err
		}
		lo = hi
	}
	if err := r.appendRange(ctx, r.service, r.SeedRows, cutoff, nil); err != nil {
		return 0, err
	}
	// A previous seed replacement can finish publishing before its
	// reservations and descriptors drain. Take the baseline after drain.
	err = mixedLoadPoll(ctx, func() (bool, error) {
		status, err := r.compact(ctx, r.control, "status", r.SeedRows)
		if err != nil {
			return false, err
		}
		if status.compaction != (compaction{generation, r.SeedRows, cutoff, "complete"}) {
			return false, fmt.Errorf("unexpected prepared predecessor: %+v", status.compaction)
		}
		return mixedLoadIdle(readRuntimeMetrics(status.Metrics, nil))
	})
	if err != nil {
		return 0, err
	}
	sample, err := r.memorySample(ctx)
	if err == nil {
		r.report.BaselineDescriptors = &sample.Descriptors
	}
	return generation, err
}

func (r *mixedLoadRun) overlap(ctx context.Context, broad *walk, predecessor uint64, cutoff, final int) error {
	for i := 0; i < 2; i++ {
		response, err := r.startSlow(ctx, broad, i)
		if err != nil {
			return err
		}
		r.slow = append(r.slow, response)
		stall, cancel := context.WithTimeout(ctx, 4*time.Second)
		err = mixedLoadPoll(stall, func() (bool, error) {
			return r.observer.Stalled(stall, response.conn)
		})
		cancel()
		if err != nil {
			return fmt.Errorf("establish backpressure on slow response %d: %w", i+1, err)
		}
	}
	if err := r.requireStalls(ctx, 0); err != nil {
		return err
	}
	start := time.Now()
	overlap := &mixedLoadOverlap{StallsBefore: len(r.slow)}
	r.report.Overlap = overlap
	timeline := backfillTimeline{HeldCutoff: r.SeedRows, FinalCutoff: final}
	opened, err := r.compact(ctx, r.control, "start", cutoff)
	if err != nil {
		return err
	}
	overlap.RunningBefore = opened.compaction
	if err := timeline.validateRunning(opened.compaction, predecessor, cutoff); err != nil || opened.CommittedRows != cutoff {
		return fmt.Errorf("maintenance did not start at the acknowledged cutoff: %+v", opened.compaction)
	}
	next := cutoff + min(r.BatchRows, final-cutoff)
	var writes []ingestSample
	if err := r.appendRange(ctx, r.service, cutoff, next, &writes); err != nil {
		return err
	}
	overlap.AckThrough, overlap.AckLatency = r.ledger.rows(), writes[0].latency
	memory, err := r.memorySample(ctx)
	if err != nil {
		return err
	}
	overlap.Memory = &memory
	running, err := r.compact(ctx, r.control, "status", cutoff)
	if err != nil {
		return err
	}
	overlap.RunningAfter = running.compaction
	if err := timeline.validateRunning(running.compaction, predecessor, next); err != nil || running.CommittedRows != next {
		return fmt.Errorf("no running maintenance observed after acknowledged ingest: %+v", running.compaction)
	}
	if err := r.requireStalls(ctx, 0); err != nil {
		return err
	}
	overlap.StallsAfter, overlap.Window = len(r.slow), time.Since(start)
	return r.appendRange(ctx, r.service, next, final, nil)
}

func (r *mixedLoadRun) publish(ctx context.Context, predecessor uint64, cutoff, final int) error {
	// Exercise both shapes under response pressure. A recognized admission
	// refusal is evidence. A malformed response or a failed transport is
	// a failed workload.
	for i, narrow := range []bool{true, false} {
		if err := r.requireStalls(ctx, 0); err != nil {
			return err
		}
		w := r.newWalk(final, narrow)
		w.phase = [2]string{"selective_under_pressure", "broad_under_pressure"}[i]
		if err := r.open(ctx, w); err != nil {
			if !errors.Is(err, errMixedLoadAdmission) {
				return err
			}
			continue
		}
		if err := r.retire(ctx, w); err != nil {
			return err
		}
	}
	c, err := r.waitCompaction(ctx, r.control, cutoff, predecessor)
	if err != nil {
		return err
	}
	if c.CommittedRows != final {
		return fmt.Errorf("replacement committed %d rows, ledger has %d", c.CommittedRows, final)
	}
	r.report.Publication = &c
	return nil
}

func (r *mixedLoadRun) cancelAndReclaim(ctx context.Context, broad *walk, predecessor uint64, cutoff, final int) error {
	if err := r.requireStalls(ctx, 1); err != nil {
		return err
	}
	token := r.cursors[broad]
	if err := r.retire(ctx, broad); err != nil {
		return err
	}
	if err := r.requireRetired(ctx, broad.filter, token); err != nil {
		return err
	}
	read := func(ctx context.Context) (runtimeSample, error) {
		status, err := r.compact(ctx, r.control, "status", cutoff)
		if err == nil && (status.compaction != *r.report.Publication || status.CommittedRows != final) {
			err = fmt.Errorf("checkpoint changed during pin drain: %+v", status.compaction)
		}
		return readRuntimeMetrics(status.Metrics, err), err
	}
	held, err := read(ctx)
	if err != nil {
		return err
	}
	r.report.PinsBeforeCancel = held.RetiredPins
	if held.RetiredPins == nil {
		return errors.New("missing retired pin count before cancellation")
	}
	if *held.RetiredPins == 0 {
		return errors.New("retirement released the predecessor while a response was still stalled")
	}
	replacement := r.report.Publication.Generation
	if _, _, err := held.HeldCapacity(predecessor); err != nil {
		return err
	}
	if credit, err := held.Reclamation(predecessor, replacement); err != nil || credit != nil {
		return errors.Join(errors.New("unexpected reclamation observation before cancellation"), err)
	}
	if err := r.requireStalls(ctx, 1); err != nil {
		return err
	}
	canceled := time.Now()
	if err := r.slow[1].close(); err != nil {
		return err
	}
	var released time.Time
	drain, stop := context.WithTimeout(ctx, drainBudget)
	defer stop()
	if err := mixedLoadPoll(drain, func() (bool, error) {
		metrics, err := read(drain)
		if err != nil {
			return false, err
		}
		if metrics.RetiredPins == nil {
			return false, errors.New("missing retired pin count after cancellation")
		}
		if *metrics.RetiredPins != 0 {
			if credit, err := metrics.Reclamation(predecessor, replacement); err != nil || credit != nil {
				return false, errors.Join(errors.New("reclamation observed with outstanding pins"), err)
			}
			return false, nil
		}
		credit, err := metrics.Reclamation(predecessor, replacement)
		if err != nil {
			return false, err
		}
		released = time.Now()
		if credit != nil {
			r.report.ReclaimedBytes = credit
			r.report.PinDrainToReclaim = new(time.Duration(0))
		}
		r.report.CancelToPinDrain = new(released.Sub(canceled))
		return true, nil
	}); err != nil {
		return fmt.Errorf("pin drain: %w", err)
	}
	reclaim, stop := context.WithDeadline(ctx, released.Add(capacityevidence.ReclamationBudget))
	defer stop()
	if err := mixedLoadPoll(reclaim, func() (bool, error) {
		metrics, err := read(reclaim)
		if err != nil {
			return false, err
		}
		idle, err := mixedLoadIdle(metrics)
		if err != nil {
			return false, err
		}
		if *metrics.RetiredPins != 0 {
			return false, errors.New("predecessor pins reappeared after drain")
		}
		credit, err := metrics.Reclamation(predecessor, replacement)
		if err != nil {
			return false, err
		}
		if credit != nil && r.report.ReclaimedBytes == nil {
			r.report.PinDrainToReclaim = new(time.Since(released))
			r.report.ReclaimedBytes = credit
		}
		sample, err := r.memorySample(reclaim)
		if err != nil {
			return false, err
		}
		r.report.DrainedDescriptors, r.report.DrainedResources = &sample.Descriptors, &metrics
		return idle && credit != nil && sample.Descriptors <= *r.report.BaselineDescriptors, nil
	}); err != nil {
		return fmt.Errorf("reclamation or reservation/descriptor cleanup: %w", err)
	}
	return nil
}

func mixedLoadIdle(m runtimeSample) (bool, error) {
	if m.RetiredPins == nil || m.Reserved == nil || m.Used == nil || m.PendingCompactions == nil {
		return false, fmt.Errorf("missing pin, staging reservation, or maintenance measurements: %v", m.Unavailable)
	}
	return *m.RetiredPins == 0 && *m.Reserved == 0 && *m.Used == 0 && *m.PendingCompactions == 0, nil
}

func mixedLoadPoll(ctx context.Context, observe func() (bool, error)) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := observe()
		if err != nil || ok {
			return errors.Join(err, ctx.Err())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
