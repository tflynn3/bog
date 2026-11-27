package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"bog/verify/capacityevidence"
)

func (p *backfillProbe) observeHeldCapacity(metrics runtimeSample, received time.Time) error {
	proof := p.Correlation
	if proof == nil {
		return nil
	}
	proof.ReplacementGeneration = &p.Publication.Generation
	proof.HeldObservedNS = new(int64(received.Sub(p.origin)))
	replay, age, err := metrics.HeldCapacity(*proof.PredecessorGeneration)
	proof.ReplayTailBytes, proof.PredecessorPinAge = replay, age
	if err != nil {
		return err
	}
	credit, err := metrics.Reclamation(*proof.PredecessorGeneration, *proof.ReplacementGeneration)
	if err == nil && credit != nil {
		return errors.New("completed reclamation preceded final predecessor pin release")
	}
	return err
}

func (p *backfillProbe) observeReclamation(ctx context.Context, metrics runtimeSample, released time.Time) error {
	proof := p.Correlation
	if proof == nil {
		return nil
	}
	proof.FinalPinReleaseNS = new(int64(released.Sub(p.origin)))
	deadline := released.Add(capacityevidence.ReclamationBudget)
	budget, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	ticker := time.NewTicker(timelineSampleInterval)
	defer ticker.Stop()
	received := released
	for {
		credit, err := metrics.Reclamation(*proof.PredecessorGeneration, *proof.ReplacementGeneration)
		if err != nil {
			return err
		}
		if credit != nil {
			proof.ReclaimedBytes = credit
			proof.ReclaimedObservedNS = new(int64(received.Sub(p.origin)))
			return nil
		}
		if err := budget.Err(); err != nil {
			return fmt.Errorf("capacity reclamation unconfirmed: %w", err)
		}
		select {
		case <-budget.Done():
			return fmt.Errorf("capacity reclamation unconfirmed: %w", budget.Err())
		case <-ticker.C:
		}
		probe, stop := context.WithTimeout(budget, diagnosticRequestLimit)
		metrics, received, err = p.observePublishedPins(probe, "reclamation_poll")
		stop()
		if err != nil {
			return fmt.Errorf("observe capacity reclamation: %w", err)
		}
		if received.After(deadline) {
			return errors.New("capacity reclamation observation exceeded its budget")
		}
		if metrics.RetiredPins == nil || *metrics.RetiredPins != 0 {
			return errors.New("reclamation poll requires an observed zero predecessor pin count")
		}
	}
}

func (p *backfillProbe) observePeak(ctx context.Context) error {
	if p.Correlation == nil {
		return nil
	}
	if p.readMemoryPeak == nil {
		return errors.New("cgroup memory.peak sampling unavailable")
	}
	start := time.Since(p.origin)
	bytes, err := p.readMemoryPeak(ctx)
	observation := &capacityevidence.PeakObservation{
		ReadStartNS: new(int64(start)),
		ReadEndNS:   new(int64(time.Since(p.origin))),
	}
	p.Correlation.CgroupPeak = observation
	if err != nil {
		observation.Error = diagnosticError(err)
		return fmt.Errorf("sample cgroup memory.peak: %w", err)
	}
	observation.Bytes = &bytes
	return nil
}
