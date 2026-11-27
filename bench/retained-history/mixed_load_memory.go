package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (m *mixedLoadMemory) add(sample mixedLoadSample) {
	m.Samples++
	m.PeakRSS = max(m.PeakRSS, sample.RSSBytes)
	m.PeakDescriptors = max(m.PeakDescriptors, sample.Descriptors)
	if sample.CgroupPeak != nil && (m.CgroupPeak == nil || *sample.CgroupPeak > *m.CgroupPeak) {
		m.CgroupPeak = new(*sample.CgroupPeak)
	}
	if sample.CgroupCurrent != nil && (m.PeakCgroupCurrent == nil || *sample.CgroupCurrent > *m.PeakCgroupCurrent) {
		m.PeakCgroupCurrent = new(*sample.CgroupCurrent)
	}
}

func (m mixedLoadMemory) validate(limit uint64) error {
	if m.PeakRSS > limit || (m.CgroupPeak != nil && *m.CgroupPeak > limit) ||
		(m.PeakCgroupCurrent != nil && *m.PeakCgroupCurrent > limit) {
		return fmt.Errorf("candidate memory exceeded %d bytes", limit)
	}
	return nil
}

func (r *mixedLoadRun) memorySample(ctx context.Context) (mixedLoadSample, error) {
	sampleCtx, cancel := context.WithTimeout(ctx, diagnosticRequestLimit)
	defer cancel()
	sample, err := r.observer.Sample(sampleCtx)
	return sample, errors.Join(err, sampleCtx.Err())
}

func (r *mixedLoadRun) measure(ctx context.Context, name string, work func(context.Context) error) error {
	var memory mixedLoadMemory
	defer func() { r.report.Phases[name] = memory }()
	first, err := r.memorySample(ctx)
	if err != nil {
		return fmt.Errorf("sample opening candidate memory: %w", err)
	}
	memory.add(first)
	if err := memory.validate(r.MemoryBytes); err != nil {
		return err
	}
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	type result struct {
		memory mixedLoadMemory
		err    error
	}
	done := make(chan result, 1)
	go func(memory mixedLoadMemory) {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-workCtx.Done():
				done <- result{memory: memory}
				return
			case <-tick.C:
				sample, err := r.memorySample(workCtx)
				if err != nil {
					if workCtx.Err() == nil {
						done <- result{memory, err}
						stop()
						return
					}
					continue
				}
				memory.add(sample)
				if err := memory.validate(r.MemoryBytes); err != nil {
					done <- result{memory, err}
					stop()
					return
				}
			}
		}
	}(memory)
	workErr := work(workCtx)
	// Long-lived slow responses use the run context, not this sampler's
	// cancellation. Stop the sampler separately from the work on success.
	stop()
	observed := <-done
	memory = observed.memory
	last, err := r.memorySample(ctx)
	if err == nil {
		memory.add(last)
	}
	if sampleErr := errors.Join(observed.err, err); sampleErr != nil {
		workErr = errors.Join(workErr, fmt.Errorf("sample candidate memory: %w", sampleErr))
	}
	if name == "stalled_overlap" && r.report.Overlap != nil && r.report.Overlap.Memory != nil {
		memory.add(*r.report.Overlap.Memory)
	}
	return errors.Join(workErr, memory.validate(r.MemoryBytes), ctx.Err())
}
