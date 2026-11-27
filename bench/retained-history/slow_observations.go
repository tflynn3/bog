package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"bog/verify/capacityevidence"
)

type socketPressure struct {
	SendQueue uint64 `json:"server_send_queue_bytes"`
}

type pageFraming struct {
	bytes int
	next  string
}

type pausedResponse interface {
	pressure(context.Context) (socketPressure, error)
	abort() error
}

type slowEvent struct {
	Name       string          `json:"event"`
	Elapsed    time.Duration   `json:"elapsed_ns"`
	Pressure   *socketPressure `json:"tcp_zero_window,omitempty"`
	Compaction *compaction     `json:"compaction,omitempty"`
	Pins       *uint64         `json:"retired_generation_pins,omitempty"`
	Rows       int             `json:"rows,omitempty"`
}

type slowResponseEvidence struct {
	Status              string                 `json:"status"`
	Phase               string                 `json:"phase,omitempty"`
	Failure             string                 `json:"failure,omitempty"`
	Events              []slowEvent            `json:"events"`
	Memory              map[string]phaseMemory `json:"phase_memory"`
	SuccessorManifest   string                 `json:"successor_manifest,omitempty"`
	PredecessorManifest string                 `json:"predecessor_manifest,omitempty"`
	origin              time.Time
}

func newSlowResponseEvidence() *slowResponseEvidence {
	return &slowResponseEvidence{
		Status: "not_started", Events: []slowEvent{}, Memory: make(map[string]phaseMemory),
	}
}

func (e *slowResponseEvidence) record(event slowEvent) {
	event.Elapsed = time.Since(e.origin)
	e.Events = append(e.Events, event)
}

func (e *slowResponseEvidence) fail(err error) {
	e.Status, e.Failure = "fail", diagnosticError(err)
}

type phaseMemory struct {
	Started time.Duration `json:"started_ns"`
	Ended   time.Duration `json:"ended_ns"`
	Peak    uint64        `json:"sampled_peak_memory_current_bytes"`
	Samples int           `json:"sample_count"`
	Error   string        `json:"error,omitempty"`
}

func (e *slowResponseEvidence) measurePhase(ctx context.Context, s service, name string, run func() error) error {
	e.Phase = name
	m := phaseMemory{Started: time.Since(e.origin)}
	var sampleErr error
	sample := func() {
		if s.readMemoryCurrent == nil {
			sampleErr = errors.New("cgroup memory.current sampling unavailable")
			return
		}
		value, err := s.readMemoryCurrent(ctx)
		if err != nil {
			sampleErr = err
		} else if value == 0 || value > capacityevidence.MemoryLimit {
			sampleErr = fmt.Errorf("memory.current outside fixture bound: %d", value)
		} else {
			m.Samples++
			m.Peak = max(m.Peak, value)
		}
	}
	sample()
	var runErr error
	if sampleErr == nil {
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
			for sampleErr == nil {
				select {
				case <-stop:
					return
				case <-ctx.Done():
					return
				case <-tick.C:
					sample()
				}
			}
		}()
		runErr = run()
		close(stop)
		<-done
		if sampleErr == nil {
			sample()
		}
	}
	m.Ended = time.Since(e.origin)
	if sampleErr != nil {
		m.Error = diagnosticError(sampleErr)
	}
	e.Memory[name] = m
	return errors.Join(runErr, sampleErr)
}

var errQuerySlotsFull = errors.New("query capacity exhausted")

func (s service) requireRejection(ctx context.Context, w walk, phase, reason string) (err error) {
	var attempt queryAttempt
	defer func() { recordQuery(s.queryPhases, phase, attempt, err) }()
	res, _, err := s.requestObserved(ctx, http.MethodGet, w.path(1), nil, &attempt)
	if err != nil {
		return err
	}
	if attempt.Admission != reason || len(res.Trailer.Values("Bog-Next-Cursor")) != 0 {
		return fmt.Errorf("expected %s, got HTTP %d (%s)", reason, res.StatusCode, attempt.Admission)
	}
	return nil
}

func waitSlowObservation(ctx context.Context, budget time.Duration, observe func(context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for ctx.Err() == nil {
		ready, err := observe(ctx)
		if err != nil {
			return err
		}
		if ready {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
	return ctx.Err()
}

// Linux procfs TCP rows are keyed here by the reversed loopback socket tuple.
// Timer 04 is the established sender's zero-window probe timer.
func readSocketPressure(table, server, client string) (socketPressure, bool, error) {
	for line := range strings.SplitSeq(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != server || fields[2] != client {
			continue
		}
		if len(fields) < 6 {
			return socketPressure{}, false, errors.New("truncated TCP socket observation")
		}
		if fields[3] != "01" {
			return socketPressure{}, false, errors.New("paused query socket is no longer established")
		}
		queue, _, ok := strings.Cut(fields[4], ":")
		timer, _, timerOK := strings.Cut(fields[5], ":")
		n, err := strconv.ParseUint(queue, 16, 64)
		if err != nil || !ok || !timerOK {
			return socketPressure{}, false, errors.New("invalid TCP queue or timer observation")
		}
		return socketPressure{SendQueue: n}, n > 0 && timer == "04", nil
	}
	return socketPressure{}, false, errors.New("paused query socket missing from candidate TCP table")
}

const manifestPrefix = "bog-checkpoint-"

func checkpointManifests(dir string) (map[string]os.FileInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make(map[string]os.FileInfo)
	for _, entry := range entries {
		id, ok := strings.CutPrefix(entry.Name(), manifestPrefix)
		if !ok || len(id) != 64 || strings.Trim(id, "0123456789abcdef") != "" {
			continue
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue // Other retired manifests may be reclaimed concurrently.
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("checkpoint manifest %q is not regular", entry.Name())
		}
		files[entry.Name()] = info
	}
	return files, nil
}

func observeManifestReplacement(before, after map[string]os.FileInfo, pinned *string) (old, next string, err error) {
	if pinned != nil {
		old = manifestPrefix + *pinned
	} else if len(before) == 1 {
		for name := range before {
			old = name
		}
	}
	if before[old] == nil || after[old] == nil || !os.SameFile(before[old], after[old]) {
		return "", "", errors.New("could not identify an unchanged pinned predecessor manifest")
	}
	for name, info := range after {
		if _, existed := before[name]; existed {
			continue
		}
		if next != "" || os.SameFile(info, before[old]) {
			return old, "", errors.New("ambiguous successor checkpoint manifest")
		}
		next = name
	}
	if next == "" {
		return old, "", errors.New("replacement did not publish a new checkpoint manifest")
	}
	return old, next, nil
}

func requireManifest(dir, name string, before os.FileInfo) error {
	info, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	if !os.SameFile(before, info) {
		return fmt.Errorf("checkpoint manifest %q changed identity", name)
	}
	return nil
}
