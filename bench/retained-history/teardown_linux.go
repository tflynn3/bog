package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const cleanupTimeout = 5 * time.Second

type cgroupTeardown struct {
	populated func() (bool, error)
	kill      func() error
	remove    func(context.Context) error
}

func teardownGroup(group string, p *child, collect func()) (cleanupProof, error, error) {
	ops := cgroupTeardown{
		populated: func() (bool, error) {
			data, err := os.ReadFile(filepath.Join(group, "cgroup.events"))
			if err != nil {
				return false, err
			}
			return parsePopulation(string(data))
		},
		kill: func() error { return writeCgroupFile(group, "cgroup.kill", "1") },
		remove: func(ctx context.Context) error {
			for {
				err := removeCgroupTree(ctx, group)
				if !errors.Is(err, syscall.EBUSY) && !errors.Is(err, syscall.ENOTEMPTY) {
					return err
				}
				if waitErr := waitCleanupPoll(ctx); waitErr != nil {
					return errors.Join(err, waitErr)
				}
			}
		},
	}
	return ops.run(p, 2*time.Second, cleanupTimeout, collect)
}

// run owns the entire shutdown sequence. A nil collect keeps the drained
// subtree for restart. Final teardown collects metrics before removing it.
// Exit status belongs to the workload. Reaping, observed emptiness and
// directory removal are independent cleanup facts.
func (ops cgroupTeardown) run(p *child, grace, timeout time.Duration, collect func()) (proof cleanupProof, shutdownErr, cleanupErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if p != nil {
		shutdownErr = p.terminate(ctx, grace)
	}

	// cgroup.events includes descendants, even when cgroup.procs is empty.
	// A failed read still warrants a kill attempt within the owned subtree.
	populated, err := ops.populated()
	if err != nil {
		cleanupErr = fmt.Errorf("read subtree population: %w", err)
	}
	if populated || err != nil {
		if err := ops.kill(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("kill capacity subtree: %w", err))
		}
	}
	if p != nil {
		// The placement gate can fail before the child joins the subtree.
		// Also cover a process still present after a failed cgroup.kill.
		select {
		case <-p.done:
		default:
			if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("kill candidate: %w", err))
			}
		}
		proof.CandidateReaped = waitChild(ctx, p.done)
		if proof.CandidateReaped {
			shutdownErr = errors.Join(shutdownErr, p.waitErr)
		} else {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("reap candidate: %w", ctx.Err()))
		}
	} else {
		proof.CandidateReaped = true
	}
	if err == nil && !populated {
		proof.SubtreeEmpty = true
	} else if err := ops.waitEmpty(ctx); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("wait for empty capacity subtree: %w", err))
	} else {
		proof.SubtreeEmpty = true
	}
	if collect != nil {
		collect()
		if proof.SubtreeEmpty {
			if err := ops.remove(ctx); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove capacity subtree: %w", err))
			} else {
				proof.SubtreeRemoved = true
			}
		}
	}
	proof.Status = "pass"
	if cleanupErr != nil {
		proof.Status, proof.Detail = "fail", cleanupErr.Error()
	}
	return proof, shutdownErr, cleanupErr
}

func (p *child) terminate(ctx context.Context, grace time.Duration) error {
	select {
	case <-p.done:
		if p.waitErr == nil {
			return errors.New("candidate exited before shutdown")
		}
		return nil // run records the exit error after reaping.
	default:
	}
	err := p.cmd.Process.Signal(syscall.SIGTERM)
	if errors.Is(err, os.ErrProcessDone) {
		err = errors.New("candidate exited before shutdown")
	}
	graceCtx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	if !waitChild(graceCtx, p.done) {
		err = errors.Join(err, errors.New("candidate shutdown exceeded grace period"))
	}
	return err
}

func waitChild(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}

func (ops cgroupTeardown) waitEmpty(ctx context.Context) error {
	for ctx.Err() == nil {
		populated, err := ops.populated()
		if err != nil || !populated {
			return err
		}
		if err := waitCleanupPoll(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func waitCleanupPoll(ctx context.Context) error {
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func parsePopulation(data string) (bool, error) {
	var value string
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "populated" {
			continue
		}
		if len(fields) != 2 || value != "" || (fields[1] != "0" && fields[1] != "1") {
			return false, errors.New("invalid populated field in cgroup.events")
		}
		value = fields[1]
	}
	if value == "" {
		return false, errors.New("missing populated field in cgroup.events")
	}
	return value == "1", nil
}

func removeCgroupTree(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		// In cgroupfs the remaining entries are kernel control files.
		if entry.IsDir() {
			if err := removeCgroupTree(ctx, filepath.Join(path, entry.Name())); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := syscall.Rmdir(path); err != nil {
		return &os.PathError{Op: "rmdir", Path: path, Err: err}
	}
	return nil
}
