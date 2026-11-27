package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type readinessMismatch struct{ error }

func (e readinessMismatch) Unwrap() error { return e.error }

func (s *server) observeDiskReadiness(ctx context.Context, phase string, want, transient diskReadiness, stableFor time.Duration) (last readinessObservation, err error) {
	var evidence *readinessEvidence
	if d := s.diagnostics; d != nil {
		evidence = &readinessEvidence{ExpectedReason: want.reason, StableFor: stableFor}
		d.Readiness[s.observationKey(phase)] = evidence
		defer func() {
			if err != nil {
				evidence.Error = diagnosticError(err)
			}
		}()
	}
	var probeErr error
	var mismatch bool
	last, err = pollDiskReadiness(ctx, want, transient, stableFor, func(ctx context.Context) (readinessObservation, error) {
		obs, err := readDiskReadiness(ctx, s.client, s.addr)
		if d := s.diagnostics; d != nil {
			obs.Lifetime = len(d.Candidates)
		}
		probeErr = errors.Join(err, ctx.Err())
		return obs, probeErr
	}, func(obs readinessObservation, err error) {
		// The poller only records an assertion failure after a clean probe.
		mismatch = err != nil && probeErr == nil
		if evidence != nil {
			s.diagnostics.observeReadiness(evidence, obs, err)
		}
	})
	if mismatch {
		err = readinessMismatch{err}
	}
	return last, err
}

func diskCase(ctx context.Context, binary, root string, diag *caseDiagnostics) (diskEvidence, error) {
	return runDiskFixture(ctx, root, linuxDiskMountOps(), func(ctx context.Context, fixture *diskFixture, proof *diskEvidence, direction diskDirection) (err error) {
		s, err := newServer(binary, fixture.link)
		if err != nil {
			return fmt.Errorf("configure candidate: %w", err)
		}
		s.diagnostics, s.direction = diag, direction
		defer func() {
			if closeErr := s.close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("clean up candidate: %w", closeErr))
			}
		}()
		switch direction {
		case healthyToFull:
			return checkHealthyToFull(ctx, s, fixture, proof)
		case fullToHealthy:
			return checkFullToHealthy(ctx, s, fixture, proof)
		default:
			return fmt.Errorf("unknown retarget direction %q", direction)
		}
	})
}

func checkFullToHealthy(ctx context.Context, s *server, fixture *diskFixture, proof *diskEvidence) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	if _, err := s.observeDiskReadiness(ctx, "before_fill", diskReady, diskPending, 0); err != nil {
		return fmt.Errorf("ready on tmpfs before fill: %w", err)
	}
	l, err := seed(ctx, s)
	if err != nil {
		return err
	}
	cursor, err := s.capture(ctx)
	if err != nil {
		return err
	}
	if err := fillFilesystem(fixture.filler); err != nil {
		return fmt.Errorf("induce kernel ENOSPC: %w", err)
	}
	proof.ObservedENOSPC, proof.Errno = true, "ENOSPC"
	if _, err := s.observeDiskReadiness(ctx, "before_retarget", diskLow, diskReady, 0); err != nil {
		return fmt.Errorf("readiness before retargeting the full filesystem: %w", err)
	}
	if err := retargetDirectory(fixture.link, fixture.healthy); err != nil {
		return err
	}
	l, err = s.checkRetarget(ctx, fixture.full, l, cursor, proof)
	if err != nil {
		return err
	}
	cursor, err = s.capture(ctx)
	if err != nil {
		return err
	}
	if err := os.Remove(fixture.filler); err != nil {
		return err
	}
	rows := fixtureRows("space-freed", 3, 1, "")
	if err := s.submit(ctx, "space_freed", rows, "", http.StatusNoContent); err != nil {
		return err
	}
	l = append(l, batch{rows, acknowledged})
	if err := s.process.stop(syscall.SIGKILL); err != nil {
		return err
	}
	if err := s.resume(ctx, cursor); err != nil {
		return err
	}
	proof.StaleCursorsRejected++
	proof.reconciliation, err = s.reconcile(ctx, l, "", nil)
	if err != nil {
		return fmt.Errorf("replay after freeing space: %w", err)
	}
	proof.RecoveredAfterSpaceFreed = true
	return s.process.stop(syscall.SIGTERM)
}

func (s *server) checkFullRetargetIngest(ctx context.Context, rows []row, proof *retargetEvidence) error {
	const phase = "retarget_ingest"
	submitErr := s.submit(ctx, phase, rows, "", http.StatusServiceUnavailable)
	if submitErr == nil {
		proof.IngestStatus = http.StatusServiceUnavailable
	}
	// A failed response can still leave an observable WAL state while the
	// same candidate is serving. Keep the original ingest error.
	post, probeErr := s.observeDiskReadiness(ctx, "after_ingest", diskRollback, diskReadiness{}, 0)
	if submitErr == nil && probeErr == nil && s.diagnostics != nil {
		s.diagnostics.confirmRollback(s.observationKey(phase), post)
	}
	if probeErr != nil {
		probeErr = fmt.Errorf("readiness after rejected ingest: %w", probeErr)
	}
	if err := errors.Join(submitErr, probeErr); err != nil {
		return err
	}
	proof.PostIngestReason = post.Reason
	proof.PostIngestCategory = post.Category
	return nil
}

func checkHealthyToFull(ctx context.Context, s *server, fixture *diskFixture, proof *diskEvidence) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	if _, err := s.observeDiskReadiness(ctx, "before_fill", diskReady, diskPending, 0); err != nil {
		return err
	}
	if err := fillFilesystem(fixture.filler); err != nil {
		return fmt.Errorf("induce kernel ENOSPC: %w", err)
	}
	proof.ObservedENOSPC, proof.Errno = true, "ENOSPC"
	if err := retargetDirectory(fixture.link, fixture.full); err != nil {
		return err
	}
	if _, err := s.checkRetarget(ctx, fixture.healthy, nil, "", proof); err != nil {
		return err
	}
	return s.process.stop(syscall.SIGTERM)
}

// Check the live and restarted ledger against the original filesystem's
// expected ingest outcome, even when readiness reported a different state.
func (s *server) checkRetarget(ctx context.Context, launchDir string, l ledger, cursor string, proof *diskEvidence) (_ ledger, err error) {
	want, evidence := diskReady, proof.HealthyToFull
	full := s.direction == fullToHealthy
	if full {
		want, evidence = diskLow, proof.FullToHealthy
	}
	after, readyErr := s.observeDiskReadiness(ctx, "after_retarget", want, diskReadiness{}, retargetObservation)
	var liveErr error
	if readyErr != nil {
		assertion := fmt.Errorf("readiness after retarget: %w", readyErr)
		if _, mismatch := errors.AsType[readinessMismatch](readyErr); !mismatch {
			return nil, assertion
		}
		defer func() { err = errors.Join(assertion, liveErr, err) }()
	}
	evidence.ReadinessStatus, evidence.ReadinessReason = *after.Status, after.Reason
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-s.process.done:
		return nil, errors.New("candidate exited before retarget ingest")
	default:
	}
	if full {
		rows := fixtureRows("disk-rejected", 2, 1, strings.Repeat("x", 32<<10))
		if err := s.checkFullRetargetIngest(ctx, rows, evidence); err != nil {
			return nil, err
		}
		l = append(l, batch{rows, rejected})
	} else {
		rows := fixtureRows("healthy-retarget", 2, 1, "")
		if err := s.submit(ctx, "retarget_ingest", rows, "", http.StatusNoContent); err != nil {
			return nil, err
		}
		evidence.IngestStatus = http.StatusNoContent
		l = append(l, batch{rows, acknowledged})
	}
	if _, liveErr = s.reconcile(ctx, l, "", nil); liveErr != nil {
		// A failed live walk still permits an independent replay check after
		// a readiness assertion, provided the candidate stops cleanly.
		liveErr = fmt.Errorf("queries after retarget ingest: %w", liveErr)
		if readyErr == nil {
			return nil, liveErr
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return nil, err
	}
	if err := retargetDirectory(s.dir, launchDir); err != nil {
		return nil, err
	}
	if err := s.start(ctx); err != nil {
		return nil, fmt.Errorf("restart on original directory: %w", err)
	}
	if full {
		if err := s.rejectCursor(ctx, cursor); err != nil {
			return nil, err
		}
		proof.StaleCursorsRejected++
	}
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return nil, fmt.Errorf("replay on original directory: %w", err)
	}
	evidence.ReplayedAfterRestart = true
	return l, nil
}

func fillFilesystem(path string) (err error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(path), &fs); err != nil {
		return err
	}
	if fs.Bavail == 0 {
		return errors.New("no space before filling filesystem")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	full := int64(fs.Bavail) * int64(fs.Bsize)
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, full); err != nil {
		return err
	}
	if _, err := f.WriteAt([]byte{1}, full); !errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("write past full filesystem: expected kernel ENOSPC, got %v", err)
	}
	return nil
}
