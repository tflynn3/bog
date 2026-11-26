package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type diskReadiness struct {
	ready            bool
	reason, category string
}

var (
	diskReady    = diskReadiness{ready: true, reason: "ready"}
	diskPending  = diskReadiness{reason: "sampling_pending"}
	diskLow      = diskReadiness{reason: "low_disk_headroom"}
	diskRollback = diskReadiness{reason: "wal_rollback", category: "enospc"}
)

func (s *server) diskReadiness(ctx context.Context) (_ diskReadiness, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.addr+"/ready", nil)
	if err != nil {
		return diskReadiness{}, err
	}
	res, err := s.client.Do(req)
	if err != nil {
		return diskReadiness{}, err
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	const maxBody = 1024
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil {
		return diskReadiness{}, err
	}
	if len(body) > maxBody {
		return diskReadiness{}, errors.New("oversized readiness response")
	}
	var decoded struct {
		Ready    *bool  `json:"ready"`
		Reason   string `json:"reason"`
		Category string `json:"category"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return diskReadiness{}, fmt.Errorf("decode readiness: %w", err)
	}
	if decoded.Ready == nil || decoded.Reason == "" ||
		(*decoded.Ready && res.StatusCode != http.StatusOK) ||
		(!*decoded.Ready && res.StatusCode != http.StatusServiceUnavailable) {
		return diskReadiness{}, fmt.Errorf("inconsistent readiness HTTP %d: %.200q", res.StatusCode, body)
	}
	return diskReadiness{*decoded.Ready, decoded.Reason, decoded.Category}, nil
}

const readinessTimeout = 10 * time.Second
const readinessPoll = 100 * time.Millisecond

// Cover the five-second readiness evidence lifetime and another sample tick.
const retargetObservation = 6 * time.Second

// observeDiskReadiness allows a known transient state only before the first
// expected sample. A stable observation fails on any subsequent divergence.
func (s *server) observeDiskReadiness(ctx context.Context, want diskReadiness, transient diskReadiness, stableFor time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	ticker := time.NewTicker(readinessPoll)
	defer ticker.Stop()
	var since time.Time
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for readiness %q: %w", want.reason, err)
		}
		got, err := s.diskReadiness(ctx)
		if err != nil {
			return fmt.Errorf("probe readiness %q: %w", want.reason, err)
		}
		if got == want {
			if since.IsZero() {
				since = time.Now()
			}
			if time.Since(since) >= stableFor {
				return ctx.Err()
			}
		} else if got != transient || !since.IsZero() {
			return fmt.Errorf("readiness %+v, want %+v", got, want)
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

func diskCase(ctx context.Context, binary, root string, diag *caseDiagnostics) (diskEvidence, error) {
	return runDiskFixture(ctx, root, linuxDiskMountOps(), func(ctx context.Context, fixture *diskFixture, proof *diskEvidence, direction diskDirection) (err error) {
		s, err := newServer(binary, fixture.link)
		if err != nil {
			return fmt.Errorf("configure candidate: %w", err)
		}
		s.diagnostics = diag
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

func checkFullToHealthy(ctx context.Context, s *server, fixture *diskFixture, proof *diskEvidence) (err error) {
	if err := s.start(ctx); err != nil {
		return err
	}
	if err := s.observeDiskReadiness(ctx, diskReady, diskPending, 0); err != nil {
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
	if err := s.observeDiskReadiness(ctx, diskLow, diskReady, 0); err != nil {
		return fmt.Errorf("readiness before retargeting the full filesystem: %w", err)
	}
	if err := retargetDirectory(fixture.link, fixture.healthy); err != nil {
		return err
	}
	if err := s.observeDiskReadiness(ctx, diskLow, diskReadiness{}, retargetObservation); err != nil {
		return fmt.Errorf("readiness after retarget: %w", err)
	}
	proof.FullToHealthy.ReadinessStatus = http.StatusServiceUnavailable
	proof.FullToHealthy.ReadinessReason = diskLow.reason
	rows := fixtureRows("disk-rejected", 2, 1, strings.Repeat("x", 32<<10))
	if err := s.submit(ctx, rows, "", http.StatusServiceUnavailable); err != nil {
		return err
	}
	proof.FullToHealthy.IngestStatus = http.StatusServiceUnavailable
	if err := s.observeDiskReadiness(ctx, diskRollback, diskReadiness{}, 0); err != nil {
		return fmt.Errorf("readiness after rejected ingest: %w", err)
	}
	proof.FullToHealthy.PostIngestReason = diskRollback.reason
	proof.FullToHealthy.PostIngestCategory = diskRollback.category
	l = append(l, batch{rows, rejected})
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return fmt.Errorf("queries on full filesystem: %w", err)
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return err
	}
	if err := retargetDirectory(fixture.link, fixture.full); err != nil {
		return err
	}
	if err := s.resume(ctx, cursor); err != nil {
		return fmt.Errorf("restart on full filesystem: %w", err)
	}
	proof.StaleCursorsRejected++
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return err
	}
	cursor, err = s.capture(ctx)
	if err != nil {
		return err
	}
	if err := os.Remove(fixture.filler); err != nil {
		return err
	}
	rows = fixtureRows("space-freed", 3, 1, "")
	if err := s.submit(ctx, rows, "", http.StatusNoContent); err != nil {
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

func checkHealthyToFull(ctx context.Context, s *server, fixture *diskFixture, proof *diskEvidence) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	if err := s.observeDiskReadiness(ctx, diskReady, diskPending, 0); err != nil {
		return err
	}
	if err := fillFilesystem(fixture.filler); err != nil {
		return fmt.Errorf("induce kernel ENOSPC: %w", err)
	}
	proof.ObservedENOSPC, proof.Errno = true, "ENOSPC"
	if err := retargetDirectory(fixture.link, fixture.full); err != nil {
		return err
	}
	if err := s.observeDiskReadiness(ctx, diskReady, diskReadiness{}, retargetObservation); err != nil {
		return err
	}
	proof.HealthyToFull.ReadinessStatus = http.StatusOK
	proof.HealthyToFull.ReadinessReason = diskReady.reason
	rows := fixtureRows("healthy-retarget", 2, 1, "")
	if err := s.submit(ctx, rows, "", http.StatusNoContent); err != nil {
		return err
	}
	proof.HealthyToFull.IngestStatus = http.StatusNoContent
	if _, err := s.reconcile(ctx, ledger{{rows, acknowledged}}, "", nil); err != nil {
		return err
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return err
	}
	// Restore the launch path and restart to confirm where the acknowledged
	// batch was committed.
	if err := retargetDirectory(fixture.link, fixture.healthy); err != nil {
		return err
	}
	if err := s.start(ctx); err != nil {
		return err
	}
	if _, err := s.reconcile(ctx, ledger{{rows, acknowledged}}, "", nil); err != nil {
		return fmt.Errorf("replay on original healthy directory: %w", err)
	}
	proof.HealthyToFull.ReplayedAfterRestart = true
	return s.process.stop(syscall.SIGTERM)
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
