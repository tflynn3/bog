package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const tmpfsSize = 8 << 20
const tmpfsMagic = 0x01021994

type diskEvidence struct {
	reconciliation
	Errno                     string            `json:"errno"`
	DisposableLocalFilesystem bool              `json:"disposable_local_filesystem"`
	ObservedENOSPC            bool              `json:"observed_enospc"`
	RecoveredAfterSpaceFreed  bool              `json:"recovered_after_space_freed"`
	StaleCursorsRejected      int               `json:"stale_cursors_rejected"`
	HealthyToFull             *retargetEvidence `json:"healthy_to_full,omitempty"`
	FullToHealthy             *retargetEvidence `json:"full_to_healthy,omitempty"`
}

type retargetEvidence struct {
	ReadinessStatus      int    `json:"readiness_status"`
	ReadinessReason      string `json:"readiness_reason"`
	IngestStatus         int    `json:"ingest_status"`
	PostIngestReason     string `json:"post_ingest_reason,omitempty"`
	PostIngestCategory   string `json:"post_ingest_category,omitempty"`
	ReplayedAfterRestart bool   `json:"replayed_after_restart,omitempty"`
}

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

// retargetDirectory replaces the symlink atomically while the server is live.
func retargetDirectory(path, target string) error {
	next := path + ".next"
	if err := os.Symlink(target, next); err != nil {
		return err
	}
	if err := os.Rename(next, path); err != nil {
		return errors.Join(err, os.Remove(next))
	}
	return nil
}

func mountCommand(ctx context.Context, name string, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, processTimeout)
	defer cancel()
	if os.Geteuid() != 0 {
		args = append([]string{"-n", name}, args...)
		name = "sudo"
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = processTimeout
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %w: %s", name, args, err, out)
	}
	return nil
}

func diskCase(ctx context.Context, binary, root string, diag *caseDiagnostics) (proof diskEvidence, err error) {
	mount := filepath.Join(root, "enospc")
	if err := os.Mkdir(mount, 0700); err != nil {
		return proof, err
	}
	var parent syscall.Stat_t
	if err := syscall.Stat(mount, &parent); err != nil {
		return proof, err
	}
	// Even a timed-out mount helper may have completed the mount syscall.
	defer func() {
		var current syscall.Stat_t
		cleanupErr := syscall.Stat(mount, &current)
		if cleanupErr == nil && current.Dev != parent.Dev {
			cleanupErr = mountCommand(context.Background(), "umount", mount)
		}
		err = errors.Join(err, cleanupErr)
	}()
	opts := fmt.Sprintf("size=%d,nr_inodes=4096,uid=%d,gid=%d,mode=0700,nodev,nosuid,noexec", tmpfsSize, os.Geteuid(), os.Getegid())
	if err := mountCommand(ctx, "mount", "-t", "tmpfs", "-o", opts, "bog-durability", mount); err != nil {
		return proof, err
	}
	var fs syscall.Statfs_t
	var mounted syscall.Stat_t
	if err := syscall.Statfs(mount, &fs); err != nil {
		return proof, err
	}
	if err := syscall.Stat(mount, &mounted); err != nil {
		return proof, err
	}
	if mounted.Dev == parent.Dev || fs.Type != tmpfsMagic || fs.Bsize <= 0 ||
		fs.Blocks != uint64(tmpfsSize)/uint64(fs.Bsize) || fs.Bavail != fs.Blocks {
		return proof, errors.New("expected a fresh private 8 MiB tmpfs")
	}
	proof.DisposableLocalFilesystem = true
	dir := filepath.Join(mount, "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		return proof, err
	}
	healthy := filepath.Join(root, "healthy")
	if err := os.Mkdir(healthy, 0700); err != nil {
		return proof, err
	}
	if err := syscall.Statfs(healthy, &fs); err != nil {
		return proof, err
	}
	if fs.Bavail < 2 || (fs.Files != 0 && fs.Ffree == 0) {
		return proof, errors.New("healthy retarget filesystem has insufficient headroom")
	}
	link := filepath.Join(root, "data-link")
	filler := filepath.Join(mount, "filler")
	if err := os.Symlink(healthy, link); err != nil {
		return proof, err
	}
	s, err := newServer(binary, link)
	if err != nil {
		return proof, err
	}
	s.diagnostics = diag
	defer func() { err = errors.Join(err, s.close()) }()
	if err := checkHealthyToFull(ctx, s, healthy, dir, filler, &proof); err != nil {
		return proof, fmt.Errorf("retarget from healthy to full: %w", err)
	}
	if err := os.Remove(filler); err != nil {
		return proof, err
	}
	if err := retargetDirectory(link, dir); err != nil {
		return proof, err
	}
	if err := s.start(ctx); err != nil {
		return proof, err
	}
	if err := s.observeDiskReadiness(ctx, diskReady, diskPending, 0); err != nil {
		return proof, fmt.Errorf("ready on tmpfs before fill: %w", err)
	}
	l, err := seed(ctx, s)
	if err != nil {
		return proof, err
	}
	cursor, err := s.capture(ctx)
	if err != nil {
		return proof, err
	}
	if err := fillFilesystem(filler); err != nil {
		return proof, err
	}
	if err := s.observeDiskReadiness(ctx, diskLow, diskReady, 0); err != nil {
		return proof, fmt.Errorf("readiness before retargeting the full filesystem: %w", err)
	}
	if err := retargetDirectory(link, healthy); err != nil {
		return proof, err
	}
	if err := s.observeDiskReadiness(ctx, diskLow, diskReadiness{}, retargetObservation); err != nil {
		return proof, fmt.Errorf("retarget from full to healthy: %w", err)
	}
	proof.FullToHealthy = &retargetEvidence{ReadinessStatus: http.StatusServiceUnavailable, ReadinessReason: diskLow.reason}
	rows := fixtureRows("disk-rejected", 2, 1, strings.Repeat("x", 32<<10))
	if err := s.submit(ctx, rows, "", http.StatusServiceUnavailable); err != nil {
		return proof, err
	}
	proof.FullToHealthy.IngestStatus = http.StatusServiceUnavailable
	if err := s.observeDiskReadiness(ctx, diskRollback, diskReadiness{}, 0); err != nil {
		return proof, fmt.Errorf("readiness after rejected ingest: %w", err)
	}
	proof.FullToHealthy.PostIngestReason = diskRollback.reason
	proof.FullToHealthy.PostIngestCategory = diskRollback.category
	l = append(l, batch{rows, rejected})
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return proof, fmt.Errorf("queries on full filesystem: %w", err)
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
		return proof, err
	}
	if err := retargetDirectory(link, dir); err != nil {
		return proof, err
	}
	if err := s.resume(ctx, cursor); err != nil {
		return proof, fmt.Errorf("restart on full filesystem: %w", err)
	}
	proof.StaleCursorsRejected++
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return proof, err
	}
	cursor, err = s.capture(ctx)
	if err != nil {
		return proof, err
	}
	if err := os.Remove(filler); err != nil {
		return proof, err
	}
	rows = fixtureRows("space-freed", 3, 1, "")
	if err := s.submit(ctx, rows, "", http.StatusNoContent); err != nil {
		return proof, err
	}
	l = append(l, batch{rows, acknowledged})
	if err := s.process.stop(syscall.SIGKILL); err != nil {
		return proof, err
	}
	if err := s.resume(ctx, cursor); err != nil {
		return proof, err
	}
	proof.StaleCursorsRejected++
	proof.reconciliation, err = s.reconcile(ctx, l, "", nil)
	proof.RecoveredAfterSpaceFreed = err == nil
	return proof, err
}

func checkHealthyToFull(ctx context.Context, s *server, healthyDir, fullDir, filler string, proof *diskEvidence) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	if err := s.observeDiskReadiness(ctx, diskReady, diskPending, 0); err != nil {
		return err
	}
	if err := fillFilesystem(filler); err != nil {
		return err
	}
	proof.ObservedENOSPC, proof.Errno = true, "ENOSPC"
	if err := retargetDirectory(s.dir, fullDir); err != nil {
		return err
	}
	if err := s.observeDiskReadiness(ctx, diskReady, diskReadiness{}, retargetObservation); err != nil {
		return err
	}
	proof.HealthyToFull = &retargetEvidence{ReadinessStatus: http.StatusOK, ReadinessReason: diskReady.reason}
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
	if err := retargetDirectory(s.dir, healthyDir); err != nil {
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
