package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

const tmpfsSize = 8 << 20
const tmpfsMagic = 0x01021994

type diskEvidence struct {
	reconciliation
	Errno                     string `json:"errno"`
	DisposableLocalFilesystem bool   `json:"disposable_local_filesystem"`
	ObservedENOSPC            bool   `json:"observed_enospc"`
	RecoveredAfterSpaceFreed  bool   `json:"recovered_after_space_freed"`
	StaleCursorsRejected      int    `json:"stale_cursors_rejected"`
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

func diskCase(ctx context.Context, binary, root string) (proof diskEvidence, err error) {
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
	s, err := newServer(binary, dir)
	if err != nil {
		return proof, err
	}
	defer func() { err = errors.Join(err, s.close()) }()
	if err := s.start(ctx); err != nil {
		return proof, err
	}
	l, err := seed(ctx, s)
	if err != nil {
		return proof, err
	}
	cursor, err := s.capture(ctx)
	if err != nil {
		return proof, err
	}
	filler := filepath.Join(mount, "filler")
	if err := fillFilesystem(filler); err != nil {
		return proof, err
	}
	proof.ObservedENOSPC, proof.Errno = true, "ENOSPC"
	rows := fixtureRows("disk-rejected", 2, 1, strings.Repeat("x", 32<<10))
	if err := s.submit(ctx, rows, "", http.StatusServiceUnavailable); err != nil {
		return proof, err
	}
	l = append(l, batch{rows, rejected})
	if _, err := s.reconcile(ctx, l, "", nil); err != nil {
		return proof, fmt.Errorf("queries on full filesystem: %w", err)
	}
	if err := s.process.stop(syscall.SIGTERM); err != nil {
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
