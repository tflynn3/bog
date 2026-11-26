package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

const tmpfsMagic = 0x01021994

func linuxDiskMountOps() diskMountOps {
	return diskMountOps{
		device: filesystemDevice,
		mount: func(ctx context.Context, path string) error {
			options := fmt.Sprintf("size=%d,nr_inodes=4096,uid=%d,gid=%d,mode=0700,nodev,nosuid,noexec",
				tmpfsSize, os.Geteuid(), os.Getegid())
			return mountCommand(ctx, "mount", "-i", "-t", "tmpfs", "-o", options, "bog-durability", path)
		},
		unmount: func(ctx context.Context, path string, lazy bool) error {
			args := []string{"-i"}
			if lazy {
				args = append(args, "-l")
			}
			return mountCommand(ctx, "umount", append(args, path)...)
		},
		verifyFresh:   verifyFreshTmpfs,
		verifyHealthy: verifyHealthyFilesystem,
	}
}

func filesystemDevice(path string) (uint64, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return 0, err
	}
	return stat.Dev, nil
}

func verifyFreshTmpfs(path string) error {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return err
	}
	if fs.Type != tmpfsMagic || fs.Bsize <= 0 ||
		fs.Blocks != uint64(tmpfsSize)/uint64(fs.Bsize) || fs.Bavail != fs.Blocks {
		return errors.New("expected a fresh private 8 MiB tmpfs")
	}
	return nil
}

func verifyHealthyFilesystem(path string) error {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return err
	}
	if fs.Bavail < 2 || (fs.Files != 0 && fs.Ffree == 0) {
		return errors.New("healthy retarget filesystem has insufficient headroom")
	}
	return nil
}

func mountCommand(ctx context.Context, name string, args ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		args = append([]string{"-n", name}, args...)
		name = "sudo"
	}
	p, err := spawn(name, "", args...)
	if err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}
	// Once a mount helper starts, allow the bounded operation to settle
	// before checking mount state. Cancellation can race a successful mount.
	timer := time.NewTimer(mountTimeout)
	defer timer.Stop()
	select {
	case <-p.done:
		err = errors.Join(p.result, p.observeErr, p.cleanGroup())
	case <-timer.C:
		// SIGTERM lets sudo forward the signal to the elevated helper
		// before stop escalates to SIGKILL and reaps the process group.
		err = errors.Join(fmt.Errorf("timed out running %s", name), p.stop(syscall.SIGTERM))
	}
	if err := errors.Join(err, ctx.Err()); err != nil {
		return fmt.Errorf("%s %v: %w: %s", name, args, err, p.logs.text())
	}
	return nil
}
