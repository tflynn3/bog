package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const imageSize = 32 << 20

type enospcEvidence struct {
	Errno                     string `json:"errno"`
	DisposableLocalFilesystem bool   `json:"disposable_local_filesystem"`
	ObservedENOSPC            bool   `json:"observed_enospc"`
	RecoveredAfterSpaceFreed  bool   `json:"recovered_after_space_freed"`
}

func diskCommand(ctx context.Context, privileged bool, name string, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if privileged && os.Geteuid() != 0 {
		name, args = "sudo", append([]string{"-n", "--", name}, args...)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = time.Second
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %v: %w: %s", name, args, err, out)
	}
	return nil
}

func mounted(path string) (bool, error) {
	var dir, parent syscall.Stat_t
	err := errors.Join(syscall.Stat(path, &dir), syscall.Stat(filepath.Dir(path), &parent))
	return dir.Dev != parent.Dev, err
}

func cleanupDisk(root, mount string) error {
	active, err := mounted(mount)
	if err != nil {
		return err
	}
	if active {
		ctx := context.Background()
		if err = diskCommand(ctx, true, "umount", mount); err != nil {
			err = errors.Join(err, diskCommand(ctx, true, "umount", "-l", mount))
		}
		active, statErr := mounted(mount)
		if active || statErr != nil {
			return errors.Join(err, statErr, fmt.Errorf("could not confirm removal of mount %s", mount))
		}
	}
	return errors.Join(err, os.RemoveAll(root))
}

// A real write or sync must return the kernel's ENOSPC. The byte budget also
// prevents filling unrelated storage if the fixture was provisioned incorrectly.
func fill(ctx context.Context, path string) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	block := bytes.Repeat([]byte{'x'}, 1<<20)
	for range imageSize/len(block) + 1 {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := f.Write(block)
		if err == nil {
			err = f.Sync()
		}
		if errors.Is(err, syscall.ENOSPC) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return errors.New("filler did not observe kernel ENOSPC within the image capacity")
}

func checkENOSPC(ctx context.Context, binary string) (proof enospcEvidence, err error) {
	ctx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "bog-disk-full-*")
	if err != nil {
		return proof, err
	}
	mount, image := filepath.Join(root, "fs"), filepath.Join(root, "ext4.img")
	if err := os.Mkdir(mount, 0700); err != nil {
		return proof, errors.Join(err, os.RemoveAll(root))
	}
	defer func() { err = errors.Join(err, cleanupDisk(root, mount)) }()
	f, err := os.OpenFile(image, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return proof, err
	}
	if err := errors.Join(f.Truncate(imageSize), f.Close()); err != nil {
		return proof, err
	}
	owner := fmt.Sprintf("root_owner=%d:%d", os.Geteuid(), os.Getegid())
	if err := diskCommand(ctx, false, "mkfs.ext4", "-q", "-F", "-m", "0", "-E", owner, image); err != nil {
		return proof, err
	}
	if err := diskCommand(ctx, true, "mount", "-t", "ext4", "-o", "loop,nodev,nosuid,noexec,nodelalloc", image, mount); err != nil {
		return proof, err
	}
	active, err := mounted(mount)
	if err != nil {
		return proof, err
	}
	if !active {
		return proof, errors.New("isolated ext4 mount did not appear")
	}
	proof.DisposableLocalFilesystem = true
	s := &fixtureServer{binary: binary, dir: filepath.Join(mount, "store")}
	if err := os.Mkdir(s.dir, 0700); err != nil {
		return proof, err
	}
	defer func() { err = errors.Join(err, s.stop()) }()
	token, err := seed(ctx, s)
	if err != nil {
		return proof, err
	}
	filler := filepath.Join(mount, "filler")
	if err := fill(ctx, filler); err != nil {
		return proof, err
	}
	proof.Errno, proof.ObservedENOSPC = "ENOSPC", true
	body, err := payload([]row{entry(25, "disk-rejected-1"), entry(25, "disk-rejected-2"+strings.Repeat("x", 512<<10))})
	if err != nil {
		return proof, err
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(mount, &stat); err != nil {
		return proof, err
	}
	if stat.Bavail*uint64(stat.Bsize) >= uint64(len(body)) {
		return proof, errors.New("filesystem has enough space for the disk-full batch")
	}
	response, _, err := s.want(ctx, "/ingest", body, http.StatusServiceUnavailable)
	if err != nil {
		return proof, err
	}
	if strings.TrimSpace(string(response)) != "WAL unavailable" {
		return proof, fmt.Errorf("expected WAL rejection under ENOSPC, got %.200q", response)
	}
	if err := s.walk(ctx, "", initial); err != nil {
		return proof, err
	}
	if err := os.Remove(filler); err != nil {
		return proof, err
	}
	resumed := []row{entry(30, "resumed-1"), entry(30, "resumed-2")}
	if err := s.ingest(ctx, resumed...); err != nil {
		return proof, err
	}
	if err := s.cycle(ctx, token); err != nil {
		return proof, err
	}
	if err := s.walk(ctx, "", append(slices.Clone(initial), resumed...)); err != nil {
		return proof, err
	}
	proof.RecoveredAfterSpaceFreed = true
	return proof, nil
}
