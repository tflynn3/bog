package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestInheritedOutputWithoutProcfs(t *testing.T) {
	root := t.TempDir()
	attr := &syscall.SysProcAttr{Chroot: root}
	if os.Geteuid() != 0 {
		// Where unprivileged user namespaces are permitted, this gives
		// the child just enough authority to chroot its own process.
		attr.Cloneflags = syscall.CLONE_NEWUSER
		attr.UidMappings = []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Geteuid(), Size: 1}}
		attr.GidMappings = []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getegid(), Size: 1}}
	}
	binary := filepath.Join(root, "exporter")
	buildCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".")
	build.Env = append(build.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build static exporter: %v\n%s", err, output)
	}
	// Probe the sandbox before exercising pipe scenarios. The empty
	// chroot contains the exporter only, so neither /proc nor /dev/fd
	// can supply a descriptor or the executable path.
	probeCtx, stopProbe := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopProbe()
	probe := exec.CommandContext(probeCtx, "/exporter", "-h")
	probe.Dir = "/"
	probe.SysProcAttr = attr
	if output, err := probe.CombinedOutput(); err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("chroot unavailable (uid %d): %v", os.Geteuid(), err)
		}
		t.Fatalf("start exporter in empty chroot: %v\n%s", err, output)
	}

	for _, stream := range []string{"stdout", "stderr"} {
		for _, stop := range []string{"deadline", "SIGTERM"} {
			t.Run(stream+"/"+stop, func(t *testing.T) {
				t.Parallel()
				checkInheritedBackpressure(t, stream, stop, func(ctx context.Context, args ...string) *exec.Cmd {
					cmd := exec.CommandContext(ctx, "/exporter", args...)
					cmd.Dir = "/"
					cmd.SysProcAttr = attr
					cmd.WaitDelay = time.Second
					return cmd
				})
			})
		}
	}
}
